#!/usr/bin/env python3
"""
custom_columnar.py
Simple Columnar File Format (CCF) implementation:
- CCFWriter: write CSV -> .ccf
- CCFReader: read .ccf (selective column reads supported)
- CLI entrypoints: --csv_to_custom and --custom_to_csv
"""

import argparse
import csv
import struct
import zlib
import io
import os
import sys
from typing import List, Tuple

MAGIC = b'CCFv1\x00\x00'  # 7 bytes (note: VERSION comes after)
VERSION = 1

# dtype codes
DT_INT32 = 0
DT_FLOAT64 = 1
DT_STRING = 2

def pack_u16(x): return struct.pack('<H', x)
def pack_u32(x): return struct.pack('<I', x)
def pack_u64(x): return struct.pack('<Q', x)
def pack_u8(x):  return struct.pack('<B', x)

def infer_type(values: List[str]):
    # simple heuristic: try int, then float, else string
    is_int = True
    is_float = True
    for v in values:
        if v == '':
            continue
        try:
            int(v)
        except:
            is_int = False
            try:
                float(v)
            except:
                is_float = False
    if is_int:
        return DT_INT32
    if is_float:
        return DT_FLOAT64
    return DT_STRING

class ColumnMeta:
    def __init__(self, name:str, dtype:int):
        self.name = name
        self.dtype = dtype
        self.offset = 0
        self.compressed_size = 0
        self.uncompressed_size = 0

class CCFWriter:
    def __init__(self, out_path:str):
        self.out_path = out_path
        self.num_rows = 0

    def write(self, header_cols: List[Tuple[str,int]], rows: List[List[str]]):
        # rows: list of rows, each row is list of str (no header row)
        self.num_rows = len(rows)
        num_cols = len(header_cols)
        # transpose values into per-column lists
        cols_values = [[] for _ in range(num_cols)]
        for r in rows:
            # pad row if shorter
            for i in range(num_cols):
                cols_values[i].append(r[i] if i < len(r) else '')

        # We'll write column blocks first, capture each block, then write header before blocks
        blocks = []
        meta_list = []
        for (name, dtype), values in zip(header_cols, cols_values):
            meta = ColumnMeta(name, dtype)

            # build null bitmap
            nb_len = (self.num_rows + 7)//8
            null_bitmap = bytearray(nb_len)
            for i, v in enumerate(values):
                if v == '':
                    byte_index = i//8
                    bit = i%8
                    null_bitmap[byte_index] |= (1<<bit)

            buf = io.BytesIO()
            buf.write(pack_u32(len(null_bitmap)))
            buf.write(bytes(null_bitmap))

            if dtype == DT_INT32:
                for i, v in enumerate(values):
                    if v == '':
                        buf.write(struct.pack('<i', 0))
                    else:
                        buf.write(struct.pack('<i', int(v)))
            elif dtype == DT_FLOAT64:
                for i, v in enumerate(values):
                    if v == '':
                        buf.write(struct.pack('<d', 0.0))
                    else:
                        buf.write(struct.pack('<d', float(v)))
            elif dtype == DT_STRING:
                offsets = [0]
                strings_buf = bytearray()
                for i, v in enumerate(values):
                    if v == '':
                        offsets.append(offsets[-1])
                    else:
                        b = v.encode('utf-8')
                        strings_buf.extend(b)
                        offsets.append(offsets[-1] + len(b))
                # write offsets (num_rows + 1) as uint32
                for off in offsets:
                    buf.write(pack_u32(off))
                buf.write(bytes(strings_buf))
            else:
                raise ValueError('Unknown dtype')

            uncompressed = buf.getvalue()
            compressed = zlib.compress(uncompressed)

            meta.offset = None  # will be set when assembling final file
            meta.compressed_size = len(compressed)
            meta.uncompressed_size = len(uncompressed)
            blocks.append(compressed)
            meta_list.append(meta)

        # build header content (we need offsets, so compute them now)
        header_bytes = io.BytesIO()
        for i, meta in enumerate(meta_list):
            name_b = header_cols[i][0].encode('utf-8')
            header_bytes.write(pack_u16(len(name_b)))
            header_bytes.write(name_b)
            header_bytes.write(pack_u8(header_cols[i][1]))  # dtype
            # placeholders for offset/comp_size/uncomp_size; fill later
            header_bytes.write(pack_u64(0))
            header_bytes.write(pack_u64(meta.compressed_size))
            header_bytes.write(pack_u64(meta.uncompressed_size))

        header_content = header_bytes.getvalue()
        header_size = len(header_content)

        # now compute final offsets: file layout = fixed prefix + header + concatenated blocks
        prefix_size = len(MAGIC) + 1 + 4 + 8 + 2  # MAGIC + VERSION + HEADER_SIZE + NUM_ROWS + NUM_COLS
        current = prefix_size + header_size
        # update meta.offset values
        for meta in meta_list:
            meta.offset = current
            current += meta.compressed_size

        # rebuild header with actual offsets
        header_bytes = io.BytesIO()
        for i, meta in enumerate(meta_list):
            name_b = header_cols[i][0].encode('utf-8')
            header_bytes.write(pack_u16(len(name_b)))
            header_bytes.write(name_b)
            header_bytes.write(pack_u8(header_cols[i][1]))  # dtype
            header_bytes.write(pack_u64(meta.offset))
            header_bytes.write(pack_u64(meta.compressed_size))
            header_bytes.write(pack_u64(meta.uncompressed_size))
        header_content = header_bytes.getvalue()
        header_size = len(header_content)

        # write final file
        with open(self.out_path, 'wb') as f:
            f.write(MAGIC)
            f.write(pack_u8(VERSION))
            f.write(pack_u32(header_size))
            f.write(pack_u64(self.num_rows))
            f.write(pack_u16(len(meta_list)))
            f.write(header_content)
            for blk in blocks:
                f.write(blk)

class CCFReader:
    def __init__(self, path:str):
        self.path = path
        self.num_rows = 0
        self.num_cols = 0
        self.columns_meta: List[ColumnMeta] = []
        self._read_header()

    def _read_header(self):
        with open(self.path, 'rb') as f:
            # read magic using MAGIC length to avoid hard-coded size mismatch
            magic = f.read(len(MAGIC))
            if magic != MAGIC:
                raise ValueError('Bad magic: not a CCF file')
            version = struct.unpack('<B', f.read(1))[0]
            header_size = struct.unpack('<I', f.read(4))[0]
            self.num_rows = struct.unpack('<Q', f.read(8))[0]
            self.num_cols = struct.unpack('<H', f.read(2))[0]
            header_bytes = f.read(header_size)
            p = 0
            metas = []
            for _ in range(self.num_cols):
                name_len = struct.unpack_from('<H', header_bytes, p)[0]; p+=2
                name = header_bytes[p:p+name_len].decode('utf-8'); p+=name_len
                dtype = struct.unpack_from('<B', header_bytes, p)[0]; p+=1
                offset = struct.unpack_from('<Q', header_bytes, p)[0]; p+=8
                comp_size = struct.unpack_from('<Q', header_bytes, p)[0]; p+=8
                uncomp_size = struct.unpack_from('<Q', header_bytes, p)[0]; p+=8
                cm = ColumnMeta(name, dtype)
                cm.offset = offset
                cm.compressed_size = comp_size
                cm.uncompressed_size = uncomp_size
                metas.append(cm)
            self.columns_meta = metas

    def list_columns(self):
        return [ (i, m.name, m.dtype) for i,m in enumerate(self.columns_meta) ]

    def read_column(self, col_name:str):
        meta = None
        for m in self.columns_meta:
            if m.name == col_name:
                meta = m
                break
        if meta is None:
            raise KeyError(f'No such column: {col_name}')
        with open(self.path, 'rb') as f:
            f.seek(meta.offset)
            comp = f.read(meta.compressed_size)
            un = zlib.decompress(comp)
            buf = io.BytesIO(un)
            nb_len = struct.unpack('<I', buf.read(4))[0]
            null_bitmap = buf.read(nb_len)
            def is_null(i):
                byte_index = i//8
                bit = i%8
                return (null_bitmap[byte_index] >> bit) & 1 == 1
            values = []
            if meta.dtype == DT_INT32:
                for i in range(self.num_rows):
                    v = struct.unpack('<i', buf.read(4))[0]
                    values.append(None if is_null(i) else str(v))
            elif meta.dtype == DT_FLOAT64:
                for i in range(self.num_rows):
                    v = struct.unpack('<d', buf.read(8))[0]
                    values.append(None if is_null(i) else repr(v))
            elif meta.dtype == DT_STRING:
                offsets = [struct.unpack('<I', buf.read(4))[0] for _ in range(self.num_rows+1)]
                concat = buf.read()
                for i in range(self.num_rows):
                    if is_null(i):
                        values.append(None)
                    else:
                        s = concat[offsets[i]:offsets[i+1]].decode('utf-8')
                        values.append(s)
            else:
                raise ValueError('Unknown dtype')
            return values

    def read_table(self, columns:List[str]=None):
        if columns is None:
            columns = [m.name for m in self.columns_meta]
        data = {name: self.read_column(name) for name in columns}
        rows = []
        for i in range(self.num_rows):
            row = []
            for name in columns:
                val = data[name][i]
                row.append('' if val is None else val)
            rows.append(row)
        return columns, rows

# CLI functions
def csv_to_custom_cli():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='infile', required=True)
    ap.add_argument('--out', dest='outfile', required=True)
    ap.add_argument('--schema', dest='schema', required=False,
                    help='Optional: comma-separated list col:type where type in int,float,str')
    args = ap.parse_args()
    infile = args.infile
    outfile = args.outfile
    schema = None
    with open(infile, newline='', encoding='utf-8') as csvf:
        rdr = csv.reader(csvf)
        rows = list(rdr)
    if len(rows) == 0:
        print('Empty input')
        return
    header = rows[0]
    data_rows = rows[1:]
    if args.schema:
        parts = args.schema.split(',')
        schema = []
        for p in parts:
            name, typ = p.split(':')
            t = DT_STRING
            if typ in ('int','int32'):
                t = DT_INT32
            elif typ in ('float','float64'):
                t = DT_FLOAT64
            schema.append((name, t))
    else:
        # infer types per column
        cols_values = list(zip(*data_rows)) if data_rows else [['']*len(header) for _ in range(len(header))]
        schema = []
        for name, colvals in zip(header, cols_values):
            schema.append((name, infer_type(colvals)))
    writer = CCFWriter(outfile)
    writer.write(schema, data_rows)
    print('Wrote', outfile)

def custom_to_csv_cli():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='infile', required=True)
    ap.add_argument('--out', dest='outfile', required=True)
    ap.add_argument('--columns', dest='columns', required=False)
    args = ap.parse_args()
    reader = CCFReader(args.infile)
    if args.columns:
        cols = args.columns.split(',')
    else:
        cols = None
    columns, rows = reader.read_table(cols)
    with open(args.outfile, 'w', newline='', encoding='utf-8') as csvf:
        w = csv.writer(csvf)
        w.writerow(columns)
        w.writerows(rows)
    print('Wrote', args.outfile)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--csv_to_custom':
        sys.argv.pop(1)
        csv_to_custom_cli()
    elif len(sys.argv) > 1 and sys.argv[1] == '--custom_to_csv':
        sys.argv.pop(1)
        custom_to_csv_cli()
    else:
        print('Usage:')
        print('  python custom_columnar.py --csv_to_custom --in sample.csv --out sample.ccf')
        print('  python custom_columnar.py --custom_to_csv --in sample.ccf --out roundtrip.csv')
