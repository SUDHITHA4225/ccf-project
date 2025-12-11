# SPEC — Compact Custom Columnar Format (CCF) v1.0

## Endianness
All multi-byte integers and floats use little-endian encoding.

## File Layout (high level)
| Offset | Size (bytes) | Description |
|--------|--------------:|------------|
| 0      | 8            | MAGIC = ASCII `CCFv1\x00\x00` |
| 8      | 1            | VERSION (uint8) |
| 9      | 4            | HEADER_SIZE (uint32) |
| 13     | 8            | NUM_ROWS (uint64) |
| 21     | 2            | NUM_COLUMNS (uint16) |
| 23     | HEADER_SIZE   | Header entries for each column |
| ...    | variable     | Column blocks (compressed) |

## Column metadata entry
- NAME_LEN: uint16
- NAME_BYTES: NAME_LEN bytes (UTF-8)
- DTYPE: uint8 (0=int32, 1=float64, 2=utf8 string)
- OFFSET: uint64 (start of compressed block)
- COMPRESSED_SIZE: uint64
- UNCOMPRESSED_SIZE: uint64

## Column block (uncompressed payload)
- NULL_BITMAP_LEN: uint32
- NULL_BITMAP: NULL_BITMAP_LEN bytes (bit i = 1 => row i is NULL)
- INT32: num_rows * int32 values
- FLOAT64: num_rows * float64 values
- STRING: OFFSETS (uint32 * (num_rows+1)) + CONCAT_BYTES

## Compression
Each column's uncompressed payload is compressed with zlib. Header stores compressed & uncompressed sizes.

## Notes
Null vs empty string: nulls are indicated by null bitmap; empty string is distinct (non-null with length 0).
