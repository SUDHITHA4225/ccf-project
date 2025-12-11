# simple round-trip test (no pytest required; run with python)
import csv
from custom_columnar import CCFWriter, CCFReader

def run_roundtrip():
    infile = 'sample.csv'
    out_ccf = 'sample.ccf'
    out_csv = 'roundtrip.csv'

    with open(infile, newline='', encoding='utf-8') as f:
        rdr = csv.reader(f)
        rows = list(rdr)
    header = rows[0]
    data_rows = rows[1:]
    # infer types
    cols_values = list(zip(*data_rows)) if data_rows else [['']*len(header) for _ in range(len(header))]
    schema = []
    from custom_columnar import infer_type
    for name, colvals in zip(header, cols_values):
        schema.append((name, infer_type(colvals)))
    w = CCFWriter(out_ccf)
    w.write(schema, data_rows)

    r = CCFReader(out_ccf)
    columns, rows2 = r.read_table()
    # reconstruct CSV style rows with header
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        import csv
        wcsv = csv.writer(f)
        wcsv.writerow(columns)
        wcsv.writerows(rows2)
    print('Round-trip written to', out_csv)

if __name__ == '__main__':
    run_roundtrip()
