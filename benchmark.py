import time, csv
from custom_columnar import CCFReader

CSV = 'sample.csv'
CCF = 'sample.ccf'
COLUMN = 'name'

# CSV parse time
t0 = time.time()
with open(CSV, newline='', encoding='utf-8') as f:
    rdr = csv.reader(f)
    header = next(rdr)
    idx = header.index(COLUMN)
    values = [ (row[idx] if len(row)>idx else '') for row in rdr ]
t_csv = time.time()-t0

# CCF selective read time
r = CCFReader(CCF)
t0 = time.time()
vals = r.read_column(COLUMN)
t_ccf = time.time()-t0

print('CSV parse time: {:.6f}s'.format(t_csv))
print('CCF selective read time: {:.6f}s'.format(t_ccf))
