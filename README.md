# Custom Columnar File Format (CCF) – Python

A lightweight custom **columnar storage format** implemented from scratch, inspired by Apache Parquet and ORC.  
This project demonstrates how analytical file formats work internally — including binary data layout, column metadata, compression, and selective reads.

CCF includes:
- A writer that converts **CSV → CCF**
- A reader that converts **CCF → CSV**
- Support for selective column reads
- Zlib compression for each column block
- A full binary specification (SPEC.md)
- Benchmarking and round-trip accuracy tests

---

## Features

- **Columnar Storage**  
  Each column is stored independently, enabling efficient access patterns.

- **Selective Column Reading**  
  Load only the columns you need without scanning the full dataset.

- **Zlib Compression**  
  Each column block is compressed using zlib for reduced file size.

- **Supported Data Types**
  - `INT32`
  - `FLOAT64`
  - `UTF-8 STRING` (stored with offset array)

- **Conversion Tools**
  - CSV → CCF
  - CCF → CSV
  - Selective column export

- **Round-Trip Accuracy**
  Ensures CSV → CCF → CSV produces identical data.

- **Simple, Documented Binary Format**
  Reference the **SPEC.md** file for structure, offsets, and metadata layout.

---

##  Project Structure
ccf-project/
│
├── custom_columnar.py        # Writer + Reader + CLI
├── SPEC.md                   # Binary layout specification
├── sample.csv                # Sample dataset
├── tests/
│   └── test_roundtrip.py     # Automated round-trip test
├── benchmark.py              # Benchmark tool
├── README.md                 # Project documentation
└── .gitignore

---

##  Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/SUDHITHA4225/ccf-project.git
cd ccf-project

2. Create Virtual Environment
python -m venv venv
venv\Scripts\activate   


 CSV → CCF Conversion
Convert any CSV file into the custom columnar format:
python custom_columnar.py --csv_to_custom --in sample.csv --out sample.ccf

CCF → CSV Conversion
Reconstruct the original CSV:
python custom_columnar.py --custom_to_csv --in sample.ccf --out output.csv

Selective Column Read
Extract only specific columns from the CCF file:
python custom_columnar.py --custom_to_csv --in sample.ccf --out selected.csv --columns name,score

File Format Documentation
The complete binary layout is documented in SPEC.md, including:
Magic header and version
Schema format
Column metadata and offsets
Compressed column block structure
String encoding using offset arrays

## Testing
Run round-trip verification:
python custom_columnar.py --csv_to_custom --in sample.csv --out sample.ccf
python custom_columnar.py --custom_to_csv --in sample.ccf --out roundtrip.csv

Compare:
diff sample.csv roundtrip.csv

Benchmark
Benchmark read/write performance:
python benchmark.py


Project Summary
This project demonstrates how modern analytical file formats operate internally.
You will learn:
Designing a binary file format
Handling schemas & metadata
Implementing columnar compression
Efficient column selection
Building CLI tools for data conversion
Verifying correctness with round-trip testing
The result is a lightweight yet fully functional columnar storage system built from scratch in Python.


