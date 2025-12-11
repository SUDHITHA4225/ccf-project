# ccf-project 
Custom Columnar File Format Project

This project implements a simple columnar file format similar to Parquet or ORC.
It includes a writer that converts CSV data into a compressed columnar binary file
and a reader that can read the full file or selectively read only specific columns.
The file format supports three data types:

32-bit integers

64-bit floating point numbers

UTF-8 strings

Columns are stored separately and compressed using zlib.
The header includes schema, offsets, and all required metadata.
Tools included:

csv_to_custom: converts CSV to the custom columnar format

custom_to_csv: converts the custom format back to CSV

The project also contains a SPEC file describing the binary structure
and a sample CSV file for testing.

If you want an even shorter version or a more detailed version, I can prepare that too.
