# Parquet Reader and Writer in Go

[Parquet](https://parquet.apache.org) is a column-oriented data file format used for
efficient data storage and retrieval. It allows for efficient, scalable queries and data exploration
especially for large datasets.

## Goals

As of writing (December 21, 2025), I have not learned Go and I have not used the Parquet file format.
However, I am interested in learning more about data engineering and Golang since I want to learn more
about backend development and data engineering. Also, I have just spent many months dealing with 
infrastructure work, and I am itching to write actual code.

Here are some of the goals I would like to achieve for this project.
- [x] Learn basic Golang constructs with standard I/O
    - [x] Go through "A Tour of Go"
- [ ] Write basic decoder for uncompressed Parquet files
    - [x] Write footer metadata reader with generated Go-Thrift library
    - [ ] Write basic unit tests for metadata processing
    - [ ] Write column and page traversal
- [ ] Write E2E test cases and compare results with the established Apache Parquet-Go implementation
- [ ] Use Golang coroutines (Goroutines) to vertically scale decoding
- [ ] Write basic encoder for uncompressed Parquet files (CSV -> Parquet)

If I feel up to it, I will try to implement the following:
- [ ] Expand decoder and encoder with gzip
- [ ] Implement "pseudo" SQL and run basic queries on this data

In other notes, I will be writing this project in Neovim, so I can officially say "I use Neovim, btw".
