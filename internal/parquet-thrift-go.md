# Generated Go Code for Parquet's Thrift

Parquet uses Thrift's TCompactProtocol for its file metadata and page header metadata.

[Thrift](https://thrift.apache.org) is a framework that helps implement a cross-language RPC.
This framework is able to encode complex data structures into a very compact format for network transport.

Unfortunately, it's beyond the scope of this small project for me to also implement the encoding/decoding of
this Thrift stuff. I want to get to the meaty Parquet stuff.

This directory holds `gen-go/`, which is generated Go code using the `thrift` command on the `parquet.thrift`
file. The `parquet.thrift` file was pulled from 
[this file on commit 4b1c72c](https://github.com/apache/parquet-format/commit/4b1c72c837bec5b792b2514f0057533030fcedf8).

If you, too, want to implement Parquet without needing to go through installing `thrift`, I suggest looking at
`https://github.com/parquet-go/parquet-go/`, which seems to not rely on this generated Thrift code. I could be
mistaken, I didn't really look at it much so I can do most of it myself.
