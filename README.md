# VBINSEQ

VBINSEQ is a high-performance binary file format for nucleotides.

It is a variant of the [BINSEQ](https://github.com/arcinstitute/binseq) file format with support for _variable length records_ and _quality scores_.

It is a block-based file format with support for parallel compression and decompression with random-access to record blocks.

## Overview

At a high-level VBINSEQ is a variant of [BINSEQ](https://github.com/arcinstitute/binseq) with fixed-size record blocks instead of fixed-size records.

Each record block is composed of repeating records which each at minimum have a single nucleotide sequence.
Each record can optionally have an extended sequence (a paired sequence) and associated quality scores.
Importantly, records **cannot** span block boundaries, so all blocks are independent.

Each block has the same size and are independent so they can be compressed and decompressed independently.
VBINSEQ tracks both the compressed and uncompressed size of each block in block headers which can then be indexed for random block access.

### Structure

The file begins with a **FILE HEADER** which provides a description of the configuration.
The remaining bytes of the file are repeated **RECORD BLOCKS**.

Each **RECORD BLOCK** is composed of three parts

1. **BLOCK HEADER**: Provides metadata on the associated block (is always uncompressed)
2. **BLOCK DATA**: Repeating complete **VBINSEQ RECORD**s (optionally ZSTD compressed).
3. **BLOCK PADDING**: Repeated null bytes to keep the virtual (uncompressed) memory of each block equivalent.

Each **VBINSEQ RECORD** is composed of two parts: **RECORD PREAMBLE**, **RECORD DATA**

1. **RECORD PREAMBLE**: Contains record metadata.
2. **RECORD DATA**: Contains the encoded primary and extended sequences as well as the quality scores.

### Description

All binary encoding is little-endian unless specifically noted otherwise.

#### **FILE HEADER**

| Field      | Type | Size (bytes) | Position (bytes) | Description                                          |
| ---------- | ---- | ------------ | ---------------- | ---------------------------------------------------- |
| magic      | u32  | 4            | 0                | A magic number to specify the file format (VSEQ)     |
| format     | u8   | 1            | 4                | Version of the file format                           |
| block      | u64  | 8            | 5                | Size of all blocks in bytes (virtual memory)         |
| qual       | bool | 1            | 13               | Whether quality scores are included on each sequence |
| compressed | bool | 1            | 14               | Whether blocks are ZSTD compressed                   |
| paired     | bool | 1            | 15               | Whether records are paired sequences                 |
| reserved   | u8   | 16           | 16               | Reserved bytes in case of future extensions          |

Total size: 32 bytes

#### **BLOCK HEADER**

| Field    | Type | Size (bytes) | Position (bytes) | Description                                                                                                               |
| -------- | ---- | ------------ | ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| magic    | u64  | 8            | 0                | A magic number to validate format (BLOCKSEQ)                                                                              |
| size     | u64  | 8            | 8                | Actual size of the block in bytes (can be different than configured block size in header depending on compression status) |
| records  | u32  | 4            | 16               | Number of records in block                                                                                                |
| reserved | u8   | 12           | 20               | Reserved bytes in case of future extensions                                                                               |

Total size: 32 bytes

#### **VBINSEQ RECORD**

| Field | Type  | Size (bytes)                 | Description                                                                                    |
| ----- | ----- | ---------------------------- | ---------------------------------------------------------------------------------------------- |
| flag  | u64   | 8                            | A binary flag for the record                                                                   |
| slen  | u64   | 8                            | The length of the primary sequence in record (basepairs)                                       |
| xlen  | u64   | 8                            | The length of the extended sequence in record (0 if not paired)                                |
| sbuf  | [u64] | ceil(slen / 32)              | Encoded primary sequence                                                                       |
| squal | [u8]  | qual ? slen : 0              | Associated quality scores of primary sequence (no bytes if not tracking quality)               |
| xbuf  | [u64] | paired ? ceil(xlen / 32) : 0 | Encoded extended sequence (no bytes if not paired)                                             |
| xqual | [u8]  | qual & paired ? xlen : 0     | Associated quality scores of extended sequence (no bytes if not paired + not tracking quality) |

Total size: 24 + x bytes

x = 8 \* (sbuf + xbuf) + (squal + xqual)
