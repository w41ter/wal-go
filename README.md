# Wal-go

[![Build Status](https://travis-ci.org/thinkermao/wal-go.svg?branch=master)](https://travis-ci.org/thinkermao/wal-go)
[![Coverage Status](https://coveralls.io/repos/github/thinkermao/wal-go/badge.svg)](https://coveralls.io/github/thinkermao/wal-go)

wal-go is an implementation of write ahead log. It provides the log persistence, recovery capabilities. wal-go is thread-safe and supports concurrent calls.

wal-go support logs are split at 64 Mb and support recovery from specified log points. wal-go Pre-allocate 64MB of space, the use of batch submission, to reduce disk sync costs.

## Usage

Create a new Wal and give it from that log point:

```go
log, _ := wal.Create("/tmp/wal", 0)
defer log.Close()
log.Write(1, []byte{ 0x1, 0x2, 0x3})    // will block until bytes has been written.
log.Write(....)
log.Sync()          // will block until bytes has been written.
```

Or after a crash, resume from a checkpoint:

```go
checkpoint = ....
log, _ := wal.Open("/tmp/wal", checkpoint, func(idx uint64, data []byte) {
    // consume record data.
})
defer log.Close()
log.Write(1, []byte{ 0x1, 0x2, 0x3})    // will block until bytes has been written.
log.Write(....)
log.Sync()          // will block until bytes has been written.
```

