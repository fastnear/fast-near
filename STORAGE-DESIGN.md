## Redis storage

TODO: Detail how current Redis storage is done

## FMDB storage

TODO: Try using FMDB as storage backend

https://github.com/kriszyp/lmdb-js looks promising.

## Native storage

TODO: Design native append-only storage for NEAR use case.

Main ideas:

- Can store full block content similar to NEAR Lake
- Append only index of block hashes by block index. Just store raw 32 byte hashes of blocks at location in file corresponding to block index
- Append only index of update block indices by account ID and data key. Just 4 bytes per block index per updated key for observable future (enough for about 4 billion blocks).
- Index of contract code and NEARFS (fs_store) blobs by SHA256 hash. Just 32 bytes per hash -> block index. Maybe remove blobs from normal block content and store them separately?