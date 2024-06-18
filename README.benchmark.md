# Benchmark

Benchmarks are done on an Apple Intel laptop with 2.6 GHz 6-Core Intel Core i7
CPU, 32 GB 2667 MHz DDR4 and an SSD disk.

## 10000 entries, 1000 bytes each

    LmdbCache         - Set: 0.0605s, Get: 0.0332s, Delete: 0.0000s
    BerkleyDbCache    - Set: 0.0741s, Get: 0.0086s, Delete: 0.0000s
    RocksdictCache    - Set: 0.1600s, Get: 0.0278s, Delete: 0.0000s
    BlobCache         - Set: 1.0514s, Get: 0.1391s, Delete: 0.0000s
    SeparateFileCache - Set: 4.2074s, Get: 1.8063s, Delete: 0.0000s

## 10000 entries, 10000 bytes each

    LmdbCache         - Set: 0.1316s, Get: 0.0548s, Delete: 0.0000s
    BerkleyDbCache    - Set: 1.2471s, Get: 0.0919s, Delete: 0.0000s
    RocksdictCache    - Set: 0.6009s, Get: 0.1236s, Delete: 0.0000s
    BlobCache         - Set: 9.1623s, Get: 0.6966s, Delete: 0.0000s
    SeparateFileCache - Set: 12.5391s, Get: 7.1718s, Delete: 0.0000s

## 100000 entries, 100 bytes each

    LmdbCache         - Set: 0.3437s, Get: 0.2637s, Delete: 0.0000s
    BerkleyDbCache    - Set: 0.6149s, Get: 0.0732s, Delete: 0.0000s
    RocksdictCache    - Set: 1.1766s, Get: 0.2779s, Delete: 0.0000s
    BlobCache         - Set: 8.6100s, Get: 0.6081s, Delete: 0.0000s
    SeparateFileCache - Set: 50.2860s, Get: 33.3733s, Delete: 0.0000s

    391M    tmp_benchmark_cache
     11M    tmp_benchmark_cache.data.blob
     17M    tmp_benchmark_cache.dbm
    9.8M    tmp_benchmark_cache.index.json
     16M    tmp_benchmark_cache.lmdb
    8.0K    tmp_benchmark_cache.lmdb-lock
     11M    tmp_benchmark_cache.rockdb

## 100000 entries, 100 bytes each

    LmdbCache         - Set: 0.3250s, Get: 0.2429s, Delete: 0.0000s
    BerkleyDbCache    - Set: 0.5924s, Get: 0.0691s, Delete: 0.0000s
    RocksdictCache    - Set: 1.1355s, Get: 0.1666s, Delete: 0.0000s
    BlobCache         - Set: 11.3350s, Get: 0.7885s, Delete: 0.0000s
    SeparateFileCache - Set: 48.4339s, Get: 27.7514s, Delete: 0.0000s

    391M    tmp_benchmark_cache
     11M    tmp_benchmark_cache.data.blob
     17M    tmp_benchmark_cache.dbm
    8.2M    tmp_benchmark_cache.index.json
     16M    tmp_benchmark_cache.lmdb
    8.0K    tmp_benchmark_cache.lmdb-lock
     11M    tmp_benchmark_cache.rockdb
    4.0K    tmp_test_benchmark_cache.data.blob
    4.0K    tmp_test_benchmark_cache.index.json

## 10000 entries, 10000 bytes each

    LmdbCache         - Set: 0.1388s, Get: 0.0582s, Delete: 0.0000s
    BerkleyDbCache    - Set: 1.3344s, Get: 0.0871s, Delete: 0.0000s
    RocksdictCache    - Set: 0.6510s, Get: 0.1291s, Delete: 0.0000s
    BlobCache         - Set: 7.0355s, Get: 1.0484s, Delete: 0.0000s
    SeparateFileCache - Set: 10.7619s, Get: 2.6830s, Delete: 0.0000s

     78M    tmp_benchmark_cache
     80M    tmp_benchmark_cache.data.bin
    120M    tmp_benchmark_cache.dbm
    344K    tmp_benchmark_cache.index.bin
    118M    tmp_benchmark_cache.lmdb
    8.0K    tmp_benchmark_cache.lmdb-lock
     96M    tmp_benchmark_cache.rockdb

## 100000 entries, 1000 bytes each

    LmdbCache         - Set: 0.5862s, Get: 0.3815s, Delete: 0.0000s
    BerkleyDbCache    - Set: 2.5659s, Get: 0.1620s, Delete: 0.0000s
    RocksdictCache    - Set: 1.9175s, Get: 0.5436s, Delete: 0.0000s
    BlobCache         - Set: 51.2843s, Get: 2.7833s, Delete: 0.0000s
    SeparateFileCache - Set: 98.7016s, Get: 44.5379s, Delete: 0.0000s

    391M    tmp_benchmark_cache
     80M    tmp_benchmark_cache.data.bin
    112M    tmp_benchmark_cache.dbm
    3.3M    tmp_benchmark_cache.index.bin
    208M    tmp_benchmark_cache.lmdb
    8.0K    tmp_benchmark_cache.lmdb-lock
     98M    tmp_benchmark_cache.rockdb


