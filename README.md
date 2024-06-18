# What is this?

Persistent key-value store with a write-ahead log (WAL) for crash recovery. The
data is compressed using zlib and stored in a single data file. The index is
stored in a separate file. Keys must be strings. Values can be any JSON-serializable
object or bytes.

The reason for this is to overcome the limitations when storing cache on the
filesystem as a separate file. Each file is basically a key-value pair, name
is the key and contents is the value. When the number of objects grows, the
number of files also grows. This can lead to errors on the file systems or
reach inodes limits.

We also needed a caching method which was supported by a Python and PHP scripts.

This approach is a little faster than storing each key-value pair in a separate
file, but it is slower than using `LMDB` or `RocksDb`. On PHP platorm those are
not always readily available and in our case performance was good enough.

There are no external dependencies.


# Drawbacks

One issue with this approach is that the data file can become fragmented, as
every update is written at the end. To overcome this, a `vacuum()` method is
provided to rebuild the data file. The fragmentation ratio can be checked with
the `fragmentation_ratio()` method.

Another big drawback is that this approach can only be used by one process at a
time. File locking mechanism is present.


# Benchmarks

See `README.benchmarks.md` for more benchmarks on Python.

    ## 10000 entries, 10000 bytes each
    
    LmdbCache         - Set: 0.1316s, Get: 0.0548s, Delete: 0.0000s
    BerkleyDbCache    - Set: 1.2471s, Get: 0.0919s, Delete: 0.0000s
    RocksdictCache    - Set: 0.6009s, Get: 0.1236s, Delete: 0.0000s
    BlobCache         - Set: 9.1623s, Get: 0.6966s, Delete: 0.0000s
    SeparateFileCache - Set: 12.5391s, Get: 7.1718s, Delete: 0.0000s

## Usage

See `test.py` for more details.

```python
from blob_cache import BlobCache
c = BlobCache('tmp_blob_cache')
c.set('a', 1)
print(c.get('a') == 1)
c.close()
```

See `test.php` for more details.

```php
require_once 'blob_cache.php';
$c = new BlobCache('tmp_test_cache');
$c->set('a', 1);
echo $c->get('a') == 1 ? 'true' : 'false';
$c->close();
```

# BLOB FORMATS

Data file:

Data file stats with a header as defined in the cache class. Then it's a linear binary data file.

    bytes     type        decription
    -------   ----        ----------
    4 bytes   long        [n] lenth of data (max 2^32-1 bytes)
    1 byte    int         0 = data json, 1 = byte data
    n bytes   *char[n]    GZIPed data (max 2^32-1 bytes)

Index file:

    bytes     type        decription
    -------   ----        ----------
    4 bytes   long        [n] length of key
    n bytes   *char[n]    key string
    8 bytes   long long   data frame position in data file
    4 bytes   long        expiration timestamp

WAL file:

    bytes     type        decription
    -------   ----        ----------
    4 bytes   long        [n] length of key
    n bytes   *char[n]    key string
    1 byte    bool        0 = delete, 1 = add/update
    8 bytes   long long   data frame position in data file
    4 bytes   long        expiration timestamp

# License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
file except in compliance with the License. You may obtain a copy of the License at:
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.

@author ddofborg <https://github.com/ddofborg>
