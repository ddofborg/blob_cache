'''

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
file except in compliance with the License. You may obtain a copy of the License at:
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.

@author ddofborg <https://github.com/ddofborg>


What is this?
-------------

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

This approach is also faster than storing each key-value pair in a separate
file, it is slower than using `LMDB` or `RocksDb`. On PHP platorm those are
not always readily available and in out case performance was good enough.

There are no external dependencies.


Drawbacks
---------

One issue with this approach is that the data file can become fragmented, as
every update is written at the end. To overcome this, a `vacuum()` method is
provided to rebuild the data file. The fragmentation ratio can be checked with
the `fragmentation_ratio()` method.

Another big drawback is that this approach can only be used by one process at a
time.



BLOB FORMATS
------------

Data file:

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


'''

import io
import zlib
import struct
import json
import os
import time
import fcntl
from typing import Callable, Union, Optional

import logging
# Set up global logging
LOG = logging.getLogger(__name__)
# Only configure logging if it has not been configured yet.
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

class BlobCache:

    header_data_file = b'blob.cache.data.01'
    ''' The header of the data file.'''

    pack_format_index = 'QII'
    ''' The format of the index entry in the index file. long long start, int length, int expires.'''

    stats = {}

    def __init__(self, data_file: str, auto_vacuum_threshold: float = 0.5):
        ''' Initialize the cache with the data file.

            data_file: The data file name without extension.
            auto_vacuum_threshold: The fragmentation ratio threshold for auto vacuuming.

            '''

        self.stats = {
            'hits': 0,
            'sets': 0,
            'deletes': 0,
            'misses': 0,
            'refreshes': 0,
        }
        self.auto_vacuum_threshold = auto_vacuum_threshold

        self.data_file = data_file + '.data.bin'
        self.index_file = data_file + '.index.bin'
        self.wal_file = data_file + '.wal.bin'

        # data file for appending
        self.data_file_append_fd = open(self.data_file, 'ab')
        self._lock_file(self.data_file_append_fd)
        _tell = self.data_file_append_fd.tell()
        if _tell == 0:
            self._write_header()
        else:
            LOG.debug('Datafile of size %d bytes is found.', _tell)
        self.data_file_append_fd.seek(0, io.SEEK_END)  # needed for PHP, so added here too

        # data file for reading
        self.data_file_read_fd = open(self.data_file, 'rb')

        # load index and read WAL file is exists to the index and remove WAL file
        self.index = self._load_index()
        # write-ahead log (WAL) file, open after loading index
        self.wal_file_fd = open(self.wal_file, 'ab')

    def _is_locked(self, fd=None, keep_locked=False):
        ''' Check if the cache data file is locked. '''
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = False
            if not keep_locked:
                fcntl.flock(fd, fcntl.LOCK_UN)
        except IOError:
            # If the lock acquisition fails, the file is locked by another process
            locked = True
        return locked

    def _lock_file(self, fd=None):
        ''' Lock the cache data file. '''
        try:
            # Try to acquire an exclusive lock on the file
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            LOG.debug("Lock aquired.")
        except IOError as ex:
            LOG.error("Lock failed. File is already locked by another process.")
            raise ex
        return fd

    def _unlock_file(self, fd=None):
        ''' Unlock the cache data file. '''
        fcntl.flock(fd, fcntl.LOCK_UN)
        LOG.debug("Lock released on file.")

    def _load_index(self) -> dict:
        ''' Load the index from the index file and the write-ahead log (WAL) file
            if it exists. Then return the index and remove the WAL file. '''
        index = {}
        now = time.time()
        LOG.debug("Loading index file...")
        if os.path.exists(self.index_file):
            with open(self.index_file, 'rb') as f:
                while True:
                    key_length_bytes = f.read(struct.calcsize('I'))
                    if not key_length_bytes:
                        break
                    key_length = struct.unpack('I', key_length_bytes)[0]
                    key = f.read(key_length).decode('utf-8')
                    entry_data = f.read(struct.calcsize(self.pack_format_index))
                    start, length, expires = struct.unpack(self.pack_format_index, entry_data)
                    if expires == 0 or expires > now:
                        index[key] = {
                            'start': start,
                            'length': length,
                            'expires': expires,
                        }

        if os.path.exists(self.wal_file):
            with open(self.wal_file, 'rb') as f:
                LOG.debug("...processing WAL file...")
                while True:
                    key_length_bytes = f.read(4)
                    if len(key_length_bytes) < 4:
                        break
                    key_length = struct.unpack('I', key_length_bytes)[0]
                    key = f.read(key_length).decode('utf-8')
                    # check if entry is deleted or not
                    entry_flag = struct.unpack('?', f.read(1))[0]
                    if entry_flag:
                        entry_data = f.read(struct.calcsize(self.pack_format_index))
                        start, length, expires = struct.unpack(self.pack_format_index, entry_data)
                        if expires == 0 or expires > now:
                            entry = {
                                'start': start,
                                'length': length,
                                'expires': expires,
                            }
                            index[key] = entry
                    else:
                        if key in index:
                            del index[key]
            os.remove(self.wal_file)

        LOG.debug('...index loaded with %d keys.', len(index))
        return index

    def _append_to_wal_file(self, key: str, entry: Optional[dict]):
        ''' Append an entry to the write-ahead log (WAL) file. '''
        buf = []
        key_bytes = key.encode('utf-8')
        # key length int
        key_length = len(key_bytes)
        buf.append(struct.pack('I', key_length))
        # key bytes
        buf.append(key_bytes)
        if entry is None:
            # entry deleted
            buf.append(struct.pack('?', False))
        else:
            # entry added or updated
            buf.append(struct.pack('?', True))
            buf.append(struct.pack(self.pack_format_index, entry['start'], entry['length'], entry['expires']))

        self.wal_file_fd.write(b''.join(buf))
        self.wal_file_fd.flush()

    def _save_index(self):
        ''' Save the index to the index file and remove the WAL file. '''
        tmp_index_file = self.index_file + '.tmp'
        with open(tmp_index_file, 'wb') as f:
            LOG.debug("Saving index file...")
            for key, entry in self.index.items():
                buf = []
                key_bytes = key.encode('utf-8')
                key_length = len(key_bytes)
                buf.append(struct.pack('I', key_length))
                buf.append(key_bytes)
                buf.append(struct.pack(self.pack_format_index, entry['start'], entry['length'], entry['expires']))
                f.write(b''.join(buf))
        os.replace(tmp_index_file, self.index_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def _write_header(self):
        ''' Write the header of the data file. '''
        self.data_file_append_fd.write(self.header_data_file)

    def _get_compressed_data(self, data: bytes):
        ''' Compress data using zlib. '''
        return zlib.compress(data, 6)

    def _append_frame_to_data_file(self, key: str, expires:int, is_bytes: int, data: bytes) -> tuple:
        ''' Append data to the data file and return the start position and length
            of the compressed data.'''
        buf = []
        # is_bytes struct int
        buf.append(struct.pack('B', is_bytes))
        # data bytes
        start = self.data_file_append_fd.tell()
        compressed_data = zlib.compress(data, 6)
        # length of gzipped data
        buf.append(struct.pack('I', len(compressed_data)))
        # append the compressed data
        buf.append(compressed_data)
        # write data to file
        self.data_file_append_fd.write(b''.join(buf))
        self.data_file_append_fd.flush()
        end = self.data_file_append_fd.tell()
        return start, end - start

    def _read_frame_from_data_file(self, start: int):
        data = None
        self.data_file_read_fd.seek(start)
        # read is_bytes (int)
        is_bytes = struct.unpack('B',self.data_file_read_fd.read(struct.calcsize('B')))[0]
        # read data length (int - long long)
        data_length = struct.unpack('I',self.data_file_read_fd.read(struct.calcsize('I')))[0]
        # read data
        compressed_data = self.data_file_read_fd.read(data_length)
        data = self._decompress_data(compressed_data)
        if is_bytes == 0:
            data = json.loads(data)
        return data

    def _decompress_data(self, compressed_data: bytes):
        ''' Decompress data using zlib. '''
        return zlib.decompress(compressed_data)

    def set_on_miss(self, key: str, value: Union[str, set, dict, list, int, float, bool, bytes], ttl: Optional[int] = None):
        ''' Set a key in the cache only if this key is not found in cache.
            The value can be a string, set, dict, list, int, float, bool or bytes. '''
        if not self.has(key):
            self.set(key, value, ttl)

    def set(self, key: str, value: Union[str, set, dict, list, int, float, bool, bytes], ttl: Optional[int] = None):
        ''' Set a key in the cache. The value can be a string, set, dict, list,
            int, float, bool or bytes. '''
        assert isinstance(key, str), 'Key must be a string'
        assert self.data_file_read_fd, 'Cache is closed'
        if isinstance(value, bytes):
            data = value
            is_bytes = 1
        elif isinstance(value, (bool, tuple, str, set, dict, list, int, float, bool)):
            data = json.dumps(value).encode()
            is_bytes = 0
        else:
            raise ValueError(f'Value must be bytes or JSON-serializable, given {type(value)}')

        expires = int(time.time() + ttl) if ttl else 0

        start, length = self._append_frame_to_data_file(key, expires, is_bytes, data)

        entry = {
            'start': start,
            'length': length,   # this is kept for faster fragmentation calculation
            'expires': expires,
            'is_bytes': is_bytes,
        }
        self.index[key] = entry
        self._append_to_wal_file(key, entry)

        self.stats['sets'] += 1

    def get(self, key: str, refresh_callback: Optional[Callable[[str], Union[str, dict]]] = None, new_ttl: Optional[int] = None):
        ''' Get a key from the cache. If the key is expired, the `refresh_callback`
            is called and its return value is stored in the cache with the new TTL.
            '''
        assert isinstance(key, str), 'Key must be a string'
        assert self.data_file_read_fd, 'Cache is closed'

        # has key and not expired
        if self.has(key):
            self.stats['hits'] += 1
            entry = self.index[key]
            return self._read_frame_from_data_file(entry['start'])

        self.stats['misses'] += 1

        # key is expired or not found, refresh?
        if refresh_callback:
            self.stats['refreshes'] += 1
            value = refresh_callback(key)
            self.set(key, value, ttl=new_ttl)
            return value

        # key is not found or expired and no refresh callback
        raise KeyError(f'Key `{key}` is not found or expired')

    def has(self, key: str) -> bool:
        ''' Check if a key is in the cache and it not expired. '''
        assert isinstance(key, str), 'Key must be a string'
        assert self.data_file_read_fd, 'Cache is closed'
        if (key not in self.index) or (self.index[key]['expires'] and time.time() > self.index[key]['expires']):
            return False
        return True

    def delete(self, key: str):
        ''' Delete a key from the cache. '''
        assert isinstance(key, str), 'Key must be a string'
        assert self.data_file_read_fd, 'Cache is closed'
        if key in self.index:
            self._append_to_wal_file(key, None)
            del self.index[key]
            self.stats['deletes'] += 1

    def delete_startswith(self, key: str):
        ''' Delete all keys from the cache that start with the given prefix. '''
        keys = [k for k in self.index.keys() if k.startswith(key)]
        for k in keys:
            self.delete(k)

    def when_expired(self, key: str, relative=False) -> int:
        ''' Return the expiration timestamp of a key. If `relative` is True,
            return the relative time in seconds. '''
        assert isinstance(key, str), 'Key must be a string'
        assert self.data_file_read_fd, 'Cache is closed'
        if key in self.index:
            return int(self.index[key]['expires'] - time.time()) if relative else self.index[key]['expires']
        raise KeyError(f'Key `{key}` is not found')

    def get_stats(self) -> dict:
        ''' Return the cache statistics. '''
        fragmentation_ratio = self.fragmentation_ratio()
        data_file_size = self.data_file_append_fd.tell()
        self.stats['fragmentation_ratio']  = fragmentation_ratio
        self.stats['total_keys']  = len(self.index)
        self.stats['data_file_size_bytes'] = data_file_size
        return self.stats

    def fragmentation_ratio(self):
        ''' Return the fragmentation ratio of the data file, meaning
            the ratio of the data which is also in the index to the file size.
            Higher value means more fragmented. 0.8 means only 20% is used for
            data, the rest of the data file is old. '''
        assert self.data_file_read_fd, 'Cache is closed'
        size_file = self.data_file_append_fd.tell() - len(self.header_data_file)
        if size_file <= 0:
            return 0
        size_index = 0
        for _, entry in self.index.items():
            size_index += entry['length']
        return 1 - (size_index / size_file)

    def vacuum(self):
        ''' Rebuild the data file to remove fragmentation by removing
            the data which is not in the index. '''
        assert self.data_file_read_fd, 'Cache is closed'
        LOG.debug("Vacuuming data file...")
        tmp_data_file = self.data_file + '.tmp'
        new_index = {}
        with open(tmp_data_file, 'wb') as new_file:
            self._write_header()
            for key, entry in self.index.items():
                self.data_file_read_fd.seek(entry['start'])
                data_frame = self.data_file_read_fd.read(entry['length'])
                new_start = new_file.tell()
                new_file.write(data_frame)
                new_index[key] = {
                    'start': new_start,
                    'length': len(data_frame),
                    'expires': entry['expires'],
                }

        os.replace(tmp_data_file, self.data_file)
        self.index = new_index
        self._save_index()

    def close(self):
        ''' Close the cache. Closes all files and saves the index. '''
        if not self.data_file_read_fd:
            raise RuntimeError('Cache is already closed')

        stats = self.get_stats()

        if self.fragmentation_ratio() > self.auto_vacuum_threshold:
            LOG.debug(f"Auto vacuuming data file as fragmentation ratio is higher than {self.auto_vacuum_threshold}.")
            self.vacuum()
        if self.data_file_read_fd:
            self.data_file_read_fd.close()
            self.data_file_read_fd = None
        if self.wal_file_fd:
            self.wal_file_fd.close()
            self.wal_file_fd = None
        if self.data_file_append_fd:
            self._unlock_file(self.data_file_append_fd)
            self.data_file_append_fd.close()
            self.data_file_append_fd = None
        self._save_index()

        LOG.debug(f"Cache closed, stats: {stats}")
