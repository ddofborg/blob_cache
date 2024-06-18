<?php
/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 *
 * @author ddofborg <https://github.com/ddofborg>
 *
 *
 * What is this?
 * -------------
 *
 * Persistent key-value store with a write-ahead log (WAL) for crash recovery.
 * The data is compressed using zlib and stored in a single data file. The index is
 * stored in a separate file. Keys must be strings. Values can be any JSON-serializable
 * object or bytes.
 *
 * The reason for this is to overcome the limitations when storing cache on the
 * filesystem as a separate file. Each file is basically a key-value pair, name
 * is the key and contents is the value. When the number of objects grows, the
 * number of files also grows. This can lead to errors on the file systems or
 * reach inodes limits.
 *
 * We also needed a caching method which was supported by a Python and PHP scripts.
 *
 * This approach is also faster than storing each key-value pair in a separate
 * file, it is slower than using `LMDB` or `RocksDb`. On PHP platorm those are
 * not always readily available and in out case performance was good enough.
 *
 * There are no external dependencies.
 *
 *
 * Drawbacks
 * ---------
 *
 * One issue with this approach is that the data file can become fragmented, as
 * every update is written at the end. To overcome this, a `vacuum()` method is
 * provided to rebuild the data file. The fragmentation ratio can be checked with
 * the `fragmentation_ratio()` method.
 *
 * Another big drawback is that this approach can only be used by one process at a
 * time.
 *
 *
 * BLOB FORMATS
 * ------------
 *
 * Data file:
 *
 *     bytes     type        decription
 *     -------   ----        ----------
 *     4 bytes   long        [n] lenth of data (max 2^32-1 bytes)
 *     1 byte    int         0 = data json, 1 = byte data
 *     n bytes   *char[n]    GZIPed data (max 2^32-1 bytes)
 *
 * Index file:
 *
 *     bytes     type        decription
 *     -------   ----        ----------
 *     4 bytes   long        [n] length of key
 *     n bytes   *char[n]    key string
 *     8 bytes   long long   data frame position in data file
 *     4 bytes   long        expiration timestamp
 *
 * WAL file:
 *
 *     bytes     type        decription
 *     -------   ----        ----------
 *     4 bytes   long        [n] length of key
 *     n bytes   *char[n]    key string
 *     1 byte    bool        0 = delete, 1 = add/update
 *     8 bytes   long long   data frame position in data file
 *     4 bytes   long        expiration timestamp
 *
 */

class BlobCache
{
    const HEADER_DATA_FILE = 'blob.cache.data.01'; // The header of the data file.
    const PACK_FORMAT_INDEX = 'QII'; // The format of the index entry in the index file. long long start, int length, int expires.

    private $stats;
    private $dataFile;
    private $indexFile;
    private $walFile;
    private $dataFileAppendFd;
    private $dataFileReadFd;
    private $walFileFd;
    private $index;
    private $autoVacuumThreshold;
    private $decodeAsArray;

    public function __construct($dataFile, $decodeAsArray=false, $autoVacuumThreshold = 0.5)
    {
        /**
         * Initialize the cache with the data file.
         * dataFile: The data file name without extension.
         * autoVacuumThreshold: The fragmentation ratio threshold for auto vacuuming.
         */
        $this->stats = array(
            'hits' => 0,
            'sets' => 0,
            'deletes' => 0,
            'misses' => 0,
            'refreshes' => 0
        );

        $this->decodeAsArray = $decodeAsArray;
        $this->autoVacuumThreshold = $autoVacuumThreshold;

        $this->dataFile = $dataFile . '.data.bin';
        $this->indexFile = $dataFile . '.index.bin';
        $this->walFile = $dataFile . '.wal.bin';

        // data file for appending
        $this->dataFileAppendFd = fopen($this->dataFile, 'ab');
        $this->lockFile($this->dataFileAppendFd);
        $tell = ftell($this->dataFileAppendFd);
        if ($tell == 0) {
            $this->writeHeader();
        }
        fseek($this->dataFileAppendFd, 0, SEEK_END); // needed for PHP, so added here too

        // data file for reading
        $this->dataFileReadFd = fopen($this->dataFile, 'rb');

        // load index and read WAL file is exists to the index and remove WAL file
        $this->index = $this->loadIndex();
        // write-ahead log (WAL) file, open after loading index
        $this->walFileFd = fopen($this->walFile, 'ab');
    }

    private function isLocked($fd = null, $keepLocked = false)
    {
        /**
         * Check if the cache data file is locked.
         */
        $locked = false;
        if (flock($fd, LOCK_EX | LOCK_NB)) {
            $locked = false;
            if (!$keepLocked) {
                flock($fd, LOCK_UN);
            }
        } else {
            $locked = true;
        }
        return $locked;
    }

    private function lockFile($fd = null)
    {
        /**
         * Lock the cache data file.
         */
        if (!flock($fd, LOCK_EX | LOCK_NB)) {
            throw new Exception("Lock failed. File is already locked by another process.");
        }
    }

    private function unlockFile($fd = null)
    {
        /**
         * Unlock the cache data file.
         */
        flock($fd, LOCK_UN);
    }

    private function loadIndex()
    {
        /**
         * Load the index from the index file and the write-ahead log (WAL) file
         * if it exists. Then return the index and remove the WAL file.
         */
        $index = array();
        $now = time();

        if (file_exists($this->indexFile)) {
            $f = fopen($this->indexFile, 'rb');
            while (!feof($f)) {
                $keyLengthBytes = fread($f, 4);
                if (strlen($keyLengthBytes) < 4) {
                    break;
                }
                $keyLength = unpack('I', $keyLengthBytes)[1];
                $key = fread($f, $keyLength);
                $entryData = fread($f, 16);
                list($start, $length, $expires) = array_values(unpack('Qstart/Ilength/Iexpires', $entryData));
                if ($expires == 0 || $expires > $now) {
                    $index[$key] = array(
                        'start' => $start,
                        'length' => $length,
                        'expires' => $expires
                    );
                }
            }
            fclose($f);
        }

        if (file_exists($this->walFile)) {
            $f = fopen($this->walFile, 'rb');
            while (!feof($f)) {
                $keyLengthBytes = fread($f, 4);
                if (strlen($keyLengthBytes) < 4) {
                    break;
                }
                $keyLength = unpack('I', $keyLengthBytes)[1];
                $key = fread($f, $keyLength);
                $entryFlag = unpack('C', fread($f, 1))[1];
                if ($entryFlag) {
                    $entryData = fread($f, 16);
                    list($start, $length, $expires) = array_values(unpack('Qstart/Ilength/Iexpires', $entryData));
                    if ($expires == 0 || $expires > $now) {
                        $index[$key] = array(
                            'start' => $start,
                            'length' => $length,
                            'expires' => $expires
                        );
                    }
                } else {
                    if (isset($index[$key])) {
                        unset($index[$key]);
                    }
                }
            }
            fclose($f);
            unlink($this->walFile);
        }

        return $index;
    }

    private function appendToWalFile($key, $entry = null)
    {
        /**
         * Append an entry to the write-ahead log (WAL) file.
         */
        $buf = array();
        $keyBytes = $key;
        $keyLength = strlen($keyBytes);
        $buf[] = pack('I', $keyLength);
        $buf[] = $keyBytes;
        if ($entry === null) {
            $buf[] = pack('C', 0);
        } else {
            $buf[] = pack('C', 1);
            $buf[] = pack('QII', $entry['start'], $entry['length'], $entry['expires']);
        }

        fwrite($this->walFileFd, implode('', $buf));
        fflush($this->walFileFd);
    }

    private function saveIndex()
    {
        /**
         * Save the index to the index file and remove the WAL file.
         */
        $tmpIndexFile = $this->indexFile . '.tmp';
        $f = fopen($tmpIndexFile, 'wb');
        foreach ($this->index as $key => $entry) {
            $buf = array();
            $keyBytes = $key;
            $keyLength = strlen($keyBytes);
            $buf[] = pack('I', $keyLength);
            $buf[] = $keyBytes;
            $buf[] = pack('QII', $entry['start'], $entry['length'], $entry['expires']);
            fwrite($f, implode('', $buf));
        }
        fclose($f);
        rename($tmpIndexFile, $this->indexFile);
        if (file_exists($this->walFile)) {
            unlink($this->walFile);
        }
    }

    private function writeHeader()
    {
        /**
         * Write the header of the data file.
         */
        fwrite($this->dataFileAppendFd, self::HEADER_DATA_FILE);
    }

    private function getCompressedData($data)
    {
        /**
         * Compress data using zlib.
         */
        return gzcompress($data, 6);
    }

    private function appendFrameToDataFile($key, $expires, $isBytes, $data)
    {
        /**
         * Append data to the data file and return the start position and length
         * of the compressed data.
         */
        $buf = array();
        $buf[] = pack('C', $isBytes);
        $start = ftell($this->dataFileAppendFd);
        $compressedData = gzcompress($data, 6);
        $buf[] = pack('I', strlen($compressedData));
        $buf[] = $compressedData;
        fwrite($this->dataFileAppendFd, implode('', $buf));
        fflush($this->dataFileAppendFd);
        $end = ftell($this->dataFileAppendFd);
        return array($start, $end - $start);
    }

    private function readFrameFromDataFile($start)
    {
        /**
         * Read data frame from the data file starting at the given position.
         */
        fseek($this->dataFileReadFd, $start);
        $isBytes = unpack('C', fread($this->dataFileReadFd, 1))[1];
        $dataLength = unpack('I', fread($this->dataFileReadFd, 4))[1];
        $compressedData = fread($this->dataFileReadFd, $dataLength);
        $data = gzuncompress($compressedData);
        if ($isBytes == 0) {
            $data = json_decode($data, $this->decodeAsArray);
        }
        return $data;
    }

    public function set($key, $value, $ttl = null)
    {
        /**
         * Set a key in the cache. The value can be a string, set, dict, list,
         * int, float, bool or bytes.
         */
        if (!is_string($key)) {
            throw new Exception('Key must be a string');
        }

        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        if (is_bool($value) || is_array($value) || is_numeric($value)) {
            $data = json_encode($value);
            $isBytes = 0;
        } elseif (is_string($value)) {
            $data = $value;
            $isBytes = 1;
        } else {
            throw new Exception('Value must be bytes or JSON-serializable');
        }

        $expires = $ttl ? time() + $ttl : 0;
        list($start, $length) = $this->appendFrameToDataFile($key, $expires, $isBytes, $data);

        $entry = array(
            'start' => $start,
            'length' => $length,
            'expires' => $expires,
            'is_bytes' => $isBytes
        );
        $this->index[$key] = $entry;
        $this->appendToWalFile($key, $entry);

        $this->stats['sets'] += 1;
    }

    public function get($key, $refreshCallback = null, $newTtl = null)
    {
        /**
         * Get a key from the cache. If the key is expired, the `refreshCallback`
         * is called and its return value is stored in the cache with the new TTL.
         */
        if (!is_string($key)) {
            throw new Exception('Key must be a string');
        }

        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        if ($this->has($key)) {
            $this->stats['hits'] += 1;
            $entry = $this->index[$key];
            return $this->readFrameFromDataFile($entry['start']);
        }

        $this->stats['misses'] += 1;

        if ($refreshCallback) {
            $this->stats['refreshes'] += 1;
            $value = call_user_func($refreshCallback, $key);
            $this->set($key, $value, $newTtl);
            return $value;
        }

        throw new Exception("Key `{$key}` is not found or expired");
    }

    public function has($key)
    {
        /**
         * Check if a key is in the cache and it not expired.
         */
        if (!is_string($key)) {
            throw new Exception('Key must be a string');
        }

        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        if (!isset($this->index[$key]) || ($this->index[$key]['expires'] && time() > $this->index[$key]['expires'])) {
            return false;
        }
        return true;
    }

    public function keys() {
        return array_keys($this->index);
    }

    public function del($key)
    {
        return $this->delete($key);
    }

    public function delete($key)
    {
        /**
         * Delete a key from the cache.
         */
        if (!is_string($key)) {
            throw new Exception('Key must be a string');
        }

        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        if (isset($this->index[$key])) {
            $this->appendToWalFile($key, null);
            unset($this->index[$key]);
            $this->stats['deletes'] += 1;
        }
    }

    public function deleteStartsWith($key)
    {
        /**
         * Delete all keys from the cache that start with the given prefix.
         */
        $keys = array();
        foreach ($this->index as $k => $v) {
            if (strpos($k, $key) === 0) {
                $keys[] = $k;
            }
        }
        foreach ($keys as $k) {
            $this->delete($k);
        }
    }

    public function whenExpired($key, $relative = false)
    {
        /**
         * Return the expiration timestamp of a key. If `relative` is True,
         * return the relative time in seconds.
         */
        if (!is_string($key)) {
            throw new Exception('Key must be a string');
        }

        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        if (isset($this->index[$key])) {
            return $relative ? $this->index[$key]['expires'] - time() : $this->index[$key]['expires'];
        }
        throw new Exception("Key `{$key}` is not found");
    }

    public function getStats()
    {
        /**
         * Return the cache statistics.
         */
        $fragmentationRatio = $this->fragmentationRatio();
        $dataFileSize = ftell($this->dataFileAppendFd);
        $this->stats['fragmentation_ratio'] = $fragmentationRatio;
        $this->stats['total_keys'] = count($this->index);
        $this->stats['data_file_size_bytes'] = $dataFileSize;
        return $this->stats;
    }

    public function fragmentationRatio()
    {
        /**
         * Return the fragmentation ratio of the data file, meaning
         * the ratio of the data which is also in the index to the file size.
         * Higher value means more fragmented. 0.8 means only 20% is used for
         * data, the rest of the data file is old.
         */
        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        $sizeFile = ftell($this->dataFileAppendFd) - strlen(self::HEADER_DATA_FILE);
        if ($sizeFile == 0) {
            return 1;
        }

        $sizeIndex = 0;
        foreach ($this->index as $entry) {
            $sizeIndex += $entry['length'];
        }
        return 1 - ($sizeIndex / $sizeFile);
    }

    public function vacuum()
    {
        /**
         * Rebuild the data file to remove fragmentation by removing
         * the data which is not in the index.
         */
        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is closed');
        }

        $tmpDataFile = $this->dataFile . '.tmp';
        $newIndex = array();
        $newFile = fopen($tmpDataFile, 'wb');
        fwrite($newFile, self::HEADER_DATA_FILE);
        foreach ($this->index as $key => $entry) {
            fseek($this->dataFileReadFd, $entry['start']);
            $dataFrame = fread($this->dataFileReadFd, $entry['length']);
            $newStart = ftell($newFile);
            fwrite($newFile, $dataFrame);
            $newIndex[$key] = array(
                'start' => $newStart,
                'length' => strlen($dataFrame),
                'expires' => $entry['expires']
            );
        }
        fclose($newFile);
        rename($tmpDataFile, $this->dataFile);
        $this->index = $newIndex;
        $this->saveIndex();
    }

    public function close()
    {
        /**
         * Close the cache. Closes all files and saves the index.
         */
        if ($this->dataFileReadFd === null) {
            throw new Exception('Cache is already closed');
        }

        $stats = $this->getStats();
        if ($this->fragmentationRatio() > $this->autoVacuumThreshold) {
            $this->vacuum();
        }

        if ($this->dataFileReadFd) {
            fclose($this->dataFileReadFd);
            $this->dataFileReadFd = null;
        }

        if ($this->walFileFd) {
            fclose($this->walFileFd);
            $this->walFileFd = null;
        }

        if ($this->dataFileAppendFd) {
            $this->unlockFile($this->dataFileAppendFd);
            fclose($this->dataFileAppendFd);
            $this->dataFileAppendFd = null;
        }

        $this->saveIndex();
    }
}

