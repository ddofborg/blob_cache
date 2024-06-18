import random
import string
import time
from blob_cache import BlobCache
from separate_file_cache import SeparateFileCache
from berkleydb_cache import BerkleyDbCache
from rocksdicts_cache import RocksdictCache
from lmdb_cache import LmdbCache

# Generate random data
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def alphanumeric_string(length=10):
    alphanumeric_characters = string.ascii_lowercase + string.ascii_uppercase + string.digits
    total_characters = len(alphanumeric_characters)
    return ''.join(alphanumeric_characters[i % total_characters] for i in range(length))

def benchmark_all(num_entries, entry_size):
    print(f'## Generating random data for {num_entries} entries, {entry_size} bytes each')
    data = { f'{c:06}': random_string(entry_size) for c,_ in enumerate(range(num_entries)) }

    def do_benchmark_class(cache_class):
        # Initialize cache
        cache = cache_class('tmp_benchmark_cache')

        # Measure set time
        start_time = time.time()
        for key, value in data.items():
            a=cache.set(key, value)
        set_time = time.time() - start_time

        # Measure get time
        start_time = time.time()
        for key in data.keys():
            a=cache.get(key)
        get_time = time.time() - start_time

        # # Measure delete time
        # start_time = time.time()
        # for key in data.keys():
        #     cache.delete(key)
        # delete_time = time.time() - start_time
        delete_time = 0

        # Cleanup
        if isinstance(cache, BlobCache):
            cache.close()

        # if os.path.exists('benchmark_cache'):
        #     for file in os.listdir('benchmark_cache'):
        #         file_path = os.path.join('benchmark_cache', file)
        #         if os.path.isfile(file_path):
        #             os.remove(file_path)
        #     os.rmdir('benchmark_cache')

        return f'{set_time:.4f}', f'{get_time:.4f}', f'{delete_time:.4f}'

    benchmark_classes = [LmdbCache, BerkleyDbCache, RocksdictCache, BlobCache, SeparateFileCache]

    print(f'## Runnning test for: {[benchmark_class.__name__ for benchmark_class in benchmark_classes]}')
    print()
    for benchmark_class in benchmark_classes:
        stats = do_benchmark_class(benchmark_class)
        print(f'    {benchmark_class.__name__:<16} - Set: {stats[0]}s, Get: {stats[1]}s, Delete: {stats[2]}s')
    print()
    print()

if __name__ == '__main__':
    import blob_cache
    import logging
    blob_cache.LOG.setLevel(logging.ERROR)

    benchmark_all(100_000, 5_000)

