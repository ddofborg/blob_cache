import threading
import random
from blob_cache_dict import BlobCacheDict
import time

def stress_test_thread(cache, thread_id, iterations=10000):
    """Run set/get operations in a thread"""
    try:
        for i in range(iterations):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}"
            
            # Set operation
            cache[key] = value
            
            # Get and verify operation
            retrieved = cache[key]
            if retrieved != value:
                print("\n")
                print(f"Thread {thread_id}: Data integrity error at iteration {i}")
                print(f"  Key: {key}")
                print(f"  Expected value: {value}")
                print(f"  Retrieved value: {retrieved}")
                print(f"  Value type: {type(retrieved)}")
                print(f"  Length of retrieved: {len(retrieved) if hasattr(retrieved, '__len__') else 'N/A'}")
                try:
                    print(f"  First 100 chars of retrieved: {str(retrieved)[:100]}")
                except:
                    print("  Could not print retrieved value")
                raise ValueError(f"Data integrity error for key {key}")
            
            # Random delete to create more contention
            if random.random() < 0.1:  # 10% chance to delete
                del cache[key]
            
            if i % 1000 == 0:
                print(f"Thread {thread_id}: {i} iterations completed")
    except Exception as e:
        print(f"Thread {thread_id} failed: {str(e)}")
        
def run_thread_safety_test():
    cache = BlobCacheDict('tmp_thread_test_cache')
    threads = []
    num_threads = 5
    
    start_time = time.time()
    
    # Create and start threads
    for i in range(num_threads):
        t = threading.Thread(target=stress_test_thread, args=(cache, i))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nTest completed in {duration:.2f} seconds")
    print(f"Final cache size: {len(cache)}")
    print(f"Fragmentation ratio: {cache.fragmentation_ratio():.2f}")
    
    cache.close()

if __name__ == "__main__":
    print("Starting thread safety test...")
    run_thread_safety_test()

