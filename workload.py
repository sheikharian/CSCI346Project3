from rediscluster import RedisCluster
import threading
import time
import random

# Redis cluster IPs and ports
startup_nodes = [
    {"host": "172.31.94.177", "port": "6379"},
    {"host": "172.31.94.40", "port": "6379"},
    {"host": "172.31.88.136", "port": "6379"},
    {"host": "172.31.92.232", "port": "6379"},
    {"host": "172.31.94.244", "port": "6379"},
    {"host": "172.31.81.134", "port": "6379"},
]

def redis_worker(thread_id, num_ops):
    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    start = time.time()
    for i in range(num_ops):
        key = f"key:{thread_id}:{i}"
        value = random.randint(1, 1000)
        rc.set(key, value)
        rc.get(key)
        time.sleep(0.01) # 0.01 sec pause
    end = time.time()
    duration = end - start
    print(f"[Thread-{thread_id}] {num_ops} ops in {duration:.4f}s | Avg latency: {duration / num_ops:.6f}s/op")

def main():
    num_threads = 5
    ops_per_thread = 5000 # increased ops for slowdown
    threads = []

    print("Starting workload...")
    start_all = time.time()

    for i in range(num_threads):
        t = threading.Thread(target=redis_worker, args=(i, ops_per_thread))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_duration = time.time() - start_all
    # print results
    print(f"\nTotal time for {num_threads * ops_per_thread} ops: {total_duration:.2f} seconds")
    print(f"Overall throughput: {num_threads * ops_per_thread / total_duration:.2f} ops/sec")

if __name__ == "__main__":
    main()
