import ollama
import time
from concurrent.futures import ThreadPoolExecutor


def test_single():
    response = ollama.chat(
        model="qwen2.5:3b",
        messages=[{"role": "user", "content": "說「好」"}],
        options={"num_predict": 5},
    )
    return response["message"]["content"]


# Test 1: Sequential
print("測試 1：串行執行 (5 次)")
start = time.time()
for i in range(5):
    test_single()
sequential_time = time.time() - start
print(f"  時間: {sequential_time:.2f} 秒")
print(f"  平均: {sequential_time / 5:.2f} 秒/次")

# Test 2: Parallel
print("\n測試 2：並行執行 (5 次, 4 workers)")
start = time.time()
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(test_single) for _ in range(5)]
    results = [f.result() for f in futures]
parallel_time = time.time() - start
print(f"  時間: {parallel_time:.2f} 秒")
print(f"  平均: {parallel_time / 5:.2f} 秒/次")

# Analysis
speedup = sequential_time / parallel_time
print("\n結果：")
print(f"  加速比: {speedup:.2f}x")
if speedup < 1.5:
    print("  ❌ 並行效果不佳（Ollama 可能不支援真並行）")
else:
    print("  ✅ 並行有效果")
