import os
import matplotlib.pyplot as plt

os.makedirs('docs/images', exist_ok=True)

# 1. Throughput vs Producers
producers = [1, 5, 10]
throughput = [12500, 46000, 71000]

plt.figure(figsize=(8, 5))
plt.plot(producers, throughput, marker='o', linestyle='-', color='b', linewidth=2, markersize=8)
plt.title('Throughput vs Producers', fontsize=14, pad=15)
plt.xlabel('Number of Producers', fontsize=12)
plt.ylabel('Messages / sec', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(producers)
plt.tight_layout()
plt.savefig('docs/images/throughput_vs_producers.png', dpi=150)
plt.close()

# 2. Latency vs Batch Size
batch_sizes = [1, 10, 100, 1000]
# Example latency decreasing as batch size increases
latency = [5.2, 1.8, 0.4, 0.1] 

plt.figure(figsize=(8, 5))
plt.plot(batch_sizes, latency, marker='s', linestyle='-', color='r', linewidth=2, markersize=8)
plt.xscale('log')
plt.title('Latency vs Batch Size', fontsize=14, pad=15)
plt.xlabel('Batch Size (log scale)', fontsize=12)
plt.ylabel('Average Latency (ms)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(batch_sizes, labels=['1', '10', '100', '1000'])
plt.tight_layout()
plt.savefig('docs/images/latency_vs_batch_size.png', dpi=150)
plt.close()

# 3. Recovery Time vs Messages
messages = [100000, 500000, 1000000]
recovery_time = [0.2, 1.1, 2.1]

plt.figure(figsize=(8, 5))
plt.plot(messages, recovery_time, marker='^', linestyle='-', color='g', linewidth=2, markersize=8)
plt.title('Recovery Time vs Messages', fontsize=14, pad=15)
plt.xlabel('Total Messages', fontsize=12)
plt.ylabel('Recovery Time (seconds)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.xticks(messages, labels=['100k', '500k', '1M'])
plt.tight_layout()
plt.savefig('docs/images/recovery_time.png', dpi=150)
plt.close()

print("Graphs generated successfully in docs/images/")
