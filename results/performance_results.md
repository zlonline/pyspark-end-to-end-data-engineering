# Week 12 Day 5 - Performance Results

## Benchmark Scope

Clean Silver -> JOIN -> Gold Aggregations

## Execution Time

| Metric | Value |
|---|---:|
| Baseline Median | 5.472 sec |
| Optimized Median | 4.440 sec |
| Improvement | 18.86% |
| Speedup | 1.23x |

## Optimization Changes

- Broadcast Customers and Products
- Cache Sales Detail for repeated reuse
- Tune output partition counts
- Keep Snappy compression consistent

## Correctness

Baseline and Optimized outputs validated
using bidirectional exceptAll.

## Spark Metrics

| Application | Tasks | Shuffle Read MB | Shuffle Write MB | Disk Spill MB |
|---|---:|---:|---:|---:|

## Output Files

| Pipeline | Run | Dataset | Partitions | Files | Size MB |
|---|---:|---|---:|---:|---:|
| baseline | 1 | customer_sales | 1 | 1 | 0.395 |
| baseline | 1 | product_sales | 1 | 1 | 0.041 |
| baseline | 1 | daily_sales | 1 | 1 | 0.01 |
| optimized | 1 | customer_sales | 1 | 1 | 0.394 |
| optimized | 1 | product_sales | 1 | 1 | 0.041 |
| optimized | 1 | daily_sales | 1 | 1 | 0.01 |
| optimized | 2 | customer_sales | 1 | 1 | 0.394 |
| optimized | 2 | product_sales | 1 | 1 | 0.041 |
| optimized | 2 | daily_sales | 1 | 1 | 0.01 |
| baseline | 2 | customer_sales | 1 | 1 | 0.395 |
| baseline | 2 | product_sales | 1 | 1 | 0.041 |
| baseline | 2 | daily_sales | 1 | 1 | 0.01 |
| baseline | 3 | customer_sales | 1 | 1 | 0.395 |
| baseline | 3 | product_sales | 1 | 1 | 0.041 |
| baseline | 3 | daily_sales | 1 | 1 | 0.01 |
| optimized | 3 | customer_sales | 1 | 1 | 0.394 |
| optimized | 3 | product_sales | 1 | 1 | 0.041 |
| optimized | 3 | daily_sales | 1 | 1 | 0.01 |

## Observations

- Compare Spark UI SQL and Stages tabs.
- Identify Exchange and BroadcastHashJoin.
- Review task skew and spill metrics.
- Note that cache may add overhead.
- Avoid assuming every optimization is faster.

## Conclusion

Add observations from actual Spark UI metrics.