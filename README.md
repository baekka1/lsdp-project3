# Project 3 Report: Rafael Singer and Katie Baek

## Part 1: MIS Validation Results
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

## Part 2: Algorithm Performance
|        Graph file       | Number of Iterations | Runtime (s) | Is a Valid MIS? |
| ----------------------- | -------------------- | ----------- | --------------- |
| small_edges.csv         | 2                    | 3           | Yes             |
| line_100_edges.csv      | 2                    | 4           | Yes             |
| twitter_100_edges.csv   | 2                    | 4           | Yes             |
| twitter_1000_edges.csv  | 3                    | 5           | Yes             |
| twitter_10000_edges.csv | 3                    | 6           | Yes             |

## Part 3: Performance Analysis with Different Configurations

### Configuration: 4x2
| Iteration | Time (s) | Active Vertices |
|-----------|----------|----------------|
| 1         | 31       | 7,056,710      |
| 2         | 29       | 31,558         |
| 3         | 30       | 418            |
| 4         | 33       | 1              |
| 5         | 30       | 0              |

**Total:** 5 iterations, 156.191 seconds, Valid MIS: true

### Configuration: 3x4
| Iteration | Time (s) | Active Vertices |
|-----------|----------|----------------|
| 1         | 15       | 6,881,060      |
| 2         | 11       | 39,734         |
| 3         | 9        | 567            |
| 4         | 10       | 1              |
| 5         | 10       | 0              |

**Total:** 5 iterations, 57.183 seconds, Valid MIS: true

### Configuration: 2x2
| Iteration | Time (s) | Active Vertices |
|-----------|----------|----------------|
| 1         | 41       | 6,971,905      |
| 2         | 31       | 33,369         |
| 3         | 31       | 404            |
| 4         | 30       | 2              |
| 5         | 31       | 0              |

**Total:** 5 iterations, 166.314 seconds, Valid MIS: true

### Performance Analysis
While the 2x2 and 4x2 configurations had similar runtime (166.314s and 156.191s respectively), the 3x4 configuration was significantly faster both per iteration and in total (57.183s). This suggests that the 3x4 configuration provided the most efficient parallelization for this particular workload.