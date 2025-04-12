# Project 3 Report: Rafael Singer and Katie Baek
## Part 1
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

## Part 2
|        Graph file       | Number of Iterations | Runtime (s) | Is a Valid MIS? |
| ----------------------- | -------------------- | ----------- | --------------- |
| small_edges.csv         | 2                    | 3           | Yes             |
| line_100_edges.csv      | 2                    | 4           | Yes             |
| twitter_100_edges.csv   | 2                    | 4           | Yes             |
| twitter_1000_edges.csv  | 3                    | 5           | Yes             |
| twitter_10000_edges.csv | 3                    | 6           | Yes             |

## Part 3
4x2:
Iteration: 1, Time: 31, Active Vertices: 7056710
Iteration: 2, Time: 29, Active Vertices: 31558
Iteration: 3, Time: 30, Active Vertices: 418
Iteration: 4, Time: 33, Active Vertices: 1
Iteration: 5, Time: 30, Active Vertices: 0
Total Iterations: 5, Time: 156.191, isValid: true
3x4:
Iteration: 1, Time: 15, Active Vertices: 6881060
Iteration: 2, Time: 11, Active Vertices: 39734
Iteration: 3, Time: 9, Active Vertices: 567
Iteration: 4, Time: 10, Active Vertices: 1
Iteration: 5, Time: 10, Active Vertices: 0
Total Iterations: 5, Time: 57.183, isValid: true
2x2:
Iteration: 1, Time: 41, Active Vertices: 6971905
Iteration: 2, Time: 31, Active Vertices: 33369
Iteration: 3, Time: 31, Active Vertices: 404
Iteration: 4, Time: 30, Active Vertices: 2
Iteration: 5, Time: 31, Active Vertices: 0
Total Iterations: 5, Time: 166.314, isValid: true
While the 2x2 and 4x2 trials had similar runtime, the 3x4 trial was significantly faster both per iteration and in total.