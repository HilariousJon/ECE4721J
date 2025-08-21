# ECE4721 lab 7

## LU decomposition results

- Run `make run_LU`

<details> <summary> program outputs </summary>

```shell
(ve472) [hadoopuser@Nuvole src]$ make run_LU
Rscript --vanilla --slave \
--no-save --no-restore \
LU.r
 Begin LU decomposition of matrix A:
     [,1] [,2]
[1,]   25  -50
[2,]  -50  101
 LU decomposition of matrix A:
$L
2 x 2 Matrix of class "dtrMatrix" (unitriangular)
     [,1] [,2]
[1,]  1.0    .
[2,] -0.5  1.0

$U
2 x 2 Matrix of class "dtrMatrix"
     [,1]  [,2]
[1,] -50.0 101.0
[2,]     .   0.5

$P
2 x 2 sparse Matrix of class "pMatrix"

[1,] . |
[2,] | .

 Time taken for LU decomposition of matrix A:  0  seconds

 Begin LU decomposition of matrix B:
     [,1] [,2] [,3]
[1,]   25   15   -5
[2,]   15   18    0
[3,]   -5    0   11
 LU decomposition of matrix B:
$L
3 x 3 Matrix of class "dtrMatrix" (unitriangular)
     [,1]       [,2]       [,3]
[1,]  1.0000000          .          .
[2,]  0.6000000  1.0000000          .
[3,] -0.2000000  0.3333333  1.0000000

$U
3 x 3 Matrix of class "dtrMatrix"
     [,1] [,2] [,3]
[1,]   25   15   -5
[2,]    .    9    3
[3,]    .    .    9

$P
3 x 3 sparse Matrix of class "pMatrix"

[1,] | . .
[2,] . | .
[3,] . . |

 Time taken for LU decomposition of matrix B:  0  seconds

 Begin LU decomposition of matrix C:
     [,1] [,2] [,3] [,4] [,5] [,6]
[1,]    7   -2   -3    0   -1    0
[2,]   -2    8    0    0   -1    0
[3,]   -3    0    4   -1    0    0
[4,]    0    0   -1    5    0   -2
[5,]   -1   -1    0    0    4    0
[6,]    0    0    0   -2    0    6
 LU decomposition of matrix C:
$L
6 x 6 Matrix of class "dtrMatrix" (unitriangular)
     [,1]       [,2]       [,3]       [,4]       [,5]       [,6]
[1,]  1.0000000          .          .          .          .          .
[2,] -0.2857143  1.0000000          .          .          .          .
[3,] -0.4285714 -0.1153846  1.0000000          .          .          .
[4,]  0.0000000  0.0000000 -0.3823529  1.0000000          .          .
[5,] -0.1428571 -0.1730769 -0.2205882 -0.0477707  1.0000000          .
[6,]  0.0000000  0.0000000  0.0000000 -0.4331210 -0.0273224  1.0000000

$U
6 x 6 Matrix of class "dtrMatrix"
     [,1]       [,2]       [,3]       [,4]       [,5]       [,6]
[1,]  7.0000000 -2.0000000 -3.0000000  0.0000000 -1.0000000  0.0000000
[2,]          .  7.4285714 -0.8571429  0.0000000 -1.2857143  0.0000000
[3,]          .          .  2.6153846 -1.0000000 -0.5769231  0.0000000
[4,]          .          .          .  4.6176471 -0.2205882 -2.0000000
[5,]          .          .          .          .  3.4968153 -0.0955414
[6,]          .          .          .          .          .  5.1311475

$P
6 x 6 sparse Matrix of class "pMatrix"

[1,] | . . . . .
[2,] . | . . . .
[3,] . . | . . .
[4,] . . . | . .
[5,] . . . . | .
[6,] . . . . . |

 Time taken for LU decomposition of matrix C:  0  seconds

 End of LU decomposition tests.
```

</details>

- runtime

```shell
 Time taken for LU decomposition of matrix A:  0  seconds
 Time taken for LU decomposition of matrix B:  0  seconds
 Time taken for LU decomposition of matrix C:  0  seconds
```

## Cholesky decomposition results

- Run `make run_cholesky`

<details> <summary> program outputs </summary>

```shell
2025-07-27 15:57:38.731 | INFO     | __main__:<module>:103 - Processing matrix M1 with size 2x2 using distributed algorithm.
2025-07-27 15:57:41.119 | INFO     | __main__:<module>:119 - Cholesky factor L for matrix M1:
[[  5.   0.]
 [-10.   1.]]
2025-07-27 15:57:41.120 | INFO     | __main__:<module>:122 - Reconstructed A from L for matrix M1:
[[ 25. -50.]
 [-50. 101.]]
2025-07-27 15:57:41.121 | SUCCESS  | __main__:<module>:126 - Validation successful for M1!
2025-07-27 15:57:41.121 | INFO     | __main__:<module>:103 - Processing matrix M2 with size 3x3 using distributed algorithm.
2025-07-27 15:57:43.916 | INFO     | __main__:<module>:119 - Cholesky factor L for matrix M2:
[[ 5.  0.  0.]
 [ 3.  3.  0.]
 [-1.  1.  3.]]
2025-07-27 15:57:43.916 | INFO     | __main__:<module>:122 - Reconstructed A from L for matrix M2:
[[25. 15. -5.]
 [15. 18.  0.]
 [-5.  0. 11.]]
2025-07-27 15:57:43.917 | SUCCESS  | __main__:<module>:126 - Validation successful for M2!
2025-07-27 15:57:43.917 | INFO     | __main__:<module>:103 - Processing matrix M3 with size 6x6 using distributed algorithm.
2025-07-27 15:57:55.145 | INFO     | __main__:<module>:119 - Cholesky factor L for matrix M3:
[[ 2.6458  0.      0.      0.      0.      0.    ]
 [-0.7559  2.7255  0.      0.      0.      0.    ]
 [-1.1339 -0.3145  1.6172  0.      0.      0.    ]
 [ 0.      0.     -0.6183  2.1489  0.      0.    ]
 [-0.378  -0.4717 -0.3567 -0.1027  1.87    0.    ]
 [ 0.      0.      0.     -0.9307 -0.0511  2.2652]]
2025-07-27 15:57:55.146 | INFO     | __main__:<module>:122 - Reconstructed A from L for matrix M3:
[[ 7. -2. -3.  0. -1.  0.]
 [-2.  8. -0.  0. -1.  0.]
 [-3. -0.  4. -1.  0.  0.]
 [ 0.  0. -1.  5.  0. -2.]
 [-1. -1.  0.  0.  4. -0.]
 [ 0.  0.  0. -2. -0.  6.]]
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:126 - Validation successful for M3!
2025-07-27 15:57:55.146 | INFO     | __main__:<module>:128 - ----------------------------------------
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:130 - Distributed Cholesky for M1 took 2.2213 seconds
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:130 - Distributed Cholesky for M2 took 2.7931 seconds
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:130 - Distributed Cholesky for M3 took 11.2264 seconds
```

</details>

- runtime

```shell
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:130 - Distributed Cholesky for M1 took 2.2213 seconds
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:130 - Distributed Cholesky for M2 took 2.7931 seconds
2025-07-27 15:57:55.146 | SUCCESS  | __main__:<module>:130 - Distributed Cholesky for M3 took 11.2264 seconds
```

## Summary

- Why in fact we see that the R program run faster than the spark program.

1. the spark program in distributed mode is way heavier than the R program, if the dataset gets larger, the advantage of our program will show.
2. R program API `lu` actually works from `C` and `Fortran` code, it directly operates on the memory, thus way faster.
3. Spark program also have serilaized and RPC network bandwith cost, resulting a way way slower program runtime.
