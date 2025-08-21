library(Matrix)

A <- matrix(c(
    25, -50,
    -50, 101
), nrow = 2, ncol = 2, byrow = TRUE)

B <- matrix(c(
    25, 15, -5, 
    15, 18, 0,
    -5, 0, 11
), nrow = 3, ncol = 3, byrow = TRUE)

C <- matrix(c(
    7, -2, -3, 0, -1, 0,
    -2, 8, 0, 0, -1, 0,
    -3, 0, 4, -1, 0, 0,
    0, 0, -1, 5, 0, -2,
    -1, -1, 0, 0, 4, 0,
    0, 0, 0, -2, 0, 6
), nrow = 6, ncol = 6, byrow = TRUE)

# A
cat(" Begin LU decomposition of matrix A:\n")
print(A)
cat(" LU decomposition of matrix A: \n")
LU_A_time <- system.time({
    LU_A <- lu(A)
})
print(expand(LU_A))
cat(" Time taken for LU decomposition of matrix A: ", LU_A_time[3], " seconds\n\n")

# B
cat(" Begin LU decomposition of matrix B:\n")
print(B)
cat(" LU decomposition of matrix B: \n")
LU_B_time <- system.time({
    LU_B <- lu(B)
})
print(expand(LU_B))
cat(" Time taken for LU decomposition of matrix B: ", LU_B_time[3], " seconds\n\n")

# C
cat(" Begin LU decomposition of matrix C:\n")
print(C)
cat(" LU decomposition of matrix C: \n")
LU_C_time <- system.time({
    LU_C <- lu(C)
})
print(expand(LU_C))
cat(" Time taken for LU decomposition of matrix C: ", LU_C_time[3], " seconds\n\n")
cat(" End of LU decomposition tests.\n")
