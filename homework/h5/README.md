# ECE 4721 Homework 5

**NOTE:** MATLAB code is not really convinient to run on linux, so we'll utilize Octave in this homework.

## Ex. 1

1; In the big data context, how beneficial would it be to increase precision? For instance what would be the gain of using double instead of float, or move on with multi-precision?

### Potential Benefits of Increasing Precision

1. **Reduced Numerical Error**
   - **Float (32-bit)** has \~7 decimal digits of precision.
   - **Double (64-bit)** has \~15-16 digits.
   - For large datasets or computations that involve **cumulative summation**, **small differences**, or **ill-conditioned numerical algorithms**, higher precision **reduces rounding errors**, which is important in:
     - Scientific simulations
     - Financial calculations
     - Machine learning (especially with gradients)
     - Large matrix operations (e.g., in linear algebra or PCA)
2. **Improved Algorithmic Stability**
   - Algorithms like iterative solvers (e.g., gradient descent) or numerical optimization are **more stable** and **converge better** with higher precision.
   - Lower precision might lead to **catastrophic cancellation**, **divergence**, or **inaccurate results**.
3. **Better Reproducibility**
   - More precision reduces sensitivity to hardware (CPU vs GPU), thread scheduling, or small differences in input data, making results more consistent across platforms.

### Trade-Offs and Costs

1. **Increased Storage and Memory Usage**
   - A `double` is twice as large as a `float`. Multi-precision (e.g., 128-bit) can be even more costly.
   - This impacts:
     - RAM usage
     - Disk I/O
     - Cache efficiency
     - Network bandwidth
2. **Slower Computations**
   - Operations on `double` are typically slower than `float`, and **multi-precision** arithmetic can be **orders of magnitude slower**, especially in languages like Python or Java without native support.
3. **Diminishing Returns**
   - In many big data applications like **log analysis**, **clickstream data**, or **simple aggregations**, `float` is already good enough.
   - Extra precision does **not meaningfully improve accuracy**, but **costs more**.

2; Generate 100 random $1000\times 100$ matrices X and measure the total time needed for MATLAB to compute results.

<details> <summary>Source code</summary>

```MATLAB
% set experimental params
num_trials = 100;
rows = 1000;
cols = 100;

% initialize timers
time_svd_X = 0;
time_svd_XT = 0;
time_eig_XXT = 0;
time_eig_XTX = 0;

for i = 1:num_trials
    X = rand(rows, cols);

    % (b) SVD of X'
    tic;
    [~, ~, ~] = svd(X', 'econ');
    time_svd_XT += toc;

    % (a) SVD of X
    tic;
    [~, ~, ~] = svd(X, 'econ');
    time_svd_X += toc;


    % (c) Eigenvalues of X * X'
    tic;
    eig(X * X');
    time_eig_XXT += toc;

    % (d) Eigenvalues of X' * X
    tic;
    eig(X' * X);
    time_eig_XTX += toc;
end

% output total time
fprintf("Total time for SVD(X): %.4f seconds\n", time_svd_X);
fprintf("Total time for SVD(X'): %.4f seconds\n", time_svd_XT);
fprintf("Total time for eig(X*X'): %.4f seconds\n", time_eig_XXT);
fprintf("Total time for eig(X'*X): %.4f seconds\n", time_eig_XTX);
```

</details>

- The following are the results of the scripts:

```shell
$ make ex1
octave --no-gui --eval "run('ex1_2.m')"
Total time for SVD(X): 1.2675 seconds
Total time for SVD(X'): 1.0232 seconds
Total time for eig(X*X'): 18.3067 seconds
Total time for eig(X'*X): 0.3317 seconds
```

3; study the variations of eigenvalues and singular values for 1000 tests

<details> <summary>Source code</summary>

```MATLAB
% set the params
num_trials = 1000;
X = [-9    11   -21    63   -252;
      70  -69   141  -421   1684;
    -575  575 -1149  3451 -13801;
    3891 -3891 7782 -23345 93365;
    1024 -1024 2048  -6144 24572];

% eps as machine precuision
perturbation_scale = eps;

% initialize matrices to store results
eig_vals = zeros(size(X, 1), num_trials);
sing_vals = zeros(min(size(X)), num_trials);

for i = 1:num_trials
    % given small perturabation as random noise
    delta_X = perturbation_scale * randn(size(X));

    % calculate the perturbed matrix
    X_perturbed = X + delta_X;

    % a) calculate the eigenvalues
    eig_vals(:, i) = sort(eig(X_perturbed));

    % b) calculate the singular values
    sing_vals(:, i) = sort(svd(X_perturbed));
end

eig_std = std(eig_vals, 0, 2);
sing_std = std(sing_vals, 0, 2);

% print the results
disp("Standard deviation of eigenvalues (per component):");
disp(eig_std);

disp("Standard deviation of singular values (per component):");
disp(sing_std);
```

</details>

- The results as follows:

```shell
$ make ex1
# octave --no-gui --eval "run('ex1_2.m')"
octave --no-gui --eval "run('ex1_3.m')"
Standard deviation of eigenvalues (per component):
   5.1374e-16
   4.4126e-16
   4.4126e-16
   8.2155e-16
   8.2155e-16
Standard deviation of singular values (per component):
            0
   1.3774e-14
   2.8214e-14
   4.2654e-14
   1.6889e-09
```

- As we can observe, the overall variations of the eigenvalues are in general smaller, while the variations of the singular values are much bigger in this sense, indicating that the eigenvalues are in general can be considered as more stable choices
- It is also worth noting that the first singular values' variations is 0, considering in this sense, the singular values is more stable if we want to perform PCA and make the dimension in a small ways
- However we should also noted that the matrix is a small one, for large matrix the size of the variations may be different from current conclusions

4; In light of the lectures content, explain your observations.

- In the lecture, we learnt that when running PCA
  - using eigenvalues:
    - fast
    - a bit unstable
  - using singular values:
    - a bit slow
    - more stable
- So in this case we look back to the experiments, we observe that:
  - Ex 1.2
    - $X X^{T}$ is in size $1000 \times 1000$, so it is harder to calculate its eigenvalues
    - $X^{T} X$ is in size $100 \times 100$, so it is easier to calculate its eigenvalues
      - $X^{T} X$ and $X X^{T}$ have the same non-zero eigenvalues, so when we perform SVD in PCA, we ll usually calculate $X^{T} X$ in order to boost the calculations
    - We notice that indeed the calculations of SVD is much slower than the calculations of eigenvalues, and SVD is much more stable than eigenvalues
    - For doing PCA
      - if we want for fast runtime and the matrix is not large, we can consider the covariance matrix and calculate its eigenvalues
      - if the data is large or contains noises, it is recommendeed to use SVD, which is stable

## Ex. 2

Proof. Given the matrix:

$$
X =
\begin{pmatrix}
1 & 2 & 3 & 4 \\
5 & 6 & 7 & 8 \\
9 & 0 & 1 & 2 \\
\end{pmatrix}
$$

Its singular value is the square root of the eigenvalues of $X^{T} X$ or $X X^{T}$. We'll then calculate $X X^{T}$, which is in smaller size. We then have that:

$$
\begin{aligned}
X X^{T} & =
\begin{pmatrix}
1 & 2 & 3 & 4 \\
5 & 6 & 7 & 8 \\
9 & 0 & 1 & 2 \\
\end{pmatrix}
\cdot
\begin{pmatrix}
1 & 5 & 9 \\
2 & 6 & 0 \\
3 & 7 & 1 \\
4 & 8 & 2 \\
\end{pmatrix} \\
& =
\begin{pmatrix}
30 & 70 & 20 \\
70 & 174 & 68 \\
20 & 68 & 86 \\
\end{pmatrix}
\end{aligned}
$$

We then calculate the following to get its characteristic polynomial, and solve the following equation:

$$
\det(XX^{T} - \lambda \cdot Id)
=
\det (
\begin{pmatrix}
30 & 70 & 20 \\
70 & 174 & 68 \\
20 & 68 & 86 \\
\end{pmatrix}
-
\begin{pmatrix}
\lambda & 0 & 0 \\
0 & \lambda & 0 \\
0 & 0 & \lambda \\
\end{pmatrix}
)
= 0
$$

and then we have:

$$
- \lambda^3 + 290 \lambda^2 - 12840 \lambda + 9600 = 0
$$

and the eigenvalues are given by:

$$
\lambda_1 \approx 0.76069\dots ,\lambda_2 \approx 53.54348\dots ,\lambda_3 \approx 235.69581\dots
$$

which are the singular values of the original matrix $X$.

## Ex. 3

- put the data `cinema_sensors.tar.gz` in a folder named `data` in the same directory as the `README.md`, and run `make extract_data` to extract the data out.

1; Explain how PCA can be of any help to Krystor?

- PCA has following features and reasons to support Krystor in good ways
  - **Dimensionality Reduction:** The dataset has 1001 sensor readings, which means 1000 features (if the last column y is the target). Analyzing and modeling such high-dimensional data is complex and computationally expensive. PCA can reduce this high number of sensor variables into a much smaller number of new variables, called principal components, while preserving most of the important information (variance) in the original data
  - **Feature Engineering and Decorrelation:** The original sensor readings may be highly correlated with each other (e.g., sensors in the same area might give similar readings). This multicollinearity can be problematic for building regression models. PCA transforms the correlated sensor variables into a set of uncorrelated principal components. These components can then be used as predictors in the linear model (as requested in question 3), leading to a more stable and interpretable model
  - **Noise Reduction:** In any sensor network, some of the measured variation is just random noise. PCA helps to separate the signal from the noise. The first few principal components capture the largest variance and are most likely to represent the true underlying patterns in the electrical consumption. The last components, which capture very little variance, are often representative of noise and can be discarded
  - **Pattern Discovery:** By examining the "loadings" of the principal components (i.e., the coefficients that define how each original sensor contributes to each component), Krystor could potentially uncover the underlying structure of the sensor network. For example, he might find that a specific group of sensors consistently contributes to the most important principal component, suggesting they are monitoring a critical part of the cinema's electrical circuit

2; How many columns of sensors1.csv are necessary to explain 90% of the data? Let n be that number.

```shell
The number of principal components needed to explain 90% of the variance is: n = 3
These 3 components explain 98.36% of the variance.
```

3; Construct the linear model.

```shell
Linear model trained successfully on sensors1.csv data.
Model Intercept (x_0): 761.0112442000003
Model Coefficients (beta_i) for first 5 components: [-8.27120796 -2.99345011  4.19853802]
Training Data (sensors1.csv) - R-squared: 0.9960
```

4; Help Krystor determine whether sensors2.csv also contains the output of the sensors in the electric circuit of Reapor Rich's new cinema.

```shell
Applied the trained pipeline to sensors2.csv data.
+-------+-----------------+
|  label|       prediction|
+-------+-----------------+
|607.245|633.2521106950521|
| 870.87|873.8424972443873|
|842.558|828.1220025659949|
|544.681|534.8221577117588|
|968.542|973.6606429902274|
+-------+-----------------+
only showing top 5 rows

Test Data (sensors2.csv) - R-squared: 0.9946
Test Data (sensors2.csv) - RMSE: 12.7611

--- Conclusion ---
- The model's performance on sensors2.csv is strong and comparable to its performance on the training data.
- This is strong evidence that 'sensors2.csv' VERY LIKELY contains output from the same sensor circuit.
```
