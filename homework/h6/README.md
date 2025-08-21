# ECE4721 Homework 6

## Ex.1

### 1; (a): Recall the definition of the Jacobian and Hessian of a multi-variate function.

#### Definition of Jacobian.

Given: $f: \mathbb R^n \to \mathbb R^m$, we have that:

$$
\begin{aligned}
f(x) &= 
\begin{bmatrix}
f_1(x_1, ..., x_n) \\
\vdots \\
f_m(x_1, ..., x_n) \\
\end{bmatrix}  \in \mathbb R^m \\
x &= 
\begin{bmatrix}
x_1 \\
\vdots \\
x_n
\end{bmatrix}
\in \mathbb R^n
\end{aligned}
$$

the Jacobian of $f$ is given by:

$$
\nabla f = J_f(x) = 
\begin{bmatrix}
\frac{\partial f_1}{\partial x_1} & \ldots & \frac{\partial f_1}{\partial x_n} \\
\vdots & \ddots & \vdots \\
\frac{\partial f_m}{\partial x_1} & \ldots & \frac{\partial f_m}{\partial x_n} \\
\end{bmatrix}
$$

#### Definition of Hessian.

Given $f : \mathbb R^n \to \mathbb R$, the Hessian of f is given by:

$$
\nabla^2 f = H_f(x) =
\begin{bmatrix}
\frac{\partial^2 f}{\partial x_1^2} & \ldots & \frac{\partial^2 f}{\partial x_1 \partial x_n} \\
\vdots & \ddots & \vdots \\
\frac{\partial^2 f}{\partial x_n \partial x_1} & \ldots & \frac{\partial^2 f}{\partial x_n^2}
\end{bmatrix}
$$

with

$$
\frac{\partial^2 f}{\partial x_i \partial x_j} = \frac{\partial^2 f}{\partial x_j \partial x_i}
$$

So we see that $\nabla^2 f$ is actually a **symmetric matrix**.

### 1;(b): Newton direction deduction

This is actually a optimization problem, we want to have:

$$
min f(x) = min (f(x_0) + (\nabla f(x_0))^{\intercal}(x-x_0)+ \frac{1}{2}(x-x_0)^{\intercal}\nabla^2f(x_0)(x-x_0))
$$

Applying gradient to both sides and let it equal to 0, we have:

$$
\nabla f(x_0) + \nabla^2f(x_0)(x-x_0) = 0
$$

which yields the **Newton Direction** as:

$$
x - x_0 = -(\nabla^2 f(x_0))^{-1} \nabla f(x_0)
$$

### 2;(a) Explain how the above strategy can be applied to gradient descent.

This gives us a good idea on how to do iteration on Gradient Descent, for each iteration, we consider the following equation:

$$
x_{k+1} = x_k - \alpha (\nabla^2 f(x_k))^{-1}\nabla f(x_k)
$$

where $\alpha$ is the steps for each iteration of Gradient Descent, usually, it is taken as $1$, so the iteration becomes:

$$
x_{k+1} = x_k - (\nabla^2 f(x_k))^{-1}\nabla f(x_k)
$$

### 2;(b) Considering the cost of inverting the Hessian, and the adjusted step-size in this version of gradient descent, discuss the speed up game provided by this approach

- Consider the size of the Hessian as $n \times n$, then the time complexity to compute the invert of this Hessian shall be $O(n^3)$, which is the major time cost of this algorithm.
- This kind of gradient descent compare to the traditional one, import the information of Hessian and thus converge more quickly (less times of iterations)
- Since the algorithm is hard to step into the local minimum, we can even directly set the step size as $1$, as I describe the default as above. i.e. the direction is "super optiomal".

### 3;(a) Determine the Jacobian of $f$ in term of $r$

Consider that:

$$
f(w) = \frac{1}{2} (r(w))^{\intercal} r(w)
$$

we have that:

$$
\begin{aligned}
\nabla f(w) &= \nabla \frac{1}{2} (r(w))^{\intercal} r(w) \\
&= \frac{1}{2} \nabla \sum_{i=1}^{m} r_i^2(w) \\
&= \sum_{i=1}^{m} \frac{\partial r_i}{\partial w} r_i \\
\end{aligned}
$$

Consider that if we express $r$ interms of $r(w) = Xw - y$, it can also be expressed as:

$$
\begin{aligned}
\nabla f (w) &= \sum_{i=1}^{n} \frac{\partial r_i}{\partial w} r_i \\
&= X^{\intercal} r(w) \\
&= J_{r}^{\intercal} r(w) \\
\end{aligned}
$$

### 3;(b) Express the Hessian of $f$ in term of its Jacobian

Consider that:

$$
\begin{aligned}
\nabla^2f(w) &= \nabla (\nabla f(w)) \\
&= \nabla \sum_{i=1}^{m} \frac{\partial r_i}{\partial w} r_i \\
&= \sum_{i=1}^{m} \nabla \frac{\partial r_i}{\partial w} r_i \\
&= \sum_{i=1}^{m} (\frac{\partial r_i}{\partial w})^2 + \sum_{i=1}^{m} \frac{\partial^2 r_i}{\partial w^2} r_i \\
&= \sum_{i=1}^{m} (\frac{\partial r_i}{\partial w})^2
\end{aligned}
$$

The last step was deduced by the fact that $r(w)$ is a linear function, so that its second partial derivative yields $0$, which is:

$$
\frac{\partial^2 r_i}{\partial w^2} = 0, \forall i \in [0, m] \land i \in \mathbb N
$$

So we then have:

$$
\begin{aligned}
\nabla^2 f(w) &= \sum_{i=1}^{m} (\frac{\partial r_i}{\partial w})^2 \\
\end{aligned}
$$

we denote the Jacobian of $r$ as $\mathbf J$, then we have:

$$
\nabla^2 f = \mathbf J^{\intercal} \mathbf J \\
$$

### 4;(a) Apply it in the Newton's Direction

In question (1.1), we already know that for the newton's direction, we have:

$$
x_{k+1} = x_k - (\nabla^2 f(x_0))^{-1}\nabla f(x_0)
$$

In least square optimization case, we have that:

$$
\begin{aligned}
\nabla f &= \mathbf J^{\intercal} r(w) \\
\nabla^2 f &= \mathbf J^{\intercal} \mathbf J \\
\end{aligned}
$$

So this directly yields that:

$$
w_{k+1} - w_k = - (\mathbf J^{\intercal} \mathbf J)^{-1} \mathbf J^{\intercal} r(w_k)
$$

So our proof is done.

### 4;(b) SVD boosting

Apply SVD to $\mathbf J^{\intercal} \mathbf J$, we have that there exists two orthogonal matrices $U$ and $V$ such that:

$$
\mathbf J^{\intercal} \mathbf J = U \Sigma V^{\intercal}
$$

To proof the final results, we only need to proof that:

$$
\begin{aligned}
(\mathbf J^{\intercal} \mathbf J)^{-1} &= (U \Sigma V^{\intercal})^{-1} \\
&= V(\Sigma^{\intercal}\Sigma)^{-1} \Sigma^{\intercal} U^{\intercal} \\
\end{aligned}
$$

notice that since $U$ and $V$ are orthogonal matrices, we have the following facts:

$$
\begin{aligned}
U^{\intercal} U &= I \\
V^{\intercal} V &= I \\
(\Sigma^{\intercal}\Sigma)^{-1} &= \Sigma^{-1} (\Sigma^{\intercal})^{-1}
\end{aligned}
$$

We then have that:

$$
\begin{aligned}
U \Sigma V^{\intercal} V(\Sigma^{\intercal}\Sigma)^{-1} \Sigma^{\intercal} U^{\intercal} &= U \Sigma V^{\intercal} V \Sigma^{-1} (\Sigma^{\intercal})^{-1}\Sigma^{\intercal} U^{\intercal} \\
&= U \Sigma V^{\intercal} V \Sigma^{-1} U^{\intercal} \\
&= U \Sigma \Sigma^{\intercal} U^{\intercal}\\
&= U U^{\intercal} \\
&= I \\
\end{aligned}
$$

So the proof is done.

### 4;(c) Equation change shape

To show:

$$
w_{k+1} -w_k = - V \Sigma^{-1} U_t \mathbf J^{\intercal} r(w_k)
$$

It is sufficient enough to see that from previous results we get:

$$
w_{k+1} - w_k = - V\Sigma^{-1}U^{\intercal} \mathbf J^{\intercal} r(w_k)\\
$$

So the truncate here is simply:

$$
U_t = U^{\intercal}
$$

So the proof is done.

### 5; Based on the previous questions derive an algorithm to compute gradient descent

Basic setup for the algorithm:

- Given a function $f(X)$, where $X \in \mathbb R^n$.
- Iteratively calculate $X_i$ as follows:

$$
x_{k+1} = x_k - (\nabla^2 f(x_k))^{-1}\nabla f(x_k)
$$

- Repeat the process until that

$$
(\nabla^2 f(x_k))^{-1}\nabla f(x_k) = 0
$$

### 6; Without proving anything, explain why the above algorithm will perform better than gradient descent as introduced in the lectures

- It converges super fast, each time the direction it find will be more optimal
- It is easier for this algorithm to escape from local minimum or even the saddle points.
- The algorithm is in general faster than the traditional gradient descent.
- It doesn't really depends on the step size, we can even set it to $1$ as very default but good situation.
- It will adjust the step size automatically by its curvature at the local points, namely the Hessian.

## Ex.2

### 1;(a) Sequential algorithm design

The algorithm is based on the fact that $\oplus$ is actually **associative**.

Algorithm:

- Input: $A$ as an array with $2^I$ elements
- Output: $S = \bigoplus_{i=1}^{2^{I}}A[i]$
- $S \leftarrow A[0]$
- for $i\leftarrow 1, ..., 2^{I}-1$ do
  - $S \leftarrow S \oplus A[i]$
- end for
- return $S$

### 1;(b) Work of the Sequential algorithm

- Work: $O(2^I)$
- Depth: $O(2^I)$

### 2;(a) Parallel algorithm design

Again, the algorithm is based on the fact that $\oplus$ is actually **associative**.

Algorithm pseudo code as follows:

```python
def parallel_xor(A):
    n = len(A)
    while n > 1:
        # parallelly calculate every pair of item and merge later
        for i in range(0, n, 2):
            A[i] ^= A[i + 1]
        n //= 2
    return A[0]
```

### 2;(b) Work of the Parallel algorithm

- Work: $O(2^I)$
- Depth: $O(I)$

### 2;(c) Evaluate the quality of your parallelized algorithm

By Brent's theorem, we have:

$$
\frac{T_1}{p} \leq T_p \leq \frac{T_1}{p} + T_{\infty}
$$

In this situation we have:

$$
T_1 = O(n), T_{\infty} = O(\log n)
$$

So we notice that:

$$
T_p = O(\frac{n}{p} + \log n)
$$

So, when:

- $p \leq \frac{n}{\log n}$, it is a perfect boost.
- $p > \frac{n}{\log n}$, $\log n$ will become the bottleneck now.

## Ex. 3

- Final results

```csv
Method,Accuracy,TrainTime(s),LearningRate,Iterations,Standardized
hogwild,0.3906,18.75,0.05,150,True
sgd,0.6076,2.87,0.05,150,True
steepest,0.3616,29.68,0.05,150,True
broadcast,0.3629,29.54,0.05,150,True
batch,0.3614,2.95,0.05,150,True
```

Some observations and ideas:

- sgd is a little bit faster than batch, which aligns with ideas in the lecture
- sgd in this case have higher accuracy, and hogwild less, probably because overwritten lead it into local minimum
- steepest and broadcast are the slowest, their calculation task is the heaviest
- hogwild is slow, there exists cache thrashing between different threads
- steepest and broadcast and hogwild are handwritten model, which probably results in slower speed compare to the original embeded model provided by Spark. Original things in Spark can use the advantage of Excutors parallelism to the full.
