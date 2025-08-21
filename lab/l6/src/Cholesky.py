import numpy as np
import time
from loguru import logger
from typing import List, Dict, Tuple
from pyspark.sql import SparkSession
from pyspark.rdd import RDD


def distributed_cholesky_from_image_algo(
    spark, A_coords_rdd: RDD, n: int
) -> np.ndarray:
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    l_coords_driver = {}
    a_rdd = A_coords_rdd.cache()

    for j in range(n):
        a_j_col_map = a_rdd.filter(
            lambda x: x[0][1] == j and x[0][0] >= j
        ).collectAsMap()

        diag_val_sq = a_j_col_map.get((j, j))
        if diag_val_sq is None:
            raise ValueError(f"Diagonal element A({j},{j}) not found.")

        if diag_val_sq < 1e-9:
            raise np.linalg.LinAlgError(
                f"Matrix is not positive definite at column {j}."
            )

        ljj = np.sqrt(diag_val_sq)
        l_coords_driver[(j, j)] = ljj

        l_j_vector = np.zeros(n)
        l_j_vector[j] = ljj

        for i in range(j + 1, n):
            aij_val = a_j_col_map.get((i, j))
            if aij_val is None:
                raise ValueError(
                    f"Element A({i},{j}) not found for column calculation."
                )

            lij = aij_val / ljj
            l_coords_driver[(i, j)] = lij
            l_j_vector[i] = lij

        if j < n - 1:
            update_terms = []
            for k in range(j + 1, n):
                for l in range(j + 1, k + 1):
                    update_val = l_j_vector[k] * l_j_vector[l]
                    if abs(update_val) > 1e-12:
                        update_terms.append(((k, l), update_val))

            if update_terms:
                update_rdd = sc.parallelize(update_terms)

                joined_rdd = a_rdd.leftOuterJoin(update_rdd)
                new_a_rdd = joined_rdd.mapValues(
                    lambda values: (
                        values[0] - values[1] if values[1] is not None else values[0]
                    )
                )

                a_rdd.unpersist()
                a_rdd = new_a_rdd.cache()
                a_rdd.count()

    final_l_matrix = np.zeros((n, n))
    for (r, c), val in l_coords_driver.items():
        final_l_matrix[r, c] = val

    return final_l_matrix


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "FinalCorrectDistributedCholesky"
    ).getOrCreate()

    m1_data = [[25.0, -50.0], [-50.0, 101.0]]
    m2_data = [[25.0, 15.0, -5.0], [15.0, 18.0, 0.0], [-5.0, 0.0, 11.0]]
    m3_data = [
        [7.0, -2.0, -3.0, 0.0, -1.0, 0.0],
        [-2.0, 8.0, 0.0, 0.0, -1.0, 0.0],
        [-3.0, 0.0, 4.0, -1.0, 0.0, 0.0],
        [0.0, 0.0, -1.0, 5.0, 0.0, -2.0],
        [-1.0, -1.0, 0.0, 0.0, 4.0, 0.0],
        [0.0, 0.0, 0.0, -2.0, 0.0, 6.0],
    ]

    matrices_to_run: Dict[str, List[List[float]]] = {
        "M1": m1_data,
        "M2": m2_data,
        "M3": m3_data,
    }
    cholesky_runtimes: Dict[str, float] = {}

    for name, data in matrices_to_run.items():
        n = len(data)
        logger.info(
            f"Processing matrix {name} with size {n}x{n} using distributed algorithm."
        )

        coord_list = []
        for i in range(n):
            for j in range(i + 1):
                coord_list.append(((i, j), data[i][j]))

        matrix_coords_rdd = spark.sparkContext.parallelize(coord_list)

        start_time = time.time()
        L = distributed_cholesky_from_image_algo(spark, matrix_coords_rdd, n)
        end_time = time.time()
        cholesky_runtimes[name] = end_time - start_time

        logger.info(f"Cholesky factor L for matrix {name}:\n{np.round(L, 4)}")

        reconstructed_A = L @ L.T
        logger.info(
            f"Reconstructed A from L for matrix {name}:\n{np.round(reconstructed_A, 4)}"
        )
        if not np.allclose(data, reconstructed_A):
            logger.error(f"Validation failed for {name}!")
        else:
            logger.success(f"Validation successful for {name}!")

    logger.info("-" * 40)
    for name, runtime in cholesky_runtimes.items():
        logger.success(f"Distributed Cholesky for {name} took {runtime:.4f} seconds")

    spark.stop()
