import argparse
import time
import os
import csv
import numpy as np
import threading
from sklearn.metrics import accuracy_score
from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.mllib.classification import (
    LogisticRegressionWithSGD,
    LogisticRegressionModel,
)
from pyspark.mllib.linalg import Vectors as MLLibVectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.linalg import Vectors


class SimpleLogisticModel:
    def __init__(self, weights):
        self.weights = weights
        self._manual_model = True

    def predict(self, features_rdd):
        return features_rdd.map(lambda x: float(self._sigmoid(np.dot(self.weights, x))))

    def _sigmoid(self, z):
        return 1 / (1 + np.exp(-z))


class HogwildLogisticRegression:
    def __init__(self, n_features, lr=0.01, n_threads=4, epochs=5):
        self.weights = np.zeros(n_features)
        self.lr = lr
        self.n_threads = n_threads
        self.epochs = epochs

    def _sigmoid(self, z):
        return 1 / (1 + np.exp(-z))

    def _thread_worker(self, X, y):
        for epoch in range(self.epochs):
            for i in np.random.permutation(len(X)):
                xi = X[i]
                yi = y[i]
                pred = self._sigmoid(np.dot(self.weights, xi))
                grad = (pred - yi) * xi
                self.weights -= self.lr * grad

    def fit(self, X, y):
        threads = []
        for _ in range(self.n_threads):
            t = threading.Thread(target=self._thread_worker, args=(X, y))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    def predict(self, X):
        return (self._sigmoid(np.dot(X, self.weights)) >= 0.5).astype(int)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Gradient Descent in Spark for PBMC Dataset"
    )
    parser.add_argument(
        "--data_path", type=str, required=True, help="Path to PBMC_16k_RNA.csv"
    )
    parser.add_argument(
        "--label_path", type=str, required=True, help="Path to PBMC_16k_RNA_label.csv"
    )
    parser.add_argument(
        "--use_standardization",
        action="store_true",
        help="Apply feature standardization",
    )
    parser.add_argument(
        "--method",
        type=str,
        default="batch",
        choices=["batch", "sgd", "hogwild", "broadcast", "steepest"],
        help="Gradient descent variant",
    )
    parser.add_argument(
        "--num_iterations", type=int, default=100, help="Number of iterations"
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        default=0.1,
        help="Learning rate for gradient descent",
    )
    parser.add_argument(
        "--log_file",
        type=str,
        default="training_log.csv",
        help="Path to save training logs",
    )
    parser.add_argument(
        "--n_threads",
        type=int,
        default=4,
        help="Number of threads for Hogwild training",
    )
    return parser.parse_args()


def load_data(spark, args):
    df = spark.read.csv(
        f"file://{os.path.abspath(args.data_path)}", header=True, inferSchema=True
    )
    labels = spark.read.csv(
        f"file://{os.path.abspath(args.label_path)}", header=True, inferSchema=True
    )

    df = df.withColumnRenamed("Unnamed: 0", "index")
    labels = labels.withColumnRenamed("Unnamed: 0", "index").withColumnRenamed(
        labels.columns[1], "x"
    )

    joined = df.join(labels, on="index")
    features = ["KLHL17", "HES4"]

    assembler = VectorAssembler(inputCols=features, outputCol="features_vec")
    data = assembler.transform(joined)

    if args.use_standardization:
        scaler = StandardScaler(
            inputCol="features_vec",
            outputCol="scaled_features",
            withMean=True,
            withStd=True,
        )
        scaler_model = scaler.fit(data)
        data = scaler_model.transform(data)
        data = data.drop("features_vec")
        data = data.withColumnRenamed("scaled_features", "features_vec")

    return data


def apply_pca(data, k=2):
    pca = PCA(k=k, inputCol="features_vec", outputCol="pca_features")
    model = pca.fit(data)
    return model.transform(data).select("index", "pca_features", "x")


def label_data(data):
    type_counts = data.groupBy("x").count().orderBy("count", ascending=False).collect()
    A_type = type_counts[0]["x"]
    labeled = data.withColumn("label", (data["x"] == A_type).cast("double"))
    return labeled.select("pca_features", "label")


def split_data(labeled):
    def convert(row):
        label = row["label"]
        vec = row["pca_features"]
        mllib_vec = MLLibVectors.dense(vec.toArray())
        return LabeledPoint(label, mllib_vec)

    rdd = labeled.rdd.map(convert)
    train_rdd, test_rdd = rdd.randomSplit([0.7, 0.3], seed=42)
    return train_rdd.cache(), test_rdd.cache()


def train_model(method, train_rdd, sc, iterations, lr):
    start_time = time.time()

    if method == "batch":
        model = LogisticRegressionWithSGD.train(
            train_rdd, iterations=iterations, step=lr
        )
    elif method == "sgd":
        model = LogisticRegressionWithSGD.train(
            train_rdd, iterations=iterations, step=lr, miniBatchFraction=0.01
        )
    elif method == "hogwild":
        X = np.array(train_rdd.map(lambda p: p.features).collect())
        y = np.array(train_rdd.map(lambda p: p.label).collect())

        model = HogwildLogisticRegression(
            n_features=X.shape[1], lr=lr, n_threads=4, epochs=iterations
        )
        model.fit(X, y)
        model._internal_hogwild = True
    elif method == "broadcast":
        weights = np.zeros(len(train_rdd.first().features))
        for i in range(iterations):
            w_b = sc.broadcast(weights)
            grad = train_rdd.map(
                lambda p: (1 / (1 + np.exp(-np.dot(w_b.value, p.features))) - p.label)
                * np.array(p.features)
            ).reduce(lambda x, y: x + y)
            weights -= lr * grad
        model = SimpleLogisticModel(weights)
    elif method == "steepest":
        weights = np.zeros(len(train_rdd.first().features))
        for i in range(iterations):
            w_b = sc.broadcast(weights)
            grad = train_rdd.map(
                lambda p: (1 / (1 + np.exp(-np.dot(w_b.value, p.features))) - p.label)
                * np.array(p.features)
            ).reduce(lambda x, y: x + y)
            direction = -grad / np.linalg.norm(grad)
            weights += lr * direction
        model = SimpleLogisticModel(weights)
    else:
        raise ValueError(f"Unsupported method: {method}")

    end_time = time.time()
    return model, end_time - start_time


def evaluate(model, test_rdd):
    if hasattr(model, "_internal_hogwild"):
        X_test = np.array(test_rdd.map(lambda p: p.features).collect())
        y_test = np.array(test_rdd.map(lambda p: p.label).collect())
        y_pred = model.predict(X_test)
        return accuracy_score(y_test, y_pred)

    elif hasattr(model, "_manual_model"):
        preds = model.predict(test_rdd.map(lambda p: p.features))
        correct = (
            test_rdd.map(lambda p: p.label)
            .zip(preds)
            .filter(lambda x: round(x[0]) == round(x[1]))
            .count()
        )
        total = test_rdd.count()
        return correct / total if total > 0 else 0.0

    else:
        preds = model.predict(test_rdd.map(lambda p: p.features))
        correct = (
            test_rdd.map(lambda p: p.label)
            .zip(preds)
            .filter(lambda x: x[0] == x[1])
            .count()
        )
        total = test_rdd.count()
        return correct / total if total > 0 else 0.0


def save_log(log_path, method, accuracy, train_time, lr, iterations, standardization):
    file_exists = os.path.exists(log_path)
    with open(log_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(
                [
                    "Method",
                    "Accuracy",
                    "TrainTime(s)",
                    "LearningRate",
                    "Iterations",
                    "Standardized",
                ]
            )
        writer.writerow(
            [
                method,
                f"{accuracy:.4f}",
                f"{train_time:.2f}",
                lr,
                iterations,
                standardization,
            ]
        )


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("PBMC Gradient Descent").getOrCreate()
    sc = spark.sparkContext

    print("Loading data...")
    data = load_data(spark, args)
    pca_data = apply_pca(data)
    labeled = label_data(pca_data)
    train_rdd, test_rdd = split_data(labeled)

    print(f"Training model using method: {args.method}")
    model, train_time = train_model(
        args.method, train_rdd, sc, args.num_iterations, args.learning_rate
    )
    accuracy = evaluate(model, test_rdd)

    print(f"\nMethod: {args.method}")
    print(f"\nAccuracy: {accuracy:.4f}")
    print(f"\nTraining time: {train_time:.2f} seconds")

    save_log(
        args.log_file,
        args.method,
        accuracy,
        train_time,
        args.learning_rate,
        args.num_iterations,
        args.use_standardization,
    )

    spark.stop()


if __name__ == "__main__":
    main()
