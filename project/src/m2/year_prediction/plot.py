import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys


def plot_accuracy_by_model(df):
    plt.figure(figsize=(14, 8))
    ax = sns.barplot(
        data=df,
        x="model_name",
        y="accuracy",
        hue="tolerance_years",
        palette="bright",  # Using a bright, eye-catching color palette
    )
    plt.title("Accuracy of Models at Different Tolerance Levels", fontsize=16)
    plt.xlabel("Model", fontsize=12)
    plt.ylabel("Accuracy", fontsize=12)
    plt.xticks(rotation=15, ha="right")
    plt.legend(title="Tolerance (Years)")
    # Format y-axis as percentage and set limits
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter("{:.0%}".format))
    plt.ylim(0, 1)

    # Add labels on top of bars
    for p in ax.patches:
        ax.annotate(
            f"{p.get_height():.1%}",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 9),
            textcoords="offset points",
        )

    plt.tight_layout()
    # Save the figure to a local file
    plt.savefig("accuracy_by_model.png", dpi=300)


def plot_error_metrics(df):
    # Calculate average metrics for each model
    avg_metrics = df.groupby("model_name")[["rmse", "mae"]].mean().reset_index()

    # Melt the DataFrame to have a single column for metric type and one for value
    df_melted = avg_metrics.melt(
        id_vars="model_name", var_name="metric_type", value_name="value"
    )

    plt.figure(figsize=(12, 7))
    ax = sns.barplot(
        data=df_melted,
        x="model_name",
        y="value",
        hue="metric_type",
        palette="Set2",  # Using a different vibrant palette
    )
    plt.title("Comparison of Average Error Metrics (RMSE & MAE)", fontsize=16)
    plt.xlabel("Model", fontsize=12)
    plt.ylabel("Error Value", fontsize=12)
    plt.xticks(rotation=15, ha="right")
    plt.legend(title="Metric")

    # Add labels on top of bars
    for p in ax.patches:
        ax.annotate(
            format(p.get_height(), ".2f"),
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 9),
            textcoords="offset points",
        )
    plt.tight_layout()
    # Save the figure to a local file
    plt.savefig("error_metrics.png", dpi=300)


def plot_training_time(df):
    avg_time = (
        df.groupby("model_name")["training_time_seconds"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(
        data=avg_time,
        x="training_time_seconds",
        y="model_name",
        palette="Paired",  # Using another distinct, colorful palette
    )
    plt.title("Average Training Time per Model", fontsize=16)
    plt.xlabel("Average Time (seconds)", fontsize=12)
    plt.ylabel("Model", fontsize=12)

    # Add labels on the bars
    for p in ax.patches:
        width = p.get_width()
        plt.text(
            width + 1,
            p.get_y() + p.get_height() / 2,
            "{:1.2f}s".format(width),
            va="center",
        )
    plt.tight_layout()
    # Save the figure to a local file
    plt.savefig("training_time.png", dpi=300)


def main():
    # Load the data directly from the CSV file
    file_path = "./experiment_results.csv"
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        print("Please make sure the CSV file is in the same directory as the script.")
        sys.exit(1)  # Exit the script if the file is not found

    # Set a nice theme for the plots
    sns.set_theme(style="whitegrid")

    # Generate the plots
    print("Generating and saving plots...")
    plot_accuracy_by_model(df)
    plot_error_metrics(df)
    plot_training_time(df)
    print("Plots saved successfully to the current directory.")

    # Display all generated plots
    plt.show()


if __name__ == "__main__":
    main()
