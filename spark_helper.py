from pyspark.sql import SparkSession

class SparkHandler:
    def __init__(self, app_name="MySparkApp"):
        """Initialize the Spark session."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        print(f"Spark session '{app_name}' started.")

    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped.")

    def read_csv(self, file_path, **options):
        """Read a CSV file into a Spark DataFrame."""
        return self.spark.read.csv(file_path, **options)

    def write_csv(self, df, file_path, **options):
        """Write a Spark DataFrame to a CSV file."""
        df.write.csv(file_path, **options)
        print(f"DataFrame written to {file_path}.")

    def show_dataframe(self, df, num_rows=5):
        """Display a specified number of rows from a DataFrame."""
        df.show(num_rows)

    def filter_dataframe(self, df, condition):
        """Filter a DataFrame based on a condition."""
        return df.filter(condition)

    def group_and_aggregate(self, df, group_by_columns, agg_column, agg_func):
        """Group and aggregate a DataFrame."""
        return df.groupBy(group_by_columns).agg({agg_column: agg_func})

# Example usage
if __name__ == "__main__":
    spark_handler = SparkHandler("ExampleApp")

    # Read a CSV file
    df = spark_handler.read_csv("path/to/your/file.csv", header=True, inferSchema=True)

    # Show the DataFrame
    spark_handler.show_dataframe(df)

    # Filter the DataFrame
    filtered_df = spark_handler.filter_dataframe(df, "column_name > value")

    # Group and aggregate
    aggregated_df = spark_handler.group_and_aggregate(filtered_df, "group_column", "agg_column", "sum")

    # Write the aggregated DataFrame to a new CSV
    spark_handler.write_csv(aggregated_df, "path/to/output.csv")

    # Stop the Spark session
    spark_handler.stop()
