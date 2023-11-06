"""CLI interface for ids706_python_template project.

Be creative! do whatever you want!

- Install click or typer and create a CLI app
- Use builtin argparse
- Start a web application
- Import things from your .base module
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def main():  # pragma: no cover
    # Initialize a Spark session
    spark = SparkSession.builder.appName("LargeDatasetProcessing").getOrCreate()

    # Load the CSV file into a DataFrame
    df = spark.read.csv("large_dataset.csv", header=True, inferSchema=True)

    # Show the first few rows of the DataFrame
    df.show()

    # Perform some data processing on the DataFrame
    # For example, calculate the average salary of the employees
    average_salary = df.groupBy().avg("salary").collect()[0][0]

    print(f"Average Salary: {average_salary:.2f}")

    # Group by department and calculate the average salary
    department_avg_salary = df.groupBy("department").agg(avg("salary").alias("avg_salary"))

    department_avg_salary.show()

    # Stop the Spark session when done
    spark.stop()

