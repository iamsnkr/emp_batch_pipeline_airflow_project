from pyspark.sql import SparkSession


def process_data():
    spark = SparkSession.builder.appName("AWS_EMR").getOrCreate()

    # Define your AWS S3 bucket and paths
    bucket = "aws-spark-jobs-bucket"
    
    emp_data_path = f"s3://{bucket}/data/input/employee.csv"
    dept_data_path = f"s3://{bucket}/data/input/department.csv"
    output_path = f"s3://{bucket}/data/output"

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)
    department = spark.read.csv(dept_data_path, header=True, inferSchema=True)

    # Filter employee data
    filtered_employee = employee.filter(employee.salary > 50000)  # Adjust the salary threshold as needed

    # Join datasets
    joined_data = filtered_employee.join(department, "dept_id", "inner")

    # Write output
    joined_data.write.csv(output_path, mode="overwrite", header=True)

    spark.stop()


if __name__ == "__main__":
    process_data()
