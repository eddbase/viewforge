from pyspark.sql import SparkSession

# Define the local path for your Iceberg warehouse
# IMPORTANT: Make sure this path exists on your machine
warehouse_path = "/tmp/iceberg_warehouse"
# Make sure this path is correct for your environment
jar_path = "/home/moham/spark-3.5.5-bin-hadoop3/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar" 

print("Starting Spark session to create Iceberg table...")

spark = SparkSession.builder \
    .appName("CreateIcebergTable") \
    .config("spark.jars", jar_path) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local_iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local_iceberg.type", "hadoop") \
    .config("spark.sql.catalog.local_iceberg.warehouse", warehouse_path) \
    .getOrCreate()

print(f"Spark session created. Warehouse is at: {warehouse_path}")

# Create a database/schema and the table
spark.sql("CREATE DATABASE IF NOT EXISTS local_iceberg.db")
spark.sql("DROP TABLE IF EXISTS local_iceberg.db.sample_iceberg_table")

print("Creating sample Iceberg table: local_iceberg.db.sample_iceberg_table")

# Create a simple DataFrame
data = [("A", 100), ("B", 200)]
columns = ["category", "value"]
df = spark.createDataFrame(data, columns)

# --- CORRECTED LINE IS HERE ---
# Use .saveAsTable() to correctly register the table in the specified catalog.
df.write.format("iceberg").mode("overwrite").saveAsTable("local_iceberg.db.sample_iceberg_table")

print("Successfully created Iceberg table with 2 rows.")
spark.stop()