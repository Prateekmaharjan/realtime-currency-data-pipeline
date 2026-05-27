from dotenv import load_dotenv
import os
load_dotenv()
from pyspark.sql import SparkSession

# JDBC connection details
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DB = "currency_warehouse"
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
JDBC_JAR = os.path.expanduser("~/currency-pipeline/jars/mysql-connector-j-8.3.0.jar")

# Initialize Spark with MySQL JDBC driver
spark = SparkSession.builder \
    .appName("CurrencyWarehouseLoader") \
    .config("spark.jars", JDBC_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read accumulated raw data from Parquet data lake
print("Reading from data lake...")
df = spark.read.parquet(os.path.expanduser("~/currency-pipeline/data-lake/raw/"))

# Register as temp view for Spark SQL queries
df.createOrReplaceTempView("exchange_rates")

# Calculate daily summary using Spark SQL
print("Computing daily summary...")
daily_summary = spark.sql("""
    SELECT
        target_currency AS currency,
        ROUND(AVG(exchange_rate), 6) AS avg_rate,
        ROUND(MIN(exchange_rate), 6) AS min_rate,
        ROUND(MAX(exchange_rate), 6) AS max_rate
    FROM exchange_rates
    GROUP BY target_currency
""")

# Calculate latest rate per currency using Spark SQL
print("Computing latest rates...")
latest_rates = spark.sql("""
    SELECT
        target_currency AS currency,
        exchange_rate AS rate,
        timestamp AS last_updated
    FROM exchange_rates
    WHERE timestamp = (SELECT MAX(timestamp) FROM exchange_rates)
""")

# Write daily summary to MySQL using JDBC
print("Loading daily_rates into warehouse...")
daily_summary.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "daily_rates") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("append") \
    .save()

# Write latest rates to MySQL using JDBC
print("Loading latest_rates into warehouse...")
latest_rates.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "latest_rates") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .save()

print("Warehouse load complete.")
print(f"Daily summary: {daily_summary.count()} currencies loaded.")
print(f"Latest rates: {latest_rates.count()} currencies loaded.")

spark.stop()
