import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

handle= os.environ.get("AWS_HANDLE")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,io.delta:delta-core_2.12:1.0.1') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

silver_schema = StructType([
StructField("marketplace", StringType(), nullable=True)
,StructField("customer_id", StringType(), nullable=True)
,StructField("review_id", StringType(), nullable=True)
,StructField("product_id", StringType(), nullable=True)
,StructField("product_parent", StringType(), nullable=True)
,StructField("product_title", StringType(), nullable=True)
,StructField("product_category", StringType(), nullable=True)
,StructField("star_rating", IntegerType(), nullable=True)
,StructField("helpful_votes", IntegerType(), nullable=True)
,StructField("total_votes", IntegerType(), nullable=True)
,StructField("vine", StringType(), nullable=True)
,StructField("verified_purchase", StringType(), nullable=True)
,StructField("review_headline", StringType(), nullable=True)
,StructField("review_body", StringType(), nullable=True)
,StructField("purchase_date", StringType(), nullable=True)
,StructField("review_timestamp", TimestampType(), nullable=True)
,StructField("customer_name", StringType(), nullable=True)
,StructField("gender", StringType(), nullable=True)
,StructField("date_of_birth", StringType(), nullable=True)
,StructField("city", StringType(), nullable=True)
,StructField("state", StringType(), nullable=True)
])

silver_reviews = spark.read \
    .format("parquet") \
    .schema(silver_schema) \
    .load(f"s3a://hwe-fall-2023/mharris/silver/reviews") 
silver_reviews.createOrReplaceTempView("silver_reviews")

# silver_reviews.show(5)

# Create dataframe for the product dimension.
product_dimension = spark.sql("""
    select distinct product_id
          ,product_title
          ,product_category
     from silver_reviews
     """)

# Write the product dimension to S3 in delta format in gold/product_dimension.
product_dimension.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"s3a://hwe-fall-2023/{handle}/gold/product_dimension")

# Create dataframe for the customer dimension.
customer_dimension = spark.sql("""
    select distinct customer_id
            ,customer_name
            ,gender
            ,date_of_birth
            ,city
            ,state
        from silver_reviews
        """)

# Write the customer dimension to S3 in delta format in gold/customer_dimension.
customer_dimension.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"s3a://hwe-fall-2023/{handle}/gold/customer_dimension")     

# Create dataframe for the date dimension using review_date.
# have year, year_month, year_quarter.
date_dimension = spark.sql("""
    select distinct date_format(purchase_date, 'yyyy-MM-dd') as review_date
            ,date_format(purchase_date, 'yyyy') as year
            ,date_format(purchase_date, 'yyyy-Q') as year_quarter
            ,date_format(purchase_date, 'yyyy-MM') as year_month
        from silver_reviews
        """)

# Write the date dimension to S3 in delta format in gold/date_dimension.
date_dimension.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"s3a://hwe-fall-2023/{handle}/gold/date_dimension")

## Stop the SparkSession
spark.stop()
