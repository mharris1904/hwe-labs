CREATE EXTERNAL TABLE IF NOT EXISTS `hwe`.`mharris_reviews_silver` (
  `customer_id` string,
  `review_id` string,
  `product_id` string,
  `product_parent` string,
  `product_title` string,
  `product_category` string,
  `star_rating` int,
  `helpful_votes` int,
  `total_votes` int,
  `vine` string,
  `verified_purchase` string,
  `review_headline` string,
  `review_body` string,
  `purchase_date` string,
  `review_timestamp` timestamp,
  `customer_name` string,
  `gender` string,
  `date_of_birth` string,
  `city` string,
  `state` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://hwe-fall-2023/mharris/silver/reviews/'
TBLPROPERTIES ('classification' = 'parquet');