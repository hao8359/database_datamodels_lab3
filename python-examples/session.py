from pyspark.sql import SparkSession

hdfs_base_uri = "hdfs://localhost:8020"

spark = SparkSession.builder \
    .appName("fhir_hudi_job") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2") \
    .config("spark.sql.hive.convertMetastoreParquet", 'false') \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
