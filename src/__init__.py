"""Initialize csv path files."""
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("BCG_Crash_Analysis").getOrCreate()
primary_person_df = spark.read.csv('data/Primary_Person_use.csv', header=True)
units_df = spark.read.csv('data/Units_use.csv', header=True)
damages_df = spark.read.csv('data/Damages_use.csv', header=True)
charges_df = spark.read.csv('data/Charges_use.csv', header=True)
