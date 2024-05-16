"""Initialize csv path files."""
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("BCG_Crash_Analysis").getOrCreate()
primary_person_path = spark.read.csv('data/Primary_Person_use.csv', header=True)
units_path = spark.read.csv('data/Units_use.csv', header=True)
damages_path = spark.read.csv('data/Damages_use.csv', header=True)
charges_path = spark.read.csv('data/Charges_use.csv', header=True)
