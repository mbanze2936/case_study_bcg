"""Initialize csv path files."""
from pyspark.sql import SparkSession

import yaml


def load_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config


config = load_config('config.yaml')

primary_person_path = config['input']['primary_person']
units_path = config['input']['units']
damages_path = config['input']['damages']
charges_path = config['input']['charges']

output_dir = config['output']['analysis_results']
logs_dir = config['output']['logs']
summary_reports_dir = config['output']['summary_reports']
visualizations_dir = config['output']['visualizations']

spark = SparkSession.builder.appName("BCG_Crash_Analysis").getOrCreate()
# primary_person_df = spark.read.csv('data/Primary_Person_use.csv', header=True)
# units_df = spark.read.csv('data/Units_use.csv', header=True)
# damages_df = spark.read.csv('data/Damages_use.csv', header=True)
# charges_df = spark.read.csv('data/Charges_use.csv', header=True)
