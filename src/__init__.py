"""Initialize csv path files."""
import os

from pyspark.sql import SparkSession

import yaml


def load_config(config_file):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(root_dir, config_file)
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


config = load_config('config.yaml')

primary_person_path = config['input']['primary_person_path']
units_path = config['input']['units_path']
damages_path = config['input']['damages_path']
charges_path = config['input']['charges_path']

output_dir = config['output']['analysis_results']
logs_dir = config['output']['logs']
summary_reports_dir = config['output']['summary_reports']
visualizations_dir = config['output']['visualizations']

spark = SparkSession.builder.appName("BCG_Crash_Analysis").getOrCreate()
# primary_person_df = spark.read.csv('data/Primary_Person_use.csv', header=True)
# units_df = spark.read.csv('data/Units_use.csv', header=True)
# damages_df = spark.read.csv('data/Damages_use.csv', header=True)
# charges_df = spark.read.csv('data/Charges_use.csv', header=True)
