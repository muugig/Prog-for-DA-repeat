# Ireland's Cycling Data Analysis

## 1. Programming for Data Analysis
# Introduction and Objectives
# Data Loading and Exploration
# Code Justifications and Quality Standards

import pandas as pd
import zipfile
import os
import mysql.connector
from sqlalchemy import create_engine
import time

# Define paths to datasets
zip_file_path = '/mnt/data/Datasets (1).zip'
extraction_dir = '/mnt/data/Datasets/'
london_data_path = '/mnt/data/london_merged.csv'

# Unzip the file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extraction_dir)

# List the contents of the extraction directory
extracted_files = os.listdir(extraction_dir)
print(f"Extracted files: {extracted_files}")

# Load the London dataset
london_df = pd.read_csv(london_data_path)
print("London DataFrame:")
print(london_df.head())

# Inspect other datasets
for file in extracted_files:
    file_path = os.path.join(extraction_dir, file)
    if file.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file.endswith('.json'):
        df = pd.read_json(file_path)
    else:
        continue
    print(f"Contents of {file}:")
    print(df.head())
    print("\n")

# Load data from a JSON file
json_file_path = os.path.join(extraction_dir, 'example.json')
json_df = pd.read_json(json_file_path)
print("JSON DataFrame:")
print(json_df.head())

# Example of connecting to a MySQL database and loading data (commented out as hypothetical)
# connection = mysql.connector.connect(
#     host='hostname',
#     user='username',
#     password='password',
#     database='database_name'
# )
# query = "SELECT * FROM table_name"
# mysql_df = pd.read_sql(query, connection)
# print("MySQL DataFrame:")
# print(mysql_df.head())

# Example of merging the London dataset with another dataset (assuming 'date' is a common column)
# merged_df = pd.merge(london_df, json_df, on='date', how='inner')
# print("Merged DataFrame:")
# print(merged_df.head())

# Testing for missing values
def test_missing_values(df):
    missing_values = df.isnull().sum().sum()
    assert missing_values == 0, f"DataFrame contains {missing_values} missing values"

# Testing for duplicate entries
def test_duplicates(df):
    duplicate_entries = df.duplicated().sum()
    assert duplicate_entries == 0, f"DataFrame contains {duplicate_entries} duplicate entries"

# Apply the tests to the London dataset
test_missing_values(london_df)
test_duplicates(london_df)

# Example of optimizing data loading
start_time = time.time()
london_df_optimized = pd.read_csv(london_data_path, dtype={'timestamp': 'str'})
end_time = time.time()
print(f"Data loading time: {end_time - start_time} seconds")

# Example of optimizing data processing
start_time = time.time()
london_df['timestamp'] = pd.to_datetime(london_df['timestamp'])
london_df.set_index('timestamp', inplace=True)
end_time = time.time()
print(f"Data processing time: {end_time - start_time} seconds")
