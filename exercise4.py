import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient
import pandas as pd
import pyodbc

# Azure Blob Storage connection
blob_service_client = BlobServiceClient.from_connection_string("your_connection_string")

# Azure Data Lake connection
data_lake_service_client = DataLakeServiceClient.from_connection_string("your_connection_string")

# MSSQL Server connection
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password')

# Loop through each blob (day)
container_client = blob_service_client.get_container_client('your_container')
for blob in container_client.list_blobs():
    # Download the blob data
    blob_client = BlobClient.from_connection_string("your_connection_string", 'your_container', blob.name)
    data = blob_client.upload_blob().readall()

    # Load blob data into Azure Data Lake
    try:
        file_system_client = data_lake_service_client.create_file_system(blob.name)
    except ResourceExistsError:
        file_system_client = data_lake_service_client.get_file_system_client(blob.name)
    file_client = file_system_client.create_file(blob.name)
    file_client.append_data(data, offset=0)
    file_client.flush_data(len(data))

# Calculate the sales for each month
df = pd.read_sql_query("SELECT DATEPART(month, date) as month, SUM(sales) as total_sales FROM your_table GROUP BY DATEPART(month, date)", conn)

# Store the output data into a MSSQL Server table
df.to_sql('executive_dashboard', conn, if_exists='replace', index=False)
