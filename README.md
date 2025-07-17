# Synapse-Analytics-Project
Synapse Analytics Project implemented by Sales &amp; order data for Northwind Traders, a fictitious gourmet food supplier, including information on customers, products, orders, shippers, and employees.

![Project - Onprem](https://github.com/user-attachments/assets/139f5bdd-9dbb-4cbe-a0a3-1c95240782ba)

Northwind Traders
Sales & order data for Northwind Traders, a fictitious gourmet food supplier, including information on customers, products, orders, shippers, and employees.

Required Analysis
- Are there any sales trends over time?
- Which are the best and worst selling products?
- Can you identify any key customers?
- Are shipping costs consistent across providers?

Dataset link 

This data was collected from Maven Analytics as [Northwind Traders](https://mavenanalytics.io/data-playground?order=date_added%2Cdesc&page=8&pageSize=5)  

7 tables 
2,985 records 

Moving data from SQL Servr OnPrem to Azure ADLS

<img width="1280" height="720" alt="Project - moving data from onprem to ADLS" src="https://github.com/user-attachments/assets/83ce9ed1-db95-490b-be3e-79b4e9aa8193" />

Ingest the data into Azure Data Factory from SQL OnPrem

<img width="319" height="125" alt="adf-synapse" src="https://github.com/user-attachments/assets/78d5729a-cd5f-4c73-bdb3-71b32c79b33f" />

Loaded files into Bronze Layer

<img width="853" height="529" alt="image" src="https://github.com/user-attachments/assets/d0717416-d71b-4ef0-9c76-afa23f790b2c" />

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

```python
spark.conf.set("fs.azure.account.auth.type.adlssuppliers.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlssuppliers.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlssuppliers.dfs.core.windows.net", "f8a2f6a4-b65f-4db8-ae4b-cb3f6d28994c")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlssuppliers.dfs.core.windows.net","iLK8Q~opRCvr8NcOLksdcm9Y1LfMOMqXCf4eob0d")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlssuppliers.dfs.core.windows.net", "https://login.microsoftonline.com/308ab8d8-35eb-4420-83eb-45ab2b909bbb/oauth2/token")
```

```python
dbutils.fs.ls("abfss://bronze@adlssuppliers.dfs.core.windows.net/")
```

<img width="1357" height="333" alt="image" src="https://github.com/user-attachments/assets/59b7d773-e7f4-4e3e-ac50-ba4c4b16e74c" />

## 1. Category Table 

```python
df_categories = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlssuppliers.dfs.core.windows.net/categories.parquet")
df_categories.display()
```

<img width="732" height="281" alt="image" src="https://github.com/user-attachments/assets/b6b63b75-7bc6-40d3-b3f9-e8ed1f0ad5ac" />

```python
# Drop a column
    df_categories = df_categories.drop("description")
    df_categories.display()
```

```python
# Write the data into Azure ADLS Silver Layer

```python
df_categories.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@adlssuppliers.dfs.core.windows.net/categories")\
    .save()
```
## 2. Customers Table

```python
df_customers = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlssuppliers.dfs.core.windows.net/customers.parquet")
display(df_customers.head(2))
```
```python
df_customers = df_customers.drop("companyName","contactName","contactTitle")
display(df_customers.head(3))
```
<img width="406" height="130" alt="image" src="https://github.com/user-attachments/assets/585e6f6b-135f-4efd-994e-9ce1bb25b46e" />

## Write the data into Azure ADLS Silver Layer
```python
df_customers.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@adlssuppliers.dfs.core.windows.net/customers")\
    .save()
```
## 3. Employees Table 
```python
df_employees = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlssuppliers.dfs.core.windows.net/employees.parquet")
display(df_employees.head(2))
```
<img width="905" height="103" alt="image" src="https://github.com/user-attachments/assets/9f59e8ab-86d9-4a90-acfd-7ea97464f0fe" />

## Write the Employees data into Azure ADLS Silver Layer
```python
df_employees.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@adlssuppliers.dfs.core.windows.net/employees")\
    .save()
```
## 4. Orders Details
```python
df_order_details = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlssuppliers.dfs.core.windows.net/order_details.parquet")
display(df_order_details.head(2))
```
```python
# Create Total_Sales Column
df_order_details2 = df_order_details.withColumn("total_sales", round( col("unitPrice") * col("quantity") * (1 - col("discount")) ,2) )\
# .withColumn("total_dis", ( round( col("unitPrice") * col("quantity") ) *col("discount") ) )
display(df_order_details2.head(30))
```
<img width="861" height="301" alt="image" src="https://github.com/user-attachments/assets/225c8e70-b2fe-4989-9e40-562cbe952088" />

## Write the Orders Details data into Azure ADLS Silver Layer
```python
df_order_details2.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@adlssuppliers.dfs.core.windows.net/order_details")\
    .save()
```
## 5. Orders
```python
df_orders = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@adlssuppliers.dfs.core.windows.net/orders.parquet")
display(df_orders.head(2))
```
<img width="1238" height="102" alt="image" src="https://github.com/user-attachments/assets/3208b184-d770-494d-b195-7d8ce47fa4d2" />

## Write the Orders data into Azure ADLS Silver Layer
```python
df_orders.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@adlssuppliers.dfs.core.windows.net/orders")\
    .save()
```

