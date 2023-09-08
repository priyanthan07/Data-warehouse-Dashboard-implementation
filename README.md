![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/aea881e4-2da3-4f19-a6b1-59b577957b1a)# Data-warehouse-Dashboard-implementation

## Introduction
The main objective of this project is to build a **data warehouse and design a dashboard** for analytics purposes.

## Requirements
        Excel
        Google Cloud Storage
        Mage.ai
        BigQuery
        Looker Studio
        python    ==> ETL
        SQL       ==> Query

## Architecture
![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/d5b4aedb-024c-4b98-9a5e-60ae6721eccd)

## Process
![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/56b97593-f4e8-41cf-9ae9-0b446438cbb3)

## ETL 
All the ETL processes were done using the Mage.ai tool.
![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/0e8bd636-815a-4931-8eb4-e7eb1917ac79)

### Extract
    The data was extracted from Google Cloud Storage.

### Transform 
        Changing the data type
        removing duplicates
        Create dimension tables
        Create Fact table

### Load
        The transformed data was loaded into the big query.
       
## DataWarehouse
### Star schema
![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/c1c71a2c-2bec-459f-906d-b8ef9f78541e)
### Tables
![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/948be5a4-5484-4ab9-9ac4-bd7db377e55c)

## Meta Data
In BigQuery, the metadata can be extracted under three qualifiers.</br>
         - Region Qualifier </br>
         - Dataset Qualifier </br>
         - Project Qualifier
 
![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/54c459d7-af5d-4c77-b889-51df0f86184c)

## Analytical Table
This table contains the required columns from the Data Warehouse for analytics purposes. So the Dashboard can be created using this table.

![image](https://github.com/priyanthan07/Data-warehouse-Dashboard-implementation/assets/129021635/458cfef1-7fe5-4462-9ee7-2bd7a30b5c25)




