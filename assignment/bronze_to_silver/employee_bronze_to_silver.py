# Databricks notebook source
# MAGIC %run "/Users/kalluri.chandana@diggibyte.com/assignment/source_to_bronze/utils"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import *
department_schema = StructType([
    StructField('DepartmentID', StringType(), True),
    StructField('DepartmentName', StringType(), True)
])

# COMMAND ----------

employee_schema = StructType([
    StructField('EmployeeID', IntegerType(), True),
    StructField('EmployeeName', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Salary', IntegerType(), True),
    StructField('Age', IntegerType(), True)
])

# COMMAND ----------

country_schema = StructType([
    StructField('CountryCode', StringType(), True),
    StructField('CountryName', StringType(), True)
])

# COMMAND ----------

employee_csv_path = '''dbfs:/FileStore/assignment/source_to_bronze/employee/part-00000-tid-1148444031106094798-05184d5e-dc92-4b67-9bcf-debd5bd84130-9-1-c000.csv'''
country_csv_path = '''dbfs:/FileStore/assignment/source_to_bronze/country/part-00000-tid-3425704577654545642-907c3a3c-3642-4f9f-be0e-c748fdd8602e-11-1-c000.csv'''
department_csv_path = '''dbfs:/FileStore/assignment/source_to_bronze/department/part-00000-tid-9168086678279097491-0754eeea-82de-4c55-822c-8522b7cf961f-10-1-c000.csv'''
#dbfs:/FileStore/assignment/source_to_bronze/country/part-00000-tid-3425704577654545642-907c3a3c-3642-4f9f-be0e-c748fdd8602e-11-1-c000.csv


# COMMAND ----------

employee_df = read_with_custom_schema(employee_csv_path,employee_schema)
country_df = read_with_custom_schema(country_csv_path, country_schema)
department_df = read_with_custom_schema(department_csv_path, department_schema)

# COMMAND ----------


employee_snake_case_df = camel_to_snake_case(employee_df)
department_snake_case_df =camel_to_snake_case(department_df)
country_snake_case_df =camel_to_snake_case(country_df)

# COMMAND ----------

employee_with_date_df = add_current_date(employee_snake_case_df)
department_with_date_df = add_current_date(department_snake_case_df)
country_with_date_df = add_current_date(country_snake_case_df)

# COMMAND ----------

spark.sql('create database employee_info')
spark.sql('use employee_info')


# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignment/silver/employee_info/dim_employee').saveAsTable('dim_employee')
#'dbfs:/FileStore/assignment/source_to_bronze/employee'