# Databricks notebook source
# MAGIC %run "/Users/kalluri.chandana@diggibyte.com/assignment/source_to_bronze/utils"

# COMMAND ----------

#Databricks notebook source
dbutils.widgets.text("data_type","","data_type")
dbutils.widgets.text("table_name","","table_name")
dbutils.widgets.text("database_name","","database_name")
data_type = dbutils.widgets.get("data_type")
data_type = dbutils.widgets.get("table_name")
data_type = dbutils.widgets.get("database_name")



# COMMAND ----------

source_path = f"dbfs:/FileStore/resource/{data_type}.csv"
bronze_path = f"d/Users/kalluri.chandana@diggibyte.com/assignment/source_to_bronze/{data_type}.csv"


# COMMAND ----------

reading_csv_files=read_csv(source_path)
reading_csv_files.display()

# COMMAND ----------

write_csv_file(reading_csv_files,bronze_path)

# COMMAND ----------

def overwrite_silver_path(database_name, table_name, df, silver_path):
    
    df.mode("overwrite").option("path",silver_path).saveAsTable(f'{database_name}.{table_name}')


# COMMAND ----------

write_delta_table(employee_df, "Employee_info", "dim_employee", "EmployeeID", "/silver/Employee_info/dim_employee")

# COMMAND ----------

display(dbutils.fs.ls('/Users/kalluri.chandana@diggibyte.com/assignment/source_to_bronze/employee_source_to_bronze'))