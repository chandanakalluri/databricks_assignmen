# Databricks notebook source
# MAGIC %run "/Users/kalluri.chandana@diggibyte.com/assignment/source_to_bronze/utils"

# COMMAND ----------

# MAGIC %run "/Users/kalluri.chandana@diggibyte.com/assignment/bronze_to_silver/employee_bronze_to_silver"

# COMMAND ----------

from pyspark.sql.functions import desc, count, avg

# COMMAND ----------

employee_df = spark.read.format("delta").load('dbfs:/FileStore/assignment/silver/employee_info/dim_employee')

# COMMAND ----------

salary_of_department = employee_with_date_df.orderBy(desc('salary'))
employee_count = employee_df.groupBy("department", "country").agg(count("employeeid").alias("employee_count"))

# COMMAND ----------

department_join_df = employee_with_date_df.join(department_with_date_df, employee_with_date_df.department == department_with_date_df.departmentid, "inner")
country_join_df = department_join_df.join(country_with_date_df, department_join_df.country == country_with_date_df.countrycode, "inner")
department_with_country = country_join_df.select('departmentname', 'countryname')

# COMMAND ----------

avg_age_employee = employee_with_date_df.groupBy('department').agg(avg("age").alias('avg_age'))

# COMMAND ----------

employee_with_date_df.display()

# COMMAND ----------

employee_with_date_df.write.format("delta").mode("overwrite").option("replaceWhere", "load_date >= '2024-04-16'")\
.save("/FileStore/assignments/gold/employee/table_name")








