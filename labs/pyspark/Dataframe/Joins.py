# Showing employees under each manager
emp_managerDf = empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("manager_id"), \
      col("emp2.name").alias("manager_name")) 

# Showing employee count under each manager
manager_empCountDf = empDF.alias("emp1").join(empDF.alias("emp2"), col("emp1.emp_id") == col("emp2.superior_emp_id"),"inner") \
    .select(col("emp1.emp_id").alias("manager_id"),col("emp1.name").alias("manager_name"),col("emp2.emp_id").alias("employee_id")).groupBy("manager_name").count()
manager_empCountDf.show()
