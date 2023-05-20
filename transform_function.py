from pyspark.sql.functions import *


def transformation(df_dict, spark):
    destination_dict = {}
    employee_catalog = df_dict["employee_catalog"]
    department_csv = df_dict["department_csv"]
    diseases_parquet = df_dict["diseases_parquet"]

    employee_catalog.registerTempTable("employee")
    department_csv.registerTempTable("dept")

    df1 = spark.sql(
        """select EMPLOYEE_ID,FIRST_NAME,LAST_NAME, DEPARTMENT_NAME from employee join dept ON 
        employee.DEPARTMENT_ID = dept.DEPARTMENT_ID""")

    destination_dict["out1"] = df1

    destination_dict["out2"] = diseases_parquet

    return destination_dict