# Databricks notebook source
import os
os.getcwd()

# COMMAND ----------

# MAGIC %pip install "kedro[spark.SparkDataSet]~=0.17.7" kedro-viz

# COMMAND ----------

from pathlib import Path
Path.home().name

# COMMAND ----------

from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
import logging
from pathlib import Path

# suppress excessive logging from py4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# copy project data into DBFS
project_root = "/Workspace/Repos/cindy.wu@databricks.com/iris_databricks_kedro" 
data_dir = project_root + "/data"
dbutils.fs.cp(
    f"file://{data_dir}", f"dbfs://{data_dir}", recurse=True
)

# make sure the data has been copied
dbutils.fs.ls((data_dir + "/01_raw"))




# COMMAND ----------

project_root = "/Workspace/Repos/cindy.wu@databricks.com/iris_databricks_kedro" 
conf_dir = project_root + "/conf"
dbutils.fs.cp(
    f"file://{conf_dir}", f"dbfs://{conf_dir}", recurse=True
)
# make sure the data has been copied
dbutils.fs.ls((conf_dir))

# COMMAND ----------

project_root = "/Workspace/Repos/cindy.wu@databricks.com/iris_databricks_kedro" 
logs_dir = project_root + "/logs"
dbutils.fs.cp(
    f"file://{logs_dir}", f"dbfs://{logs_dir}", recurse=True
)
# make sure the data has been copied
dbutils.fs.ls((logs_dir))

# COMMAND ----------

from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
import logging
from pathlib import Path

bootstrap_project(project_root)
# suppress excessive logging from py4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# copy project data into DBFS
project_root = "/Workspace/Repos/cindy.wu@databricks.com/iris_databricks_kedro" 
with KedroSession.create(project_path=project_root) as session:
    session.run()

# COMMAND ----------


