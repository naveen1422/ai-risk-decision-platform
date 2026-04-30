from core.spark_session import spark

spark.sql("SHOW DATABASES").show()

spark.stop()