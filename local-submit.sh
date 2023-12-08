spark-submit \
--deploy-mode client \
--files conf/pyspark_project.conf,conf/spark.conf,log4j.properties \
main.py local 2023-12-10