spark-submit \
--master yarn \
--deploy-mode cluster \
--py-files pyspark_project_lib.zip \
--files conf/pyspark_project.conf,conf/spark.conf,log4j.properties \
main.py qa 2023-12-10 