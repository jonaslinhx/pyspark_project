spark-submit \
--master yarn \
--deploy-mode cluster \
--py-files pyspark_project_lib.zip \
--files conf/pyspark_project.conf,conf/spark.conf,log4j.properties \
--driver-cores 2 \
--driver-memory 3G \
--conf spark.driver.memoryOverhead=1G
main.py qa 2023-12-10 