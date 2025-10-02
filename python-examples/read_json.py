#!/usr/bin/env python3

from session import spark, hdfs_base_uri

spark.read().format("hudi").load(hdfs_base_uri+"/fhir/patients-json").createOrReplaceTempView("patients")

spark.sql("""
SELECT * FROM patients
""")
