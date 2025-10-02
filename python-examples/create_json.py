#!/usr/bin/env python3

from session import spark

relative_data_path = "./data"

df = spark.read().option("multiline", "true").json(relative_data_path+"/patient-*.json")
