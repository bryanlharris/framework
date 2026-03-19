# Databricks notebook source
import xml.etree.ElementTree as ET

tree = ET.parse("/Volumes/staging/fpds_raw/landing/fpds_output_2025-01-28.xml")
root = tree.getroot()

for elem in root.iter():
    print(elem.tag)

# COMMAND ----------

namespace_uri = "{https://www.fpds.gov/FPDS}"
rows = root.findall(f".//{{{namespace_uri}}}award")
print(rows)