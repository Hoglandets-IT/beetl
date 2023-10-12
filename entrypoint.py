#!/usr/bin/python3
import os
from beetl.beetl import BeetlConfig, Beetl

if not os.getenv("FILEPATH"):
  print("FILEPATH is not set")
  exit(1)

if os.getenv("FILETYPE"):
  print("FILETYPE is not set")
  exit(1)

if os.getenv("FILETYPE").lower() == "json":
  beetlConfig = BeetlConfig.from_json_file(
    os.getenv("FILEPATH")
  )

if os.getenv("FILETYPE").lower() == "yaml":
  beetlConfig = BeetlConfig.from_yaml_file(
    os.getenv("FILEPATH")
  )

try:
  beetl = Beetl(beetlConfig)
  beetl.sync()
except Exception as e:
  print("Beetl failed to sync: " + str(e))
  exit(1)