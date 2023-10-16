#!/usr/bin/python3
import os
import sys
from beetl.beetl import BeetlConfig, Beetl

configs = []

def try_load(filename):
  extension = filename.split('.')[-1]
  
  if extension.lower() in ['yaml', 'yml']:
    return BeetlConfig.from_yaml_file(filename)
  
  if extension.lower() in ['json', 'jsonp', 'js']:
    return BeetlConfig.from_json_file(filename)
  
  if extension.lower() in ['py']:
    print("Please call the python file directly and not via the entrypoint. Set the ent"
          "rypoint to /app/.venv/bin/python3 and provide the python file as command")
    exit(1)
  
if not os.getenv("FILEPATH"):
  args = sys.argv[1:]
  
  if len(args) < 1:
    print("Neither environment FILEPATH or command arguments are set")
    exit(1)
  
  for item in args:
    if item == "":
      print("Empty argument provided")
      exit(1)
    
    configs.append(try_load(item))

if os.getenv("FILEPATH", None) is not None:
  for item in os.getenv("FILEPATH").split(';'):
    configs.append(try_load(item))

try:
  for config in configs:
    beetl = Beetl(config)
    beetl.sync()
except Exception as e:
  print("Beetl failed to sync: " + str(e))
  exit(1)