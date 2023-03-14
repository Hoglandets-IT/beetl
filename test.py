# MySQL Test
# from src.beetl.modules.mysql import MySQLSource
# from src.beetl.config import Config
# from yaml import safe_load

# config = Config.from_yaml_file('test.yaml')

# Test MySQL Source
# sr = config.sources['mysql']
# print(sr.query())

# Test SQL Server Source
# sr = config.sources['sqlserver']
# print(sr.query())

# Test Json file source
# sr = config.sources['json']
# print(sr.query())

from src.beetl.beetl import Beetl

sync = Beetl.from_yaml('test.yaml')
sync.sync()
print("Hold")