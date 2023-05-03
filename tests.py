# import unittest
# from tests.test_functions import *

# if __name__ == '__main__':
#     unittest.main()
import polars as pl


data = pl.DataFrame({
    "id": [7, 5, 6],
    "name": ["John", "Jane", "Alice"],
    "email": ["john@example.com", "jane@example.com", "alice@example.com"]
})


data.write_database(
    "dsttable",
    "mysql+pymysql://root@localhost:3306/testdb",
    if_exists="append"
)