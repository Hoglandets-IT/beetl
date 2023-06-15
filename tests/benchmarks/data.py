from faker import Faker
import pandas as pd
from random import randint
faker = Faker()

class BenchmarkData:

    @staticmethod
    def generateData(amount: int):
        """Generates data with three columns, ID, Name and Email

        Args:
            amount (int): Amount of records to generate

        Returns:
            DataFrame, DataFrame: Source and destination dataframes
        """
        fakeSrc = []
        fakeDst = []
        
        for i in range(amount):
            obj = {
                "id": i,
                "name": faker.name(),
                "email": faker.email()
            }
            
            # Lower than 33: insert (remove from dst)
            if i < amount//3:
                fakeSrc.append(dict(obj))
            elif i > (amount//3)*2:
                fakeDst.append(dict(obj))
            else:
                fakeSrc.append(dict(obj))
                fakeDst.append(dict(obj)) 
        
        # Updates
        for i in range(0, amount//3):
            if randint(0, 1) == 1:
                fakeDst[i]["name"] = fakeDst[i]["name"][::-1]
            if randint(0, 1) == 1:
                fakeDst[i]["email"] = fakeDst[i]["email"][::-1]
        
        return pd.DataFrame(fakeSrc), pd.DataFrame(fakeDst)