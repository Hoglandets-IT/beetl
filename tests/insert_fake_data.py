import json
from faker import Faker
from tqdm import tqdm
from random import randint
import sqlalchemy as sqla
import pandas as pd
import pyodbc
from sqlalchemy import (
    Table as sqtable,
    Column as sqcolumn,
    Integer as sqinteger,
    String as sqstring,
    MetaData as sqmetadata
)

faker = Faker()

# Columns: username, userprincipalname, name, email, phone, address, city
def generateSource(amount: int = 10000):
    fakedataSrc = []
    usernames = [""]
    
    for i in tqdm(range(amount)):
        obj = {
            "name": faker.name(),
            "username": "",
            "userprincipalname": "",
            "email": faker.email(),
            "phone": faker.phone_number(),
            "address": faker.street_address(),
            "city": faker.city()
        }
        nameparts = obj["name"].split(" ")
        j = 0
        while obj["username"] in usernames:
            obj["username"] = nameparts[0].lower()[0:3] + nameparts[1].lower()[0:3] + str(j)
            j += 1
        
        obj["userprincipalname"] = f"{nameparts[0]}.{nameparts[1]}{j}@example.com"
        
        fakedataSrc.append(obj)

    return fakedataSrc

def generateDestination(fakedataDst, amount: int = 10000):
    # Columns: username, userprincipalname, name, email, phone, address, city
    usernames = [x['username'] for x in fakedataDst]
    choices = ("name", "email", "phone", "address", "city")
    updates = [randint(0, int(amount*0.75)) for i in range(int(amount*0.25))]


    for i in tqdm(updates):
        tmp = choices[0:randint(1, 5)]
        for z in tmp:
            fakedataDst[i][z] = fakedataDst[i][z][::-1]

    for i in tqdm(range(int(amount*0.25), int(amount-1))):
        obj = {
            "name": faker.name(),
            "username": "",
            "userprincipalname": "",
            "email": faker.email(),
            "phone": faker.phone_number(),
            "address": faker.street_address(),
            "city": faker.city()
        }
        nameparts = obj["name"].split(" ")
        j = 0
        while obj["username"] in usernames:
            obj["username"] = nameparts[0].lower()[0:3] + nameparts[1].lower()[0:3] + str(j)
            j += 1
        
        obj["userprincipalname"] = f"{nameparts[0]}.{nameparts[1]}{j}@example.com"
        
        fakedataDst[i] = obj

    return fakedataDst


# Database
# Source Table
# name: src
# fields: id, username, userprincipalname, password, email, phone, address, city
# Target Table
# name: dst
# fields: id, username, userprincipalname, password, email, phone, address, city
def insert_mysql(sourceData, destinationData):
    meta = sqmetadata()
    # Start connection to mysql database
    # ngine = sqla.create_engine('mysql+pymysql://tester:hoglandet@localhost/SingleID')
    ngine = sqla.create_engine(
        'mysql://' +
        'root:Password123@' +
        "127.0.0.1:13306" +
        "/Test"
    )
    
    @sqla.event.listens_for(ngine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    # Drop table if exists
    srcTable = sqtable(
        'src', meta,
        sqcolumn('id', sqinteger, primary_key=True, autoincrement=True),
        sqcolumn('name', sqstring(255)),
        sqcolumn('username', sqstring(255)),
        sqcolumn('userprincipalname', sqstring(255)),
        sqcolumn('email', sqstring(255)),
        sqcolumn('phone', sqstring(255)),
        sqcolumn('address', sqstring(255)),
        sqcolumn('city', sqstring(255))
    )

    dstTable = sqtable(
        'dst', meta,
        sqcolumn('id', sqinteger, primary_key=True, autoincrement=True),
        sqcolumn('name', sqstring(255)),
        sqcolumn('username', sqstring(255)),
        sqcolumn('userprincipalname', sqstring(255)),
        sqcolumn('email', sqstring(255)),
        sqcolumn('phone', sqstring(255)),
        sqcolumn('address', sqstring(255)),
        sqcolumn('city', sqstring(255))
    )

    srcTable.drop(ngine, checkfirst=True)
    dstTable.drop(ngine, checkfirst=True)

    meta.create_all(ngine)    

    # Insert data into database
    pd.DataFrame(sourceData).to_sql('src', con=ngine, if_exists='append', index=False)
    pd.DataFrame(destinationData).to_sql('dst', con=ngine, if_exists='append', index=False)



# Database
# Source Table
# name: src
# fields: id, username, userprincipalname, password, email, phone, address, city
# Target Table
# name: dst
# fields: id, username, userprincipalname, password, email, phone, address, city
def insert_mssql(sourceData, destinationData):
    meta = sqmetadata()
    # Start connection to mysql database
    ngine = sqla.create_engine(
        'mssql://' +
        'sa:Password123@' +
        "127.0.0.1" +
        "/Test" +
        "?TrustServerCertificate=yes" +
        "&Encrypt=no" +
        "&driver=" + pyodbc.drivers()[0].replace(' ', '+')
    )
    
    @sqla.event.listens_for(ngine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    # Drop table if exists
    srcTable = sqtable(
        'src', meta,
        sqcolumn('id', sqinteger, primary_key=True, autoincrement=True),
        sqcolumn('name', sqstring(255)),
        sqcolumn('username', sqstring(255)),
        sqcolumn('userprincipalname', sqstring(255)),
        sqcolumn('email', sqstring(255)),
        sqcolumn('phone', sqstring(255)),
        sqcolumn('address', sqstring(255)),
        sqcolumn('city', sqstring(255))
    )

    dstTable = sqtable(
        'dst', meta,
        sqcolumn('id', sqinteger, primary_key=True, autoincrement=True),
        sqcolumn('name', sqstring(255)),
        sqcolumn('username', sqstring(255)),
        sqcolumn('userprincipalname', sqstring(255)),
        sqcolumn('email', sqstring(255)),
        sqcolumn('phone', sqstring(255)),
        sqcolumn('address', sqstring(255)),
        sqcolumn('city', sqstring(255))
    )

    srcTable.drop(ngine, checkfirst=True)
    dstTable.drop(ngine, checkfirst=True)

    meta.create_all(ngine)    

    # Insert data into database
    pd.DataFrame(sourceData).to_sql('src', con=ngine, if_exists='append', index=False)
    pd.DataFrame(destinationData).to_sql('dst', con=ngine, if_exists='append', index=False)

if __name__ == '__main__':
    fakedataSrc = generateSource(10000)
    fakedataDst = generateDestination(fakedataSrc, 10000)
    insert_mysql(fakedataSrc, fakedataDst)
    insert_mssql(fakedataSrc, fakedataDst)
