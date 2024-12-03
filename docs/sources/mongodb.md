# MongoDB
The MongoDB source will load and save data to MongoDB servers.

- Type identifier: `Mongodb`

The MongoDB source has quirks related to their unique implementation of indentifiers see [Quirks](#quirks) below.

## Source Configuration 
Declare how the source should be connecting to the database instance.
```yaml
sources:
  - name: database_1
    type: Mongodb
    connection:
      settings:
        # connection_string, <string>, (optional*)
        # Mandatory if [host, port, username, password, database] aren't specified.
        # If provided it takes precedence over [host, port, username, password, database]
        # Database and collection is declared in the sync settings
        connection_string: "mongodb://[username]:[password]@[host]:[port]"
        # host: <string>, (optional)
        # Can be ip-address or hostname
        host: "localhost"
        # port: <string>, (optional)
        port: "1234"
        # username: <string>, (optional)
        username: "admin"
        # password: <string>, (optional)
        password: "password"
        # database: <string>, (mandatory)
        # Name of the database within the database server
        database: "database-name"
```

## Sync Settings
Declare how the data should be queried and the schema for comparing the source with the destination.
```yaml
sync:
  - source: database_1
    sourceConfig:
      # collection: <string>, (mandatory)
      # Name of the collection to sync from
      collection: "people"
      # projection, <object>, (optional), default=all fields
      # Declares what fields will be queried from the collection.
      # https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
      projection: 
        _id: 1,
        name: 1,
        email: 1,
        address: 0
      # unique_fields, <List<string>>, (mandatory)
      # Declares what fields will be used to define a unique row.
      # Is used on insert and delete.
      unique_fields: ["_id"]
      # columns, <List[ColumnDefinition]>, (mandatory)
      # Defines how the data will look at compare time
      # See /sources/types/column-definition.html for schema.
      # Id will define a rows identity, while name will be compared against the name of the row in destination sharing the same identity.
      columns: [
        {
          "name": "id",
          "type": "Utf8",
          "unique": True
        },
        {
          "name": "name",
          "type": "Utf8",
        },
        {
          "name": "email",
          "type": "Utf8",
        }
      ]
    # The mongodb configs are the same regardless of direction of the sync
    destination: database_1
    destinationConfig:
      collection: "people"
      projection: 
        _id: 1,
        name: 1,
        email: 1,
        address: 0
      columns: [
        {
          "name": "id",
          "type": "Utf8",
          "unique": True
        },
        {
          "name": "name",
          "type": "Utf8",
        },
        {
          "name": "email",
          "type": "Utf8",
        }
      ]
```
*Related types:*
- [ColumnDefinitions](/sources/types/column-definition.html)

## Quirks
### ObjectId'
MongoDB uses ObjectId's as identifiers.

Polars, the package we're using to perform data manipulation and comparisons, lacks support for this type. Due to that ObjectId's are automatically casted to their string representations.

This makes it easy for you to compare uniqueness of rows when syncing from a mongodb database to e.g. a relational database.

However, if you want to sync with mongodb as destination and keep the ObjectId type in the database you will need to use the transformer `strings.to_object_id`.

For an example, see [From MongoDB to MongoDB](#from-mongodb-to-mongodb) below.

## Example

### From MongoDB to MongoDB
This example syncs data from table `people` in database 1 to table `people` in database 2.

Since ObjectId's are being converted to string automatically we use the `strings.to_object_id` transformer upon insert and delete.


#### What will happen?
**People of database 1**

```json
[
  {
    "_id": ObjectId("1"),
    "name": "John Doe",
    "email": "johndoe@example.com",
    "city": "New York"
  },
  {
    "_id": ObjectId("2"),
    "name": "Jane Doe",
    "email": "janedoe@example.com",
    "city": "London"
  },
  {
    "_id": ObjectId("3"),
    "name": "Jim Doe",
    "email": "jimdoe@example.com",
    "city": "Stockholm"
  }
]
```

**People of database 2**

```json
[
  {
    "_id": ObjectId("1"),
    "name": "John Doe",
    "email": "johndoe@example.com",
  },
  {
    "_id": ObjectId("2"),
    "name": "Jane Doe",
    "email": "janedoe2@example.com",
  },
  {
    "_id": ObjectId("4"),
    "name": "July Doe",
    "email": "julydoe@example.com",
  }
]
```

**Result**
- Id will be used to uniquely identify each row across the databases.
- Name and email will be compared to see if a row needs updating or not.
- All rows in in database 1 not present in database 2 will be created.
- All rows in database 2 not present in database 1 will be deleted.
- City will be ignored as it has been excluded from the source by projection.


**People of database 2 after sync**

```json
[
  {
    "_id": ObjectId("1"),
    "name": "John Doe",
    "email": "johndoe@example.com",
  },
  {
    "_id": ObjectId("2"),
    "name": "Jane Doe",
    "email": "janedoe@example.com",
  },
  {
    "_id": ObjectId("3"),
    "name": "Jim Doe",
    "email": "jimdoe@example.com",
  }
]
```
- Jane Doe's email was updated.
- Jim Doe was created
- July Doe was dropped

#### Configuration

```yaml
sources:
  - name: database_1
    type: Mongodb
    connection:
      settings:
        connection_string: "mongodb://admin:password@localhost:1234"
        database: "database-1"
  - name: database_2
    type: Mongodb
    connection:
      settings:
        host: "localhost"
        port: "1234"
        username: "admin"
        password: "password"
        database: "database-2"
sync:
  - source: database_1
    sourceConfig:
      collection: "people"
      projection:
        _id: 1
        name: 1
        email: 1
      unique_fields: ["_id"]
      columns: [
        {
          "name": "_id",
          "type": "Utf8",
          "unique": True
        },
        {
          "name": "name",
          "type": "Utf8",
        },
        {
          "name": "email",
          "type": "Utf8",
        },
      ]
    destination: database_2
    destinationConfig:
      collection: "people"
      unique_fields: ["_id"]
      columns: [
        {
          "name": "_id",
          "type": "Utf8",
          "unique": True
        },
        {
          "name": "name",
          "type": "Utf8",
        },
        {
          "name": "email",
          "type": "Utf8",
        },
      ]
    # These transformers makes sure that the database is being sent ObjectIds's of the _id field values instead of the string representations being used in beetl. 
    insertionTransformers:
      - transformer: strings.to_object_id
        config:
          inField: _id
    deletionTransformers:
      - transformer: strings.to_object_id
        config:
          inField: _id
```
**links**:
- [strings.to_object_id](/transformers/strings#to-object-id)
