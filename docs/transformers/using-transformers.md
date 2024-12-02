# Using Transformers

Transformers as the name suggests transform data to a different state.


For example, the [int.to_int64](/transformers/int.html#to-int64) casts a field from int32 to int64, while [structs.compose_struct](/transformers/structs.html#compose-struct) composes structs from multiple fields on each row.


They are powerful and allow for mutation and restructuring of data in multiple ways.

There are four steps in the beetl sync process where transformers are used:
- After fetching data from the source.
- After fetching data from the destination.
- Before inserting into the destination.
- Before deleting from the destination.

All transformers work the same way regardless of where in the process they are used. Read more about when transformers are used below.

## Source and destination transformers

Source and destination transformers are applied on the DataFrame created from the data fetched from the source and destination prior to comparing them against each other.

You can use these steps to mutate the data into a format where the source and destination schema is the same so that they are able to be compared.
For example, you might have a document database with addresses that you want to sync into a relational database table. When doing this you might want to use the [structs.jsonpath](/transformers/structs.html#jsonpath) transformer multiple times to flatten the address document into a row with multiple fields.

```yaml
sync:
  - source: MongoDB
    sourceConfig:
      ...
    destination: MySQL
    destinationConfig:
      ...
    sourceTransformers:
      - transformer: structs.jsonpath
        config:
          inField: address
          jsonPath: $.city
          outField: city
      - transformer: structs.jsonpath
        config:
          inField: address
          jsonPath: $.street
          outField: street
      - transformer: structs.jsonpath
        config:
          inField: address
          jsonPath: $.zip
          outField: zip
      # The project_colums transformer will only keep the columns specified in the input, dropping the original address struct column.
      - transformer: frames.project_columns
        config:
          columns: ["city", "street", "zip"]
```

The DataFrame that you have after the source transformer is then used when inserting into the destination, so transforming the source data into the destination format is usually how this is used. But you can ofcourse transform the destination data prior to comparing as well.

## Insertion and deletion transformers

These transformers only apply to the source DataFrame before the insertions or deletions are being carried out by the destination data source adapter.

An example of when these are needed is when you work with MongoDB databases as both source and destination. Since beetl is converting MongoDB's ObjectId's to strings automatically for comparison reasons, you might want to convert these back into their original form before trying to insert of delete. You can use the [strings.to_object_id](/transformers/strings.html#to-object-id) in the insertionTransformers and deletionTransformers section to do so.

```yaml
sync:
  - source: MongoDB
    sourceConfig:
      ...
    destination: MongoDB
    destinationConfig:
      ...
    insertionTransformers:
      - transformer: strings.to_object_id
        config:
          inField: _id
    deletionTransformers:
      - transformer: strings.to_object_id
        config:
          inField: _id
```
