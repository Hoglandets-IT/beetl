# Using Transformers

Transformers as the name suggests transform data to a different state.

They are powerful and allow for mutation and restructuring of data in multiple ways.

Transformers can be applied four times during the sync process:
- After fetching data from the source.
- After fetching data from the destination.
- Before inserting into the destination.
- Before deleting from the destination.

All transformers work the same way regardless of where in the process they are used. Read more about when transformers are used below.

## Source and destination transformers

Source and destination transformers are applied on the datasets created when fetching from the sources defined in your configuration.

Usually they are used to achieve a common structure between your sources for easy comparison.

Let's take a real world example: You have a document database as a source that contains address information in nestled documents. You want to sync these to a relational database and therefor want to flatten the document structure to a single row. You can achieve that using the [structs.jsonpath](/transformers/structs.html#jsonpath) transformer multiple times like this. 

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

## Insertion and deletion transformers

These transformers are applied to the source dataset prior to insertion of new rows or deletion of existing rows in the destination.

As an example you might want to use these when you work with a MongoDB database as both the source and the destination. Since beetl is converting MongoDB's ObjectId's to strings automatically for comparison reasons, you have to convert these back into their original form before insertion or deletion. That can be easily done using the [strings.to_object_id](/transformers/strings.html#to-object-id) transformer.

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
