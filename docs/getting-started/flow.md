
# Flow
## Diagram
Scroll down to see more in detail information about each step.
```mermaid
 flowchart TD
  subgraph init[Initialization]
  raw_config[YAML/JSON/PYDICT configuration] -- Parsing + Validation --> built_config[Built configuration]
  built_config -- Initialization --> beetl[Beetl instance]
  end
  subgraph query[Querying data]
  beetl -- Source Initialization --> source[Source adapter] & destination[Destination adapter]
  end
  subgraph source_transformation[Transforming input]
  source -- Query --> original_source_dataset[Original source dataset]
  destination -- Query --> original_destination_dataset[Original destination dataset]
  original_source_dataset -- Source Transformation --> source_dataset[Transformed source dataset]
  original_destination_dataset -- Source Transformation --> destination_dataset[Transformed destination dataset]
  end
  subgraph comparing[Comparing data]
  source_dataset & destination_dataset -- Compare datasets --> compare_result[Comparison result]
  compare_result --> create[Create dataset] & update[Update dataset] & delete[Delete dataset]
  end
  subgraph insertion_transformation[Transforming output]
  create -- Apply insertion transformers --> transformed_create[Transformed create dataset]
  update -- Apply insertion transformers --> transformed_update[Transformed update dataset]
  delete -- Apply deletion transformers --> transformed_delete[Transformed delete dataset]
  end
  subgraph action[Applying changes]
  transformed_create -- Destination adapter insert --> result[Beetl sync result]
  transformed_update -- Destination adapter update --> result
  transformed_delete -- Destination adapter delete --> result
  end
```

## Initialization
First beetl will instantiate a BeetlConfig from the YAML, JSON or Dictionary configuration that you provided. Beetl will detect the version specified in the configuration in order to be backwards compatible. In addition to this a sanity check will be performed to make sure mandatory properties are present.

After that the beetl instance will be initialized and the sources for your data created.

## Querying data
Beetl requests the original dataset from your source and destination.

For MongoDB database sources, columns with the type of ObjectId will be automatically converted to strings.


## Transforming input
Beetl will apply the source and destination transformers in the order that you declared in the configuration. The result is a transformed source and destination dataset that will be used for comparison.

## Comparing data
The goal of comparing the data is to provide a comparison result in the form of three new datasets.

1. Create: The rows that will be inserted at the destination.
   - These are the items present in source but not present in destination.
1. Update: The rows that will be updated in the destination.
   - These are the items present in both but with columns that differ in value.
1. Delete: The row that will be deleted from the destination.
   - These are the item present in destination but not in source.

Beetl will use the columns property of your source and destination configuration to identify unique rows across the datasets and compare their values.

The resuling datasets will have the schema of the transformed source dataset.

## Transforming output
Beetl will now apply the intertionTransformers to the Create and Update datasets, while applying the deletionTransformers to the Delete dataset.

## Applying changes
The changes are sent to the destination source insert, update and delete methods to be applied to the destination.

The Result object returned from the beetl.Sync method contains information about the insert, update and delete count.
