# Changenotes

## WIP: 0.5.0

### Breaking changes
- MySQL source now defaults to selecting all columns from the target table `SELECT * FROM <table>`.
  - Previously if no query was specified, the source composed a query based on the sync columns. This is now being removed, see information about ongoing changes to sync columns below. 
  - If you were using this automatic fallback functionality you will now have to provide a query in the source configuration.
- same for postgres
- same for sqlserver
- iTop
   - source no longer fetches fields defined as unique columns + comparison columns + columns that has custom_options.itop.comparison_field populated. Instead you have to explicitly provide the fields in the comparison_columns prop
   - In addition, if a field that was fetched only to have a value to compare against in the transformer should be provided in link_columns, old custom.itop.comparison_field
   - skip_columns old column.skip_update
- comparisonColumns are mandatory


### New features
- Introducing deletion transformers.
   - If your destination requires a unique identifier to be in a specific format you can now perform tranformations on those fields.
- MongoDB is now supported as source and destination [documentation](documentation) 
- PostgreSQL is now supported as source and destination
- Transformers
  - frames.project_columns: Select columns of a dataframe that you want to keep, drop the rest.
  - int.to_int64 transformer: Cast an Int32 column into an Int64 column.
  - strings.split_into_listfield: Split a string column into a list column using a string to split on.
  - strings.join_listfield: Now supports both array and list datatype as source.
  - strings.to_object_id: Transform a string column into a bson.ObjectId column. Useful for MongoDB integrations.
  - structs.compose_struct: Specify multiple column names and compose a new column of structs with the values of those fields mapped to your liking.
  - structs.compose_list_of_structs: Does the same as structs.compose_structs but works on list columns to create a list of structs.
  - structs.jsonpath: Support for the `*` selector which allows selecting properties nested within lists.

### Changes to sync columns
With development on new document database sources it's becoming clear that the source configuration columns list is being used to define the data structure of both the source data and the data being compared. This is now clearly not always the case since transformers can be applied to vastly change the structure between query and comparison.

To accomodate the need of allowing the data in these two stages of the sync to look different we will also need to change the configuration.

// TODO: Add more information here when we know the direction this is taking.

### For developers
- Dependencies updated
   - Polars version bumped from 0.17.5 to 1.14.0.
   - Pandas version was locked at 2.2.3.
- Function to compare datasets now return the rows to create, update and delete in the format of the source table instead of the keys that are part of the columns list.
   - This is done to create a separation between comparing the data, and modifying the data which should be done by transformers.
- Beetl result now defaults to no changes (0) when none are passed as arguments
