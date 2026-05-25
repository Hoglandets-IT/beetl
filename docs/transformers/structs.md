# Struct Transformers
The struct transformers apply on all objects of a column


## Compose struct
Composes a struct from fields on each row

```yaml
- transformer: structs.compose_struct
  config:
    # map: A mapping of struct field names to column names from which the values should be taken (Mandatory)
    map: {
      "city": "city",
      "street": "street_address",
      "zip": "zip_code"
    }
    # outField: The destination field.
    outField: address
```

### Example

If the configuration above was used on this DataFrame:

| city        | street_address     | zip_code |
| ----------- | ------------------ | -------- |
| "new york"  | "Kennedy st. 1"    | "11224"  |
| "stockholm" | "Drottninggatan 5" | "55667"  |

The resulting DataFrame would look like this:

| city        | street_address     | zip_code | address                                                             |
| ----------- | ------------------ | -------- |---------------------------------------------------------------------|
| "new york"  | "Kennedy st. 1"    | "11224"  | `{"city": "new york", "street": "Kennedy st. 1", "zip": "11224"}`     |
| "stockholm" | "Drottninggatan 5" | "55667"  | `{"city": "stockholm", "street": "Drottninggatan 5", "zip": "55667"}` |

You could then, as an example, follow up with the `frames.project_columns` transformer to drop all other columns but `address` and insert it into a document database.


## Compose list of structs
Composes a list of structs from list fields on each row
Much like [structs.compose_struct](#compose-struct) but operates on lists and outputs a list of structs at the destination column.

If the list are of differing lengths the transformer will fill the shorter ones with None values.
```yaml
- transformer: structs.compose_list_of_structs
  config:
    # map: A mapping of struct field names to column names from which the values should be taken (Mandatory)
    map: {
      "id": "group_ids",
      "technical_name": "group_technical_names",
      "display_name": "group_display_names" 
    }
    # outField: The destination field.
    outField: groups

```

### Example:

If the configuration above was used on this DataFrame:

| group_ids | group_technical_names | group_display_names |
| --------- | --------------------- | ------------------- |
| `[1, 2]`      | `["admin", "user"]`       | `["Administrator"]`     |

The resulting DataFrame would look like this:

| group_ids | group_technical_names | group_display_names | groups |
| --------- | --------------------- | ------------------- | ------ |
| `[1, 2]`      | `["admin", "user"]`       | `["Administrator"]`     | `[{"id": 1, "technical_name": "admin", "display_name": "Administrator"},{"id": 2, "technical_name": "user", "display_name": None}]` |

You could then, as an example, follow up with the `frames.project_columns` transformer to drop all other columns but `groups` and insert it into a document database.



## Json Path
Extracts a value from a json-, object- or listfield. Strings are interpreted as object keys, numbers are first tried as list indicies, otherwise as object keys

```yaml
- transformer: structs.jsonpath
  config:
    inField: field_name
    outField: new_field
    jsonPath: "path.to.value.0"
    # defaultVal: "default_value" (Optional, default empty)

```
## Staticfield
Adds a static field to the dataset

```yaml
- transformer: structs.staticfield
  config:
    field: new_field
    value: "static_value"
    # only add the static field if it is missing from the dataset
    # default=False
    only_add_if_missing: False
```

## List Columns to Key Value Rows
Converts a list-valued column into key-value rows. Each processed column name is used as the key, and each item in that column's list becomes a separate row.

This is useful when an object/map has been normalized into DataFrame columns, where the original object keys are now column names and the column values are lists.

```yaml
- transformer: structs.list_columns_to_key_value_rows
  config:
    # keyField: The destination field that will contain the source column name. (Mandatory)
    keyField: id
    # listField: The destination field that will contain each item from the list. (Mandatory)
    listField: names
    # objSource: Only process this column. If omitted, all columns are processed. (Optional)
```

### Example
If the configuration above was used on this DataFrame:

|       someID      | 
| ----------------- | 
| `["John", "Jane"]`| 

You would get the following DataFrame:

|   id   |  name  |
| ------ | ------ | 
| someID | "John" |
| someID | "Jane" |

If used with a vlue for objSource it would then use the it would transform that column.