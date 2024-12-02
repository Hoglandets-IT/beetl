# Struct Transformers
The struct transformers apply on all objects of a column

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
```
