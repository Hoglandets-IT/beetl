# String Tranformers
Various string transformers that can be applied to a column.

## Add Prefix
Adds a prefix to a string

```yaml
- transformer: strings.add_prefix
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
    prefix: "prefix_"
```

## Staticfield
Adds a static field to the dataset

```yaml
- transformer: strings.staticfield
  config:
    field: new_field
    value: "static_value"
```

## Set Default
Sets a default value for a field if it is missing

```yaml
- transformer: strings.set_default
  config:
    inField: field_name
    default: "default_value"
```

## Strip
Strips given characters from a string field

```yaml
- transformer: strings.strip
  config:
    inField: field_name
    stripChars: "characters_to_strip"
    # outField: new_field (Optional, default is same as inField)
```

## Lowercase
Converts a string to lowercase

```yaml
- transformer: strings.lowercase
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
    # inOutMap: (can be used instead of inField/outField)
    #   field_name: new_field
    #   field2_name: new_field2
    # 
```

## Uppercase
Converts a string to uppercase

```yaml
- transformer: strings.uppercase
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
    # inOutMap: (can be used instead of inField/outField)
    #   field_name: new_field
    #   field2_name: new_field2
    # 
```

## Join
Joins multiple fields into a single string

```yaml
- transformer: strings.join
  config:
    inFields:
      - field_a
      - field_b
    outField: new_field
    separator: ", "
```

## Join Listfield
Joins a list field into a single string

```yaml
- transformer: strings.join_listfield
  config:
    inField: field_name
    outField: new_field
    separator: ", "
```

## Split
Split a given field into multiple fields

```yaml
- transformer: strings.split
  config:
    inField: field_name
    outFields:
      - field_a
      - field_b
    separator: ","
```

## Quote
Adds quotes around a string field, useful for some insertions into databases

```yaml
- transformers: strings.quote
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
```

## Replace
Replaces a string with another string

```yaml
- transformers: strings.replace
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
    search: "search_string"
    replace: "replace_string"
```

## Replace All
Replaces all occurrences of a string with another string

```yaml
- transformers: strings.replace_all
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
    search: "search_string"
    replace: "replace_string"
```
## Split into Listfield
Splits a string by specified separator.

```yaml
- transformers: strings.split_into_listfield
  config:
    # inField: Name of field to transform (Mandatory)
    inField: "old_field"
    # outField: Destination of transformed fields (Optional, defaults to value of inField)
    outField: "new_field"
    # separator: Value to split the string by (Optional, defaults to "")
    separator: ","
```

## Substring
Extracts a substring from a string

```yaml
- transformer: strings.substring
  config:
    inField: field_name
    # outField: new_field (Optional, default is same as inField)
    start: 1
    # length: 5 (Optional, default is until end of string)
```
## To object id
Casts values of a string column into [bson.ObjectId's](https://pymongo.readthedocs.io/en/stable/api/bson/objectid.html).

```yaml
- transformer: strings.to_object_id
  config:
    # inField: Name of the field to convert (Mandatory)
    inField: field_name

```

## Format
Interpolates the original value into a string template.

If you have the value `123` and the template `The value is {value}!` the resulting string will be `The value is 123!`.

You can place `{value}` anywhere in the string and it will be substituted with the actual value.

```yaml
- transformer: strings.format
  config:
    # inField: <string> (Mandatory)
    # Name of the field to convert
    inField: field_name
    # outField: <string> (Optional, default=same as inField)
    # Name of the field where the output should be placed.
    outField: field_name
    # format_string: <string> (Optional, default="{value}")
    # The format string where the value will be interpolated into.
    format_string: "This is the value: {value}"
```