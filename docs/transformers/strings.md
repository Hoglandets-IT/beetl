# String Tranformers
Various string transformers that can be applied to a column.

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