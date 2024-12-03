# Integer Transformer
The integer transformer performs operations on integer values.

## Divide
The divide transformer divides the input integer by a divisor.

```yaml
- transformer: int.divide
  config:
    inField: input_field
    outField: output_field
    inType: Int64
    outType: Int32
    factor: 2
```

## FillNA
The fillNA transformer replaces NaN and null values with a specified integer.

```yaml
- transformer: int.fillNA
  config:
    inField: input_field
    outField: output_field
    value: 0
```


## To Int64
Converts Int32 fields to Int64

```yaml
- transformer: int.to_int64
  config:
    # inField: The field to convert (Mandatory)
    inField: input_field
    # outField: The destination field (Optional, defaults to inField value)
    outField: output_field
```
