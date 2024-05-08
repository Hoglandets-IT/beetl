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