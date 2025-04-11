# Regex Transformers
Apply regular expressions on entire columns

## Replace
Replace a pattern in a column with a given string

```yaml
- transformer: regex.replace
  config:
    inField: input_field
    outField: output_field
    query: "pattern"
    replace: "replacement"
    maxN: -1
```

## Match
Extract the target capture group from a provided expression.

```yaml
- transformer: regex.match_single
  config:
    inField: input_field
    # outField: output_field (Optional)
    query: "pattern"
```