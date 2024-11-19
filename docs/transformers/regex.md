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