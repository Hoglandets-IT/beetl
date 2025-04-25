---
name: Transformer Request
about: Suggest or request additional transformers
title: "[Transformer Request] - "
labels: enhancement, good first issue
assignees: ''

---

**What kind of transformer would you like to add?**
A short description of the transformer to add, ex. "Transformer to change all letters to uppercase"

**Describe the solution you'd like**
Describe the transformer and what you would like to be able to do with it., eg. "Transform all letters to uppercase"

** (optional) Add some Python code in the below template to demonstrate**
```python
# data: polars.DataFrame containing all the data to be transformed
# inField: Input field (if applicable)
# outField: Output field (if applicable). If left empty, this should default to the same as the input field.
# inOutMap: A list of input: output fields to apply the transformer to (if applicable)
func transform(data: polars.DataFrame, inField: str = None, outField: str = None, inOutMap: dict = None):
      if len(inOutMap) == 0:
            inOutMap = {
                inField: (
                    outField if outField is not None and outField != "" else inField
                )
            }

        for inf, outf in inOutMap.items():
            data = data.with_columns(data[inf].str.to_uppercase().alias(outf))
    
        return data
```
