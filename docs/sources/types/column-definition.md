# Column definitions
Column definitions define the columns at compare time in the sync process.

At this time some sources and transformers use columns to perform changes, this will most likely change in the future.
```python
{
  # name, string, (mandatory)
  "name": "column-name",
  # type, string, (mandatory)
  # String representation of polar types.
  # https://docs.pola.rs/api/python/stable/reference/datatypes.html
  "type": "Int32",
  # unique, boolean, (optional), default=False
  # Column will be used to determine the unique identity of a row.
  # If false the column will be used to determine if the row has changed.
  "unique": False ,
  # skip_update, boolean, (optional), default=False
  # If True, the destination will not have this column updated.
  "skip_update": False,
  # custom_options, Dict, (optional), default=None
  # Is only used for certain transformers itop transformers.
  "custom_options": None
}
```
