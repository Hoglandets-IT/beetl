# Column definitions
Column definitions define the columns at compare time in the sync process.

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
}
```
