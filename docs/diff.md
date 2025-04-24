# Diff Functionality

The **diff functionality** in our application allows you to track and store the changes made during each run of a sync. This includes **inserts**, **updates**, and **deletes**. The diff object provides a structured summary of the differences, making it easy to audit and analyze changes between sync runs.

## Example Output

Below is an example of the diff output structure:

```json
{
  "name": "name",
  "date": "2025-04-03T13:43:42.372858",
  "uuid": "53d684c1-5639-4efe-a455-760ff7494f1f",
  "version": "1.0.0",
  "updates": [
    {
      "old": { "id": 2, "name": "test2" },
      "new": { "id": 2, "name": "test2-updated" }
    }
  ],
  "inserts": [
    { "id": 1, "name": "test1" }
  ],
  "deletes": [
    { "id": 2 }
  ],
  "stats": {
    "updates": 1,
    "inserts": 1,
    "deletes": 1
  }
}
```

- `updates`: List of changed records, including old and new values.
- `inserts`: List of newly inserted records.
- `deletes`: List of deleted record identifiers.
- `stats`: Summary of the number of each operation.

The output might be stored in different ways depending on the destination.
## How to Enable Diff Tracking

To enable diff tracking in your sync, you need to define a `diff` property inside the `sync` section of your YAML configuration.

### YAML Configuration Example

```yaml
version: V1
sources:
  - name: src
    type: Sqlserver
    connection: 
      settings:
        connection_string: connection_string
  - name: dst
    type: Sqlserver
    connection: 
      settings:
        connection_string: connection_string
  - name: diff
    type: Sqlserver
    connection: 
      settings:
        connection_string: connection_string
sync:
  - name: sync-with-diff
    source: src
    sourceConfig:
      table: "src_table"
    destination: dst
    destinationConfig:
      table: "dst_table"
    diff:
      destination:
        type: Sqlserver
        name: diff
        config:
          table: sync_diffs
      transformers:
        - transformer: frames.rename_columns
          config:
            columns:
              - name: full_name
              - id: identifier
```

### Configuration Details

- **`destination`**: Defines where the diff data will be stored.
  - `type`: The type of the destination (e.g., `Sqlserver`).
  - `name`: The name of the destination source (must match a source defined in the `sources` section).
  - `config`: Type-specific configuration. For Sqlserver, this includes:
    - `table`: The name of the table where diffs will be written.

Check out the documentation for the specific source types to see what config values are available.

- **`transformers`**: A list of transformers applied to the diff data before it's written to the destination. These follow the same pattern as other transformers used in your configuration and are applied on updates, inserts and deletes before being converted into the diff object.
