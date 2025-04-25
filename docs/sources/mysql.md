## Diff settings
Configure the diff config as following.

```yaml
sync:
  - name: test
    source: srcname
    destination: dstname
    sourceConfig: {}
    destinationConfig: {}
    diff:
      destination: 
        # type: string
        # Identifies the type of diff destination to use
        type: Mysql
        # name: string
        # Points to a destination defined in the sources section by name
        name: diffsourcename
        # config: dict
        # The destination type specific configuration
        config:
          # table: string
          # The table to use in the database
          table: difftablename
```
Make sure that your diff table exists in the destination with the following schema:

| Column Name | Type         | Constraints          |
| :---------- | :----------- | :------------------- |
| uuid        | CHAR(36)     | NOT NULL PRIMARY KEY |
| name        | VARCHAR(255) |                      |
| date        | DATETIME     |                      |
| version     | VARCHAR(64)  |                      |
| updates     | TEXT         |                      |
| inserts     | TEXT         |                      |
| deletes     | TEXT         |                      |
| stats       | TEXT         |                      |


