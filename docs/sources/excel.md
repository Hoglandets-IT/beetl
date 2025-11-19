# Excel

## Source Configuration
```yaml
sources:
  - name: src
    type: Excel
    connection:
      path: "path/to/excel.xlsx"
```

## Sync Settings
The Excel source does not have any sync settings and can be left empty.
```yaml
sync:
  - source: src
    sourceConfig: {}
```

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
        type: Excel
        # name: string
        # Points to a destination defined in the sources section by name
        name: diffsourcename
        # config: dict
        # The destination type specific configuration
        # The excel source does not have any specific config
        config: {}
```

The file will be automatically created if it doesn't exist. Otherwise beetl will read its contents and extend it.
