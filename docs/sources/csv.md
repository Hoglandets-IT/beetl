# CSV
This page is a work in progress..

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
        # nype: string
        # Identifies the type of diff destination to use
        type: Csv
        # name: string
        # Points to a destination defined in the sources section by name
        name: diffsourcename
        # config: dict
        # The destination type specific configuration
        # The csv source does not have any specific config
        config: {}
```

The file will be automatically created if it doesn't exist. Otherwise beetl will read its contents and extend it.