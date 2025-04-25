# Static
The static source has predefined data defined by you in the configuration.
This source is very good for testing out a sync with static data and is often used for internal beetl development.

- Type identifier: `Static`

## Source Configuration 
Declare the data in the source configuration
```yaml
sources:
  - name: src
    type: Source
    connection:
      # static: <list<dict<string, any>>>
      # List of dictionaries representing the data in the source..
      # You can think of the dictionaries as rows in a database table.
      static: 
        - id: 1
          name: John
        - id: 2
          name: Jane
```

## Sync Settings
There are no static specific sync settings. Pass an empty object.
```yaml
sync:
  - source: src
    sourceConfig: {}
    destination: dst
    destinationConfig: {}
```

## Diff settings
The static source will simply write the json serialized diff to the terminal output.
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
        type: Static
        # name: string
        # Points to a destination defined in the sources section by name
        name: diffsourcename
        # config: dict
        # The destination type specific configuration
        # The Static source does not have any specific config, pass an empty dict.
        config: {}
```
