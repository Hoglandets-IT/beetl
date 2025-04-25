# Faker
The faker source has not yet been implemented completely. Its purpose is to automatically generate data for you but as of right now it just functions exactly like the static source. If you are looking to use static data, then use the static source instead.

- Type identifier: `Faker`

## Source Configuration 
Declare the data in the source configuration
```yaml
sources:
  - name: src
    type: Faker
    connection:
      # faker: <list<dict<string, any>>>
      # List of dictionaries representing the data in the source..
      # You can think of the dictionaries as rows in a database table.
      faker: 
        - id: 1
          name: John
        - id: 2
          name: Jane
```

## Sync Settings
There are no faker specific sync settings. Pass an empty object.
```yaml
sync:
  - source: src
    sourceConfig: {}
    destination: dst
    destinationConfig: {}
```

## Diff settings
The faker source will simply write the json serialized diff to the terminal output.
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
        type: Faker
        # name: string
        # Points to a destination defined in the sources section by name
        name: diffsourcename
        # config: dict
        # The destination type specific configuration
        # The Faker source does not have any specific config, pass an empty dict.
        config: {}
```
