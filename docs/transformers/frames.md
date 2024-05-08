# Frames Transformers
The frames transformers will perform actions on the structure of the dataset, such as adding, removing, or renaming columns. The transformers are applied in the order they are defined in the configuration file.

## Rename Columns
```yaml
- transformer: frames.rename_columns
  config:
    columns:
      - from: old_name
        to: new_name
```

## Copy/Duplicate Columns
```yaml
- transformer: frames.copy_columns
  config:
    columns:
      - from: old_name
        to: new_name
```

## Drop Columns
```yaml
- transformer: frames.drop_columns
  config:
    columns:
      - column_name
```

## Extract nested rows
This is a more advanced feature that will extract nested rows into their own dataset. This is useful when you have a nested structure with sub-items that you might want to use as their own dataset.

If you, for example, have a list of virtual machines that each have their own NIC and disks, and you would like to extract the disks into their own table, you can use this transformer to do so.

### Example: Virtual Machines
```yaml
table: virtual_machines
data:
  - name: vm01
    ip_a: 1.2.3.4
    disk_list:
      - name: disk01
        size: 100
      - name: disk02
        size: 200
    nic_list:
      - name: nic01
        mac: 00:11:22:33:44:55
      - name: nic02
        mac: 00:11:22:33:44:56
  - name: vm02
    ip_a: 5.6.7.8
    disk_list:
      - name: disk01
        size: 100
      - name: disk02
        size: 200
    nic_list:
      - name: nic01
        mac: 00:11:22:33:44:55
      - name: nic02
        mac: 00:11:22:33:44:56
```

To extract a table with the disks in the following format, you can use the transformers like below

| vm_name | disk_id | disk_name | disk_size |
|---------|---------|-----------|-----------|
| vm01    | 1       | disk01    | 100       |
| vm01    | 2       | disk02    | 200       |

```yaml
sourceTransformers:
  - transformer: frames.extract_nested_rows
    config:
      # The nested field that contains the rows to extract. Can be jsonpath (e.g. metadata.storage.0.disk_list)
      iterField: disk_list
      # The fields to grab from the nested object
      fieldMap:
        - from: name
          to: disk_name
        - from: size
          to: disk_size
      # The fields to copy from the parent object
      colMap:
        - from: name
          to: vm_name
  - transformer: string.substring
    config:
      inField: disk_name
      start: -2
      outField: disk_id
  - transformer: int.parse
    config:
      inField: disk_id
```