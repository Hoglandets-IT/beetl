
# From SqlServer to iTop
This example syncs data from table `organizations` in sqlserver to itop

## What will happen?
**Organizations of SqlServer**

```json
[
  {
    "level1": "Beetl Municipality",
    "level2": "School of Beetl",
    "level3": "A1",
  },
  {
    "level1": "Beetl Municipality",
    "level2": "School of Beetl",
    "level3": "B1",
  }
]
```

**Organizations of iTop**

```json
[
  {
    "code": 1,
    "name": "Beetl Municipality",
    "orgpath": "Top->Beetl Municipality",
    "status": "active",
    "parent_code": null,
  },
  {
    "code": 2,
    "name": "School of Beetl",
    "orgpath": "Top->Beetl Municipality->School of Beetl",
    "status": "active",
    "parent_code": 1,
  },
  {
    "code": 3,
    "name": "A1",
    "orgpath": "Top->Beetl Municipality->School of Beetl->A1",
    "status": "active",
    "parent_code": 2,
  },
]
```

**Result**
- The itop.orgtree will transform the rows with different org levels into separate org rows.
- Code will be used to uniquely identify each item organization.
- All rows in in SqlServer not present in iTop will be created.


**People of database 2 after sync**

```json
[
  {
    "code": 1,
    "name": "Beetl Municipality",
    "orgpath": "Top->Beetl Municipality",
    "status": "active",
    "parent_code": null,
  },
  {
    "code": 2,
    "name": "School of Beetl",
    "orgpath": "Top->Beetl Municipality->School of Beetl",
    "status": "active",
    "parent_code": 1,
  },
  {
    "code": 3,
    "name": "A1",
    "orgpath": "Top->Beetl Municipality->School of Beetl->A1",
    "status": "active",
    "parent_code": 2,
  },
  {
    "code": 4,
    "name": "B1",
    "orgpath": "Top->Beetl Municipality->School of Beetl->B1",
    "status": "active",
    "parent_code": 2,
  },
]
```
- Organization B1 was inserted into iTop

## Configuration

```yaml
sources:
  - name: sqlserver
    type: sqlserver
    connection:
      settings:
        connection_string: "mongodb://admin:password@localhost:1234/database"
  - name: itop
    type: itop
    connection:
      settings:
        host: "localhost"
        username: "admin"
        password: "password"
sync:
  - source: sqlserver
    sourceConfig:
      table: organizations
      query: "SELECT level1, level2, level3 FROM organizations"
      unique_fields: ["level1", "level2", "level3"]
    destination: itop
    destinationConfig:
      datamodel: Organization
      oql_key: "SELECT Organizations"
      unique_columns:
        - code
      comparison_columns:
        - name
        - orgpath
        - code
        - status
        - parent_id
      link_columns:
        - parent_code
    comparisonColumns:
      - name: code
        type: Utf8
        unique: true
      - name: name
        type: Utf8
      - name: orgpath
        type: Utf8
      - name: status
        type: Utf8
      - name: parent_code
        type: Utf8
    sourceTransformers:
      - transformer: itop.orgtree
        config:
          treeFields: 
            - level1
            - level2
            - level3
    insertionTransformers:
      - transformer: itop.relations
        config:
          field_relations: 
            source_field: parent_id
            source_comparison_field: parent_code
            foreign_class_type: Organization
            foreign_comparison_field: code
            use_like_operator: false

```
**links**:
- [itop.orgtree](/transformers/itop.html#orgtree)
- [itop.relations](/transformers/itop.html#relations)