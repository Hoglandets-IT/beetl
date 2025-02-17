# iTop Transformers
Specific transformers for iTop, the open source CMDB

## Orgcode Transformer
Concatenates and hashes the contents of a list of fields to create a unique identifier for an organization that doesn't have one

This will output a field called `org_code` from the organization fields `company`, `department`, and `division`. The `toplevel` field is used to specify the top level organization name. This is used to create a unique identifier for the entire organization.

```yaml
transformers:
  - transformer: itop.orgcode
    config:
      inFields:
        - company
        - department
        - division
      outField: org_code
      toplevel: MyParentCompany
```

## Orgtree Transformer
This transformer aids in creating an organization tree based on two-dimensional data (e.g. see above)

Given the table:

| Company | Department | Division |
|---------|------------|----------|
| A       | D1         | D1.1     |
| A       | D1         | D1.2     |
| A       | D2         | D2.1     |
| B       | D1         | D1.1     |

It will create an organization tree that can be used in iTop to create a hierarchy.

```yaml
transformers:
  - transformer: itop.orgtree
    config:
      treeFields:
        - company
        - department
        - division
      toplevel: MyParentCompany
      
      # The field to put the org name in
      name_field: name 

      # The field to put the org path in (basically concatenated all parent levels for any given org level)
      path_field: org_path

      # The field to put the org code in, concatenated and hashed from the org path
      code_field: org_code

      # The field to put the parent organization in
      parent_field: parent_code
```

## Relations
This insertion transformer is one of the more advanced transformers, and is used pre-insertion to replace the values of link fields with queries that point out the correct resource in iTop. Itop will use the query to resolve you want to point at.

Take this config as an example:
```yaml
    sourceConfig:
      datamodel: "Organization"
      oql_key: "SELECT Organization WHERE orgpath LIKE 'Top->Testing->%'"
      soft_delete:
        enabled: True
        field: status
        active_value: active
        inactive_value: inactive
      unique_columns:
        - code
      comparison_columns:
        - name
        - orgpath
        - status
      link_columns:
        - parent_code
    destination: itop_2
    destinationConfig:
      datamodel: "Organization"
      oql_key: "SELECT Organization"
      unique_columns:
        - code
      comparison_columns:
        - name
        - orgpath
        - status
        - parent_id
      link_columns:
        - parent_code
    comparisonColumns:
      - name: "code",
        type: "Utf8",
        unique: True
      - name: "name",
        type: "Utf8",
      - name: "orgpath",
        type: "Utf8",
      - name: "status",
        type: "Utf8",
      - name: "parent_code",
        type: "Utf8",
    insertionTransformers:
      - transformer: itop.relations
        config:
          field_relations:
          - source_field: parent_id
            source_comparison_field: parent_code
            foreign_class_type: Organization
            foreign_comparison_field: code
            use_like_operator: False
```

The queried data from the source might look like this:
|code|name|orgpath|status|parent_code|
|--|---|--|--|--|
|1|Testing|Top->Testing|active|null|
|2|Unit Testing|Top->Testing->Unit Testing|active|1|

Organization is the root and has no parent code. Unit testing has Testing as its parent.


And the queried data from the destination might look like this:
|code|name|orgpath|status|parent_code|
|--|---|--|--|--|
|1|Testing|Top->Testing|active|null|

The root exists since before but the Unit Testing child will be inserted. The insert dataset will look like this prior to transformation:


|code|name|orgpath|status|parent_code|
|--|---|--|--|--|
|2|Unit Testing|Top->Testing->Unit Testing|active|1|

During transformation a query will be built using the value of the `source_comparison_field` in this case `parent_code` i.e `1` in our case above. It will select another `foreign_class_type` i.e. `Organization` and compare the rows value against the `foreign_comparison_field` column i.e. `code`.

The `use_like_operator` is false, so the equal operator `=` will be used instead.

The value is constructed like this `SELECT Organization WHERE code = 1`

And is then appended to the now transformed dataset:
|code|name|orgpath|status|parent_code|parent_id|
|--|---|--|--|--|--|
|2|Unit Testing|Top->Testing->Unit Testing|active|1|`SELECT Organization WHERE code = 1`|

Since `parent_code` is defined in `link_columns` it will be excluded from the insertions, updates and deletions.

Since `parent_id` is defined in the `comparison_columns` it will be sent to the destination and itop will resolve the resource for us as the organizations parent.

### Default value
By default, if the value that you use for source_comparision_field is None or an empty string `None` will be used as the output for that row.
If you want to change this you can provide the `default_value` property.

```yaml
insertionTransformers:
  - transformer: itop.relations
    config:
      field_relations:
      - source_field: parent_id
        source_comparison_field: parent_code
        foreign_class_type: Organization
        foreign_comparison_field: code
        use_like_operator: False
        # None values will be transformed into 0's
        default_value: 0
```