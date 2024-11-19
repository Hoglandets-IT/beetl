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

## Insertion Transformer
This insertion transformer is one of the more advanced transformers, and is used pre-insertion to replace the values of link fields with queries that point out the correct resource in iTop.

If you have a foreign key field (e.g. org_id) and a linked field (e.g. org_code), and want to make the comparisons between source and destination on the org_code field but insert the org_id field into iTop you can do that like this:

```yaml
destinationConfig:
  columns:
    - name: org_code
      type: utf8
    - name: org_id
      type: Utf8
      custom_options:
        itop:
          target_class: Organization
          comparison_field: org_code
          reconciliation_key: code
...
insertionTransformers:
  - transformer: itop.relations


```

This will, on insertion, set the value of the `org_id` field to:

``` SELECT FROM Organization WHERE code = `{org_code}` ```

For more information, see the example sqlserver-to-itop in the examples section.