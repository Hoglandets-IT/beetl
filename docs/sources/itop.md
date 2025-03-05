
# iTop
The iTop source will load and save data to iTop servers.

- Type identifier: `itop`

## Source Configuration 
Declare how the source should be connecting to the iTop instance.
```yaml
sources:
  - name: itop_1
    type: itop
    connection:
      settings:
        # host: <string>, (mandatory)
        # Can be ip-address or hostname
        host: "itop.example.com"
        # username: <string>, (mandatory)
        username: "admin"
        # password: <string>, (mandatory)
        password: "password"
        # verify_ssl: <string>, (optional, default="true")
        # Wether to verify or skip verifying the server ssl certificate
        veryfy_ssl: "true"
```

## Sync Settings
Declare how the data should be queried and the schema for comparing the source with the destination.
```yaml
sync:
  - source: itop_1
    sourceConfig:
      # datamodel: <string> (mandatory)
      # The iTop datamodel to sync
      datamodel: "Organization"
      # oql_key: <string> (mandatory)
      # The query to use when fetching data
      oql_key: "SELECT Organization WHERE orgpath LIKE 'Top->Testing->%'"
      # soft_delete: <object> (optional)
      # if provided and enabled is True resources will be set to inactive instead of being acually deleted.
      soft_delete:
        # enabled: <bool> (optional, default=False)
        # False = same as not defining the soft_delete object.
        enabled: True
        # field: <string> (mandatory)
        # The field on the resource to affect and compare when using soft_delete (`Organization.status = active|inactive`).
        field: status
        # active_value: <string> (mandatory)
        # The value to represent that the resource is active.
        # What beetl will set as `Organization.status` if the resource is marked for insert or update.
        active_value: active
        # inactive_value: <string> (mandatory)
        # The value to represent that the resource is inactive.
        # What beetl will set as `Organization.status` if the resource is marked for removal.
        inactive_value: inactive
      # unique_columns: <list<string>> (mandatory)
      # defines how to idenfify a unique resource when inserting, updating and deleting.
      # is included in fields fetched when querying itop
      unique_columns:
        - code
      # comparison_columns: <list<string>> (mandatory)
      # defines what field values will be changed for a resource when inserting and updating.
      # is included in fields fetched when querying itop.
      comparison_columns:
        - name
        - orgpath
        - status
      # link_columns: <list<string>> (optional)
      # defines fields that you want fetched when querying itop for existing data but that are only needed to resolve relation links using the itop.relations transformer. See an example of the relations transformer below.
      # is included in fields fetched when querying itop, but excluded when inserting, updating and deleting.
      link_columns:
        - parent_code
      # foreign_key_columns: <list<string>> (optional)
      # Columns specified here will have their `None` values changed into `0`'s. 
      # This should be used for fields that are references to other related resources in iTop such as people and organizations.
      # Setting these values to 0 means that iTop will 
      foreign_key_columns:
        - parent_id
      # type_overrides: <dict<string,string>> (optional)
      # Lets you explicitly set what type any column should be interpreted as.
      # Might be necessary is some cases when first value in a column is null and beetl incorrectly identifies the data as Int64.
      # Format is {"column_name": "polars_datatype_as_string"}
      # See related types section below for valid values.
      type_overrides:
        name: Utf8
        id: Int64
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
      - name: "id",
        type: "Utf8",
        unique: True
      - name: "name",
        type: "Utf8",
      - name: "email",
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
*Related types:*
- [ColumnDefinitions](/sources/types/column-definition.html)
- [PolarDatatypesAsString](/sources/types/polar-datatypes.html)

## Explaining itop.relations
The best way is to read the up to date documentation that can be found [here](/transformers/itop.html#relations).


## Examples

- [SqlServer to iTop](/examples/sqlserver-to-itop.html)