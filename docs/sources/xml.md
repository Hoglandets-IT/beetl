# Xml
The Xml source will load and save data to local Xml files.
Under the hood it uses the etree module to do the parsing.

- Type identifier: `Xml`

## Source Configuration 
Declare the path to your xml file.
```yaml
sources:
  - name: src
    type: Xml
    connection:
      settings:
        # path: <string>, (mandatory)
        # The filepath of your xml file.
        path: "my-file.xml"
        # encoding: <string>, (optional, default="utf-8")
        # The encoding of your xml file.
        encoding: "utf-8"
```

## Sync Settings
Declare how your file should be read, and if used as destination how the file should be written.
```yaml
sync:
  - source: src
    sourceConfig:
      # xpath: <string>, (optional, default="./*", only used when configured as source)
      # Querystring defining how the xml file should be read, see the link below.
      # Etree supported xpath syntax https://docs.python.org/3/library/xml.etree.elementtree.html#supported-xpath-syntax
      # E.g: ".//People" will select all all People objects on all levels.
      xpath: ".//People"
      # type: <Dict[str, str]>, (optional, only used when configured as source)
      # Declares what types the properties will be parsed as.
      # The types supported are string representations of the polars datatypes, same as used in the rest of the project.
      # E.g: {"Name": "Utf-8", "Age": "Int32"}
      types:
        Name: Utf-8
        Age: Int32
    destination: dst
    destinationConfig:
      # unique_columns: <set[string]>, (mandatory, only used when configured as destination)
      # Used when performing mutations to identify unique rows.
      unique_columns:
        - Name
      # root_name: <string>, (optional, default="root", only used when configured as destination)
      # Name of the root element in the resulting xml.
      root_name: root
      # row_name: <string>, (optional, default="row", only used when configured as destination)
      # Name of the row elemets in the resulting xml.
      row_name: row
```

## Good to know's
Since the datasource for this source is a plain file beetl will hold all the changes in memory until all the mutations have been done to an in memory dataframe behind the scenes. It will then flush whole dataframe to file. 

This means that when you're working with the xml source as a destination you will keep an extra copy of all the data in memory.

On the upside this is probably more speed efficient than constantly querying and updating portions of the xml file on disk.


## Example

### From Xml to Xml
This example syncs three people from one xml file into another.

#### What will happen?
**People of xml source**

```xml
<PersonResponse>
  <Body>
    <PBSExportResponse>
      <PBSExportResult>
        <NewDataSet>
          <PERSON>
            <Id>1</Id>
            <Name>John Doe</Name>
            <Age>25</Age>
          </PERSON>
          <PERSON>
            <Id>10</Id>
            <Name>Jimmy Doe</Name>
            <Age>25</Age>
          </PERSON>
          <PERSON>
            <Id>11</Id>
            <Name>Jane Doe</Name>
            <Age>25</Age>
          </PERSON>
        </NewDataSet>
      </PBSExportResult>
    </PBSExportResponse>
  </Body>
</PersonResponse>
```

Destination file is empty or does not exist.

**Result**
- All persons will be synced to the destination xml as rows on a root element.
- Id has been removed


**People of database 2 after sync**

```xml
<PersonExport>
  <Person>
    <Name>John Doe</Name>
    <Age>25</Age>
  </Person>
  <Person>
    <Name>Jimmy Doe</Name>
    <Age>25</Age>
  </Person>
  <Person>
    <Name>Jane Doe</Name>
    <Age>25</Age>
  </Person>
</PersonExport>
```

#### Configuration

```yaml
sources:
  - name: src
    type: Xml
    connection:
      settings:
        path: source.xml
  - name: dst
    type: Xml
    connection:
      settings:
        path: destination.xml
sync:
  - name: Syncing persons
    source: src
    sourceConfig:
      xpath: .//PERSON
      types:
        Name: Utf-8
        Age: Int32
    destination: dst
    destinationConfig:
      unique_columns:
        - Name
      root_name: PersonExport
      row_name: Person
    comparisonColumns:
      - Name: Utf-8
      - Age: Int32
    # Only keep the columns we want
    sourceTransformers:
      - transformer: frames.project_columns
        config:
          columns:
            - Name
            - Age
```
