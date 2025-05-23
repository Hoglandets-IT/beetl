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
      # namespaces: <dict<string, string>>, (optional, default=None, only used when reading xml)
      # namespaces defines what namespaces are present in the xml file and is necessary when trying to select elements nested within a namespace, see example:
      # E.g: If your root element has its namespace set to "http://example.com/beetl/ns" and there are nested <item> elements within it you can send in {'ns': 'http://example.com/beetl/ns'} as your namespaces and then use ".//ns:items" as the xpath to select all items within the namespace.
      namespaces:
        namespace_name: http://example.com/beetl/ns
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
      # xsl: <string>, (optional, default=None)
      # Can be used to perform transformations on the xml before parsing it.
      # Recommended to use if you need to change the structure of your data.
      # Requires lxml to be installed, see examples below.
      xsl: null
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

### Using XSL to transform the data
This is a short example showcasing how the xsl property can be used. This is not in any way, shape or form a guide to xsl's themselves.

:::info
To enable XSL you need to install the lxml parser. It can be done either by installing beetl using the xsl tag `pip install beetl[xsl]` or independently `pip install lxml`.
:::

The XSL tranformations are applied prior to beetl converting the XML to a dataframe.

We're going to restructure this xml from being persons with addresses to addresses with person names.

**Source**
```xml
<body>
  <person>
    <name>Sarah</name>
    <address>
      <street>1 main st</street>
    </address>
    <address>
      <street>2 main st</street>
    </address>
  </person>
  <person>
    <name>Sophie</name>
    <address>
      <street>3 main st</street>
    </address>
    <address>
      <street>4 main st</street>
    </address>
  </person>
</body>
```

By specifying a xsl like this and providing it as a string to the xsl parameter:

```xsl
<?xml version="1.0" encoding="UTF-8"?>
        <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">xsl
            <xsl:output method="xml" indent="yes"/>
            <xsl:template match="//body">
                <Output>
                    <xsl:for-each select=".//person">
                        <xsl:variable name="person_name" select=".//name"/>
                        <xsl:for-each select=".//address">
                            <Record>
                                <name><xsl:value-of select="$person_name"/></name>
                                <street><xsl:value-of select=".//street"/></street>
                            </Record>
                        </xsl:for-each>
                    </xsl:for-each>
                </Output>
            </xsl:template>
        </xsl:stylesheet>
```

The transformed xml that will be parsed by beetl will be this:

```xml
<body>
  <address>
    <name>Sarah</name>
    <street>1 main st</street>
  </address>
  <address>
    <name>Sarah</name>
    <street>2 main st</street>
  </address>
  <address>
    <name>Sophie</name>
    <street>3 main st</street>
  </address>
  <address>
    <name>Sophie</name>
    <street>4 main st</street>
  </address>
</body>
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
        type: Xml
        # name: string
        # Points to a destination defined in the sources section by name
        name: diffsourcename
        # config: dict
        # The destination type specific configuration
        # The Xml source does not have any specific config
        config: {}
```

The file will be automatically created if it doesn't exist. Otherwise beetl will read its contents and extend it.