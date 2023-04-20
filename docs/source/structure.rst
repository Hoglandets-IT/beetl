BeETL Structure
***************

BeETL is based on three main components, which are explained in more detail below:

- Synchronizations
- Data Sources
- Transformers

The easiest way to explain this is with an example. Let's say you want a database full of your Azure users, with 
some minor modifications along the way. 

Example
-------

The example below will cover Config, Source Transformers, Field Transformers and Sync with a YAML configuration file.

The data you fetch from Azure (and example)::

    userprincipalname (user@domain.com)
    givenname (John)
    sn (Doe)
    department (IT)

The data format in your database (and example)::

    userprincipalname (user@domain.com)
    displayname (John Doe)
    department (Information Technology)
    email (user@domain.com)
    username (user)
    domain (domain.com)

This means we want to do the following:

1. Fetch data from Azure
2. Translate the Azure "department" column to the extended names (source transformer) 
3. Split the "userprincipalname" column into "username" and "domain" (field transformer)
4. Join the "givenname" and "sn" columns into "displayname" (field transformer)
5. Clone the "userprincipalname" column into "email" (field transformer)
6. Run the sync


You'd create a configuration as follows (can be done in YAML, Python or JSON)::

    version: "V1"
    datasources:
      # Azure AD
      - name: "azusers"
        type: "MSGraph"
        connection:
          graph_object: "users"
          tenant_id: "tenant_id"
          client_id: "client_id"
          client_secret: "client_secret"
          client_scope: "https://graph.microsoft.com/.default"
          client_type: "client_credentials"
        config:
          columns:
            - name: "userprincipalname"
              type: "Utf8"
              unique: True
              skip_update: True
            
            - name: "givenname"
              type: "Utf8"
              unique: False
              skip_update: False
            
            - name: "sn"
              type: "Utf8"
              unique: False
              skip_update: False
            
            - name: "department"
              type: "Utf8"
              unique: False
              skip_update: False
        
        # Database
      - name: "mydatabase"
        type: "SQLServer"
        connection:
          connection_string: "mssql+pyodbc://user:password@server/database"
          fast_executemany: True
        config:
          columns:
            - name: "userprincipalname"
              type: "Utf8"
              unique: True
              skip_update: True
            
            - name: "displayname"
              type: "Utf8"
              unique: False
              skip_update: False
            
            - name: "department"
              type: "Utf8"
              unique: False
              skip_update: False
            
            - name: "email"
              type: "Utf8"
              unique: False
              skip_update: False
            
            - name: "username"
              type: "Utf8"
              unique: True
              skip_update: False
            
            - name: "domain"
              type: "Utf8"
              unique: False
              skip_update: False
    
    sync:
      - source: "azusers"
        destination: "mydatabase"
        sourceTransformer: "mycustom.transformer"
        fieldTransformers:
          
            # Split userprincipalname into username and domain
          - transformer: "strings.split"
            config:
              inField: "userprincipalname"
              outFields:
                - "username"
                - "domain"
              separator: "@"

            # Join givenname and sn into displayname
          - transformer: 
            config:
              inFields:
                - "givenname"
                - "sn"
              outField: "displayname"
          
          - transformer: "frames.clone_field"
            config:
              inField: "userprincipalname"
              outField: "email"

          # The above transformers will always preserve the original data,
          # you can use the frames.drop_field transformer to remove those.
          # Although, it's not necessary since the source column specification
          # is used to determine which columns to compare.

            - transformer: "frames.drop_field"
                config:
                inField: "givenname"


And using the following Python code::

      from beetl.beetl import Beetl
      from beetl.transformers.interface import register_transformer

      # Register your custom transformer
      @register_transformer('source', 'mycustom', 'transformer')
      def translate_department(dataset: polars.DataFrame) -> polars.DataFrame:
          return dataset.with_column(
              polars.col('department').str.replace('IT', 'Information Technology')
          )

      # Create a Beetl instance with the configuration
      beetlsync = Beetl.from_yaml("config.yaml", "utf-8")

      # Start the sync
      beetlsync = beetlsync.sync()



Datasources
-----------
A datasource is a connection to a storage unit for data, such as a database, file, API, manually specified or faked data.
In this example, the two datasources are "MSGraph" and "SQLServer".

If we start by taking a look at the overall structure, a datasource has three configuration parts::

    datasources:
      - name: "azusers"
        type: "MSGraph"
        connection:
          graph_object: "users"
          tenant_id: "tenant_id"
          client_id: "client_id"
          client_secret: "client_secret"
          client_scope: "https://graph.microsoft.com/.default"
          client_type: "client_credentials"
        config:
          columns:
            - name: "userprincipalname"
              type: "Utf8"
              unique: True
              skip_update: True

Name is used to identify the datasource when specifying the sync later on, type identifies which connector to use
for the connection.

In the "connection" settings, you specify the details for how to connect and to what.

In the "config" settings, you describe the data that is to be retrieved from the source.

The "columns" section will determine how the comparison is made by looking at the unique and skip_update fields,
the "type" field will ensure the data from both sides is in the same format.

Synchronizations
----------------

A synchronization is a description of the process to follow when retrieving, comparing and updating
data in the destination. Given this example::

    sync:
      - source: "azusers"
        destination: "mydatabase"
        sourceTransformer: "mycustom.transformer"
        fieldTransformers:
          
            # Split userprincipalname into username and domain
          - transformer: "strings.split"
            config:
              inField: "userprincipalname"
              outFields:
                - "username"
                - "domain"
              separator: "@"

            # Join givenname and sn into displayname
          - transformer: 
            config:
              inFields:
                - "givenname"
                - "sn"
              outField: "displayname"
          
          - transformer: "frames.clone_field"
            config:
              inField: "userprincipalname"
              outField: "email"

          # The above transformers will always preserve the original data,
          # you can use the frames.drop_field transformer to remove those.
          # Although, it's not necessary since the source column specification
          # is used to determine which columns to compare.

            - transformer: "frames.drop_field"
                config:
                inField: "givenname"

1. Data Retrieval Stage
^^^^^^^^^^^^^^^^^^^^^^^

In this stage, the datasource specified under source will be queried for the data
according to its settings. For databases, this can be done by specifying a manual
query or by setting the table and columns to retrieve in the configuration above.

When the data is retrieved and loaded into memory, this starts Stage 2

2. Transformation Stage
^^^^^^^^^^^^^^^^^^^^^^^

In this stage, the data from the source is tranformed in such a way so that
it matches the destination so that a comparison can occur on equal terms.

There are two types of tranformers, source transformers (1 per sync) which are run first and
meant to provide a more advanced way of tranforming data in the form of a Python function.
A simple type of source transformer is the one in the example::

      @register_transformer('source', 'mycustom', 'transformer')
      def translate_department(dataset: polars.DataFrame) -> polars.DataFrame:
          return dataset.with_column(
              polars.col('department').str.replace('IT', 'Information Technology')
          )    

This will check the "department" column for the contents "IT" and translate them to "Information Technology".

The second type of transformer is a field transformer, which are run after the source transformers.
There are a number of built-in field transformers (see list under "Classes" in the menu),
you can also register your own in the same way as a source transformer. The inputs and
outputs are the same, but there can be multiple field transformers per sync and they behave
slightly differently.


3. Comparison Stage
^^^^^^^^^^^^^^^^^^^

At this stage, the data should be formatted roughly the same. If there are a couple of extra columns
in the source or destination data, this shouldn't matter much. When starting the comparison,
the fields of the destination are used to choose fields for comparison.

The first comparison stage is for inserts, where we compare the series of columns marked "unique"
between the dataset and determine which rows are missing in the destination.

The second comparison stage is for updates, where we use the unique fields as 
identifiers to find the rows that are present in both the source and destination and 
compare them to determine which rows need to be updated.

The third comparison stage is for deletes, which basically is a reverse comparison
to the inserts.

4. Insertion Stage
^^^^^^^^^^^^^^^^^^

At this stage, the data is sent to the datasource class for the destination to be inserted, updated and deleted


This is a rough overview over what happens in the various steps for the software. You can find more of the technical
documentation, advanced options, fields and examples in the configuration and classes-sections of the documentation.