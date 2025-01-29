def to_xml(source_file_path: str, destination_file_path: str) -> dict:
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Xml",
                "connection": {
                    "path": source_file_path,
                },
            },
            {
                "name": "dst",
                "type": "Xml",
                "connection": {
                    "path": destination_file_path,
                },
            },
        ],
        "sync": [
            {
                "source": "src",
                "destination": "dst",
                "sourceConfig": {
                    "xpath": ".//PERSON",
                    "types": {
                        "Id": "Int64",
                        "Name": "Utf8",
                        "Age": "UInt8",
                    },
                },
                "destinationConfig": {
                    "xpath": ".//Person",
                    "root_name": "PersonExport",
                    "row_name": "Person",
                    "unique_columns": ("Id",),
                },
                "comparisonColumns": [
                    {
                        "name": "Id",
                        "type": "Int64",
                        "unique": True,
                    },
                    {
                        "name": "Name",
                        "type": "Utf8",
                    },
                    {
                        "name": "Age",
                        "type": "UInt8",
                    },
                ],
                "sourceTransformers": [],
                "destinationTransformers": [],
                "insertionTransformers": [],
            }
        ],
    }
