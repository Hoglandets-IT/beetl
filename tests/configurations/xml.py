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


def diff_to_xml(
    diff_file_path: str,
) -> dict:
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {"id": 2, "name": "test2", "age": 20},
                        {"id": 3, "name": "test3", "age": 20},
                        {"id": 4, "name": "test4", "age": 20},
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Static",
                "connection": {
                    "static": [
                        {"id": 3, "name": "test", "age": 20},
                        {"id": 4, "name": "test4", "age": 20},
                        {"id": 5, "name": "test5", "age": 20},
                    ]
                },
            },
            {
                "name": "diff",
                "type": "Xml",
                "connection": {
                    "path": diff_file_path,
                },
            },
        ],
        "sync": [
            {
                "name": "test",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {},
                "comparisonColumns": [
                    {
                        "name": "id",
                        "type": "Int32",
                        "unique": True,
                    },
                    {
                        "name": "name",
                        "type": "Utf8",
                    },
                    {
                        "name": "age",
                        "type": "Int64",
                    },
                ],
                "diff": {
                    "destination": {
                        "type": "Xml",
                        "name": "diff",
                        "config": {},
                    }
                },
            }
        ],
    }
