def to_xlsx(source_file_path: str, destination_file_path: str) -> dict:
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Excel",
                "connection": {
                    "path": source_file_path,
                },
            },
            {
                "name": "dst",
                "type": "Excel",
                "connection": {
                    "path": destination_file_path,
                },
            },
        ],
        "sync": [
            {
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {},
                "comparisonColumns": {
                    "ID": "Int32",
                    "Postcode": "Utf8",
                    "Sales_Rep_ID": "Utf8",
                    "Sales_Rep_Name": "Utf8",
                    "Year": "Utf8",
                    "Value": "Utf8",
                },
                "sourceTransformers": [],
                "destinationTransformers": [],
                "insertionTransformers": [],
            }
        ],
    }


def diff_to_xlsx(diff_file_path: str) -> dict:
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
                "name": "diff",
                "type": "Excel",
                "connection": {
                    "path": diff_file_path,
                },
            },
            {
                "type": "Static",
                "name": "dst",
                "connection": {
                    "static": [
                        {"id": 3, "name": "test", "age": 20},
                        {"id": 4, "name": "test4", "age": 20},
                        {"id": 5, "name": "test5", "age": 20},
                    ]
                },
            },
        ],
        "sync": [
            {
                "name": "testing_diff",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {},
                "comparisonColumns": {
                    "id": "Int64",
                    "name": "Utf8",
                    "age": "Int64",
                },
                "diff": {
                    "destination": {"type": "Excel", "name": "diff", "config": {}}
                },
            }
        ],
    }
