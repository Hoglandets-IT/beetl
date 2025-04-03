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
