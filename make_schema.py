import json
from src.beetl.config.v1.v1_schema import BeetlConfigSchemaV1

jsonschema = BeetlConfigSchemaV1.model_json_schema()

with open("docs/.vitepress/dist/schema_v1.json") as f:
    json.dump(jsonschema, f, indent=2)

print("Wrote schema to docs/.vitepress/dist/schema_v1.json")