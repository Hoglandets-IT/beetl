# Changenotes

## 0.5.0

### Breaking changes
- compare datasets returning full source
- Mysql source defaults to selecting all columns from the target table. Use query on source configuration to specify which columns you want to use.


### New features
- Deletion transformers
- MongoDB is now supported as source and destination [documentation](documentation) 
- frames.project_columns transformer
- int.to_int64 transformer
- strings.join_listfield support for list as well as array
- strings.split_into_listfield transformer
- strings.to_object_id transformer
- structs.jsonpath support for * selector to get values nestled in lists
- structs.compose_struct
- structs.compose_list_of_structs


### Changed features
- polars updated, code updated to reflect changes
- 


### Bugfixes
- Beetl result now defaults to no changes (0) when none are passed as arguments