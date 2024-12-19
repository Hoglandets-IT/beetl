from typing import Any, Dict
import polars as pl
from .interface import TransformerInterface, register_transformer_class
import json


@register_transformer_class("structs")
class StructTransformers(TransformerInterface):
    @staticmethod
    def staticfield(data: pl.DataFrame, field: str, value: Any, only_add_if_missing: bool = False):
        """Add a struct field to the DataFrame

        Args:
            data (pl.DataFrame): The dataFrame to modify
            field (str): The field to add
            value (str): The value of the field to add
            only_add_if_missing (bool, optional): If true, the field will only be added if it does not already exist. Defaults to False.

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        if only_add_if_missing and field in data.columns:
            return data

        data = data.with_columns(pl.Series(field, [value] * len(data)))
        return data

    @staticmethod
    def jsonpath(
        data: pl.DataFrame,
        inField: str,
        outField: str,
        jsonPath: str,
        defaultValue: str = "",
    ):
        """
        Retrieve a value from a jsonpath inside of a column and add it to a new column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            field (str): The field to extract the jsonpath from
            outField (str): The field to add the extracted value to
            jsonPath (str): The jsonpath to extract
        """
        split_jsonpath = jsonPath.split(".")

        if len(split_jsonpath) == 1 and "$" in split_jsonpath:
            return data.with_columns(pl.Series(outField, data[inField]))

        def get_value_at_jsonpath(fData: Any, start_at: int = 0):
            if isinstance(fData, str):
                fData = json.loads(fData)

            try:
                for index, selector in enumerate(
                    split_jsonpath[start_at:], start=start_at
                ):
                    if selector == "$":
                        continue
                    if selector == "*":
                        if not isinstance(fData, list) and not isinstance(
                            fData, pl.Series
                        ):
                            raise NotImplementedError(
                                "Wildcard selector is only supported for lists"
                            )
                        fData = [
                            get_value_at_jsonpath(fData[j], index + 1)
                            for j in range(len(fData))
                        ]
                        return fData
                    try:
                        ix = int(selector)
                        fData = fData[ix]
                    except:
                        fData = fData[selector]
                return fData
            except:
                return defaultValue

        dCol = data[inField]
        outCol = []
        for i in range(len(dCol)):
            outCol.append(get_value_at_jsonpath(dCol[i]))

        data = data.with_columns(pl.Series(outField, outCol))

        return data

    @staticmethod
    def compose_struct(data: pl.DataFrame, map: Dict[str, str], outField: str):
        """Compose a struct from other columns as a new column in the DataFrame

        Given a DataFrame with the columns `age` containing `30` and `name` containing `John`. Providing the map `{"name": "name", "age", "age"}` with outField `person` will result in a new column `person` with the value `{"name": "John", "age": 30}`

        Args:
            data (pl.DataFrame): The dataFrame to modify
            map (dict): Dict of fields to compose the struct from, {"name_of_field_in_struct": "name_of_column"}.
            outField (str): Name of the column to add

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        fields = list(map.keys())
        __class__._validate_fields(data.columns, fields)

        rows = len(data)
        newStructs = []
        for i in range(rows):
            newStructs.append({name: data[field][i]
                              for name, field in map.items()})

        series = pl.Series(outField, newStructs)
        data = data.with_columns(series)

        return data

    @staticmethod
    def compose_list_of_structs(data: pl.DataFrame, map: Dict[str, str], outField: str):
        """Compose a list of structs from other columns as a new column in the DataFrame

        All source columns must be lists. If the lists are of different lengths, the resulting list will be the length of the longest list with None as the value for missing elements.
        table

        Args:
            data (pl.DataFrame): The dataFrame to modify.
            map (dict): Dict of fields to compose the struct from, {"name_of_field_in_struct": "name_of_column"}.
            outField (str): Name of the column to add.

        Returns:
            pl.DataFrame: The resulting DataFrame.

        Example:
            The source dataframe

            +----+----------------------------+----------+----------+
            | id | names                      | ages     | city     |
            +----+----------------------------+----------+----------+
            | 1  | ["John", "Mary", "Sam"]    | [30, 40] | New York |
            +----+----------------------------+----------+----------+

            Transformed with this configuration

            `compose_list_of_structs(data, {"name": "names", "age": "ages"}, "people")`

            Will result in a people column composed of the folowing list

            [
                {"name": "John", "age": 30},
                {"name": "Mary", "age": 40},
                {"name": "Sam", "age": None}
            ]
        """
        field_names = list(map.values())
        __class__._validate_fields(data.columns, field_names)

        dataframe_to_map = data.select(field_names)

        longest_series_row = 0
        for series in dataframe_to_map:
            for row in series:
                if len(row) > longest_series_row:
                    longest_series_row = len(row)

        new_series = None
        row_count = len(data)
        for row_index in range(row_count):
            new_structs = []
            for i in range(longest_series_row):
                new_struct = {}
                for name, series in map.items():
                    try:
                        new_struct[name] = dataframe_to_map[series][row_index][i]
                    except IndexError:
                        new_struct[name] = None

                new_structs.append(new_struct)
            if new_series is None:
                new_series = pl.Series([new_structs])
                continue
            new_series.append(pl.Series([new_structs]))

        data = data.with_columns(new_series.alias(outField))

        return data
