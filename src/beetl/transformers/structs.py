from typing import List
import polars as pl
from .interface import TransformerInterface, register_transformer_class
import json

@register_transformer_class("structs")
class StructTransformers(TransformerInterface):
    @staticmethod
    def staticfield(data: pl.DataFrame, field: str, value):
        """Add a struct field to the DataFrame

        Args:
            data (pl.DataFrame): The dataFrame to modify
            field (str): The field to add
            value (str): The value of the field to add

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        data = data.with_columns(pl.Series(field, [value] * len(data)))
        return data

    @staticmethod
    def jsonpath(data: pl.DataFrame, inField: str, outField: str, jsonPath: str, defaultValue: str = ""):
        """
        Retrieve a value from a jsonpath inside of a column and add it to a new column
        
        Args:
            data (pl.DataFrame): The dataFrame to modify
            field (str): The field to extract the jsonpath from
            outField (str): The field to add the extracted value to
            jsonPath (str): The jsonpath to extract
        """
        split_jsonpath = jsonPath.split(".")
                
        def jsonpathFunction(fData):
            if isinstance(fData, str):
                fData = json.loads(fData)

            try:
                for i in split_jsonpath:
                    try:
                        ix = int(i)
                        fData = fData[ix]
                    except:
                        fData = fData[i]
                return fData
            except:
                return defaultValue
            

            return "OK"
        
        dCol = data[inField]
        outCol = []
        for i in range(len(dCol)):
            outCol.append(jsonpathFunction(dCol[i]))
        
        data = data.with_columns(pl.Series(outField, outCol))
        
        return data
