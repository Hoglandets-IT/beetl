from typing import List
import json
import polars as pl
from .interface import TransformerInterface, register_transformer_class


@register_transformer_class("frames")
class FrameTransformer(TransformerInterface):
    @staticmethod
    def filter(data: pl.DataFrame, filter: dict):
        """Filter a DataFrame

        Args:
            data (pl.DataFrame): The DataFrame to filter
            filter (dict): The filter to apply

        Returns:
            pl.DataFrame: The filtered DataFrame
        """
        for key, value in filter.items():
            data = data.filter(data[key] == value)
        
        return data
    
    @staticmethod
    def conditional(data: pl.DataFrame, conditionField: str, ifTrue: str, ifFalse: str):
        """Apply a conditional to a DataFrame

        Args:
            data (pl.DataFrame): The DataFrame to apply the conditional to
            conditionField (str): The field to check
            ifTrue (str): The value to set if the condition is true
            ifFalse (str): The value to set if the condition is false

        Returns:
            pl.DataFrame: The DataFrame with the conditional applied
        """
        data = data.with_column(
            pl.when(data[conditionField] == True).then(ifTrue).otherwise(ifFalse)
        )
        
        return data
    
    @staticmethod
    def rename_columns(data: pl.DataFrame, columns: List[dict]):
        """Rename columns in the dataset

        Args:
            data (pl.DataFrame): The dataset
            columns (List[dict]): A list of dicts (from: col1, to: col2)

        Returns:
            pl.DataFrame: DataFrame with renamed columns
        """
        ncolumns = columns
        
        if isinstance(columns, dict):
            ncolumns = []
            for k, v in columns.items():
                ncolumns.append({
                    "from": k,
                    "to": v
                })
        
        for column in ncolumns:
            data = data.rename({column["from"]: column["to"]})
        return data

    @staticmethod
    def copy_columns(data: pl.DataFrame, columns: List[dict]):
        """Copies a given column to another column

        Args:
            data (pl.DataFrame): The dataset
            columns (List[dict]): A list of dicts (from: col1, to: col2)

        Returns:
            pl.DataFrame: DataFrame with renamed columns
        """
        for column in columns:
            data[column["to"]] = data[column["from"]]

        return data

    @staticmethod
    def drop_columns(data: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Drop columns from a DataFrame

        Args:
            data (pl.DataFrame): Drop columns from this dataframe
            columns (List[str]): The columns to dr op

        Returns:
            pl.DataFrame: The dataframe without the columns
        """
        return data.drop(columns)

    @staticmethod
    def extract_nested_rows(data: pl.DataFrame, iterField: str = "", fieldMap: dict = {}, colMap: dict = {}):
        """
        Convert each list item in a list column to a row

        Args:
            data (pl.DataFrame): Multiply rows in this database
            inField (str): The list column to get values from
            outField (str): The column in the result to put the iterated value in
            column_path (str): The path in the list item to the value of the new column
            includeColumns (List[str]): Columns to include in the result
        """
        new_obj = []
        
        def extract_field(data: dict, path: str):
            splitPath = path.split(".")
            for segment in splitPath:
                try:
                    iSegment = int(segment)
                    if len(data) <= iSegment:
                        return ""
                    
                    data = data[iSegment]
                except Exception:
                    try:
                        data = data[segment]
                    except Exception:
                        return ""
            
            return data
        
        for row in data.iter_rows(named=True):
            cData = row[iterField]
            if isinstance(cData, str):
                cData = json.loads(cData)
            
            for i in range(len(cData)):
                new_row = {}
                
                if fieldMap is None or len(fieldMap) == 0:
                    new_row["value"] = cData[i]
                
                if isinstance(cData[i], str):
                    if len(fieldMap) == 1:
                        new_row[list(fieldMap.values())[0]] = cData[i]
                else:
                    for field, path in fieldMap.items():
                        new_row[field] = extract_field(cData[i], path)
                
                for dst, src in colMap.items():
                    new_row[dst] = row[src]
            
            
                new_obj.append(new_row)
        
        return pl.DataFrame(new_obj)