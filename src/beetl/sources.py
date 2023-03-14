from typing import List 
import polars
import sqlalchemy as sqla
import sqlalchemy.sql as sqf



class FileSource(SourceInterface):
    """ 
        Interface for sources from the filesystem
        Needs a path to the file and charset
    """
    def _configure(self, path: str, charset: str = "utf-8"):
        self.path, self.charset = path, charset
        self.content = None
        
    def _retrieve(self):
        with open(self.path, "r", encoding=self.charset) as f:
            self.content = f.read()

class JsonSource(SourceInterface):
    """ 
        Sources in JSON format
    """
    def _parse(self):
        return polars.read_json(self.content)

class CsvSource(SourceInterface):
    """ 
        Sources in CSV format
    """
    def _parse(self):
        return polars.read_csv(self.content)

class ExcelSource(SourceInterface):
    """
        Sources in Excel format
    """
    def _parse(self):
        return polars.read_excel(self.content)

class SqlDatabaseSource(SourceInterface):
    """
        Database/relational data sources. 
        Needs a connection string and the respective driver installed
    """
    
    def _configure(self, connection_string: str):
        self.connection = connection_string
    
    def _retrieve(self, query: str) -> polars.DataFrame:
        return polars.read_sql(
            query,
            self.connection
        )

    def _parse(self, content) -> polars.DataFrame:
        return content

class MysqlSource(SqlDatabaseSource):
    """
        MySQL database source
    """
    pass

class SqlServerSource(SqlDatabaseSource):
    """
        SQL Server database source
    """
    pass

class PostgresSource(SqlDatabaseSource):
    """
        Postgres database source
    """
    pass

class JsonFileSource(JsonSource, FileSource):
    """
        Retrieve files from filesystem and parse as JSON
    """
    pass

class CsvFileSource(CsvSource, FileSource):
    """
        Retrieve files from filesystem and parse as CSV
    """
    pass

class ExcelFileSource(ExcelSource, FileSource):
    """
        Retrieve files from filesystem and parse as Excel
    """
    pass
