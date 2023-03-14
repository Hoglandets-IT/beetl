import pyodbc
from dataclasses import dataclass
from .mysql import MySQLSourceConfig, MySQLSourceConnectionSettings, MySQLSource

@dataclass
class SQLServerSourceConfig(MySQLSourceConfig):
    pass

@dataclass
class SQLServerSourceConnectionSettings(MySQLSourceConnectionSettings):
    driver: str = None

    def __post_init__(self):
        super().__post_init__()
        self.connection_string = self.connection_string.replace('mysql://', 'mssql://')
        if self.driver is None:
            try:
                self.driver = pyodbc.drivers()[0]
            except IndexError:
                raise Exception("No PyODBC drivers found. Please install a driver and try again.")

            self.connection_string += "?TrustServerCertificate=True&driver=" + self.driver.replace(' ', '+')

class SQLServerSource(MySQLSource):
    ConnectionSettingsClass = SQLServerSourceConnectionSettings
    SourceConfigClass = SQLServerSourceConfig