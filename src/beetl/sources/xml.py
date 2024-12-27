from typing import IO, Any
import pandas as pd
import polars as pl
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class XmlSyncConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for XML file sources"""

    pass


class XmlConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for XML file sources"""

    path: str
    encoding: str = "utf-8"

    def __init__(self, path: str, encoding: str = "utf-8"):
        self.path = path
        self.encoding = encoding


@register_source("xml", XmlSyncConfiguration, XmlConnectionSettings)
class XmlSource(SourceInterface):
    """ A source for reading and writing XML files """
    ConnectionSettingsClass = XmlConnectionSettings
    SourceConfigClass = XmlSyncConfiguration

    connection_settings: XmlConnectionSettings = None
    source_configuration: XmlSyncConfiguration = None

    def _configure(self):
        pass

    def _connect(self):
        """Atomic operations not supported by this source"""
        pass

    def _disconnect(self):
        """Atomic operations not supported by this source"""
        pass

    def _query(self, params=None) -> pl.DataFrame:
        """Reads the XML file and returns a polars DataFrame"""

        return pl.from_pandas(pd.read_xml(self.connection_settings.path, encoding=self.connection_settings.encoding))

    def insert(self, data: pl.DataFrame):
        """Xml files are not yet supported as a destination"""
        return len(data)

    def update(self, data: pl.DataFrame):
        """Xml files are not yet supported as a destination"""
        return len(data)

    def delete(self, data: pl.DataFrame):
        """Xml files are not yet supported as a destination"""
        return len(data)
