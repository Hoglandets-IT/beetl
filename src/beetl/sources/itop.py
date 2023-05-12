import os
import json
# import asyncio
import urllib3
import polars as pl
import requests
import requests.adapters
from alive_progress import alive_bar
from ..transformers.interface import (
    TransformerInterface,
    register_transformer
)
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ItopInsertionTransformer(TransformerInterface):   
    @staticmethod
    @register_transformer('itop', 'relations')
    def relations(data: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """ Insert data into iTop

        Args:
            data (pl.DataFrame): The dataFrame to modify
            connection (SourceInterface): The connection to the source
            configuration (SourceInterfaceConfiguration): The configuration for the source

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        if not kwargs.get('sync', False):
            raise Exception("Error: include_sync option is not set for transformer itop.relations")
        
        
        
        if kwargs['sync'].destination.source_configuration.has_foreign:
            transformed = data.clone()
            for field in kwargs['sync'].destination.source_configuration.columns:
                fk_def = getattr(field, 'custom_options', {})
                if fk_def is not None:
                    fk_def = fk_def.get('itop', None)
                    if fk_def is not None:
                        try:
                            query = f'SELECT {fk_def["target_class"]} WHERE {fk_def["reconciliation_key"]} = '
                        
                            transformed = transformed.with_columns(
                                transformed[fk_def['comparison_field']].apply(
                                    lambda x: query + f"'{x}'"
                                ).alias(field.name)
                            )
                        except KeyError as e:
                            raise Exception("Error: The key definition is not valid") from e
        
            return transformed
        
        return data
                

class ItopSourceConfiguration(SourceInterfaceConfiguration):
    """ The configuration class used for iTop sources """
    datamodel: str = None
    oql_key: str = None
    has_foreign: bool = None
    
    def __init__(self, columns: list, datamodel: str = None, oql_key: str = None):
        super().__init__(columns)
        self.datamodel = datamodel
        self.oql_key = oql_key
        self.has_foreign = False
        self.comparison_columns = self.get_output_fields_for_request()
        
    def get_output_fields_for_request(self):
        output = []
        for field in self.columns:
            if field.custom_options is not None:
                if field.custom_options.get(
                    'itop', {} ).get('comparison_field', False):
                    self.has_foreign = True
                    output.append(field.custom_options['itop']['comparison_field'])
                    continue
            
            if not field.unique:
                output.append(field.name)

        return output
        

class ItopSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for iTop sources """
    host: str
    username: str = None
    password: str = None
    verify_ssl: bool = True
    
    def __init__(self, settings: dict):
        self.host = settings.get('host', False)
        self.username = settings.get('username', False)
        self.password = settings.get('password', False)
        self.verify_ssl = settings.get('verify_ssl', 'true') == 'true'

class RequestsConnectionPool:
    session: requests.Session = None
    pool: requests.adapters.HTTPAdapter = None
    
    @staticmethod
    def is_ready() -> bool:
        return None not in (RequestsConnectionPool.session, RequestsConnectionPool.pool)
    
    @staticmethod
    def create_pool(num_connections: int, num_maxsize: int):
        RequestsConnectionPool.session = requests.Session()
        RequestsConnectionPool.adapter = requests.adapters.HTTPAdapter(
            pool_connections=num_connections,
            pool_maxsize=num_maxsize
        )
        RequestsConnectionPool.session.mount('https://', RequestsConnectionPool.adapter)

@register_source('itop', ItopSourceConfiguration, ItopSourceConnectionSettings)
class ItopSource(SourceInterface):
    ConnectionSettingsClass = ItopSourceConnectionSettings
    SourceConfigClass = ItopSourceConfiguration
    auth_data: dict = None
    """ A source for Combodo iTop Data data """
    
    def _configure(self):
        self.auth_data = {
            "auth_user": self.connection_settings.username,
            "auth_pwd": self.connection_settings.password
        }
        
        self.url = os.path.join(
            f'https://{self.connection_settings.host}',
            'webservices',
            'rest.php?version=1.3&loagin_mode=form'
        )
        
        # Test Connection
        files = self._create_body(
            'core/check_credentials',
            {
                "user": self.connection_settings.username, 
                "password": self.connection_settings.password
        })
        
        res = requests.post(
            self.url, files=files, 
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl
        )
        
        try:
            res_json = res.json()
            assert res_json['code'] == 0
        except AssertionError:
            raise Exception(
                f"Invalid response received from iTop Server: {res_json['code']}"
            )
        except requests.exceptions.JSONDecodeError:
            raise Exception(
                "Invalid response received from iTop Server, "
                "check your authentication settings: " + res.text
            )
        
    def _create_body(self, operation: str, params: dict = None):
        data = {}
        if params is not None:
            data = params

        data['operation'] = operation
        
        if params is not None:
            for key, value in params.items():
                data[key] = value
        
        return {
            "json_data": json.dumps(data)
        }
        
    def _connect(self): pass
    def _disconnect(self): pass
    
    def _query(self, params = None) -> pl.DataFrame:
        """
        In: 
            Object: The type of object to retrieve from iTop
            OQL: OQL Query
            Output Fields: List of fields to return           
        """
        files = self._create_body(
            'core/get',
            {
                "class": self.source_configuration.datamodel,
                "key": self.source_configuration.oql_key,
                "output_fields": ",".join(
                    self.source_configuration.unique_columns +
                    self.source_configuration.comparison_columns
                )
            }
        )
        
        res = requests.post(
            self.url, files=files,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl
        )
        
        if not res.ok or res.json()['code'] != 0:
            raise Exception(f"Error retrieving {self.source_configuration.datamodel} data from iTop: {res.text}")
        
        re_obj = []
        
        try:
            res_json = res.json()
            objects = res_json.get('objects', None)
            
            if objects is not None:
                for _, item in res_json.get('objects', {}).items():
                    re_obj.append({
                        "id": item['key'],
                        **item['fields']
                    })
                return pl.DataFrame(re_obj)
            return pl.DataFrame()
        
        except Exception as e:
            raise Exception("An error occured while trying to decode the response", e) from e

    
    def create_item(self, type: str, data: dict, comment: str = ""):
        body = self._create_body(
            'core/create',
            {
                'comment': comment,
                'class': type,
                'output_fields': '*',
                'fields': data
            }
        )
        
        res = requests.post(
            self.url, files=body,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl
        )
        
        if not res.ok:
            return False
        
        try:
            res_json = res.json()
            assert res_json['code'] == 0
        except Exception as e:
            print(res.text + str(e))
            return False
        
        return True
    
    def update_item(self, type: str, data: dict, comment: str = ""):
        
        identifiers = {}
        
        for k in tuple(data.keys()):
            if k in self.source_configuration.unique_columns:
                identifiers[k] = data.pop(k)
        
        wh_string = ' AND '.join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )
        
        body = self._create_body(
            'core/update',
            {
                'comment': comment,
                'class': type,
                'key': f'SELECT {type} WHERE {wh_string}',
                'fields': data
            }
        )
        
        res = requests.post(
            self.url, files=body,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl
        )
        
        if not res.ok:
            return False
        
        try:
            res_json = res.json()
            assert res_json['code'] == 0
        except Exception as e:
            print(res.text + str(e))
            return False
        
        return True
    
    def delete_item(self, type: str, data: dict, comment: str = ""):
        
        identifiers = {}
        
        for k in tuple(data.keys()):
            if k in self.source_configuration.unique_columns:
                identifiers[k] = data.pop(k)
        
        wh_string = ' AND '.join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )
        
        body = self._create_body(
            'core/delete',
            {
                'comment': comment,
                'class': type,
                'key': f'SELECT {type} WHERE {wh_string}'
            }
        )
        
        res = requests.post(
            self.url, files=body,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl
        )
        
        if not res.ok:
            return False
        
        try:
            res_json = res.json()
            assert res_json['code'] == 0
        except Exception as e:
            print(res.text + str(e))
            return False
        
        return True

    
    def insert(self, data: pl.DataFrame):
        insertData = data.clone()
        
        insert_cols = tuple(x.name for x in self.source_configuration.columns if x.skip_update is False)
        
        for column in data.columns:
            if column not in insert_cols:
                insertData.drop_in_place(column)

        with alive_bar(len(data)) as pr_bar:
            for item in insertData.iter_rows(named=True):
                self.create_item(
                    self.source_configuration.datamodel,
                    item,
                    "Created via API Sync"
                )
                
                pr_bar()
        
        return len(data)
    
    def update(self, data: pl.DataFrame):
        updateData = data.clone()
        
        update_cols = tuple(
            x.name for x 
            in self.source_configuration.columns 
            if x.skip_update is False 
        )
                
        for column in data.columns:
            if column not in update_cols:
                updateData.drop_in_place(column)
        
        with alive_bar(len(data)) as pr_bar:
            for item in updateData.iter_rows(named=True):
                self.update_item(
                    self.source_configuration.datamodel,
                    item,
                    "Updated via API Sync"
                )

                pr_bar()
        
        return len(data)
    
    def delete(self, data: pl.DataFrame):
        deleteData = data.clone()
        with alive_bar(len(data)) as pr_bar:

            for item in deleteData.iter_rows(named=True):
                self.delete_item(
                    self.source_configuration.datamodel,
                    item,
                    "Deleted via API Sync"
                )
                pr_bar()
                        
        return len(data)