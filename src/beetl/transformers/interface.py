from enum import Enum
from dataclasses import dataclass
from polars import DataFrame as POLARS_DF

FUNC_TYPE = type(print)
CLS_TYPE = type(Enum)

def register_transformer(tr_type: str, namespace: str, name: str):
    def wrapper(func: FUNC_TYPE):
        Transformers._registerFunction(tr_type, namespace, name, func)
        return func
    return wrapper

# def register_sourcetransformer(namespace: str, name: str):
#     def wrapper(func: FUNC_TYPE):

class TransformerTypes(Enum):
    FIELD = "field"
    SOURCE = "source"
        
class FieldTransformerInterface:
    pass          

class Transformers:
    field_transformers: dict = {}
    source_transformers: dict = {}

    @staticmethod
    def _registerFunction(tr_type: str, namespace: str, name: str, func: FUNC_TYPE):
        getattr(__class__, f'{tr_type}_transformers')[f'{namespace}.{name}'] = func

    @staticmethod
    def _registerClass(tr_type: str, cls: CLS_TYPE):
        for fun in dir(cls):
            if not fun.startswith('_'):
                Transformers._registerFunction(f'{tr_type}_transformers', cls.__name__, fun, getattr(cls, fun))
    
    @staticmethod
    def _runTransformer(tr_type: str, transformer: str, data: POLARS_DF, **kwargs) -> POLARS_DF:
        try:
            return getattr(__class__, f'{tr_type}_transformers')[transformer](data, **kwargs)
            # return FieldTransformers.transformers[transformer](data, **kwargs)
        except TypeError as e:
            raise TypeError(f'The wrong arguments supplied for transformer {transformer}: {str(e)}') from e
        except KeyError as e:
            raise KeyError(f'The transformer {transformer} does not exist') from e
        except Exception as e:
            raise Exception(f'An error occurred while running transformer {transformer}: {str(e)}') from e

class FieldTransformers(Transformers):
    """Static class holding all available field transformers"""    
    @staticmethod
    def registerFunction(namespace: str, name: str, func: FUNC_TYPE): __class__._registerFunction('field', namespace, name, func)
    
    @staticmethod
    def registerClass(cls: CLS_TYPE): __class__._registerClass('field', cls)
    
    @staticmethod
    def runTransformer(transformer: str, data: POLARS_DF, **kwargs) -> POLARS_DF: return __class__._runTransformer('field', transformer, data, **kwargs)

class SourceTransformers(Transformers):
    """Static class holding all available source transformers"""
    @staticmethod
    def registerFunction(namespace: str, name: str, func: FUNC_TYPE): __class__._registerFunction('source', namespace, name, func)
    
    @staticmethod
    def registerClass(cls: CLS_TYPE): __class__._registerClass('source', cls)
    
    @staticmethod
    def runTransformer(transformer: str, data: POLARS_DF, **kwargs) -> POLARS_DF: return __class__._runTransformer('source', transformer, data, **kwargs)

class TransformerConfiguration:
    """The configuration class for transformers"""
    kind: TransformerTypes
    identifier: str
    config: dict = None
    
    def __init__(self, kind: str, identifier: str, config: dict = None) -> None:
        self.kind = TransformerTypes(kind)
        self.identifier = identifier
        self.config = config
    
    def transform(self, data: POLARS_DF):
        if self.kind == TransformerTypes.SOURCE:
            return SourceTransformers.runTransformer(self.identifier, data, **self.config)

        return FieldTransformers.runTransformer(self.identifier, data, **self.config)
        
