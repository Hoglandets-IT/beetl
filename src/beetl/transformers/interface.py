from enum import Enum
from typing import Union
from polars import DataFrame as POLARS_DF

FUNC_TYPE = type(print)
CLS_TYPE = type(Enum)


def register_transformer(namespace: str, name: str):
    def wrapper(func: FUNC_TYPE):
        Transformers._registerFunction(namespace, name, func)
        return func

    return wrapper


def register_transformer_class(namespace: str):
    def wrapper(cls: CLS_TYPE):
        Transformers._registerClass(cls, namespace)
        return cls

    return wrapper


class TransformerInterface:
    @staticmethod
    def _validate_fields(
        columns: Union[list, set, tuple], fields: Union[str, list, set, tuple]
    ):
        if isinstance(fields, str):
            fields = [fields]

        fields_not_in_columns = [
            field for field in fields if field not in columns]
        if any(fields_not_in_columns):
            raise KeyError(
                f'Some of the field(s) {",".join(fields_not_in_columns)} are not present '
                "in the dataset. Valid columns at this stage are:"
                f'{",".join(columns)}'
            )

        return


class Transformers:
    transformers: dict = {}

    @staticmethod
    def _registerFunction(namespace: str, name: str, func: FUNC_TYPE):
        Transformers.transformers[f"{namespace}.{name}"] = func

    @staticmethod
    def _registerClass(cls: CLS_TYPE, namespace: str = None):
        namespace = namespace if namespace is not None else cls.__name__
        for fun in dir(cls):
            if not fun.startswith("_"):
                Transformers._registerFunction(
                    namespace, fun, getattr(cls, fun))

    @staticmethod
    def runTransformer(transformer: str, data: POLARS_DF, **kwargs) -> POLARS_DF:
        try:
            return Transformers.transformers[transformer](data, **kwargs)
        except TypeError as e:
            raise TypeError(
                f"The wrong arguments supplied for transformer {transformer}: {str(e)}"
            ) from e
        except KeyError as e:
            raise KeyError(
                f"The transformer {transformer} does not exist or an error occured in the transformer: ",
                str(e),
            ) from e
        except Exception as e:
            raise Exception(
                f"An error occurred while running transformer {transformer}: {str(e)}"
            ) from e


class TransformerConfiguration:
    """The configuration class for transformers"""

    identifier: str
    config: dict = None
    include_sync: bool = None

    def __init__(
        self, identifier: str, config: dict = None, include_sync: bool = False
    ) -> None:
        self.identifier = identifier
        self.config = config
        self.include_sync = include_sync

    def transform(self, data: POLARS_DF, **kwargs):
        config = self.config or {}
        if data.is_empty():
            return data

        if len(data) == 0:
            return data

        return Transformers.runTransformer(self.identifier, data, **config, **kwargs)
