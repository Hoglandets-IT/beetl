from .interface import SourceInterface


class RegistratedSource:
    name: str
    cls: SourceInterface

    def __init__(self, name: str, cls: SourceInterface):
        self.name = name
        self.cls = cls


def register_source(name: str):
    def wrapper(cls: type):
        Sources.sources[name] = RegistratedSource(name, cls)

        return cls

    return wrapper


class Sources:
    sources: dict[str, RegistratedSource] = {}
