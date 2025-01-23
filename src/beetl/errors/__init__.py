class ConfigValidationError(Exception):
    """An error for when a model fails to validate, can contain multiple errors"""

    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return "\n\t" + "\n\t".join(str(error) for error in self.errors)


class ConfigValueError(Exception):
    """An error for when a field fails to validate"""
    message: str
    location: tuple[str]

    def __init__(self, message: str, location: tuple[str]):
        self.message = message
        self.location = location

    def __str__(self):
        return f"{'.'.join(self.location)}: {self.message}"


class RequiredDestinationFieldError(ConfigValueError):
    def __init__(self, field: str, destination_location: tuple[str]):
        location = destination_location + (field,)
        super().__init__(
            f"Field '{field}' is required when used as destination", location)


class RequiredSourceFieldError(ConfigValueError):
    def __init__(self, field: str, source_location: tuple[str]):
        location = source_location + (field,)
        super().__init__(
            f"Field '{field}' is required when used as source", location)
