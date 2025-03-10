class ConfigValidationError(Exception):
    """An error for when a model fails to validate, can contain multiple errors"""

    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return "".join(str(error) for error in self.errors)


class ConfigValueError(Exception):
    """An error for when a field fails to validate"""

    message: str
    location: tuple[str]
    further_information_url: str = ""

    def __init__(self, field: str, message: str, location: tuple[str]):
        self.message = message
        self.location = location + (field,)

    def create_further_information_string(self):
        if self.further_information_url:
            return f"\n{'    '}For further information visit: {self.further_information_url}"

        return "\n    No further information available for this error type"

    def __str__(self):
        location = ".".join(self.location)
        further_information = self.create_further_information_string()
        return f"\n{location}\n{'  '}{self.message}{further_information}"


class InvalidDependencyError(ConfigValueError):
    def __init__(self, field: str, details: str, location: tuple[str]):
        super().__init__(
            field,
            f"Field '{field}' results in an invalid dependency: {details}",
            location,
        )

class MissingDependencyError(ConfigValueError):
    def __init__(self, field: str, details: str, location: tuple[str]):
        super().__init__(
            field,
            f"Field '{field}' results in a missing dependency: {details}",
            location,
        )

class RequiredDestinationFieldError(ConfigValueError):
    def __init__(self, field: str, destination_location: tuple[str]):
        super().__init__(
            field,
            f"Field '{field}' is required when used as destination",
            destination_location,
        )


class RequiredDestinationFieldError(ConfigValueError):
    def __init__(self, field: str, destination_location: tuple[str]):
        super().__init__(
            field,
            f"Field '{field}' is required when used as destination",
            destination_location,
        )


class RequiredSourceFieldError(ConfigValueError):
    def __init__(self, field: str, source_location: tuple[str]):
        super().__init__(
            field, f"Field '{field}' is required when used as source", source_location
        )


class ForbiddenSourceFieldError(ConfigValueError):
    def __init__(self, field: str, source_location: tuple[str]):
        super().__init__(
            field, f"Field '{field}' is forbidden when used as source", source_location
        )

