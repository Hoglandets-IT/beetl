"""The main part of the pytemplate-package."""
class PythonTemplate:
    """
    The PythonTemplate class does something.
    """
    def __init__(self, some_key: str):
        self.some_key = some_key
        self.some_key_list = [some_key]

    def __str__(self):
        return self.some_key

    def get_x_times(self, x: int):
        """
        Returns the class value X times.

        Args:
            x (int): How many times to return

        Returns:
            str: The class value some_key X times
        """
        return "".join([self.some_key for _ in range(x)])
