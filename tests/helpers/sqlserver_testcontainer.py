import pyodbc


def to_connection_string(connection_url: str) -> str:
    drivers = pyodbc.drivers()
    if not drivers:
        raise Exception(
            "No ODBC drivers found for SQL Server/pyodbc. Please make sure at least one is installed"
        )
    driver_param = f"driver={str.replace(drivers[0], ' ', '+')}"
    accept_cert_param = "TrustServerCertificate=Yes"
    connection_url_without_dialect = connection_url.replace(
        "mssql+pymssql://", "mssql://")
    return f"{connection_url_without_dialect}?{driver_param}&{accept_cert_param}"
