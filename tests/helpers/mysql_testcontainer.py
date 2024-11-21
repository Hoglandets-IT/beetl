def to_connection_string(testContainerConnectionUrl: str) -> str:
    return testContainerConnectionUrl.replace("mysql+pymysql://", "mysql://")
