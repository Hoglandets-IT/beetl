curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list \
    && sudo apt-get update \
    && export ACCEPT_EULA=Y \
    && sudo apt-get -y install libmariadb-dev libpq-dev unixodbc unixodbc-dev python3-venv msodbcsql18 libgssapi-krb5-2 \
    && sudo apt-get clean \
    && pip3 install --user -r requirements.txt \
    && pip3 install --user -r doc-requirements.txt