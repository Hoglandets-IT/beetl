#!/bin/sh

mkdir /tmp/beetl \
    && curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list \
    && sudo apt-get update \
    && sudo ACCEPT_EULA=Y apt-get -y install libmariadb-dev libpq-dev unixodbc unixodbc-dev python3-venv msodbcsql18 libgssapi-krb5-2 \
    && sudo apt-get clean \
    && python -m pip install --user -r requirements.txt \
    && python -m pip install build twine