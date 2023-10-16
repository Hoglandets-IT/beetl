FROM python:3.11-bullseye

WORKDIR /app

RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install libmariadb-dev libpq-dev unixodbc unixodbc-dev python3-venv msodbcsql18 libgssapi-krb5-2 \
    && apt-get clean \
    && chown -R 1000:1000 /app \
    && adduser \
        --disabled-password \
        --gecos "" \
        --home /app \
        --uid 1000 \
        app

USER 1000

RUN pip3 install --upgrade pip \
    && python3 -m venv .venv \
    && . .venv/bin/activate \
    && pip3 install --no-cache beetl

COPY --chown=1000:1000 entrypoint.py entrypoint.py

ENTRYPOINT ["/app/.venv/bin/python3", "entrypoint.py"]
CMD [""]
