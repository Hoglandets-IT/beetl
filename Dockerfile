FROM python:3.11-bullseye

WORKDIR /app

RUN apt-get update \
    && apt-get -y install libmariadb-dev libpq-dev unixodbc unixodbc-dev python3-venv \
    && apt-get clean \
    && chown -R 1000:1000 /app \
    && adduser \
        --disabled-password \
        --gecos "" \
        --home /app \
        --uid 1000 \
        app

USER 1000

RUN python3 -m venv .venv \
    && .venv/bin/python3 -m pip install --upgrade beetl

COPY --chown=1000:1000 entrypoint.py entrypoint.py

ENTRYPOINT ["/app/.venv/bin/python3", "/app/entrypoint.py"]
