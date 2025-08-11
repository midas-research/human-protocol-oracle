FROM python:3.10

WORKDIR /app

RUN apt-get update -y && \
    apt-get install -y jq ffmpeg libsm6 libxext6 && \
    pip install --no-cache poetry

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root \
    && poetry cache clear pypi --all

COPY . .

EXPOSE 8010 6379 5432

CMD ["sh", "./bin/start_dev.sh"]
