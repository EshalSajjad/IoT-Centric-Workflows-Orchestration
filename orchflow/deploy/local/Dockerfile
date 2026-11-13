# One image for every Orchflow node (coordinator, peers, devices).
# Build context = the orchflow/ folder (see docker-compose.yml).
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[live]"
COPY workflows ./workflows
COPY configs ./configs
COPY deploy ./deploy
ENTRYPOINT ["orchflow"]
