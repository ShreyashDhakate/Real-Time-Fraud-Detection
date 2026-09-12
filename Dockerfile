FROM python:3.12.9-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md constraints.txt ./
COPY src ./src
RUN pip install -c constraints.txt . && useradd --create-home app && mkdir -p /app/data /app/artifacts /app/reports && chown -R app:app /app
COPY contracts ./contracts
COPY scripts ./scripts
USER app
ENTRYPOINT ["fraud"]
CMD ["serve"]
