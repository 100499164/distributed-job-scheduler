FROM python:3.12.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml ./
COPY scheduler ./scheduler
COPY benchmarks ./benchmarks
COPY deploy ./deploy
RUN python -m pip install --no-cache-dir --no-deps --no-build-isolation . \
    && useradd --create-home --uid 10001 scheduler
USER 10001:10001
CMD ["python", "-m", "uvicorn", "scheduler.control_plane.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
