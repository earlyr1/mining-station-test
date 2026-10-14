# syntax=docker/dockerfile:1
# Один образ приложения на все команды: vq-dispatcher, vq-generator, vq-viewer, vq-verify, vq-bench (§7).
FROM python:3.13.15-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.12.20 /uv /uvx /bin/

# Байткод при установке; копирование вместо хардлинков (кеш uv на другом томе);
# только системный Python образа — от него зависит детерминизм (§2.7).
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# Слой зависимостей пересобирается только при смене uv.lock / pyproject.toml.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Код, конфигурация площадки и миграции; проект ставится в окружение (editable, из /app/src).
COPY src ./src
COPY config ./config
COPY migrations ./migrations
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Непривилегированный пользователь по умолчанию. Compose запускает сервисы от пользователя хоста
# (user: VQ_UID:VQ_GID из Makefile), чтобы писать в ./out; файлы образа доступны любому на чтение.
RUN useradd --uid 1000 --user-group --create-home --shell /usr/sbin/nologin vq
USER vq

CMD ["vq-dispatcher"]
