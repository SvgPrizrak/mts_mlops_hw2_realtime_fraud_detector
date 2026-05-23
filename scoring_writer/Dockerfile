FROM python:3.11-slim

WORKDIR /app

# Копирование файла зависимостей
COPY requirements.txt .

# Обновление pip и установка зависимостей
RUN python -m pip install --upgrade pip --root-user-action=ignore && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Копирование исходного кода
COPY . .

# Порт для Prometheus metrics, если метрики используются в сервисе
EXPOSE 8001

# Запуск сервиса записи результатов скоринга в PostgreSQL
CMD ["python", "app.py"]