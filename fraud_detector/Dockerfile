FROM python:3.11-slim

WORKDIR /app

# Создание директории для логов
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs

# Копирование файла зависимостей
COPY requirements.txt .

# Обновление pip и установка зависимостей
RUN python -m pip install --upgrade pip --root-user-action=ignore && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

    

# Копирование исходного кода
COPY . .

# Порт для Prometheus metrics, если метрики используются в сервисе
EXPOSE 8000

# Запуск сервиса скоринга
CMD ["python", "./app/app.py"]