FROM python:3.11-slim

WORKDIR /app

# Создаём директорию для Streamlit config
RUN mkdir -p /root/.streamlit

# Копируем Streamlit config
COPY .streamlit/config.toml /root/.streamlit/config.toml

# Обновление pip и установка зависимостей
COPY requirements.txt .

RUN python -m pip install --upgrade pip --root-user-action=ignore && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Копируем код приложения
COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]