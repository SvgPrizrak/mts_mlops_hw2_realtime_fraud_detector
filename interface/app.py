import json
import os
import time

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from kafka import KafkaProducer

st.set_page_config(
    page_title="Realtime Fraud Detection Service",
    layout="wide",
)


def create_kafka_producer() -> KafkaProducer:
    """
    Создаёт Kafka producer для отправки транзакций.
    """
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        ).encode("utf-8"),
    )


def get_postgres_connection():
    """
    Создаёт подключение к PostgreSQL.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fraud_db"),
        user=os.getenv("POSTGRES_USER", "fraud_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_pass"),
    )


def prepare_message(row: pd.Series, transaction_id: str) -> dict:
    """
    Преобразует строку DataFrame в JSON-сообщение для Kafka.
    """
    message = row.where(pd.notnull(row), None).to_dict()

    message["transaction_id"] = transaction_id

    return message


def send_transactions_to_kafka(
    df: pd.DataFrame,
    delay_seconds: float = 0.0,
    max_rows: int | None = None,
) -> int:
    """
    Отправляет строки из DataFrame в Kafka topic transactions.
    """
    transactions_topic = os.getenv(
        "KAFKA_TRANSACTIONS_TOPIC",
        "transactions",
    )

    producer = create_kafka_producer()

    if max_rows is not None and max_rows > 0:
        df_to_send = df.head(max_rows)
    else:
        df_to_send = df

    sent_count = 0

    for idx, row in df_to_send.iterrows():
        transaction_id = str(idx)

        message = prepare_message(
            row=row,
            transaction_id=transaction_id,
        )

        producer.send(
            transactions_topic,
            value=message,
        )

        sent_count += 1

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    producer.flush()
    producer.close()

    return sent_count


def read_total_scores_count() -> int:
    """
    Возвращает количество записей в таблице scores.
    """
    query = """
        SELECT COUNT(*) AS cnt
        FROM scores;
    """

    with get_postgres_connection() as conn:
        df = pd.read_sql_query(query, conn)

    return int(df.loc[0, "cnt"])


def read_last_fraud_transactions() -> pd.DataFrame:
    """
    Возвращает последние 10 транзакций с fraud_flag == 1.
    """
    query = """
        SELECT
            transaction_id,
            score,
            fraud_flag,
            us_state,
            merch,
            cat_id,
            created_at
        FROM scores
        WHERE fraud_flag = 1
        ORDER BY created_at DESC
        LIMIT 10;
    """

    with get_postgres_connection() as conn:
        return pd.read_sql_query(query, conn)


def read_last_scores() -> pd.DataFrame:
    """
    Возвращает последние 100 транзакций для гистограммы score.
    """
    query = """
        SELECT
            transaction_id,
            score,
            fraud_flag,
            us_state,
            merch,
            cat_id,
            created_at
        FROM scores
        ORDER BY created_at DESC
        LIMIT 100;
    """

    with get_postgres_connection() as conn:
        return pd.read_sql_query(query, conn)


st.title("Realtime Fraud Detection Service")

st.markdown(
    """
    Сервис имитирует поток транзакций через Kafka, применяет ML-модель для fraud scoring
    и сохраняет результаты в PostgreSQL.
    """
)

tab_upload, tab_results = st.tabs(
    [
        "Отправка транзакций",
        "Посмотреть результаты",
    ]
)

with tab_upload:
    st.header("Отправка test.csv в Kafka")

    uploaded_file = st.file_uploader(
        "Загрузите test.csv",
        type=["csv"],
    )

    col_1, col_2 = st.columns(2)

    with col_1:
        delay_seconds = st.number_input(
            "Задержка между сообщениями, секунд",
            min_value=0.0,
            max_value=5.0,
            value=0.0,
            step=0.1,
        )

    with col_2:
        max_rows = st.number_input(
            "Сколько строк отправить, 0 = все строки",
            min_value=0,
            value=1000,
            step=100,
        )

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)

        st.subheader("Preview загруженного файла")
        st.dataframe(df.head())

        st.write(f"Количество строк в файле: {len(df)}")

        if st.button("Отправить транзакции в Kafka"):
            rows_limit = None if max_rows == 0 else int(max_rows)

            with st.spinner("Отправляем транзакции в Kafka..."):
                sent_count = send_transactions_to_kafka(
                    df=df,
                    delay_seconds=float(delay_seconds),
                    max_rows=rows_limit,
                )

            st.success(f"Отправлено сообщений в Kafka: {sent_count}")


with tab_results:
    st.header("Результаты скоринга из PostgreSQL")

    if st.button("Посмотреть результаты"):
        try:
            total_count = read_total_scores_count()

            st.metric(
                label="Всего записей в PostgreSQL",
                value=total_count,
            )

            fraud_df = read_last_fraud_transactions()

            st.subheader("10 последних транзакций с fraud_flag == 1")

            if fraud_df.empty:
                st.info("Фродовых транзакций пока нет.")
            else:
                st.dataframe(fraud_df)

            scores_df = read_last_scores()

            st.subheader("Гистограмма скоров последних 100 транзакций")

            if scores_df.empty:
                st.info("В базе пока нет записей.")
            else:
                fig = px.histogram(
                    scores_df,
                    x="score",
                    nbins=30,
                    title="Score distribution for last 100 transactions",
                )

                st.plotly_chart(
                    fig,
                    use_container_width=True,
                )

                st.subheader("Последние 100 транзакций")
                st.dataframe(scores_df)

        except Exception as error:
            st.error(f"Ошибка при чтении результатов из PostgreSQL: {error}")
