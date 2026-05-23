import logging
import os

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

MODEL_PATH = "./models/my_catboost.cbm"
MODEL_THRESHOLD = float(os.getenv("MODEL_THRESHOLD", "0.98"))


def load_model() -> CatBoostClassifier:
    """
    Загружает обученную CatBoost-модель.
    """
    logger.info("Importing pretrained model...")

    model = CatBoostClassifier()
    model.load_model(MODEL_PATH)

    logger.info("Pretrained model imported successfully")

    return model


def prepare_features_for_model(
    model: CatBoostClassifier,
    processed_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Приводит processed_df к формату, который ожидает модель.

    Это нужно для realtime-сценария, где мы скорим по одной транзакции из Kafka.
    В HW1 модель получала весь batch test.csv, а здесь получает DataFrame из одной строки.
    """
    model_features = list(model.feature_names_)

    if not model_features:
        logger.warning("Model has no feature_names_. Using processed_df as is.")
        return processed_df

    prepared_df = processed_df.copy()

    missing_features = [
        feature for feature in model_features if feature not in prepared_df.columns
    ]

    extra_features = [
        feature for feature in prepared_df.columns if feature not in model_features
    ]

    if missing_features:
        logger.warning("Missing features will be filled with 0: %s", missing_features)

    if extra_features:
        logger.warning("Extra features will be dropped: %s", extra_features)

    for feature in missing_features:
        prepared_df[feature] = 0

    prepared_df = prepared_df[model_features]

    cat_feature_indices = set(model.get_cat_feature_indices())

    for feature_idx, feature_name in enumerate(model_features):
        if feature_idx in cat_feature_indices:
            prepared_df[feature_name] = (
                prepared_df[feature_name].fillna("missing").astype(str)
            )
        else:
            prepared_df[feature_name] = pd.to_numeric(
                prepared_df[feature_name],
                errors="coerce",
            ).fillna(0)

    return prepared_df


def make_predictions(
    model: CatBoostClassifier,
    processed_df: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Логика как в HW1.

    Делает batch inference:
    - scores: вероятности положительного класса;
    - predictions: бинарные предсказания 0/1.
    """
    prepared_df = prepare_features_for_model(
        model=model,
        processed_df=processed_df,
    )

    scores = model.predict_proba(prepared_df)[:, 1]
    predictions = (scores > MODEL_THRESHOLD).astype(int)

    logger.info("Prediction complete")
    logger.info("Scores min: %.6f", scores.min())
    logger.info("Scores max: %.6f", scores.max())
    logger.info("Scores mean: %.6f", scores.mean())
    logger.info("Predicted positives: %s", predictions.sum())
    logger.info("Predicted negatives: %s", len(predictions) - predictions.sum())

    return scores, predictions


def make_prediction(
    model: CatBoostClassifier,
    processed_df: pd.DataFrame,
) -> tuple[float, int]:
    """
    Обёртка для HW2.

    На вход приходит одна транзакция из Kafka,
    но внутри используется та же логика make_predictions, что и в HW1.
    """
    scores, predictions = make_predictions(
        model=model,
        processed_df=processed_df,
    )

    score = float(scores[0])
    fraud_flag = int(predictions[0])

    logger.info(
        "Prediction completed. score=%.6f fraud_flag=%s",
        score,
        fraud_flag,
    )

    return score, fraud_flag
