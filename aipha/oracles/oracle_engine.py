import pandas as pd
import lightgbm as lgb
from typing import List, Dict, Any
import joblib

class OracleEngine:
    """
    Motor del Oráculo, responsable de entrenar y utilizar un modelo de Gradient Boosting
    para predecir el potencial de riesgo/recompensa de eventos de trading.
    """

    def __init__(self, model_path: str = None):
        """
        Inicializa el motor.

        Args:
            model_path (str, optional): Ruta para cargar un modelo pre-entrenado.
        """
        self.model = None
        if model_path:
            self.load_model(model_path)

    def train(self, features: pd.DataFrame, targets: pd.DataFrame, params: Dict[str, Any]):
        """
        Entrena un nuevo modelo de Oráculo.

        El modelo aprenderá a predecir dos valores simultáneamente:
        - potential_score_long
        - potential_score_short

        Args:
            features (pd.DataFrame): DataFrame con las características de entrada.
            targets (pd.DataFrame): DataFrame con las columnas 'potential_score_long' y 'potential_score_short'.
            params (Dict[str, Any]): Parámetros para el modelo LightGBM.
        """
        print("Iniciando entrenamiento del Oráculo...")

        # LightGBM puede entrenar un modelo para predecir múltiples salidas directamente
        self.model = lgb.LGBMRegressor(**params)
        self.model.fit(features, targets)

        print("Entrenamiento del Oráculo finalizado.")

    def predict(self, features: pd.DataFrame, prediction_column_name: str = 'prediction') -> pd.DataFrame:
        """
        Realiza predicciones sobre nuevos datos.

        Args:
            features (pd.DataFrame): DataFrame con las características de entrada.

        Returns:
            pd.DataFrame: Un DataFrame con las predicciones 'predicted_score_long' y 'predicted_score_short'.
        """
        if self.model is None:
            raise ValueError("El modelo no ha sido entrenado o cargado. Llama a .train() o .load_model().")

        predictions = self.model.predict(features)
        
        pred_df = pd.DataFrame(predictions, index=features.index, columns=[prediction_column_name])
        
        
        
        return pred_df

    def save_model(self, path: str):
        """
        Guarda el modelo entrenado en un archivo.

        Args:
            path (str): Ruta donde guardar el modelo.
        """
        if self.model:
            joblib.dump(self.model, path)
            print(f"Modelo del Oráculo guardado en: {path}")
        else:
            print("No hay modelo entrenado para guardar.")

    def load_model(self, path: str):
        """
        Carga un modelo desde un archivo.

        Args:
            path (str): Ruta del modelo a cargar.
        """
        self.model = joblib.load(path)
        print(f"Modelo del Oráculo cargado desde: {path}")

