import pandas as pd
import numpy as np
from typing import Optional


class SignalDetector:
    """Contiene métodos estáticos para detectar
    señales y patrones técnicos en DataFrames de klines.
    """
    
    @staticmethod
    def detect_key_candles(
        df: pd.DataFrame,
        volume_lookback: int = 50,
        volume_percentile_threshold: int = 80,
        body_percentile_threshold: int = 30
    ) -> pd.DataFrame:
        """Identifica Velas Clave basadas en volumen y morfología.
        
        Añade nuevas columnas al DataFrame para el análisis de velas clave,
        incluyendo umbrales de volumen y cuerpo de vela.
        
        Args:
            df: DataFrame con datos de klines
            volume_lookback: Período de lookback para el cálculo de percentiles de volumen
            volume_percentile_threshold: Percentil para considerar volumen alto
            body_percentile_threshold: Percentil para considerar cuerpo de vela pequeño
            
        Returns:
            pd.DataFrame: DataFrame original con columnas adicionales de análisis
        """
        # Crear una copia del DataFrame para evitar modificar el original
        df = df.copy()
        
        # Calcular el umbral de volumen basado en el percentil histórico
        df['volume_threshold'] = df['Volume'].rolling(
            window=volume_lookback,
            min_periods=volume_lookback // 2
        ).quantile(volume_percentile_threshold / 100.0).shift(1)
        
        # Calcular métricas de morfología de la vela
        df['body_size'] = (df['Close'] - df['Open']).abs()
        df['candle_range'] = df['High'] - df['Low']
        
        # Calcular el porcentaje del cuerpo evitando división por cero
        candle_range_safe = df['candle_range'].replace(0, np.nan)
        df['body_percentage'] = (df['body_size'] / candle_range_safe) * 100
        df['body_percentage'] = df['body_percentage'].fillna(100)  # Rellenar NaNs con 100
        
        # Detectar velas clave basadas en volumen alto y cuerpo pequeño
        high_volume_condition = df['Volume'] >= df['volume_threshold']
        small_body_condition = df['body_percentage'] <= body_percentile_threshold
        df['is_key_candle'] = high_volume_condition & small_body_condition
        
        return df
