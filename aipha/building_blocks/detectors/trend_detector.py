
# trend_detector.py

import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any

class TrendDetector:
    """
    Detecta segmentos de mini-tendencia en un DataFrame de klines
    usando un algoritmo ZigZag y validando con regresión lineal.
    """

    @staticmethod
    def _detect_zigzag_pivots(
        df: pd.DataFrame, 
        threshold: float = 0.02  # Corresponde a un cambio del 2%
    ) -> List[int]:
        """
        Detecta pivotes ZigZag basados en un umbral de cambio de precio porcentual.
        Devuelve una lista con los índices del DataFrame que son pivotes.
        """
        pivots = [0]
        last_pivot_idx = 0
        trend = 0  # 1 para alcista, -1 para bajista, 0 para no definido
        
        highs = df['High']
        lows = df['Low']
        
        for i in range(1, len(df)):
            if trend == 0:
                # Establecer tendencia inicial
                if highs.iloc[i] / lows.iloc[last_pivot_idx] - 1 > threshold:
                    trend = 1
                    last_pivot_idx = i
                elif lows.iloc[i] / highs.iloc[last_pivot_idx] - 1 < -threshold:
                    trend = -1
                    last_pivot_idx = i
            elif trend == 1:
                # En tendencia alcista, buscar un nuevo máximo o un cambio de tendencia
                if highs.iloc[i] > highs.iloc[last_pivot_idx]:
                    last_pivot_idx = i
                elif lows.iloc[i] / highs.iloc[last_pivot_idx] - 1 < -threshold:
                    pivots.append(last_pivot_idx)
                    trend = -1
                    last_pivot_idx = i
            elif trend == -1:
                # En tendencia bajista, buscar un nuevo mínimo o un cambio de tendencia
                if lows.iloc[i] < lows.iloc[last_pivot_idx]:
                    last_pivot_idx = i
                elif highs.iloc[i] / lows.iloc[last_pivot_idx] - 1 > threshold:
                    pivots.append(last_pivot_idx)
                    trend = 1
                    last_pivot_idx = i
        
        # Añadir el último pivote
        if last_pivot_idx not in pivots:
            pivots.append(last_pivot_idx)
            
        return sorted(list(set(pivots)))

    @staticmethod
    def _calculate_segment_regression(segment_df: pd.DataFrame) -> Dict[str, Any]:
        """Calcula la regresión lineal para un segmento de tendencia."""
        if len(segment_df) < 2:
            return {'slope': 0, 'r_squared': 0, 'direction': 'plana'}
        
        x = np.arange(len(segment_df))
        y = segment_df['Close'].values
        
        # Evitar error en segmentos planos
        if np.all(y == y[0]):
             return {'slope': 0, 'r_squared': 1, 'direction': 'plana'}

        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        
        p = np.poly1d(coeffs)
        y_fit = p(x)
        
        ss_total = np.sum((y - np.mean(y))**2)
        ss_residual = np.sum((y - y_fit)**2)
        
        r_squared = 1 - (ss_residual / ss_total) if ss_total > 0 else 0
        
        direction = 'alcista' if slope > 0 else 'bajista'
        
        return {'slope': slope, 'r_squared': r_squared, 'direction': direction}

    @staticmethod
    def detect(
        df: pd.DataFrame,
        zigzag_threshold: float = 0.02,
        min_trend_bars: int = 5
    ) -> pd.DataFrame:
        """
        Detecta mini-tendencias, calcula su calidad y anota el DataFrame.
        
        Añade las columnas: 'is_in_trend', 'trend_id', 'trend_r_squared', 
        'trend_slope', y 'trend_direction'.
        """
        df_res = df.copy()
        
        # 1. Inicializar columnas de resultados
        df_res['is_in_trend'] = False
        df_res['trend_id'] = 0
        df_res['trend_r_squared'] = 0.0
        df_res['trend_slope'] = 0.0
        df_res['trend_direction'] = 'ninguna'
        
        # 2. Detectar pivotes
        pivots = TrendDetector._detect_zigzag_pivots(df_res, threshold=zigzag_threshold)
        
        if len(pivots) < 2:
            return df_res # No hay suficientes pivotes para formar tendencias
            
        # 3. Procesar segmentos entre pivotes
        trend_counter = 0
        for i in range(len(pivots) - 1):
            start_idx = pivots[i]
            end_idx = pivots[i+1]
            
            # Validar longitud del segmento
            if (end_idx - start_idx + 1) < min_trend_bars:
                continue
            
            trend_counter += 1
            segment_df = df_res.iloc[start_idx : end_idx + 1]
            
            # Calcular regresión para el segmento
            regression_stats = TrendDetector._calculate_segment_regression(segment_df)
            
            # Anotar el DataFrame con los resultados de la tendencia
            df_res.loc[start_idx : end_idx, 'is_in_trend'] = True
            df_res.loc[start_idx : end_idx, 'trend_id'] = trend_counter
            df_res.loc[start_idx : end_idx, 'trend_r_squared'] = regression_stats['r_squared']
            df_res.loc[start_idx : end_idx, 'trend_slope'] = regression_stats['slope']
            df_res.loc[start_idx : end_idx, 'trend_direction'] = regression_stats['direction']
            
        return df_res
