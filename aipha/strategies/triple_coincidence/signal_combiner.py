# signal_combiner.py

import pandas as pd
import numpy as np

class SignalCombiner:
    """
    Combina las señales detectadas para identificar eventos de "Triple Coincidencia".
    """

    @staticmethod
    def combine(
        df: pd.DataFrame,
        proximity_lookback: int = 8,
        min_zone_quality: float = 0.0,
        min_trend_r2: float = 0.0
    ) -> pd.DataFrame:
        """
        Identifica velas donde una Vela Clave coincide con una Zona de Acumulación
        y una Mini-Tendencia que cumplen con umbrales de calidad mínimos.

        Args:
            df (pd.DataFrame): DataFrame enriquecido por los detectores.
            proximity_lookback (int): Ventana de búsqueda para la coincidencia.
            min_zone_quality (float): Umbral mínimo de calidad para la zona.
            min_trend_r2 (float): Umbral mínimo de R^2 para la tendencia.

        Returns:
            pd.DataFrame: DataFrame con la columna 'is_triple_coincidence'.
        """
        required_cols = [
            'is_key_candle', 'in_accumulation_zone', 'zone_quality_score',
            'is_in_trend', 'trend_r_squared', 'trend_slope', 'trend_direction'
        ]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"La columna requerida '{col}' no se encuentra en el DataFrame.")

        df_res = df.copy()
        df_res['is_triple_coincidence'] = False
        df_res['coincident_zone_score'] = np.nan
        df_res['coincident_trend_r2'] = np.nan
        df_res['coincident_trend_slope'] = np.nan
        df_res['coincident_trend_direction'] = 'ninguna'

        key_candle_indices = df_res.index[df_res['is_key_candle']]

        for idx in key_candle_indices:
            start_window = max(0, idx - proximity_lookback)
            end_window = idx
            
            if start_window >= end_window:
                continue

            window_df = df_res.iloc[start_window:end_window]

            # Filtrar por zonas y tendencias que cumplan el umbral de calidad
            valid_zones = window_df[window_df['zone_quality_score'] >= min_zone_quality]
            valid_trends = window_df[window_df['trend_r_squared'] >= min_trend_r2]

            if not valid_zones.empty and not valid_trends.empty:
                df_res.loc[idx, 'is_triple_coincidence'] = True
                
                # Usar la ocurrencia más reciente que cumple los criterios
                last_valid_zone = valid_zones.iloc[-1]
                last_valid_trend = valid_trends.iloc[-1]
                
                df_res.loc[idx, 'coincident_zone_score'] = last_valid_zone['zone_quality_score']
                df_res.loc[idx, 'coincident_trend_r2'] = last_valid_trend['trend_r_squared']
                df_res.loc[idx, 'coincident_trend_slope'] = last_valid_trend['trend_slope']
                df_res.loc[idx, 'coincident_trend_direction'] = last_valid_trend['trend_direction']

        return df_res