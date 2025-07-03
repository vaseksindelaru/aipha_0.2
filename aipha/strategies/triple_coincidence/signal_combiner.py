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
        proximity_lookback: int = 8
    ) -> pd.DataFrame:
        """
        Identifica velas donde una Vela Clave coincide con una Zona de Acumulación
        y una Mini-Tendencia dentro de una ventana de proximidad.

        Args:
            df (pd.DataFrame): DataFrame enriquecido por los detectores, debe contener
                               las columnas 'is_key_candle', 'in_accumulation_zone',
                               'is_in_trend' y sus métricas asociadas.
            proximity_lookback (int): Número de velas hacia atrás para buscar
                                      la coincidencia de zona y tendencia.

        Returns:
            pd.DataFrame: El DataFrame con una nueva columna booleana 
                          'is_triple_coincidence' y columnas con el contexto
                          de la coincidencia.
        """
        # Asegurarse de que las columnas requeridas existen
        required_cols = [
            'is_key_candle', 'in_accumulation_zone', 'zone_quality_score',
            'is_in_trend', 'trend_r_squared', 'trend_slope', 'trend_direction'
        ]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"La columna requerida '{col}' no se encuentra en el DataFrame.")

        df_res = df.copy()
        
        # Inicializar columnas de resultados
        df_res['is_triple_coincidence'] = False
        df_res['coincident_zone_score'] = np.nan
        df_res['coincident_trend_r2'] = np.nan
        df_res['coincident_trend_slope'] = np.nan
        df_res['coincident_trend_direction'] = 'ninguna'

        # Obtener los índices de todas las velas clave para iterar eficientemente
        key_candle_indices = df_res.index[df_res['is_key_candle']]

        # Iterar solo sobre las velas clave
        for idx in key_candle_indices:
            # Definir la ventana de búsqueda hacia atrás (excluyendo la vela actual)
            start_window = max(0, idx - proximity_lookback)
            end_window = idx  # La ventana llega HASTA la vela anterior a la actual
            
            window_df = df_res.iloc[start_window:end_window]
            
            # Comprobar si hay una zona y una tendencia en la ventana
            in_zone_in_window = window_df['in_accumulation_zone'].any()
            in_trend_in_window = window_df['is_in_trend'].any()
            
            # Si ambas condiciones se cumplen, es una triple coincidencia
            if in_zone_in_window and in_trend_in_window:
                df_res.loc[idx, 'is_triple_coincidence'] = True
                
                # Capturar el contexto de la coincidencia (usando la ocurrencia más reciente)
                last_zone_in_window = window_df[window_df['in_accumulation_zone']].iloc[-1]
                last_trend_in_window = window_df[window_df['is_in_trend']].iloc[-1]
                
                df_res.loc[idx, 'coincident_zone_score'] = last_zone_in_window['zone_quality_score']
                df_res.loc[idx, 'coincident_trend_r2'] = last_trend_in_window['trend_r_squared']
                df_res.loc[idx, 'coincident_trend_slope'] = last_trend_in_window['trend_slope']
                df_res.loc[idx, 'coincident_trend_direction'] = last_trend_in_window['trend_direction']

        return df_res