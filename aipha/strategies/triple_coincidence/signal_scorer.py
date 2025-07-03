# signal_scorer.py

import pandas as pd
import numpy as np

class SignalScorer:
    """
    Calcula una puntuación detallada para las señales de Triple Coincidencia.
    """

    @staticmethod
    def _normalize(value: float, min_val: float, max_val: float) -> float:
        """Normaliza un valor a un rango de 0 a 1."""
        if max_val == min_val:
            return 0.5 # Avoid division by zero, return neutral value
        return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))

    @staticmethod
    def _score_candle(candle_row: pd.Series) -> float:
        """Puntúa el componente Vela Clave (30% del peso base)."""
        # Puntuación de Volumen (60% de la puntuación de la vela)
        # Asumimos que un volumen muy alto es bueno. Usamos el umbral ya calculado.
        # Un volumen justo en el umbral obtiene 0.5, el doble del umbral obtiene 1.0
        vol_score = SignalScorer._normalize(candle_row['Volume'], candle_row['volume_threshold'], candle_row['volume_threshold'] * 2)
        
        # Puntuación de Morfología (40% de la puntuación de la vela)
        # Basado en la documentación, un cuerpo de 15-40% es óptimo.
        body_perc = candle_row['body_percentage']
        if 15 <= body_perc <= 40:
            morph_score = 1.0
        elif 40 < body_perc <= 60:
            morph_score = 0.8
        elif 5 < body_perc < 15:
            morph_score = 0.6
        else: # < 5%
            morph_score = 0.3
            
        return (vol_score * 0.6) + (morph_score * 0.4)

    @staticmethod
    def _score_zone(zone_score_value: float) -> float:
        """Puntúa el componente Zona de Acumulación (35% del peso base)."""
        # Normaliza el quality_score de la zona.
        # Tu doc dice: (quality_score - 0.45) / 0.4
        # Esto es una normalización en el rango [0.45, 0.85]
        return SignalScorer._normalize(zone_score_value, 0.45, 0.85)

    @staticmethod
    def _score_trend(trend_r2: float, trend_direction: str, trend_slope: float) -> float:
        """Puntúa el componente Mini-Tendencia (35% del peso base)."""
        # 1. Puntuación base por R-cuadrado
        if trend_r2 >= 0.6:
            r2_score = 1.0 * 1.3 # Premio
        elif trend_r2 >= 0.45:
            r2_score = 1.0
        else:
            r2_score = 1.0 * 0.9 # Penalización
        
        # 2. Factor direccional
        direction_factor = 1.15 if trend_direction == 'alcista' else 0.90
        
        # 3. Factor de pendiente (normalizado)
        # Asumimos que una pendiente más pronunciada es mejor, hasta cierto punto.
        # Normalizamos la pendiente absoluta en un rango esperado, ej. 0 a 0.5
        slope_factor = SignalScorer._normalize(abs(trend_slope), 0, 0.5)

        # La puntuación final de la tendencia es una combinación, con R2 como el más importante.
        final_trend_score = r2_score * direction_factor * slope_factor
        return min(1.5, final_trend_score) # Evitar puntuaciones excesivas

    @staticmethod
    def score(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula la puntuación final para todas las señales de triple coincidencia.
        """
        df_res = df.copy()
        signals = df_res[df_res['is_triple_coincidence']].index

        # Inicializar columnas de puntuación
        df_res['candle_score'] = np.nan
        df_res['zone_score'] = np.nan
        df_res['trend_score'] = np.nan
        df_res['base_score'] = np.nan
        df_res['advanced_score'] = np.nan
        df_res['final_score'] = np.nan

        for idx in signals:
            signal_row = df_res.loc[idx]

            # 1. Puntuación de Componentes Básicos
            candle_score = SignalScorer._score_candle(signal_row)
            zone_score = SignalScorer._score_zone(signal_row['coincident_zone_score'])
            trend_score = SignalScorer._score_trend(
                signal_row['coincident_trend_r2'],
                signal_row['coincident_trend_direction'],
                signal_row['coincident_trend_slope']
            )
            
            df_res.loc[idx, 'candle_score'] = candle_score
            df_res.loc[idx, 'zone_score'] = zone_score
            df_res.loc[idx, 'trend_score'] = trend_score
            
            # Puntuación básica ponderada
            base_score = (candle_score * 0.30) + (zone_score * 0.35) + (trend_score * 0.35)
            df_res.loc[idx, 'base_score'] = base_score

            # 2. Puntuación de Factores Avanzados (30% del total)
            # - Fiabilidad (Bonus por R² alto > 0.75)
            reliability_bonus = 0.15 if signal_row['coincident_trend_r2'] > 0.75 else 0.0
            
            # - Potencial de Rentabilidad (basado en dirección y volumen)
            profit_potential = 0.6 # Base
            if signal_row['coincident_trend_direction'] == 'alcista':
                if signal_row['Volume'] > 80: profit_potential = 0.85
                elif signal_row['Volume'] > 50: profit_potential = 0.75
            
            # Puntuación avanzada simple
            # (Una implementación más compleja podría incluir "Divergencia")
            advanced_score = (reliability_bonus * 0.5) + (profit_potential * 0.5)
            df_res.loc[idx, 'advanced_score'] = advanced_score

            # 3. Cálculo de Puntuación Final
            # Puntuación final es 70% base + 30% avanzado
            final_score = (base_score * 0.7) + (advanced_score * 0.3)
            df_res.loc[idx, 'final_score'] = final_score

        return df_res