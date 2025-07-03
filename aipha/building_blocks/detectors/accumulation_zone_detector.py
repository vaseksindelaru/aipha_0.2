# accumulation_zone_detector.py

import pandas as pd
import numpy as np
from typing import Dict, Any

class AccumulationZoneDetector:
    """
    Detecta Zonas de Acumulación en un DataFrame de klines.
    Añade las columnas 'in_accumulation_zone' y 'zone_quality_score'.
    """

    @staticmethod
    def _calculate_atr(df: pd.DataFrame, period: int) -> pd.Series:
        """Calcula el Average True Range (ATR) usando EWM para suavizado."""
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift(1)).abs()
        low_close = (df['Low'] - df['Close'].shift(1)).abs()
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
        return atr

    @staticmethod
    def _calculate_zone_quality(df_slice: pd.DataFrame, zone_stats: Dict[str, Any]) -> float:
        """Calcula la puntuación de calidad para una zona de acumulación."""
        if df_slice.empty or zone_stats['periods'] == 0: return 0.0

        avg_atr = df_slice['atr'].mean()
        avg_vol_ma = df_slice['volume_ma'].mean()
        if avg_atr == 0 or avg_vol_ma == 0: return 0.0
            
        volume_factor = zone_stats['volume_sum'] / (avg_vol_ma * zone_stats['periods'])
        price_range_vs_atr = (zone_stats['price_high'] - zone_stats['price_low']) / avg_atr
        time_factor = min(1.0, zone_stats['periods'] / 20.0)
        price_factor = 1.0 / (price_range_vs_atr + 1e-6)
        
        score = (volume_factor * 0.4) + (price_factor * 0.4) + (time_factor * 0.2)
        return min(2.0, score)

    @staticmethod
    def detect(
        df: pd.DataFrame,
        volume_threshold: float = 1.1,
        atr_multiplier: float = 1.0,
        min_zone_periods: int = 5,
        volume_ma_period: int = 30,
        atr_period: int = 14
    ) -> pd.DataFrame:
        """
        Detecta zonas de acumulación y añade la información al DataFrame.
        """
        df_res = df.copy()
        
        df_res['atr'] = AccumulationZoneDetector._calculate_atr(df_res, period=atr_period)
        df_res['volume_ma'] = df_res['Volume'].rolling(window=volume_ma_period, min_periods=volume_ma_period // 2).mean().shift(1)
        
        df_res['in_accumulation_zone'] = False
        df_res['zone_quality_score'] = 0.0
        current_zone = None
        
        for i in range(len(df_res)):
            if pd.isna(df_res['atr'].iloc[i]) or pd.isna(df_res['volume_ma'].iloc[i]): continue
            
            volume_condition = df_res['Volume'].iloc[i] > df_res['volume_ma'].iloc[i] * volume_threshold
            
            if current_zone is None:
                if volume_condition:
                    current_zone = {'start_idx': i, 'price_high': df_res['High'].iloc[i], 'price_low': df_res['Low'].iloc[i], 'volume_sum': df_res['Volume'].iloc[i], 'periods': 1}
            else:
                current_high = max(current_zone['price_high'], df_res['High'].iloc[i])
                current_low = min(current_zone['price_low'], df_res['Low'].iloc[i])
                zone_height = current_high - current_low
                max_height_allowed = df_res['atr'].iloc[i] * atr_multiplier
                
                if zone_height <= max_height_allowed:
                    current_zone['price_high'], current_zone['price_low'], current_zone['volume_sum'], current_zone['periods'] = current_high, current_low, current_zone['volume_sum'] + df_res['Volume'].iloc[i], current_zone['periods'] + 1
                else:
                    if current_zone['periods'] >= min_zone_periods:
                        start, end = current_zone['start_idx'], i
                        zone_slice = df_res.iloc[start:end]
                        quality_score = AccumulationZoneDetector._calculate_zone_quality(zone_slice, current_zone)
                        df_res.loc[start:end-1, ['in_accumulation_zone', 'zone_quality_score']] = True, quality_score
                    
                    current_zone = {'start_idx': i, 'price_high': df_res['High'].iloc[i], 'price_low': df_res['Low'].iloc[i], 'volume_sum': df_res['Volume'].iloc[i], 'periods': 1} if volume_condition else None
        
        if current_zone is not None and current_zone['periods'] >= min_zone_periods:
            start, end = current_zone['start_idx'], len(df_res)
            zone_slice = df_res.iloc[start:end]
            quality_score = AccumulationZoneDetector._calculate_zone_quality(zone_slice, current_zone)
            df_res.loc[start:end, ['in_accumulation_zone', 'zone_quality_score']] = True, quality_score
            
        return df_res