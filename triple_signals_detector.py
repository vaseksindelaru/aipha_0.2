#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Detección de señales de triple coincidencia para trading algorítmico.
Este script identifica señales de trading basadas en la convergencia de:
1. Velas clave (alto volumen y rango de precios significativo)
2. Zonas de acumulación
3. Mini-tendencias
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import json
import argparse
from typing import Dict, List, Optional, Tuple, Union

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/triple_signals.log")
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

class TripleSignalDetector:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.symbol = "BTCUSDT"
        self.timeframe = "5m"
        self.data = None
        self.base_url = "https://api.binance.com/api/v3"
        self.limit = 1000  # Número máximo de velas a obtener

    def connect(self) -> bool:
        """Establece conexión a la base de datos."""
        try:
            self.conn = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USER'),
                password=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
            )
            self.cursor = self.conn.cursor(dictionary=True)
            logger.info("Conexión a la base de datos establecida correctamente")
            return True
        except Error as e:
            logger.error(f"Error al conectar a la base de datos: {e}")
            return False

    def close(self) -> None:
        """Cierra la conexión a la base de datos."""
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            logger.info("Conexión a la base de datos cerrada")
            
    def run(self, data_dir: str = None, days: int = 7, return_results: bool = False):
        """
        Ejecuta el proceso completo de detección de señales.
        
        Args:
            data_dir: Directorio que contiene los archivos CSV con datos históricos
            days: Número de días de datos a analizar
            return_results: Si es True, devuelve los resultados en lugar de guardarlos
            
        Returns:
            Si return_results es True, devuelve una tupla con:
            - df: DataFrame con los datos históricos
            - key_candles: DataFrame con las velas clave detectadas
            - accumulation_zones: DataFrame con las zonas de acumulación detectadas
            - mini_trends: DataFrame con las mini-tendencias detectadas
            - triple_signals: DataFrame con las señales de triple coincidencia
        """
        logger.info(f"Iniciando detección de señales para {self.symbol}-{self.timeframe}")
        
        # Si no se especifica el directorio de datos, usar el predeterminado
        if data_dir is None:
            # Obtener la ruta absoluta del directorio actual
            current_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(current_dir, "data", "data_detect")
            
            # Crear el directorio si no existe
            os.makedirs(data_dir, exist_ok=True)
        
        logger.info(f"Buscando datos en: {data_dir}")
        
        # Obtener datos históricos desde archivos CSV locales
        df = self.load_historical_data(data_dir, days)
        
        if df is None or df.empty:
            logger.error("No se pudieron cargar datos históricos desde los archivos CSV")
            logger.info(f"Asegúrate de que hay archivos CSV en: {data_dir}")
            logger.info("Los archivos deben tener el formato: {symbol}-{timeframe}-YYYY-MM-DD.csv")
            return (None, None, None, None, None) if return_results else None
        
        # Guardar datos históricos para referencia
        self.historical_data = df
        
        # 1. Detectar velas clave
        key_candles = self.detect_key_candles(df)
        logger.info(f"Se identificaron {len(key_candles)} velas clave")
        
        # 2. Detectar zonas de acumulación
        accumulation_zones = self.detect_accumulation_zones(df)
        logger.info(f"Se identificaron {len(accumulation_zones)} zonas de acumulación")
        
        # 3. Detectar mini-tendencias
        mini_trends = self.detect_mini_trends(df)
        logger.info(f"Se identificaron {len(mini_trends)} mini-tendencias")
        
        # 4. Buscar coincidencias de señales
        triple_signals = self.find_triple_signals(key_candles, accumulation_zones, mini_trends)
        
        # Convertir a DataFrame si es necesario
        if isinstance(triple_signals, list):
            if triple_signals:  # Si la lista no está vacía
                triple_signals_df = pd.DataFrame(triple_signals)
                signals_count = len(triple_signals)
            else:
                triple_signals_df = pd.DataFrame()
                signals_count = 0
        else:
            triple_signals_df = triple_signals
            signals_count = len(triple_signals_df) if not triple_signals_df.empty else 0
        
        if signals_count > 0:
            logger.info(f"Se encontraron {signals_count} señales de triple coincidencia")
            # Guardar señales en la base de datos si no se solicitan los resultados
            if not return_results:
                if not triple_signals_df.empty:
                    self.save_triple_signals(triple_signals_df)
        else:
            logger.info("No se encontraron señales de triple coincidencia")
        
        # Guardar señales en la base de datos si no se están devolviendo resultados
        if not return_results and signals_count > 0:
            if self.save_signals_to_db(triple_signals if isinstance(triple_signals, list) else triple_signals.to_dict('records')):
                logger.info("Señales guardadas exitosamente en la base de datos")
            else:
                logger.error("Ocurrió un error al guardar las señales en la base de datos")
        
        # Devolver resultados si se solicitan
        if return_results:
            return df, key_candles, accumulation_zones, mini_trends, triple_signals_df if signals_count > 0 else pd.DataFrame()
        
        logger.info("Proceso completado exitosamente")

    def load_csv_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Carga datos históricos desde un archivo CSV local."""
        try:
            # Leer el archivo CSV
            df = pd.read_csv(
                file_path,
                header=None,
                names=[
                    'open_time', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ]
            )
            
            # Convertir tipos de datos numéricos
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 
                             'quote_asset_volume', 'number_of_trades',
                             'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
            
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Eliminar filas con valores NaN que puedan causar problemas
            df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume', 'open_time'])
            
            # Convertir timestamp de microsegundos a datetime64[ms] (milisegundos)
            # Primero, dividir por 1000 para convertir a milisegundos
            df['timestamp_ms'] = df['open_time'] // 1000
            
            # Verificar que los timestamps estén dentro de los límites de pandas
            min_ts = pd.Timestamp.min.value // 10**6  # Convertir nanosegundos a milisegundos
            max_ts = pd.Timestamp.max.value // 10**6
            
            # Filtrar filas con timestamps fuera de rango
            valid_timestamps = (df['timestamp_ms'] >= min_ts) & (df['timestamp_ms'] <= max_ts)
            df = df[valid_timestamps].copy()
            
            if len(df) == 0:
                logger.warning(f"No hay datos válidos en {file_path} después de filtrar timestamps")
                return None
                
            # Convertir a datetime
            df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
            
            # Ordenar por timestamp para asegurar el orden cronológico
            df = df.sort_values('timestamp')
            
            # Eliminar columnas temporales
            df = df.drop(columns=['timestamp_ms'])
            
            logger.info(f"Datos cargados desde {file_path}: {len(df)} registros")
            return df
            
        except Exception as e:
            import traceback
            logger.error(f"Error al cargar datos desde {file_path}: {e}")
            logger.error(traceback.format_exc())
            return None
            
    def load_historical_data(self, data_dir: str, days: int = 7) -> Optional[pd.DataFrame]:
        """Carga datos históricos de múltiples archivos CSV."""
        try:
            # Obtener la lista de archivos CSV en el directorio
            import os
            import glob
            
            # Patrón para buscar archivos CSV del par de trading actual
            pattern = os.path.join(data_dir, f"{self.symbol}-{self.timeframe}-*.csv")
            csv_files = sorted(glob.glob(pattern))
            
            if not csv_files:
                logger.error(f"No se encontraron archivos CSV que coincidan con el patrón: {pattern}")
                return None
                
            logger.info(f"Encontrados {len(csv_files)} archivos CSV: {csv_files}")
            
            # Cargar y combinar datos de todos los archivos
            dfs = []
            for file_path in csv_files[-days:]:  # Limitar a los últimos 'days' archivos
                df = self.load_csv_data(file_path)
                if df is not None and not df.empty:
                    dfs.append(df)
            
            if not dfs:
                logger.error("No se pudieron cargar datos de ningún archivo")
                return None
                
            # Combinar todos los DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Eliminar duplicados y ordenar por timestamp
            combined_df = combined_df.drop_duplicates(subset=['open_time']).sort_values('timestamp')
            
            logger.info(f"Total de registros cargados: {len(combined_df)}")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error al cargar datos históricos: {e}")
            return None

    def detect_key_candles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identifica velas clave basadas en volumen y rango de precios."""
        # Calcular métricas para identificar velas clave
        df['body'] = abs(df['close'] - df['open'])
        df['body_pct'] = df['body'] / df['open'] * 100
        df['range'] = df['high'] - df['low']
        
        # Umbrales para identificar velas clave
        volume_ma = df['volume'].rolling(window=20).mean()
        body_pct_ma = df['body_pct'].rolling(window=20).mean()
        
        # Identificar velas clave (alto volumen y cuerpo grande)
        df['is_key_candle'] = (df['volume'] > volume_ma * 1.5) & \
                             (df['body_pct'] > body_pct_ma * 1.5)
        
        logger.info(f"Se identificaron {df['is_key_candle'].sum()} velas clave")
        return df

    def detect_accumulation_zones(self, df: pd.DataFrame, window: int = 20) -> List[Dict]:
        """Identifica zonas de acumulación en el DataFrame."""
        # Calcular métricas para identificar zonas de acumulación
        df['sma_volume'] = df['volume'].rolling(window=window).mean()
        df['price_range'] = df['high'] - df['low']
        df['avg_range'] = df['price_range'].rolling(window=window).mean()
        
        # Identificar velas en zonas de acumulación (alto volumen, bajo rango de precios)
        df['is_accumulation'] = (df['volume'] > df['sma_volume']) & \
                               (df['price_range'] < df['avg_range'] * 0.8)
        
        # Agrupar velas adyacentes en zonas
        zones = []
        in_zone = False
        zone_start = None
        
        for idx, row in df.iterrows():
            if row['is_accumulation'] and not in_zone:
                in_zone = True
                zone_start = idx
            elif not row['is_accumulation'] and in_zone:
                in_zone = False
                zone_end = idx - 1
                
                # Solo considerar zonas con al menos 3 velas
                if zone_end - zone_start >= 2:
                    zone_data = df.loc[zone_start:zone_end]
                    zones.append({
                        'start_idx': zone_start,
                        'end_idx': zone_end,
                        'start_datetime': df.loc[zone_start, 'timestamp'],
                        'end_datetime': df.loc[zone_end, 'timestamp'],
                        'high': zone_data['high'].max(),
                        'low': zone_data['low'].min(),
                        'volume': zone_data['volume'].sum(),
                        'quality_score': min(1.0, len(zone_data) / window)
                    })
        
        logger.info(f"Se identificaron {len(zones)} zonas de acumulación")
        return zones

    def detect_mini_trends(self, df: pd.DataFrame, window: int = 10) -> List[Dict]:
        """Identifica mini-tendencias usando el método ZigZag."""
        trends = []
        
        # Identificar máximos y mínimos locales
        pivot_highs = []
        pivot_lows = []
        
        for i in range(window, len(df) - window):
            window_high = df['high'].iloc[i-window:i+window+1]
            window_low = df['low'].iloc[i-window:i+window+1]
            
            # Identificar máximos locales
            if df['high'].iloc[i] == window_high.max():
                pivot_highs.append({
                    'idx': i,
                    'price': df['high'].iloc[i],
                    'timestamp': df['timestamp'].iloc[i]
                })
            
            # Identificar mínimos locales
            if df['low'].iloc[i] == window_low.min():
                pivot_lows.append({
                    'idx': i,
                    'price': df['low'].iloc[i],
                    'timestamp': df['timestamp'].iloc[i]
                })
        
        # Identificar tendencias basadas en pivotes
        for i in range(1, min(len(pivot_highs), len(pivot_lows))):
            # Tendencias alcistas (mínimo -> máximo)
            if pivot_lows[i-1]['idx'] < pivot_highs[i]['idx']:
                start_price = pivot_lows[i-1]['price']
                end_price = pivot_highs[i]['price']
                start_time = pivot_lows[i-1]['timestamp']
                end_time = pivot_highs[i]['timestamp']
                
                # Calcular pendiente (precio/tiempo)
                time_diff = (end_time - start_time).total_seconds() / 3600  # en horas
                price_diff = end_price - start_price
                slope = price_diff / time_diff if time_diff > 0 else 0
                
                # Calcular R² (simplificado)
                x = np.array(range(len(df.loc[pivot_lows[i-1]['idx']:pivot_highs[i]['idx']])))
                y = df['close'].loc[pivot_lows[i-1]['idx']:pivot_highs[i]['idx']].values
                if len(x) > 1 and len(y) > 1:
                    try:
                        coeffs = np.polyfit(x, y, 1)
                        r_squared = np.corrcoef(y, np.polyval(coeffs, x))[0, 1] ** 2
                    except:
                        r_squared = 0.5
                else:
                    r_squared = 0.5
                
                trends.append({
                    'type': 'uptrend',
                    'start_idx': pivot_lows[i-1]['idx'],
                    'end_idx': pivot_highs[i]['idx'],
                    'start_datetime': start_time,
                    'end_datetime': end_time,
                    'start_price': start_price,
                    'end_price': end_price,
                    'slope': slope,
                    'r_squared': r_squared
                })
            
            # Tendencias bajistas (máximo -> mínimo)
            if i < len(pivot_highs) and i < len(pivot_lows):
                if pivot_highs[i-1]['idx'] < pivot_lows[i]['idx']:
                    start_price = pivot_highs[i-1]['price']
                    end_price = pivot_lows[i]['price']
                    start_time = pivot_highs[i-1]['timestamp']
                    end_time = pivot_lows[i]['timestamp']
                    
                    # Calcular pendiente (precio/tiempo)
                    time_diff = (end_time - start_time).total_seconds() / 3600  # en horas
                    price_diff = end_price - start_price
                    slope = price_diff / time_diff if time_diff > 0 else 0
                    
                    # Calcular R² (simplificado)
                    x = np.array(range(len(df.loc[pivot_highs[i-1]['idx']:pivot_lows[i]['idx']])))
                    y = df['close'].loc[pivot_highs[i-1]['idx']:pivot_lows[i]['idx']].values
                    if len(x) > 1 and len(y) > 1:
                        try:
                            coeffs = np.polyfit(x, y, 1)
                            r_squared = np.corrcoef(y, np.polyval(coeffs, x))[0, 1] ** 2
                        except:
                            r_squared = 0.5
                    else:
                        r_squared = 0.5
                    
                    trends.append({
                        'type': 'downtrend',
                        'start_idx': pivot_highs[i-1]['idx'],
                        'end_idx': pivot_lows[i]['idx'],
                        'start_datetime': start_time,
                        'end_datetime': end_time,
                        'start_price': start_price,
                        'end_price': end_price,
                        'slope': slope,
                        'r_squared': r_squared
                    })
        
        logger.info(f"Se identificaron {len(trends)} mini-tendencias")
        return trends

    def find_triple_signals(self, key_candles, accumulation_zones, mini_trends):
        """
        Encuentra señales de triple coincidencia entre velas clave, zonas de acumulación y mini-tendencias.
        
        Args:
            key_candles: DataFrame o lista con las velas clave detectadas
            accumulation_zones: DataFrame o lista con las zonas de acumulación detectadas
            mini_trends: DataFrame o lista con las mini-tendencias detectadas
            
        Returns:
            DataFrame con las señales de triple coincidencia únicas y puntuadas
        """
        logger.info("Buscando señales de triple coincidencia...")
        
        # Convertir a DataFrame si es necesario
        if not isinstance(key_candles, pd.DataFrame):
            key_candles = pd.DataFrame(key_candles)
        if not isinstance(accumulation_zones, pd.DataFrame):
            accumulation_zones = pd.DataFrame(accumulation_zones)
        if not isinstance(mini_trends, pd.DataFrame):
            mini_trends = pd.DataFrame(mini_trends)
        
        # Verificar si hay suficientes datos
        if len(key_candles) == 0 or len(accumulation_zones) == 0 or len(mini_trends) == 0:
            logger.warning("No hay suficientes datos para buscar señales de triple coincidencia")
            return pd.DataFrame()
            
        # Crear lista para almacenar las señales
        signals = []
        processed_signals = set()  # Para evitar duplicados
        
        # Ordenar los datos para asegurar consistencia
        key_candles = key_candles.sort_values('timestamp')
        accumulation_zones = accumulation_zones.sort_values('start_datetime')
        mini_trends = mini_trends.sort_values('start_datetime')
        
        # Iterar sobre las velas clave
        for _, candle in key_candles.iterrows():
            # Verificar si la vela está dentro de una zona de acumulación
            for _, zone in accumulation_zones.iterrows():
                if not (candle['timestamp'] >= zone['start_datetime'] and 
                        candle['timestamp'] <= zone['end_datetime'] and
                        candle['close'] >= zone['low'] and 
                        candle['close'] <= zone['high']):
                    continue
                    
                # Verificar si hay una mini-tendencia que coincida con la vela
                for _, trend in mini_trends.iterrows():
                    # Verificar si la vela está dentro del rango de tiempo de la mini-tendencia
                    if not (candle['timestamp'] >= trend['start_datetime'] and 
                            candle['timestamp'] <= trend['end_datetime']):
                        continue
                        
                    # Verificar si la dirección de la tendencia es compatible con la vela
                    is_bullish = candle['close'] > candle['open']
                    is_uptrend = trend['type'] == 'uptrend'
                    is_compatible = (is_bullish and is_uptrend) or (not is_bullish and not is_uptrend)
                    
                    if not is_compatible:
                        continue
                        
                    # Crear una clave única para esta señal
                    signal_key = (str(candle['timestamp']), 
                                 round(float(candle['close']), 2), 
                                 zone['start_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                                 trend['start_datetime'].strftime('%Y-%m-%d %H:%M:%S'))
                    
                    # Evitar duplicados
                    if signal_key in processed_signals:
                        continue
                    processed_signals.add(signal_key)
                    
                    # Calcular métricas adicionales
                    body_size = abs(candle['close'] - candle['open'])
                    candle_range = candle['high'] - candle['low']
                    body_percentage = (body_size / candle_range * 100) if candle_range > 0 else 0
                    
                    # Calcular puntuación compuesta (0-100)
                    zone_score = zone.get('quality_score', 0.5) * 20  # Hasta 20 puntos
                    trend_score = min(trend.get('r_squared', 0.5) * 30, 30)  # Hasta 30 puntos
                    
                    # Puntuación basada en el tamaño del cuerpo de la vela (hasta 20 puntos)
                    candle_score = min(body_percentage * 0.4, 20)
                    
                    # Puntuación basada en el volumen relativo (hasta 15 puntos)
                    volume_ma = key_candles['volume'].rolling(window=20).mean()
                    if len(volume_ma) > 0 and not np.isnan(volume_ma.iloc[-1]) and volume_ma.iloc[-1] > 0:
                        volume_ratio = candle['volume'] / volume_ma.iloc[-1]
                        volume_score = min(np.log1p(volume_ratio) * 5, 15)  # Log scale para suavizar
                    else:
                        volume_score = 5  # Puntuación base si no hay datos de volumen
                    
                    # Puntuación basada en la posición en la tendencia (hasta 15 puntos)
                    trend_duration = (trend['end_datetime'] - trend['start_datetime']).total_seconds() / 60  # en minutos
                    time_in_trend = (candle['timestamp'] - trend['start_datetime']).total_seconds() / 60
                    trend_position = min(time_in_trend / (trend_duration + 1e-6), 1.0)  # Evitar división por cero
                    position_score = 15 * (1 - abs(trend_position - 0.5) * 2)  # Máximo en el medio de la tendencia
                    
                    # Calcular puntuación total
                    total_score = zone_score + trend_score + candle_score + volume_score + position_score
                    
                    # Calcular puntuación combinada (promedio ponderado)
                    combined_score = (zone_score * 0.4) + (trend_score * 0.4) + (candle_score * 0.2)
                    
                    # Crear señal de triple coincidencia
                    signal = {
                        'timestamp': candle['timestamp'],
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle['volume'],
                        'body_percentage': body_percentage,
                        'score': total_score,  # Añadir la puntuación total
                        
                        # Información de la zona de acumulación
                        'zone_id': zone.get('id', 0),
                        'zone_score': zone_score,
                        'zone_start': zone.get('start_datetime'),
                        'zone_end': zone.get('end_datetime'),
                        'zone_min_price': zone.get('low'),
                        'zone_max_price': zone.get('high'),
                        
                        # Información de la mini-tendencia
                        'trend_type': trend.get('type'),
                        'trend_score': trend_score,
                        'trend_start': trend.get('start_datetime'),
                        'trend_end': trend.get('end_datetime'),
                        'trend_r_squared': trend.get('r_squared'),
                        'trend_slope': trend.get('slope')
                    }
                    
                    # Añadir señal a la lista
                    signals.append(signal)
                    
        # Convertir a DataFrame y ordenar por puntuación
        if signals:
            signals_df = pd.DataFrame(signals)
            signals_df = signals_df.sort_values('score', ascending=False)
            logger.info(f"Se encontraron {len(signals_df)} señales de triple coincidencia")
            return signals_df
        else:
            logger.info("No se encontraron señales de triple coincidencia")
            return pd.DataFrame()

    def save_signals_to_db(self, signals: List[Dict]) -> bool:
        """Guarda las señales en la base de datos."""
        if not self.connect():
            return False
        
        try:
            # Crear tabla si no existe
            create_table_query = """
            CREATE TABLE IF NOT EXISTS triple_signals (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(10) NOT NULL,
                candle_index INT NOT NULL,
                datetime DATETIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                body_percentage FLOAT,
                zone_id INT,
                zone_quality_score FLOAT,
                zone_start_datetime DATETIME,
                zone_end_datetime DATETIME,
                mini_trend_id INT,
                trend_direction VARCHAR(20),
                trend_slope FLOAT,
                trend_r_squared FLOAT,
                trend_start_datetime DATETIME,
                trend_end_datetime DATETIME,
                signal_strength FLOAT,
                combined_score FLOAT,
                scoring_details JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_signal (symbol, timeframe, candle_index)
            )
            """
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logger.info("Tabla triple_signals verificada/creada correctamente")
            
            # Insertar señales
            insert_query = """
            INSERT INTO triple_signals (
                symbol, timeframe, candle_index, datetime, 
                open, high, low, close, volume, body_percentage,
                zone_id, zone_quality_score, zone_start_datetime, zone_end_datetime,
                mini_trend_id, trend_direction, trend_slope, trend_r_squared,
                trend_start_datetime, trend_end_datetime,
                signal_strength, combined_score, scoring_details
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                datetime = VALUES(datetime),
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                body_percentage = VALUES(body_percentage),
                zone_id = VALUES(zone_id),
                zone_quality_score = VALUES(zone_quality_score),
                zone_start_datetime = VALUES(zone_start_datetime),
                zone_end_datetime = VALUES(zone_end_datetime),
                mini_trend_id = VALUES(mini_trend_id),
                trend_direction = VALUES(trend_direction),
                trend_slope = VALUES(trend_slope),
                trend_r_squared = VALUES(trend_r_squared),
                trend_start_datetime = VALUES(trend_start_datetime),
                trend_end_datetime = VALUES(trend_end_datetime),
                signal_strength = VALUES(signal_strength),
                combined_score = VALUES(combined_score),
                scoring_details = VALUES(scoring_details)
            """
            
            saved_count = 0
            for signal in signals:
                try:
                    # Preparar parámetros
                    params = (
                        signal['symbol'],
                        signal['timeframe'],
                        signal['candle_index'],
                        signal['datetime'],
                        signal['open'],
                        signal['high'],
                        signal['low'],
                        signal['close'],
                        signal['volume'],
                        signal['body_percentage'],
                        signal['zone_id'],
                        signal['zone_quality_score'],
                        signal['zone_start_datetime'],
                        signal['zone_end_datetime'],
                        signal['mini_trend_id'],
                        signal['trend_direction'],
                        signal['trend_slope'],
                        signal['trend_r_squared'],
                        signal['trend_start_datetime'],
                        signal['trend_end_datetime'],
                        signal['signal_strength'],
                        signal['combined_score'],
                        json.dumps(signal['scoring_details'])
                    )
                    
                    self.cursor.execute(insert_query, params)
                    saved_count += 1
                    
                    # Hacer commit cada 10 inserciones
                    if saved_count % 10 == 0:
                        self.conn.commit()
                        logger.debug(f"Progreso: {saved_count} señales guardadas...")
                    
                except Exception as e:
                    logger.error(f"Error al guardar señal: {e}")
                    continue
            
            # Hacer commit final
            self.conn.commit()
            logger.info(f"Se guardaron {saved_count} señales en la base de datos")
            return True
            
        except Exception as e:
            logger.error(f"Error al guardar señales en la base de datos: {e}")
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            self.close()

def save_signals_to_db(self, signals: List[Dict]) -> bool:
    """Guarda las señales en la base de datos."""
    if not self.connect():
        return False
    
    try:
        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS triple_signals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            candle_index INT NOT NULL,
            datetime DATETIME,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            body_percentage FLOAT,
            zone_id INT,
            zone_quality_score FLOAT,
            zone_start_datetime DATETIME,
            zone_end_datetime DATETIME,
            mini_trend_id INT,
            trend_direction VARCHAR(20),
            trend_slope FLOAT,
            trend_r_squared FLOAT,
            trend_start_datetime DATETIME,
            trend_end_datetime DATETIME,
            signal_strength FLOAT,
            combined_score FLOAT,
            scoring_details JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_signal (symbol, timeframe, candle_index)
        )
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()
        logger.info("Tabla triple_signals verificada/creada correctamente")
        
        # Insertar señales
        insert_query = """
        INSERT INTO triple_signals (
            symbol, timeframe, candle_index, datetime, 
            open, high, low, close, volume, body_percentage,
            zone_id, zone_quality_score, zone_start_datetime, zone_end_datetime,
            mini_trend_id, trend_direction, trend_slope, trend_r_squared,
            trend_start_datetime, trend_end_datetime,
            signal_strength, combined_score, scoring_details
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            datetime = VALUES(datetime),
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume),
            body_percentage = VALUES(body_percentage),
            zone_id = VALUES(zone_id),
            zone_quality_score = VALUES(zone_quality_score),
            zone_start_datetime = VALUES(zone_start_datetime),
            zone_end_datetime = VALUES(zone_end_datetime),
            mini_trend_id = VALUES(mini_trend_id),
            trend_direction = VALUES(trend_direction),
            trend_slope = VALUES(trend_slope),
            trend_r_squared = VALUES(trend_r_squared),
            trend_start_datetime = VALUES(trend_start_datetime),
            trend_end_datetime = VALUES(trend_end_datetime),
            signal_strength = VALUES(signal_strength),
            combined_score = VALUES(combined_score),
            scoring_details = VALUES(scoring_details)
        """
        
        saved_count = 0
        for signal in signals:
            try:
                # Preparar parámetros
                params = (
                    signal['symbol'],
                    signal['timeframe'],
                    signal['candle_index'],
                    signal['datetime'],
                    signal['open'],
                    signal['high'],
                    signal['low'],
                    signal['close'],
                    signal['volume'],
                    signal['body_percentage'],
                    signal['zone_id'],
                    signal['zone_quality_score'],
                    signal['zone_start_datetime'],
                    signal['zone_end_datetime'],
                    signal['mini_trend_id'],
                    signal['trend_direction'],
                    signal['trend_slope'],
                    signal['trend_r_squared'],
                    signal['trend_start_datetime'],
                    signal['trend_end_datetime'],
                    signal['signal_strength'],
                    signal['combined_score'],
                    json.dumps(signal['scoring_details'])
                )
                
                self.cursor.execute(insert_query, params)
                saved_count += 1
                
                # Hacer commit cada 10 inserciones
                if saved_count % 10 == 0:
                    self.conn.commit()
                    logger.debug(f"Progreso: {saved_count} señales guardadas...")
                
            except Exception as e:
                logger.error(f"Error al guardar señal: {e}")
                continue
        
        # Hacer commit final
        self.conn.commit()
        logger.info(f"Se guardaron {saved_count} señales en la base de datos")
        return True
        
    except Exception as e:
        logger.error(f"Error al guardar señales en la base de datos: {e}")
        if self.conn:
            self.conn.rollback()
        return False
    finally:
        self.close()



def parse_args():
    parser = argparse.ArgumentParser(description='Detección de señales de triple coincidencia')
    parser.add_argument('--symbol', type=str, default='BTCUSDT', help='Símbolo de trading (ej: BTCUSDT)')
    parser.add_argument('--timeframe', type=str, default='5m', help='Timeframe (ej: 5m, 15m, 1h)')
    parser.add_argument('--days', type=int, default=7, help='Número de días de datos históricos a analizar')
    parser.add_argument('--data-dir', type=str, default=None, 
                       help='Directorio que contiene los archivos CSV (por defecto: ./data/data_detect)')
    parser.add_argument('--debug', action='store_true', help='Mostrar información de depuración')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Crear directorio de logs si no existe
    os.makedirs("logs", exist_ok=True)
    
    # Configurar nivel de logging según la opción de depuración
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    # Ejecutar detector
    detector = TripleSignalDetector()
    detector.symbol = args.symbol
    detector.timeframe = args.timeframe
    
    # Obtener la ruta absoluta del directorio de datos si se proporciona
    data_dir = None
    if args.data_dir:
        data_dir = os.path.abspath(args.data_dir)
    
    # Ejecutar el detector con los parámetros proporcionados
    detector.run(data_dir=data_dir, days=args.days)

if __name__ == "__main__":
    main()
