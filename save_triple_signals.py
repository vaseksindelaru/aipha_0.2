#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
save_triple_signals.py - Guarda señales de triple coincidencia en una tabla dedicada.

Este script identifica y almacena en una tabla todas las señales que cumplen con las tres condiciones:
1. Son velas clave (según criterios de volumen y tamaño de cuerpo)
2. Están dentro de zonas de acumulación
3. Forman parte de mini-tendencias identificables

Estas señales de "triple verificación" tienen mayor probabilidad de éxito.
"""

import os
import sys
import argparse
import logging
import mysql.connector
from mysql.connector import errors
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/triple_signals.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

class TripleSignalSaver:
    """Clase para identificar y guardar señales de triple coincidencia."""
    
    def __init__(self):
        """Inicializa la conexión a la base de datos y configuraciones."""
        self.host = os.getenv('MYSQL_HOST', 'localhost')
        self.user = os.getenv('MYSQL_USER', 'root')
        self.password = os.getenv('MYSQL_PASSWORD', '21blackjack')
        self.database = os.getenv('MYSQL_DATABASE', 'binance_lob')
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establece conexión con la base de datos MySQL."""
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor(dictionary=True)
            logger.info(f"Conectado a base de datos MySQL: {self.database}")
            return True
        except Exception as e:
            logger.error(f"Error conectando a la base de datos: {e}")
            return False
    
    def close(self):
        """Cierra la conexión a la base de datos."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Conexión a la base de datos cerrada")
    
    def create_triple_signals_table(self):
        """Crea la tabla para almacenar señales de triple coincidencia si no existe."""
        try:
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
                trend_direction VARCHAR(10),
                trend_slope FLOAT,
                trend_r_squared FLOAT,
                trend_start_datetime DATETIME,
                trend_end_datetime DATETIME,
                signal_strength FLOAT,
                combined_score FLOAT,
                zone_score FLOAT,
                trend_score FLOAT,
                candle_score FLOAT,
                direction_factor FLOAT,
                slope_factor FLOAT,
                divergence_factor FLOAT,
                reliability_bonus FLOAT,
                profit_potential FLOAT,
                scoring_details JSON,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_signal (symbol, timeframe, candle_index)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logger.info("Tabla triple_signals verificada/creada correctamente")
            return True
        except Exception as e:
            logger.error(f"Error al crear/verificar la tabla triple_signals: {e}")
            return False
    
    def find_triple_signals(self, symbol, timeframe):
        """
        Encuentra señales que coinciden en los tres criterios:
        1. Velas clave
        2. Dentro de zonas de acumulación
        3. Parte de mini-tendencias
        """
        try:
            # Verificar tabla de mini-tendencias
            mini_trend_table = 'mini_trend_results'
            logger.info(f"Usando tabla de mini-tendencias: {mini_trend_table}")
            
            # Construir la consulta para encontrar señales
            tolerance = 8  # Tolerancia de índices para la detección
            
            query = f"""
            SELECT 
                kc.id as key_candle_id,
                '{symbol}' as symbol,
                '{timeframe}' as timeframe,
                kc.candle_index,
                kc.open,
                kc.high,
                kc.low,
                kc.close,
                kc.volume,
                kc.body_percentage,
                daz.id as zone_id,
                daz.quality_score as zone_quality_score,
                daz.datetime_start as zone_start_datetime,
                daz.datetime_end as zone_end_datetime,
                mt.id as trend_id,
                mt.direction as trend_direction,
                mt.slope as trend_slope,
                mt.r_squared as trend_r_squared,
                mt.start_time as trend_start_datetime,
                mt.end_time as trend_end_datetime
            FROM key_candles kc
            JOIN detect_accumulation_zone_results daz ON 
                daz.symbol = '{symbol}' AND
                daz.timeframe = '{timeframe}' AND
                (
                    (kc.candle_index >= daz.start_idx - {tolerance} AND kc.candle_index <= daz.end_idx + {tolerance}) OR
                    (kc.candle_index >= daz.start_idx AND kc.candle_index <= daz.end_idx)
                )
            JOIN {mini_trend_table} mt ON 
                (
                    (kc.candle_index >= mt.start_idx - {tolerance} AND kc.candle_index <= mt.end_idx + {tolerance}) OR
                    (kc.candle_index >= mt.start_idx AND kc.candle_index <= mt.end_idx)
                )
            WHERE 
                kc.is_key_candle = TRUE
                AND daz.quality_score >= 0.5 
                AND mt.r_squared >= 0.45
            ORDER BY kc.candle_index
            """
            
            logger.info("Ejecutando consulta para encontrar señales de triple coincidencia")
            self.cursor.execute(query)
            signals = self.cursor.fetchall()
            logger.info(f"Encontradas {len(signals)} señales de triple coincidencia para {symbol}-{timeframe}")
            return signals
            
        except Exception as e:
            logger.error(f"Error buscando señales de triple coincidencia: {e}")
            return []
    
    def calculate_signal_strength(self, signal):
        """
        Calcula la fuerza de la señal basada en varios factores.
        Devuelve un valor entre 0 y 1, donde 1 es la señal más fuerte.
        """
        try:
            # Factores de ponderación
            zone_weight = 0.35
            trend_weight = 0.35
            candle_weight = 0.30
            
            # Calcular puntuación de la zona
            zone_quality = min(signal['zone_quality_score'], 1.0)
            zone_score = (zone_quality - 0.45) / 0.4 if zone_quality > 0.45 else 0.1
            zone_score = min(zone_score, 1.0)
            
            # Calcular puntuación de la tendencia
            trend_quality = signal['trend_r_squared']
            if trend_quality >= 0.6:
                trend_quality *= 1.3
            elif trend_quality >= 0.45:
                trend_quality *= 1.0
            else:
                trend_quality *= 0.9
            
            # Ajustar por dirección de la tendencia
            trend_direction = signal.get('trend_direction', '').lower()
            direction_factor = 1.15 if trend_direction == 'alcista' else 0.9
            
            # Ajustar por pendiente
            slope_abs = abs(float(signal['trend_slope']))
            slope_factor = min(slope_abs / 100, 1.2)
            
            # Puntuación final de la tendencia
            trend_score = trend_quality * direction_factor * slope_factor
            trend_score = min(trend_score, 1.0)
            
            # Calcular puntuación de la vela
            volume = float(signal['volume'])
            volume_norm = min(volume / 150, 1.0)
            
            body_pct = float(signal['body_percentage'])
            if body_pct < 5:
                body_norm = 0.3
            elif body_pct <= 15:
                body_norm = 0.6
            elif body_pct <= 40:
                body_norm = 1.0
            elif body_pct <= 60:
                body_norm = 0.8
            else:
                body_norm = 0.6
            
            candle_score = 0.6 * volume_norm + 0.4 * body_norm
            
            # Calcular puntuación general
            signal_strength = (
                zone_weight * zone_score +
                trend_weight * trend_score +
                candle_weight * candle_score
            )
            
            # Detalles de puntuación
            scoring_details = {
                'zone_score': round(zone_score, 4),
                'trend_score': round(trend_score, 4),
                'candle_score': round(candle_score, 4),
                'direction_factor': round(direction_factor, 4),
                'slope_factor': round(slope_factor, 4)
            }
            
            return round(signal_strength, 4), scoring_details
            
        except Exception as e:
            logger.error(f"Error calculando fuerza de señal: {e}")
            return 0.5, {'error': str(e)}
    
    def calculate_combined_score(self, signal, strength_details=None):
        """
        Calcula una puntuación combinada que incluye factores adicionales.
        """
        try:
            if strength_details is None:
                base_strength, strength_details = self.calculate_signal_strength(signal)
            else:
                base_strength = 0.5
            
            # Calcular divergencia entre componentes
            component_scores = [
                strength_details.get('zone_score', 0.5),
                strength_details.get('trend_score', 0.5),
                strength_details.get('candle_score', 0.5)
            ]
            max_score = max(component_scores)
            min_score = min(component_scores)
            score_divergence = max_score - min_score
            divergence_factor = 1 - (score_divergence * 0.5)
            
            # Factor de confiabilidad basado en R²
            r_squared = float(signal.get('trend_r_squared', 0.5))
            reliability_bonus = 0.2 if r_squared > 0.8 else 0.1 if r_squared > 0.7 else 0.0
            
            # Evaluar potencial de rentabilidad
            trend_direction = signal.get('trend_direction', '').lower()
            volume = float(signal.get('volume', 0))
            
            if trend_direction == 'alcista' and volume > 80:
                profit_potential = 0.85
            elif trend_direction == 'alcista' and volume > 50:
                profit_potential = 0.75
            elif trend_direction == 'bajista' and float(signal.get('body_percentage', 0)) > 20:
                profit_potential = 0.7
            else:
                profit_potential = 0.6
            
            # Calcular puntuación combinada
            combined_score = (
                0.5 * base_strength +
                0.2 * divergence_factor +
                0.15 * (1.0 + reliability_bonus) +
                0.15 * profit_potential
            )
            
            # Asegurar que no exceda 1.0
            combined_score = min(combined_score, 1.0)
            
            # Detalles extendidos
            extended_details = {
                **strength_details,
                'divergence_factor': round(divergence_factor, 4),
                'reliability_bonus': round(reliability_bonus, 4),
                'profit_potential': round(profit_potential, 4),
                'base_strength': round(base_strength, 4),
                'final_score': round(combined_score, 4)
            }
            
            return round(combined_score, 4), extended_details
            
        except Exception as e:
            logger.error(f"Error calculando puntuación combinada: {e}")
            return 0.5, {'error': str(e)}
    
    def save_signals(self, symbol, timeframe):
        """
        Encuentra y guarda las señales de triple coincidencia en la base de datos.
        """
        if not self.connect():
            logger.error("No se pudo conectar a la base de datos")
            return False
        
        try:
            # Crear tabla si no existe
            if not self.create_triple_signals_table():
                logger.error("No se pudo crear/verificar la tabla triple_signals")
                return False
            
            logger.info(f"Buscando señales para {symbol}-{timeframe}...")
            signals = self.find_triple_signals(symbol, timeframe)
            
            if not signals:
                logger.info(f"No se encontraron señales de triple coincidencia para {symbol}-{timeframe}")
                return True
                
            logger.info(f"Procesando {len(signals)} señales encontradas...")
            
            # Eliminar señales existentes para este par y timeframe
            try:
                delete_query = """
                DELETE FROM triple_signals 
                WHERE symbol = %s AND timeframe = %s
                """
                self.cursor.execute(delete_query, (symbol, timeframe))
                self.conn.commit()
                logger.info(f"Se eliminaron señales existentes para {symbol}-{timeframe}")
            except Exception as e:
                logger.error(f"Error al eliminar señales existentes: {e}")
                self.conn.rollback()
                return False
            
            # Insertar nuevas señales
            insert_query = """
            INSERT INTO triple_signals (
                symbol, timeframe, candle_index, datetime, 
                open, high, low, close, volume, body_percentage,
                zone_id, zone_quality_score, zone_start_datetime, zone_end_datetime,
                mini_trend_id, trend_direction, trend_slope, trend_r_squared, 
                trend_start_datetime, trend_end_datetime,
                signal_strength, combined_score,
                zone_score, trend_score, candle_score, 
                direction_factor, slope_factor,
                divergence_factor, reliability_bonus, profit_potential,
                scoring_details, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, NOW()
            )
            """
            
            saved_count = 0
            error_count = 0
            
            for signal in signals:
                try:
                    # Calcular puntuaciones
                    signal_strength, strength_details = self.calculate_signal_strength(signal)
                    combined_score, extended_details = self.calculate_combined_score(signal, strength_details)
                    
                    # Calcular timestamp preciso para la vela
                    try:
                        # Intentar obtener la fecha de la vela de la zona
                        zone_start = signal.get('zone_start_datetime')
                        zone_end = signal.get('zone_end_datetime')
                        
                        if zone_start and zone_end:
                            if isinstance(zone_start, str):
                                zone_start = datetime.strptime(zone_start, '%Y-%m-%d %H:%M:%S')
                            if isinstance(zone_end, str):
                                zone_end = datetime.strptime(zone_end, '%Y-%m-%d %H:%M:%S')
                            
                            zone_duration = (zone_end - zone_start).total_seconds()
                            zone_start_idx = signal.get('zone_start_idx', 0)
                            zone_end_idx = signal.get('zone_end_idx', 0)
                            
                            if zone_end_idx > zone_start_idx and zone_duration > 0:
                                # Calcular la posición relativa de la vela en la zona
                                bar_offset = signal['candle_index'] - zone_start_idx
                                bar_duration = zone_duration / (zone_end_idx - zone_start_idx + 1)
                                offset_seconds = bar_offset * bar_duration
                                candle_datetime = zone_start + timedelta(seconds=offset_seconds)
                            else:
                                candle_datetime = zone_start or datetime.now()
                        else:
                            candle_datetime = datetime.now()
                            
                        # Asegurar que es un objeto datetime
                        if not isinstance(candle_datetime, datetime):
                            candle_datetime = datetime.now()
                            
                    except Exception as time_error:
                        logger.warning(f"Error calculando timestamp: {time_error}")
                        candle_datetime = datetime.now()
                    
                    # Preparar parámetros para la consulta
                    params = (
                        signal.get('symbol', symbol),
                        signal.get('timeframe', timeframe),
                        signal.get('candle_index', 0),
                        candle_datetime.strftime('%Y-%m-%d %H:%M:%S') if candle_datetime else None,
                        signal.get('open', 0.0),
                        signal.get('high', 0.0),
                        signal.get('low', 0.0),
                        signal.get('close', 0.0),
                        signal.get('volume', 0.0),
                        signal.get('body_percentage', 0.0),
                        signal.get('zone_id'),
                        signal.get('zone_quality_score', 0.0),
                        signal.get('zone_start_datetime'),
                        signal.get('zone_end_datetime'),
                        signal.get('trend_id'),
                        signal.get('trend_direction'),
                        signal.get('trend_slope', 0.0),
                        signal.get('trend_r_squared', 0.0),
                        signal.get('trend_start_datetime'),
                        signal.get('trend_end_datetime'),
                        signal_strength,
                        combined_score,
                        extended_details.get('zone_score', 0.0),
                        extended_details.get('trend_score', 0.0),
                        extended_details.get('candle_score', 0.0),
                        extended_details.get('direction_factor', 0.0),
                        extended_details.get('slope_factor', 0.0),
                        extended_details.get('divergence_factor', 0.0),
                        extended_details.get('reliability_bonus', 0.0),
                        extended_details.get('profit_potential', 0.0),
                        json.dumps(extended_details)
                    )
                    
                    # Insertar señal
                    try:
                        self.cursor.execute(insert_query, params)
                        saved_count += 1
                        if saved_count % 10 == 0:  # Hacer commit cada 10 inserciones
                            self.conn.commit()
                            logger.debug(f"Progreso: {saved_count} señales guardadas...")
                            
                    except mysql.connector.errors.IntegrityError as ie:
                        if "Duplicate entry" in str(ie):
                            logger.warning(f"Señal duplicada para candle_index={signal.get('candle_index')}")
                        else:
                            logger.error(f"Error de integridad al insertar señal: {ie}")
                            error_count += 1
                    except Exception as insert_error:
                        logger.error(f"Error al insertar señal: {insert_error}")
                        error_count += 1
                    
                except Exception as e:
                    logger.error(f"Error procesando señal: {e}")
                    error_count += 1
                    continue
            
            # Hacer commit final
            self.conn.commit()
            
            logger.info(f"Proceso completado. Señales guardadas: {saved_count}, Errores: {error_count}")
            return True
            
        except Exception as e:
            logger.error(f"Error crítico en save_signals: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            return False
            
        finally:
            self.close()

def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Guardar señales de triple coincidencia en una tabla')
    parser.add_argument('--symbol', type=str, default='BTCUSDT', help='Símbolo (por defecto: BTCUSDT)')
    parser.add_argument('--timeframe', type=str, default='5m', help='Timeframe (por defecto: 5m)')
    args = parser.parse_args()
    
    logger.info(f"Iniciando guardado de señales de triple coincidencia para {args.symbol}-{args.timeframe}")
    
    saver = TripleSignalSaver()
    if saver.save_signals(args.symbol, args.timeframe):
        logger.info("Proceso completado exitosamente")
        return 0
    else:
        logger.error("Error en el proceso de guardado de señales")
        return 1

if __name__ == "__main__":
    sys.exit(main())
