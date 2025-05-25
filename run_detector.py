#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para ejecutar el detector de señales de triple coincidencia y mostrar los resultados.
"""

import os
import sys
import logging
import pandas as pd
from triple_signals_detector import TripleSignalDetector

def setup_logging(debug=False):
    """Configura el sistema de logging."""
    log_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('triple_signals.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def print_results(df, key_candles, accumulation_zones, mini_trends, triple_signals):
    """Muestra los resultados del análisis en la terminal."""
    print("\n" + "="*80)
    print("RESULTADOS DEL ANÁLISIS DE SEÑALES DE TRIPLE COINCIDENCIA")
    print("="*80)
    
    # Convertir a DataFrames si es necesario
    if not isinstance(key_candles, pd.DataFrame):
        key_candles = pd.DataFrame(key_candles)
    if not isinstance(accumulation_zones, pd.DataFrame):
        accumulation_zones = pd.DataFrame(accumulation_zones)
    if not isinstance(mini_trends, pd.DataFrame):
        mini_trends = pd.DataFrame(mini_trends)
    if not isinstance(triple_signals, pd.DataFrame):
        triple_signals = pd.DataFrame(triple_signals)
    
    # Mostrar resumen
    print(f"\n{'RESUMEN':^80}")
    print("-"*80)
    print(f"Total de velas analizadas: {len(df) if df is not None else 0}")
    print(f"Rango de fechas: {df['timestamp'].min() if not df.empty else 'N/A'} - {df['timestamp'].max() if not df.empty else 'N/A'}")
    print(f"Velas clave identificadas: {len(key_candles) if not key_candles.empty else 0}")
    print(f"Zonas de acumulación identificadas: {len(accumulation_zones) if not accumulation_zones.empty else 0}")
    print(f"Mini-tendencias identificadas: {len(mini_trends) if not mini_trends.empty else 0}")
    print(f"Señales de triple coincidencia encontradas: {len(triple_signals) if not triple_signals.empty else 0}")
    
    # Mostrar velas clave
    if not key_candles.empty:
        print("\n" + " VELAS CLAVE IDENTIFICADAS ".center(80, '-'))
        for i, candle in key_candles.iterrows():
            candle_type = "Alcista" if candle['close'] > candle['open'] else "Bajista"
            print(f"Vela {i+1}:")
            print(f"  - Fecha: {candle['timestamp']}")
            print(f"  - Tipo: {candle_type}")
            print(f"  - Apertura: {candle['open']:.2f} | Cierre: {candle['close']:.2f}")
            print(f"  - Mínimo: {candle['low']:.2f} | Máximo: {candle['high']:.2f}")
            print(f"  - Volumen: {candle['volume']:.2f}")
    
    # Mostrar zonas de acumulación
    if not accumulation_zones.empty:
        print("\n" + " ZONAS DE ACUMULACIÓN IDENTIFICADAS ".center(80, '-'))
        for i, zone in accumulation_zones.iterrows():
            print(f"\nZona {i+1}:")
            print(f"  - Rango de precios: {zone['low']:.2f} - {zone['high']:.2f}" if 'low' in zone and 'high' in zone else "  - Sin datos de rango")
            print(f"  - Inicio: {zone['start_datetime'] if 'start_datetime' in zone else 'N/A'}")
            print(f"  - Fin: {zone['end_datetime'] if 'end_datetime' in zone else 'N/A'}")
            print(f"  - Volumen: {zone.get('volume', 'N/A')}")
            print(f"  - Puntuación: {zone.get('quality_score', 'N/A'):.2f}" if 'quality_score' in zone else "  - Sin puntuación")
    
    # Mostrar mini-tendencias
    if not mini_trends.empty:
        print("\n" + " MINI-TENDENCIAS IDENTIFICADAS ".center(80, '-'))
        for i, trend in mini_trends.iterrows():
            print(f"\nTendencia {i+1}:")
            print(f"  - Tipo: {trend.get('type', 'Desconocido')}")
            print(f"  - Inicio: {trend.get('start_datetime', 'N/A')} (Precio: {trend.get('start_price', 'N/A'):.2f})")
            print(f"  - Fin: {trend.get('end_datetime', 'N/A')} (Precio: {trend.get('end_price', 'N/A'):.2f})")
            print(f"  - Pendiente: {trend.get('slope', 'N/A'):.6f}" if 'slope' in trend else "  - Sin pendiente")
            print(f"  - R²: {trend.get('r_squared', 'N/A'):.2f}" if 'r_squared' in trend else "  - Sin R²")
    
    # Mostrar señales de triple coincidencia
    if not triple_signals.empty:
        print("\n" + " SEÑALES DE TRIPLE COINCIDENCIA ".center(80, '-'))
        for i, signal in triple_signals.iterrows():
            print(f"\nSeñal {i+1}:")
            print(f"  - Fecha: {signal.get('timestamp', 'N/A')}")
            print(f"  - Precio: {signal.get('close', 'N/A'):.2f}" if 'close' in signal else "  - Sin precio")
            print(f"  - Volumen: {signal.get('volume', 'N/A'):.2f}" if 'volume' in signal else "  - Sin volumen")
            print(f"  - Puntuación: {signal.get('score', 'N/A'):.2f}" if 'score' in signal else "  - Sin puntuación")
    
    print("\n" + "="*80 + "\n")

def main():
    # Configuración
    symbol = "BTCUSDT"
    timeframe = "5m"
    days = 7
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "data_detect")
    
    # Configurar logging
    logger = setup_logging(debug=True)
    
    try:
        # Crear y configurar el detector
        detector = TripleSignalDetector()
        detector.symbol = symbol
        detector.timeframe = timeframe
        
        # Ejecutar el detector y obtener resultados
        logger.info("Iniciando análisis...")
        results = detector.run(data_dir=data_dir, days=days, return_results=True)
        
        if results is None:
            logger.error("No se pudieron obtener resultados del análisis")
            return 1
            
        df, key_candles, accumulation_zones, mini_trends, triple_signals = results
        
        # Mostrar resultados
        print_results(df, key_candles, accumulation_zones, mini_trends, triple_signals)
        
        # Guardar resultados en archivos CSV
        os.makedirs("results", exist_ok=True)
        df.to_csv("results/historical_data.csv", index=False)
        key_candles.to_csv("results/key_candles.csv", index=False)
        accumulation_zones.to_csv("results/accumulation_zones.csv", index=False)
        mini_trends.to_csv("results/mini_trends.csv", index=False)
        
        if not triple_signals.empty:
            triple_signals.to_csv("results/triple_signals.csv", index=False)
        
        logger.info("Análisis completado. Los resultados se han guardado en el directorio 'results/'")
        return 0
        
    except Exception as e:
        logger.error(f"Error durante la ejecución: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
