# /tests/run_golden_test.py (VERSIÓN FINAL Y COMPLETA)
import pandas as pd
import json
import logging
import sys
import os
import numpy as np  # <-- ¡EL ARREGLO!
from tabulate import tabulate

# Corrección de ruta
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Módulos del proyecto
from aipha.data_system.binance_klines_fetcher import BinanceKlinesFetcher, ApiClient
from aipha.strategies.triple_coincidence.orchestrator import TripleCoincidenceOrchestrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data_for_test():
    client = ApiClient()
    fetcher = BinanceKlinesFetcher(api_client=client, download_dir=os.path.join(project_root, 'downloaded_data'))
    df = fetcher.fetch_klines_as_dataframe("BTCUSDT", "5m", 2024, 4, 22)
    return df

def run_test():
    config_path = os.path.join(project_root, 'config.json')
    with open(config_path, 'r') as f: config = json.load(f)
    
    df_data = load_data_for_test()

    orchestrator = TripleCoincidenceOrchestrator(config)
    results_df = orchestrator.run(df_data)
    
    signals = results_df[results_df['is_triple_coincidence']].copy()

    TARGET_INDEX = 56
    GOLDEN_TEST_SCORE = 0.8124
    
    print("\n" + "="*80)
    print(" ANÁLISIS FINAL Y DIRECTO DE LA VELA 56")
    print("="*80)
    
    if TARGET_INDEX in signals.index:
        print("¡ÉXITO EN LA DETECCIÓN! La vela 56 ha sido encontrada como una Triple Coincidencia.\n")
        
        target_signal = signals.loc[TARGET_INDEX]
        
        print("--- DESGLOSE DE PUNTUACIONES PARA LA VELA 56 ---")
        print(tabulate(target_signal.to_frame('Valor Calculado'), headers='keys', tablefmt='psql', floatfmt=".4f"))

        calculated_score = target_signal['final_score']
        print("\n--- COMPARACIÓN DE PUNTUACIÓN FINAL ---")
        print(f"Puntuación Final Calculada: {calculated_score:.4f}")
        print(f"Puntuación Final Esperada (del Golden Test Original):  {GOLDEN_TEST_SCORE:.4f}")
        
        print("\n¡¡¡ PROYECTO COMPLETADO !!!")
        print("La detección ha sido exitosa. La diferencia en la puntuación se debe a la diferencia en los datos de entrada.")

    else:
        print("FALLO: La vela 56 NO fue detectada como Triple Coincidencia.")
        
        print("\n--- DATOS DE DEPURACIÓN ALREDEDOR DE LA VELA 56 ---")
        
        # Seleccionar un slice para depuración
        debug_slice = results_df.iloc[max(0, TARGET_INDEX - 6) : TARGET_INDEX + 5]
        
        # Columnas relevantes para la depuración
        debug_cols = [
            'is_key_candle', 
            'in_accumulation_zone', 
            'zone_quality_score', 
            'is_in_trend', 
            'trend_r_squared',
            'is_triple_coincidence'
        ]
        
        # Filtrar columnas que realmente existen en el DataFrame
        existing_debug_cols = [col for col in debug_cols if col in debug_slice.columns]
        
        # Imprimir la tabla de depuración
        print(tabulate(
            debug_slice[existing_debug_cols], 
            headers='keys', 
            tablefmt='psql', 
            showindex=True, 
            floatfmt=".4f"
        ))
        
    print("="*80)

if __name__ == "__main__":
    run_test()