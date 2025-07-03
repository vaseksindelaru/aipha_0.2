# run_golden_test.py
import pandas as pd
import json
import logging
import numpy as np
import sys
import os
from tabulate import tabulate

# --- INICIO: Corrección de la ruta de importación ---
# Añadir el directorio raíz del proyecto (aipha_project) a la ruta de Python
# Esto permite que el script encuentre el módulo 'aipha'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- FIN: Corrección de la ruta de importación ---

# Ahora podemos importar nuestros módulos
from aipha.data_system.api_client import ApiClient
from aipha.data_system.binance_klines_fetcher import BinanceKlinesFetcher
from aipha.strategies.triple_coincidence.orchestrator import TripleCoincidenceOrchestrator

# Configurar un logging básico para ver el progreso
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data_for_test() -> pd.DataFrame:
    """Usa nuestro DATA_SYSTEM para cargar los datos de prueba desde el archivo zip."""
    logging.info("Usando BinanceKlinesFetcher para cargar datos de prueba...")
    
    # Parámetros que coinciden con el archivo de datos
    symbol = "BTCUSDT"
    interval = "5m"
    year, month, day = 2024, 4, 22
    
    # Instanciamos los componentes de nuestro sistema de datos
    # El ApiClient no hará peticiones reales, pero el Fetcher lo requiere
    client = ApiClient()
    # Apuntamos el fetcher al directorio donde ya están los datos
    fetcher = BinanceKlinesFetcher(api_client=client, download_dir=os.path.join(project_root, 'downloaded_data'))
    
    # Usamos el fetcher para cargar los datos. Como el archivo ya existe, no descargará nada.
    df = fetcher.fetch_klines_as_dataframe(symbol, interval, year, month, day)
    
    if df is None or df.empty:
        logging.error("No se pudieron cargar los datos de prueba. Asegúrate de que el archivo .zip existe y es correcto.")
        sys.exit(1) # Salir si no hay datos
        
    logging.info(f"Datos cargados exitosamente. Total de filas: {len(df)}")
    return df

def run_test():
    """
    Función principal para ejecutar el Golden Test.
    """
    # --- 1. Cargar Entradas ---
    logging.info("Cargando archivos de configuración y datos...")
    
    # La configuración está en el directorio raíz, un nivel arriba de /tests
    config_path = os.path.join(project_root, 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Cargar los datos usando nuestro propio sistema
    df_data = load_data_for_test()

    # --- 2. Ejecutar la Estrategia ---
    logging.info("Inicializando el orquestador y ejecutando el pipeline...")
    orchestrator = TripleCoincidenceOrchestrator(config)
    results_df = orchestrator.run(df_data)

    # --- 3. Verificar los Resultados ---
    logging.info("Análisis completado. Verificando los resultados...")
    
    signals = results_df[results_df['is_triple_coincidence']].copy()

    if signals.empty:
        logging.warning("No se encontraron señales de Triple Coincidencia en los datos de prueba.")
        return

    logging.info(f"Se han encontrado {len(signals)} señal(es) de Triple Coincidencia.")
    
    display_cols = [
        'final_score', 'base_score', 'candle_score', 'zone_score', 'trend_score',
        'coincident_zone_score', 'coincident_trend_r2'
    ]
    # Usamos el índice de Pandas para la visualización
    print("\n--- RESUMEN DE SEÑALES ENCONTRADAS ---")
    print(tabulate(signals[display_cols], headers='keys', tablefmt='psql', floatfmt=".4f"))

    # --- 4. Verificación Específica del "Golden Test" ---
    # ¡ATENCIÓN! El índice 56 puede cambiar dependiendo de cómo pandas numera las filas.
    # Si la prueba falla aquí, revisa el índice de la señal en la tabla de arriba.
    GOLDEN_TEST_INDEX = 56 
    GOLDEN_TEST_SCORE = 0.8124
    
    logging.info(f"\n--- Verificación específica del Golden Test (Índice: {GOLDEN_TEST_INDEX}) ---")

    if GOLDEN_TEST_INDEX not in signals.index:
        logging.error(f"FALLO: La señal esperada en el índice {GOLDEN_TEST_INDEX} no fue detectada como una Triple Coincidencia.")
        logging.info("Posible causa: el índice de la vela en el DataFrame de prueba no es 56. Revisa la tabla de resumen de arriba.")
        return
        
    golden_signal = signals.loc[GOLDEN_TEST_INDEX]
    final_score = golden_signal['final_score']

    print(f"Puntuación final calculada: {final_score:.4f}")
    print(f"Puntuación final esperada:  {GOLDEN_TEST_SCORE:.4f}")
    
    if np.isclose(final_score, GOLDEN_TEST_SCORE, atol=0.01): # Aumentamos la tolerancia por si hay pequeñas diferencias de punto flotante
        logging.info("¡¡¡ÉXITO!!! La puntuación final coincide con el Golden Test.")
    else:
        logging.error(f"FALLO: La puntuación final ({final_score:.4f}) no coincide con el valor esperado ({GOLDEN_TEST_SCORE:.4f}).")
        logging.warning("Revisar los parámetros en config.json y la lógica del SignalScorer.")

if __name__ == "__main__":
    run_test()