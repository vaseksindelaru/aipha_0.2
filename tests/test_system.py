# tests/test_system.py

import logging
import sys
import os
from datetime import date

# Añadir la raíz del proyecto al sys.path para que las importaciones 'aipha' funcionen
# Esto es necesario si ejecutas este script directamente.
# Herramientas como pytest a menudo manejan esto automáticamente.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Ahora las importaciones absolutas deberían funcionar
from aipha.data_system.api_client import ApiClient
from aipha.data_system.binance_klines_fetcher import BinanceKlinesFetcher
from aipha.data_system.templates import KlinesDataRequestTemplate

# Configuración del Logging
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)-25s - %(levelname)-8s - %(message)s',
        stream=sys.stdout
    )

logger = logging.getLogger(__name__)

def test_full_fetcher_flow():
    """Prueba el flujo completo de obtención de datos con BinanceKlinesFetcher."""
    logger.info("--- INICIANDO PRUEBA COMPLETA ---")

    api_client = ApiClient(timeout=60)
    klines_fetcher = BinanceKlinesFetcher(api_client=api_client, download_dir="downloaded_data")
    
    logger.info("Creando plantilla de solicitud de datos...")
    target_date = date(2024, 4, 22) # Usamos una fecha pasada conocida
    
    test_template = KlinesDataRequestTemplate(
        name="Test_BTCUSDT_5m_SingleDay",
        symbol="BTCUSDT",
        interval="5m",
        start_date=target_date,
        end_date=target_date,
        description="Plantilla de prueba para un día."
    )
    
    logger.info(f"Llamando a fetch_klines_by_template con plantilla '{test_template.name}'...")
    df_klines = klines_fetcher.fetch_klines_by_template(template=test_template)

    logger.info("--- ANÁLISIS DEL RESULTADO ---")
    assert df_klines is not None, "El DataFrame no debería ser None."
    assert not df_klines.empty, "El DataFrame no debería estar vacío."
    
    logger.info(f"Éxito! Se recibió un DataFrame con {len(df_klines)} filas.")
    print("\n--- Muestra del DataFrame Obtenido ---")
    print(df_klines.head())
    
    primera_fecha = df_klines.iloc[0]['Open_Time'].date()
    assert primera_fecha == target_date, f"La fecha de inicio de los datos ({primera_fecha}) no coincide con la fecha objetivo ({target_date})."
    logger.info("Verificación de fecha: ¡Correcto!")

if __name__ == "__main__":
    test_full_fetcher_flow()