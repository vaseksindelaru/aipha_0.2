# aipha/data_system/binance_klines_fetcher.py

import os
import zipfile
import logging
from datetime import date
from typing import Optional, List, Dict, Any

import pandas as pd

from aipha.data_system.api_client import ApiClient
from aipha.data_system.templates import KlinesDataRequestTemplate

logger = logging.getLogger(__name__)


class BinanceKlinesFetcher:
    """Se especializa en obtener datos de klines de Binance Vision."""
    
    BASE_URL = "https://data.binance.vision/data/spot/daily/klines"
    
    def __init__(self, api_client: ApiClient, download_dir: str = "downloaded_data"):
        """Inicializa el fetcher de klines de Binance.
        
        Args:
            api_client: Cliente API para realizar las descargas
            download_dir: Directorio donde se guardarán los archivos descargados
        """
        if api_client is None:
            raise ValueError("La instancia de api_client no puede ser None.")
        self._api_client = api_client
        self._download_dir = download_dir
        os.makedirs(self._download_dir, exist_ok=True)
        logger.debug(f"BinanceKlinesFetcher inicializado con download_dir='{self._download_dir}'")

    def _build_download_url(self, symbol: str, interval: str, year: int, month: int, day: int) -> str:
        """Construye la URL para descargar los datos de un día específico."""
        symbol_upper = symbol.upper()
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        return f"{self.BASE_URL}/{symbol_upper}/{interval}/{symbol_upper}-{interval}-{date_str}.zip"

    def _get_csv_filename_from_zip_name(self, zip_filename: str) -> str:
        """Obtiene el nombre del archivo CSV a partir del nombre del ZIP."""
        base_name, _ = os.path.splitext(zip_filename)
        return f"{base_name}.csv"

    def _define_klines_columns(self) -> List[str]:
        """Define los nombres de las columnas para los datos de klines."""
        return [
            "Open_Time", "Open", "High", "Low", "Close", "Volume", "Close_Time", 
            "Quote_Asset_Volume", "Number_of_Trades", "Taker_Buy_Base_Asset_Volume", 
            "Taker_Buy_Quote_Asset_Volume", "Ignore"
        ]

    def fetch_klines_as_dataframe(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int,
        day: int,
        force_download: bool = False
    ) -> Optional[pd.DataFrame]:
        """Obtiene los datos de klines para un día específico.
        
        Args:
            symbol: Símbolo del par de trading (ej: 'BTCUSDT')
            interval: Intervalo de tiempo (ej: '1m', '5m', '1h', '1d')
            year: Año de los datos
            month: Mes de los datos (1-12)
            day: Día del mes (1-31)
            force_download: Si es True, fuerza la descarga incluso si el archivo existe localmente
            
        Returns:
            DataFrame con los datos de klines o None si ocurre un error
        """
        # Preparar variables y rutas
        fetch_date = date(year, month, day)
        symbol_upper = symbol.upper()
        logger.info(f"Obteniendo datos de klines para {symbol_upper} {interval} en {fetch_date.isoformat()}")

        # Construir URLs y rutas de archivos
        url = self._build_download_url(symbol_upper, interval, year, month, day)
        zip_filename = os.path.basename(url)
        zip_dir = os.path.join(self._download_dir, symbol_upper, interval)
        zip_path = os.path.join(zip_dir, zip_filename)
        os.makedirs(zip_dir, exist_ok=True)
        csv_filename = self._get_csv_filename_from_zip_name(zip_filename)

        # Descargar el archivo si es necesario
        if force_download or not os.path.exists(zip_path):
            logger.info(f"Descargando archivo: {zip_filename}")
            if not self._api_client.download_file(url, zip_path):
                logger.error(f"Fallo al descargar el archivo ZIP desde {url}")
                return None
        else:
            logger.info(f"Usando archivo ZIP local existente: {zip_path}")

        # Procesar el archivo ZIP y CSV
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Verificar que el archivo CSV existe en el ZIP
                if csv_filename not in zf.namelist():
                    logger.error(f"El archivo CSV '{csv_filename}' no se encontró en el ZIP.")
                    return None
                
                # Leer el archivo CSV desde el ZIP
                with zf.open(csv_filename) as csv_file:
                    # Cargar los datos en un DataFrame
                    columns = self._define_klines_columns()
                    df = pd.read_csv(csv_file, header=None, names=columns)
                    
                    if df.empty:
                        logger.warning(f"El archivo CSV '{csv_filename}' está vacío.")
                        return df
                    
                    # Convertir tipos de datos
                    df["Open_Time"] = pd.to_datetime(df["Open_Time"], unit="ms")
                    df["Close_Time"] = pd.to_datetime(df["Close_Time"], unit="ms")
                    
                    # Convertir columnas numéricas
                    numeric_cols = [
                        "Open", "High", "Low", "Close", "Volume", 
                        "Quote_Asset_Volume", "Taker_Buy_Base_Asset_Volume", 
                        "Taker_Buy_Quote_Asset_Volume"
                    ]
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    # Convertir número de trades a entero
                    if "Number_of_Trades" in df.columns:
                        df["Number_of_Trades"] = pd.to_numeric(df["Number_of_Trades"], errors="coerce").astype('Int64')
                    
                    # Eliminar columna 'Ignore' si existe
                    if "Ignore" in df.columns:
                        df = df.drop(columns=["Ignore"])
                    
                    logger.info(f"Datos cargados exitosamente desde '{csv_filename}' ({len(df)} filas)")
                    return df

        except zipfile.BadZipFile:
            logger.error(f"El archivo ZIP está corrupto: {zip_path}")
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo ZIP: {zip_path}")
        except Exception as e:
            logger.critical(f"Error al procesar el archivo '{zip_path}': {e}", exc_info=True)
        
        return None

    def fetch_klines_by_template(
        self, 
        template: KlinesDataRequestTemplate, 
        force_download_all: bool = False
    ) -> Optional[pd.DataFrame]:
        """Obtiene datos de klines según una plantilla de solicitud.
        
        Args:
            template: Plantilla con los parámetros de la solicitud
            force_download_all: Si es True, fuerza la descarga de todos los archivos
            
        Returns:
            DataFrame con los datos concatenados o None si ocurre un error
        """
        logger.info(f"Procesando plantilla '{template.name}' para {template.symbol}...")
        
        # Obtener rango de fechas de la plantilla
        date_range = template.get_date_range()
        if not date_range:
            logger.warning("Rango de fechas de la plantilla vacío.")
            return pd.DataFrame()

        # Recopilar datos para cada día
        all_daily_dataframes = []
        for current_date in date_range:
            daily_df = self.fetch_klines_as_dataframe(
                symbol=template.symbol, 
                interval=template.interval,
                year=current_date.year, 
                month=current_date.month, 
                day=current_date.day,
                force_download=force_download_all
            )
            
            if daily_df is not None and not daily_df.empty:
                all_daily_dataframes.append(daily_df)

        # Verificar si se obtuvieron datos
        if not all_daily_dataframes:
            logger.warning(f"No se obtuvieron datos para el rango de la plantilla '{template.name}'.")
            return None

        # Combinar todos los DataFrames diarios
        try:
            logger.info(f"Concatenando {len(all_daily_dataframes)} DataFrames diarios...")
            final_df = pd.concat(all_daily_dataframes, ignore_index=True)
            
            # Ordenar por tiempo si la columna existe
            if "Open_Time" in final_df.columns:
                final_df = final_df.sort_values(by="Open_Time").reset_index(drop=True)
            
            logger.info(f"Proceso completado. Total de filas: {len(final_df)}")
            return final_df
            
        except Exception as e:
            logger.critical(f"Error al concatenar los DataFrames: {e}", exc_info=True)
            return None