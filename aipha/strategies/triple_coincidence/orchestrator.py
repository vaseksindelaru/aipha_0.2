# orchestrator.py

import pandas as pd
from typing import Dict, Any

# Importar nuestros bloques de construcción y componentes de la estrategia
from aipha.building_blocks.detectors.key_candle_detector import SignalDetector
from aipha.building_blocks.detectors.accumulation_zone_detector import AccumulationZoneDetector
from aipha.building_blocks.detectors.trend_detector import TrendDetector
from aipha.strategies.triple_coincidence.signal_combiner import SignalCombiner
from aipha.strategies.triple_coincidence.signal_scorer import SignalScorer

class TripleCoincidenceOrchestrator:
    """
    Orquesta el flujo completo de la estrategia de Triple Coincidencia.
    Toma los datos brutos y una configuración, y devuelve un DataFrame 
    con las señales puntuadas.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa el orquestador con una configuración específica.
        
        Args:
            config (Dict[str, Any]): Un diccionario que contiene todos los 
                                     parámetros para cada componente de la 
                                     estrategia.
        """
        self.config = config
        # Podríamos añadir validación de la configuración aquí si fuera necesario
        
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de la estrategia sobre un DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame con datos de klines (OHLCV).
            
        Returns:
            pd.DataFrame: El DataFrame original enriquecido con todas las 
                          columnas de análisis, combinación y puntuación.
        """
        print("Iniciando pipeline de la estrategia Triple Coincidencia...")
        
        # Copiamos el DataFrame para no modificar el original
        df_processed = df.copy()
        
        # --- Paso 1: Detección de Componentes Individuales ---
        # Los parámetros se extraen del diccionario de configuración
        print("Paso 1.1: Detectando Velas Clave...")
        df_processed = SignalDetector.detect_key_candles(
            df_processed, **self.config['key_candle']
        )
        
        print("Paso 1.2: Detectando Zonas de Acumulación...")
        df_processed = AccumulationZoneDetector.detect(
            df_processed, **self.config['accumulation_zone']
        )
        
        print("Paso 1.3: Detectando Mini-Tendencias...")
        df_processed = TrendDetector.detect(
            df_processed, **self.config['trend']
        )
        
        # --- Paso 2: Combinación de Señales ---
        print("Paso 2: Combinando señales para encontrar coincidencias...")
        df_processed = SignalCombiner.combine(
            df_processed, **self.config['combiner']
        )
        
        # --- Paso 3: Puntuación de Señales ---
        print("Paso 3: Puntuando las señales de Triple Coincidencia encontradas...")
        df_processed = SignalScorer.score(df_processed)
        
        print("Pipeline de la estrategia finalizado.")
        return df_processed