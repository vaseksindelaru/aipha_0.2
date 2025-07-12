# orchestrator.py

import pandas as pd
from typing import Dict, Any, Optional

# Importar nuestros bloques de construcción y componentes de la estrategia
from aipha.building_blocks.detectors.key_candle_detector import SignalDetector
from aipha.building_blocks.detectors.accumulation_zone_detector import AccumulationZoneDetector
from aipha.building_blocks.detectors.trend_detector import TrendDetector
from aipha.strategies.triple_coincidence.signal_combiner import SignalCombiner
from aipha.strategies.triple_coincidence.signal_scorer import SignalScorer
from aipha.building_blocks.labelers.potential_capture_engine import get_enhanced_triple_barrier_labels

class TripleCoincidenceOrchestrator:
    """
    Orquesta el flujo completo de la estrategia de Triple Coincidencia.
    Toma los datos brutos y una configuración, y devuelve un DataFrame 
    con las señales puntuadas y, opcionalmente, etiquetadas.
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
        
    def run(self, df: pd.DataFrame, labeling_config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de la estrategia sobre un DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame con datos de klines (OHLCV).
            labeling_config (Optional[Dict[str, Any]]): Configuración para el 
                                                       motor de etiquetado. 
                                                       Si es None, no se etiquetan.
            
        Returns:
            pd.DataFrame: El DataFrame original enriquecido con todas las 
                          columnas de análisis, y opcionalmente, las etiquetas.
        """
        print("Iniciando pipeline de la estrategia Triple Coincidencia...")
        
        df_processed = df.copy()
        
        # --- Pasos 1-3: Detección, Combinación y Puntuación ---
        print("Paso 1.1: Detectando Velas Clave...")
        df_processed = SignalDetector.detect_key_candles(df_processed, **self.config['key_candle'])
        
        print("Paso 1.2: Detectando Zonas de Acumulación...")
        df_processed = AccumulationZoneDetector.detect(df_processed, **self.config['accumulation_zone'])
        
        print("Paso 1.3: Detectando Mini-Tendencias...")
        df_processed = TrendDetector.detect(df_processed, **self.config['trend'])
        
        print("Paso 2: Combinando señales...")
        df_processed = SignalCombiner.combine(df_processed, **self.config['combiner'])
        
        print("Paso 3: Puntuando las señales...")
        df_processed = SignalScorer.score(df_processed)
        
        # --- Paso 4: Etiquetado (Opcional) ---
        if labeling_config:
            print("Paso 4: Etiquetando señales con PotentialCaptureEngine...")
            
            # Extraer los eventos (velas con triple coincidencia)
            t_events = df_processed[df_processed['triple_coincidence'] == 1].index
            
            if not t_events.empty:
                # Llamar al motor de etiquetado
                labels = get_enhanced_triple_barrier_labels(
                    prices=df_processed,
                    t_events=t_events,
                    **labeling_config
                )
                
                # Añadir las etiquetas al DataFrame
                df_processed['label'] = labels
                # Llenar los NaNs con 0 para las filas que no fueron etiquetadas
                df_processed['label'].fillna(0, inplace=True)
            else:
                print("No se encontraron eventos de triple coincidencia para etiquetar.")
                df_processed['label'] = 0

        print("Pipeline de la estrategia finalizado.")
        return df_processed