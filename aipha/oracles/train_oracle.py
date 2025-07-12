import pandas as pd
import os

import numpy as np
from aipha.strategies.triple_coincidence.orchestrator import TripleCoincidenceOrchestrator
from aipha.building_blocks.labelers.potential_capture_engine import get_enhanced_triple_barrier_labels # Asumiendo que esta es la ubicación
from aipha.oracles.oracle_engine import OracleEngine

# --- 1. Carga de Datos ---
# Cargar datos históricos desde los archivos descargados
from aipha.data_system.api_client import ApiClient
from aipha.data_system.binance_klines_fetcher import BinanceKlinesFetcher

print("Cargando datos de mercado desde archivos locales...")
api_client = ApiClient()
fetcher = BinanceKlinesFetcher(api_client=api_client, download_dir="downloaded_data")

# Especificar los detalles del archivo que queremos cargar
df_market = fetcher.fetch_klines_as_dataframe(
    symbol='BTCUSDT',
    interval='5m',
    year=2024,
    month=4,
    day=22
)

if df_market is None or df_market.empty:
    raise ValueError("No se pudieron cargar los datos. Asegúrate de que el archivo exista y no esté vacío.")



# --- 2. Ejecución de la Estrategia y Etiquetado ---
# Configuración de ejemplo para la estrategia y el etiquetado
strategy_config = {
    'key_candle': {
        'volume_lookback': 50,
        'volume_percentile_threshold': 60,
        'body_percentile_threshold': 50
    },
    'accumulation_zone': {
        'volume_threshold': 1.1,
        'atr_multiplier': 1.0,
        'min_zone_periods': 5,
        'volume_ma_period': 30,
        'atr_period': 14
    },
    'trend': {
        'zigzag_threshold': 0.005,
        'min_trend_bars': 5
    },
    'combiner': {
        'proximity_lookback': 8,
        'min_zone_quality': 0.0,
        'min_trend_r2': 0.0
    }
}

labeling_config = {
    'profit_factors': [1.5, 3.0],
    'stop_loss_factor': 1.0,
    'time_limit': 10,
    'drawdown_threshold': 0.5
}

print("Ejecutando la estrategia de Triple Coincidencia...")
orchestrator = TripleCoincidenceOrchestrator(strategy_config)
df_processed = orchestrator.run(df_market, labeling_config=labeling_config)

# Simular la salida del PotentialCaptureEngine para el ejemplo
# En un caso real, esta lógica estaría en el PotentialCaptureEngine
def simulate_potential_capture(df, events):
    # ... (Lógica compleja del PotentialCaptureEngine iría aquí)
    # Por ahora, generamos datos falsos para el ejemplo
    long_scores = pd.Series(np.random.uniform(0.5, 5.0, size=len(events)), index=events)
    short_scores = pd.Series(np.random.uniform(0.5, 5.0, size=len(events)), index=events)
    return long_scores, short_scores

events_to_label = df_processed[df_processed['is_triple_coincidence'] == 1].index
if not events_to_label.empty:
    print(f"Se encontraron {len(events_to_label)} eventos. Generando el informe forense...")
    # Aquí llamaríamos al PotentialCaptureEngine real
    # df_forensic_report = PotentialCaptureEngine.run(df_processed, events_to_label, ...)
    
    # Simulación:
    long_scores, short_scores = simulate_potential_capture(df_processed, events_to_label)
    df_processed['potential_score_long'] = long_scores
    df_processed['potential_score_short'] = short_scores
    df_processed.fillna(0, inplace=True)


# --- 3. Entrenamiento del Oráculo ---
# Preparar los datos para el entrenamiento
training_data = df_processed[df_processed['is_triple_coincidence'] == 1]

if not training_data.empty:
    # Seleccionar características (features) para el modelo
    # Estas serían las características que describen la calidad de la señal
    features = training_data[[
        'candle_score',
        'zone_score',
        'trend_score',
        'base_score',
        'advanced_score',
        'final_score'
    ]].copy()
    features.fillna(0, inplace=True) # Limpieza simple

    # Seleccionar los objetivos (targets)
    targets_long = training_data['potential_score_long']
    targets_short = training_data['potential_score_short']

    # Configuración del modelo LightGBM
    lgbm_params = {
        'objective': 'regression_l1', # MAE
        'n_estimators': 100,
        'learning_rate': 0.05,
        'num_leaves': 31,
        'max_depth': -1,
        'min_child_samples': 20,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'random_state': 42,
        'n_jobs': -1
    }

    # --- Entrenamiento del Oráculo para Longs ---
    print("Iniciando el entrenamiento del Oráculo para Longs...")
    oracle_long = OracleEngine()
    oracle_long.train(features, targets_long, lgbm_params)
    oracle_long.save_model("aipha/oracles/oracle_model_long.joblib")

    # --- Entrenamiento del Oráculo para Shorts ---
    print("Iniciando el entrenamiento del Oráculo para Shorts...")
    oracle_short = OracleEngine()
    oracle_short.train(features, targets_short, lgbm_params)
    oracle_short.save_model("aipha/oracles/oracle_model_short.joblib")

    # --- 4. Ejemplo de Predicción ---
    print("\nRealizando una predicción de ejemplo con los mismos datos...")
    sample_features = features.head(5)
    predictions_long_df = oracle_long.predict(sample_features, prediction_column_name='predicted_score_long')
    predictions_short_df = oracle_short.predict(sample_features, prediction_column_name='predicted_score_short')

    # Combinar las predicciones en un solo DataFrame
    predictions = pd.concat([predictions_long_df, predictions_short_df], axis=1)

    # Añadir la decisión final basada en el score más alto
    predictions['oracle_decision'] = predictions.apply(
        lambda row: 'long' if row['predicted_score_long'] > row['predicted_score_short'] else 'short',
        axis=1
    )
    predictions['oracle_confidence'] = predictions.apply(
        lambda row: row['predicted_score_long'] if row['oracle_decision'] == 'long' else row['predicted_score_short'],
        axis=1
    )

    print("Características de entrada de ejemplo:")
    print(sample_features)
    print("\nPredicciones del Oráculo:")
    print(predictions)

    # --- 4. Guardar Resultados en Archivos CSV ---
    print("Guardando resultados en archivos CSV...")
    try:
        # Guardar los datos de entrenamiento
        training_data_to_save = training_data[features.columns.tolist() + ['potential_score_long', 'potential_score_short']].copy()
        training_data_to_save.index.name = 'event_timestamp'
        training_events_path = 'training_events.csv'
        training_data_to_save.to_csv(training_events_path, index=True)
        print(f"Se guardaron {len(training_data_to_save)} eventos de entrenamiento en '{training_events_path}'.")

        # Guardar la predicción de ejemplo
        last_event = training_data.index[-1]
        prediction_long = oracle_long.predict(features.tail(1), prediction_column_name='predicted_score_long').iloc[0]
        prediction_short = oracle_short.predict(features.tail(1), prediction_column_name='predicted_score_short').iloc[0]
        prediction_df = pd.DataFrame([{
            'timestamp': last_event,
            'prediction_long': prediction_long,
            'prediction_short': prediction_short
        }])
        predictions_path = 'oracle_predictions.csv'
        # Añadir al archivo si existe, de lo contrario crearlo con cabecera
        header = not os.path.exists(predictions_path)
        prediction_df.to_csv(predictions_path, mode='a', header=header, index=False)
        print(f"Se guardó la predicción de ejemplo en '{predictions_path}'.")

    except Exception as e:
        print(f"Error al guardar los archivos CSV: {e}")



else:
    print("No se encontraron eventos de triple coincidencia para entrenar el Oráculo.")