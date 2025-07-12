
import pandas as pd
from aipha.oracles.oracle_engine import OracleEngine
from aipha.data_system.results_saver import save_results_to_cloud

# 1. Cargar el modelo del Oráculo
print("Cargando el modelo del Oráculo...")
oracle = OracleEngine(model_path='path/to/your/model.joblib') # Reemplaza con la ruta a tu modelo

# 2. Cargar o crear los datos de entrada para la predicción
#    (Aquí deberías cargar los datos que quieres predecir)
print("Creando datos de entrada de ejemplo...")
features_data = {
    'feature1': [0.1, 0.2, 0.3],
    'feature2': [0.4, 0.5, 0.6],
    'feature3': [0.7, 0.8, 0.9]
}
features_df = pd.DataFrame(features_data)

# 3. Realizar predicciones
print("Realizando predicciones...")
predictions_df = oracle.predict(features_df)

# 4. Definir el nombre de la tabla en la base de datos
table_name = "oracle_predictions_v1"

# 5. Guardar las predicciones en Google Cloud SQL
save_results_to_cloud(predictions_df, table_name)
