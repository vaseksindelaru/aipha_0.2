import pandas as pd
import sqlalchemy
from google.cloud.sql.connector import Connector

# --- CONFIGURACIÓN ---
# Estos son los datos de tu instancia en Google Cloud
PROJECT_ID = "superb-vigil-465705-d2"
REGION = "europe-west3"
INSTANCE_NAME = "aipha022db"
DB_NAME = "aipha_main"         # El nombre de la base de datos que creaste
DB_USER = "aipha022db"           # El usuario por defecto de PostgreSQL
DB_PASSWORD = "21blackjack" # La contraseña que estableciste

# --- FUNCIÓN PRINCIPAL ---

def create_cloud_sql_engine():
    """
    Initializes the connection to Google Cloud SQL and returns a SQLAlchemy engine.
    """
    print("Initializing Google Cloud SQL connector...")
    connector = Connector()

    def getconn() -> sqlalchemy.engine.base.Connection:
        conn = connector.connect(
            f"{PROJECT_ID}:{REGION}:{INSTANCE_NAME}",
            "pg8000",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
        )
        return conn

    return sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)

def save_results_to_cloud(df: pd.DataFrame, table_name: str, engine=None):
    """
    Connects to a Google Cloud SQL instance and saves a DataFrame to a table.
    If no engine is provided, a new one is created.

    Args:
        df (pd.DataFrame): The DataFrame containing the results to save.
        table_name (str): The name of the SQL table where the data will be saved.
        engine: An optional existing SQLAlchemy engine to use for the connection.
    """
    """
    Conecta a una instancia de Google Cloud SQL y guarda un DataFrame en una tabla.

    Args:
        df (pd.DataFrame): El DataFrame que contiene los resultados a guardar.
        table_name (str): El nombre de la tabla SQL donde se guardarán los datos.
    """
    print(f"Iniciando conexión a la base de datos en Google Cloud...")

    # If no engine is provided, create a new one for this operation.
    if engine is None:
        print("No existing engine provided, creating a new one...")
        engine = create_cloud_sql_engine()

    # Conectar y subir los datos
    with engine.connect() as db_conn:
        with db_conn.begin():  # Inicia una transacción que se confirma automáticamente
            print(f"Conexión exitosa. Guardando {len(df)} filas en la tabla '{table_name}'...")
            
            # Usar to_sql para subir el DataFrame.
            # if_exists='replace' borrará la tabla si ya existe y la creará de nuevo.
            # if_exists='append' would add the data to the end of the existing table.
            df.to_sql(table_name, db_conn, if_exists="append", index=False)
        
        print(f"Data saved successfully to '{table_name}' in Google Cloud SQL!")

# --- EJEMPLO DE USO ---
if __name__ == '__main__':
    # 1. Crear un DataFrame de ejemplo (simulando la salida de tu análisis)
    print("Creando un DataFrame de resultados de ejemplo...")
    forensic_data = {
        'event_time': pd.to_datetime(['2023-01-01 05:00', '2023-01-01 05:00']),
        'potential_direction': ['long', 'short'],
        'realized_outcome': ['PROFIT_TAKE_2', 'STOP_LOSS'],
        'potential_score': [4.5, 0.8],
        'max_favorable_excursion_r': [5.2, 1.1],
        'max_adverse_excursion_r': [0.8, 1.0]
    }
    df_results = pd.DataFrame(forensic_data)

    # 2. Definir el nombre de la tabla
    table_name = "forensic_reports_v1"

    # 3. Create a single engine and use it to save the data
    db_engine = create_cloud_sql_engine()
    save_results_to_cloud(df_results, table_name, engine=db_engine)
