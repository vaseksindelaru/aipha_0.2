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

def save_results_to_cloud(df, table_name):
    try:
        with db_conn.begin() as trans:
            df.to_sql(table_name, db_conn, if_exists='append', index=False)
            trans.commit()
        print(f"Datos guardados exitosamente en {table_name}")
    except Exception as e:
        print(f"Error al guardar en {table_name}: {str(e)}")
        raise

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
