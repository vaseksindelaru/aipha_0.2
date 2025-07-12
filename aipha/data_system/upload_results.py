import pandas as pd
import os
from aipha.data_system.results_saver import create_cloud_sql_engine, save_results_to_cloud

def main():
    """
    Reads results from local CSV files and uploads them to Google Cloud SQL.
    """
    # Define paths to the CSV files in the project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    training_events_csv = os.path.join(project_root, 'training_events.csv')
    oracle_predictions_csv = os.path.join(project_root, 'oracle_predictions.csv')

    files_to_upload = {
        'training_events': training_events_csv,
        'oracle_predictions': oracle_predictions_csv
    }

    try:
        print("Initializing database connection...")
        db_engine = create_cloud_sql_engine()
        print("Database connection successful.")

        for table_name, file_path in files_to_upload.items():
            if os.path.exists(file_path):
                print(f"Reading {file_path}...")
                df = pd.read_csv(file_path)
                print(f"DataFrame loaded for '{table_name}': shape={df.shape}")
                print(df.head())
                if df.empty:
                    print(f"Warning: DataFrame for '{table_name}' is empty. Skipping upload.")
                    continue
                print(f"Uploading data to '{table_name}' table...")
                try:
                    save_results_to_cloud(df, table_name, engine=db_engine)
                except Exception as upload_err:
                    print(f"Error uploading data to '{table_name}': {upload_err}")
            else:
                print(f"Warning: File not found at {file_path}. Skipping upload.")

    except Exception as e:
        print(f"An error occurred during the upload process: {e}")
        print("Please ensure that the required packages (sqlalchemy, google-cloud-sql-connector[pg8000]) are installed correctly.")

if __name__ == '__main__':
    main()
