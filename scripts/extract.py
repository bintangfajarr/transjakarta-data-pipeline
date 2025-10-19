import pandas as pd
import logging
from sqlalchemy import create_engine
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExtractor:    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.engine = create_engine(db_connection_string)
    
    def extract_csv_files(self, base_path: str = '/opt/airflow/data/input') -> Dict[str, pd.DataFrame]:
    
        logger.info("Starting CSV extraction...")
        
        dataframes = {}
        
        try:
            logger.info("Extracting dummy_realisasi_bus.csv")
            df_realisasi = pd.read_csv(f'{base_path}/dummy_realisasi_bus.csv')
            dataframes['realisasi_bus'] = df_realisasi
            logger.info(f"Loaded {len(df_realisasi)} rows from dummy_realisasi_bus.csv")
            
            logger.info("Extracting dummy_routes.csv")
            df_routes = pd.read_csv(f'{base_path}/dummy_routes.csv')
            dataframes['routes'] = df_routes
            logger.info(f"Loaded {len(df_routes)} rows from dummy_routes.csv")
            
            logger.info("Extracting dummy_shelter_corridor.csv")
            df_shelter = pd.read_csv(f'{base_path}/dummy_shelter_corridor.csv')
            dataframes['shelter_corridor'] = df_shelter
            logger.info(f"Loaded {len(df_shelter)} rows from dummy_shelter_corridor.csv")
            
            logger.info("CSV extraction completed successfully")
            return dataframes
            
        except Exception as e:
            logger.error(f"Error during CSV extraction: {str(e)}")
            raise
    
    def load_csv_to_postgres(self, base_path: str = '/opt/airflow/data/input'):
        logger.info("Starting CSV to PostgreSQL conversion...")
        
        try:
            logger.info("Loading dummy_transaksi_bus.csv to PostgreSQL")
            df_transaksi_bus = pd.read_csv(f'{base_path}/dummy_transaksi_bus.csv')
            
            df_transaksi_bus['waktu_transaksi'] = pd.to_datetime(df_transaksi_bus['waktu_transaksi'])
            df_transaksi_bus['insert_on_dtm'] = pd.to_datetime(df_transaksi_bus['insert_on_dtm'])
            df_transaksi_bus['gate_in_boo'] = df_transaksi_bus['gate_in_boo'].astype(bool)
            df_transaksi_bus['free_service_boo'] = df_transaksi_bus['free_service_boo'].astype(bool)
            
            df_transaksi_bus.to_sql(
                'dummy_transaksi_bus', 
                self.engine, 
                if_exists='replace', 
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Loaded {len(df_transaksi_bus)} rows to dummy_transaksi_bus table")
            
            logger.info("Loading dummy_transaksi_halte.csv to PostgreSQL")
            df_transaksi_halte = pd.read_csv(f'{base_path}/dummy_transaksi_halte.csv')
            
            df_transaksi_halte['waktu_transaksi'] = pd.to_datetime(df_transaksi_halte['waktu_transaksi'])
            df_transaksi_halte['insert_on_dtm'] = pd.to_datetime(df_transaksi_halte['insert_on_dtm'])
            df_transaksi_halte['gate_in_boo'] = df_transaksi_halte['gate_in_boo'].astype(bool)
            df_transaksi_halte['free_service_boo'] = df_transaksi_halte['free_service_boo'].astype(bool)
            
            df_transaksi_halte.to_sql(
                'dummy_transaksi_halte', 
                self.engine, 
                if_exists='replace', 
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Loaded {len(df_transaksi_halte)} rows to dummy_transaksi_halte table")
            
            logger.info("CSV to PostgreSQL conversion completed successfully")
            
        except Exception as e:
            logger.error(f"Error during CSV to PostgreSQL conversion: {str(e)}")
            raise
    
    def extract_from_postgres(self) -> Dict[str, pd.DataFrame]:
        logger.info("Starting PostgreSQL extraction...")
        
        dataframes = {}
        
        try:
            logger.info("Extracting dummy_transaksi_bus from PostgreSQL")
            df_transaksi_bus = pd.read_sql_table('dummy_transaksi_bus', self.engine)
            dataframes['transaksi_bus'] = df_transaksi_bus
            logger.info(f"Loaded {len(df_transaksi_bus)} rows from dummy_transaksi_bus table")
            
            logger.info("Extracting dummy_transaksi_halte from PostgreSQL")
            df_transaksi_halte = pd.read_sql_table('dummy_transaksi_halte', self.engine)
            dataframes['transaksi_halte'] = df_transaksi_halte
            logger.info(f"Loaded {len(df_transaksi_halte)} rows from dummy_transaksi_halte table")
            
            logger.info("PostgreSQL extraction completed successfully")
            return dataframes
            
        except Exception as e:
            logger.error(f"Error during PostgreSQL extraction: {str(e)}")
            raise


def run_extract(**kwargs):
    db_conn = "postgresql://airflow:airflow@postgres/transjakarta_dwh"
    extractor = DataExtractor(db_conn)
    
    logger.info("=" * 50)
    logger.info("STEP 1: Converting CSV to PostgreSQL")
    logger.info("=" * 50)
    extractor.load_csv_to_postgres()
    
    logger.info("=" * 50)
    logger.info("STEP 2: Extracting all data")
    logger.info("=" * 50)
    csv_data = extractor.extract_csv_files()
    postgres_data = extractor.extract_from_postgres()
    
    all_data = {**csv_data, **postgres_data}
    
    ti = kwargs['ti']
    ti.xcom_push(key='extracted_data_keys', value=list(all_data.keys()))
    
    logger.info("Extract task completed successfully")
    return "Extract completed"