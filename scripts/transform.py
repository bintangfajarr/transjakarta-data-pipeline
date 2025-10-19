import pandas as pd
import logging
import re
from sqlalchemy import create_engine
from typing import Dict, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.engine = create_engine(db_connection_string)
    
    def standardize_no_body(self, no_body: str) -> str:
        if pd.isna(no_body):
            return no_body
        
        no_body = str(no_body).upper().strip()
        
        letters = re.findall(r'[A-Z]+', no_body)
        numbers = re.findall(r'\d+', no_body)
        
        if letters and numbers:
            letter_part = letters[0][:3]
            number_part = numbers[0].zfill(3)
            return f"{letter_part}-{number_part}"
        
        return no_body
    
    def clean_and_transform(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
  
        logger.info("Starting data transformation...")
        
        try:
            logger.info("Loading data from PostgreSQL...")
            df_bus = pd.read_sql_table('dummy_transaksi_bus', self.engine)
            df_halte = pd.read_sql_table('dummy_transaksi_halte', self.engine)
            
            logger.info(f"Loaded {len(df_bus)} rows from transaksi_bus")
            logger.info(f"Loaded {len(df_halte)} rows from transaksi_halte")
            
            logger.info("Cleaning transaksi_bus data...")
            
            duplicates_bus = df_bus.duplicated(subset=['uuid']).sum()
            logger.info(f"Found {duplicates_bus} duplicate rows in transaksi_bus")
            df_bus = df_bus.drop_duplicates(subset=['uuid'], keep='first')
            
            logger.info("Standardizing no_body_var in transaksi_bus...")
            df_bus['no_body_var_original'] = df_bus['no_body_var']
            df_bus['no_body_var'] = df_bus['no_body_var'].apply(self.standardize_no_body)
            
            sample_standardization = df_bus[['no_body_var_original', 'no_body_var']].drop_duplicates().head(10)
            logger.info(f"Standardization examples:\n{sample_standardization}")
            
            logger.info("Handling missing values in transaksi_bus...")
            missing_before = df_bus.isnull().sum()
            logger.info(f"Missing values before cleaning:\n{missing_before[missing_before > 0]}")
            
            df_bus['card_type_var'] = df_bus['card_type_var'].fillna('UNKNOWN')
            df_bus['balance_before_int'] = df_bus['balance_before_int'].fillna(0)
            df_bus['balance_after_int'] = df_bus['balance_after_int'].fillna(0)
            df_bus['fare_int'] = df_bus['fare_int'].fillna(0)
            
            df_bus['waktu_transaksi'] = pd.to_datetime(df_bus['waktu_transaksi'])
            df_bus['tanggal'] = df_bus['waktu_transaksi'].dt.date
            
            logger.info(f"Total rows before filtering: {len(df_bus)}")
            df_bus_pelanggan = df_bus[df_bus['status_var'] == 'S'].copy()
            logger.info(f"Total pelanggan rows (status='S'): {len(df_bus_pelanggan)}")
            
            logger.info("Cleaning transaksi_halte data...")
            
            duplicates_halte = df_halte.duplicated(subset=['uuid']).sum()
            logger.info(f"Found {duplicates_halte} duplicate rows in transaksi_halte")
            df_halte = df_halte.drop_duplicates(subset=['uuid'], keep='first')
            
            logger.info("Handling missing values in transaksi_halte...")
            missing_before_halte = df_halte.isnull().sum()
            logger.info(f"Missing values before cleaning:\n{missing_before_halte[missing_before_halte > 0]}")
            
            df_halte['card_type_var'] = df_halte['card_type_var'].fillna('UNKNOWN')
            df_halte['balance_before_int'] = df_halte['balance_before_int'].fillna(0)
            df_halte['balance_after_int'] = df_halte['balance_after_int'].fillna(0)
            df_halte['fare_int'] = df_halte['fare_int'].fillna(0)
            df_halte['shelter_name_var'] = df_halte['shelter_name_var'].fillna('UNKNOWN')
            
            df_halte['waktu_transaksi'] = pd.to_datetime(df_halte['waktu_transaksi'])
            df_halte['tanggal'] = df_halte['waktu_transaksi'].dt.date
            
            logger.info(f"Total rows before filtering: {len(df_halte)}")
            df_halte_pelanggan = df_halte[df_halte['status_var'] == 'S'].copy()
            logger.info(f"Total pelanggan rows (status='S'): {len(df_halte_pelanggan)}")
            
            logger.info("Data transformation completed successfully")
            
            return df_bus_pelanggan, df_halte_pelanggan
            
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise
    
    def aggregate_data(self, df_bus: pd.DataFrame, df_halte: pd.DataFrame) -> Dict[str, pd.DataFrame]:
  
        logger.info("Starting data aggregation...")
        
        aggregated_results = {}
        
        try:
            df_routes = pd.read_csv('/opt/airflow/data/input/dummy_routes.csv')
            df_realisasi = pd.read_csv('/opt/airflow/data/input/dummy_realisasi_bus.csv')
            df_shelter = pd.read_csv('/opt/airflow/data/input/dummy_shelter_corridor.csv')
            
            logger.info("Aggregating by card type...")
            
            df_combined_card = pd.concat([
                df_bus[['tanggal', 'card_type_var', 'gate_in_boo', 'fare_int', 'card_number_var']],
                df_halte[['tanggal', 'card_type_var', 'gate_in_boo', 'fare_int', 'card_number_var']]
            ], ignore_index=True)
            
            agg_card_type = df_combined_card.groupby(['tanggal', 'card_type_var', 'gate_in_boo']).agg(
                jumlah_pelanggan=('card_number_var', 'count'),
                total_amount=('fare_int', 'sum')
            ).reset_index()
            
            agg_card_type.columns = ['tanggal', 'card_type', 'gate_in_boo', 'jumlah_pelanggan', 'total_amount']
            aggregated_results['by_card_type'] = agg_card_type
            logger.info(f"Card type aggregation: {len(agg_card_type)} rows")
            
            logger.info("Aggregating by route...")
            
            df_realisasi['tanggal_realisasi'] = pd.to_datetime(df_realisasi['tanggal_realisasi']).dt.date
            
            df_realisasi['bus_body_no'] = df_realisasi['bus_body_no'].apply(self.standardize_no_body)
            
            df_bus_with_route = df_bus.merge(
                df_realisasi[['tanggal_realisasi', 'bus_body_no', 'rute_realisasi']],
                left_on=['tanggal', 'no_body_var'],
                right_on=['tanggal_realisasi', 'bus_body_no'],
                how='left'
            )
            
            df_bus_with_route = df_bus_with_route.merge(
                df_routes,
                left_on='rute_realisasi',
                right_on='route_code',
                how='left'
            )
            
            df_bus_route = df_bus_with_route[df_bus_with_route['route_code'].notna()].copy()
            
            agg_route = df_bus_route.groupby(['tanggal', 'route_code', 'route_name', 'gate_in_boo']).agg(
                jumlah_pelanggan=('card_number_var', 'count'),
                total_amount=('fare_int', 'sum')
            ).reset_index()
            
            aggregated_results['by_route'] = agg_route
            logger.info(f"Route aggregation: {len(agg_route)} rows")
            
            logger.info("Aggregating by tarif...")
            
            df_combined_tarif = pd.concat([
                df_bus[['tanggal', 'fare_int', 'gate_in_boo', 'card_number_var']],
                df_halte[['tanggal', 'fare_int', 'gate_in_boo', 'card_number_var']]
            ], ignore_index=True)
            
            agg_tarif = df_combined_tarif.groupby(['tanggal', 'fare_int', 'gate_in_boo']).agg(
                jumlah_pelanggan=('card_number_var', 'count'),
                total_amount=('fare_int', 'sum')
            ).reset_index()
            
            agg_tarif.columns = ['tanggal', 'tarif', 'gate_in_boo', 'jumlah_pelanggan', 'total_amount']
            aggregated_results['by_tarif'] = agg_tarif
            logger.info(f"Tarif aggregation: {len(agg_tarif)} rows")
            
            logger.info("Data aggregation completed successfully")
            return aggregated_results
            
        except Exception as e:
            logger.error(f"Error during data aggregation: {str(e)}")
            raise


def run_transform(**kwargs):
    db_conn = "postgresql://airflow:airflow@postgres/transjakarta_dwh"
    transformer = DataTransformer(db_conn)
    
    logger.info("=" * 50)
    logger.info("TRANSFORM TASK STARTED")
    logger.info("=" * 50)
    
    df_bus_clean, df_halte_clean = transformer.clean_and_transform()
    
    aggregated_data = transformer.aggregate_data(df_bus_clean, df_halte_clean)
    
    ti = kwargs['ti']
    ti.xcom_push(key='aggregated_data_keys', value=list(aggregated_data.keys()))
    
    logger.info("Transform task completed successfully")
    return "Transform completed"