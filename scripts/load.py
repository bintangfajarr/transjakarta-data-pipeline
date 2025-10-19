import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.engine = create_engine(db_connection_string)
        self.output_path = '/opt/airflow/data/output'
    
    def load_aggregated_data(self):
        logger.info("Starting data loading...")
        
        try:

            from transform import DataTransformer
            
            transformer = DataTransformer(self.db_connection_string)
            df_bus_clean, df_halte_clean = transformer.clean_and_transform()
            aggregated_data = transformer.aggregate_data(df_bus_clean, df_halte_clean)
            
            logger.info("Saving aggregated data to CSV files...")
            
            csv_card_path = f'{self.output_path}/output_by_card_type.csv'
            aggregated_data['by_card_type'].to_csv(csv_card_path, index=False)
            logger.info(f"Saved to {csv_card_path}")
            
            csv_route_path = f'{self.output_path}/output_by_route.csv'
            aggregated_data['by_route'].to_csv(csv_route_path, index=False)
            logger.info(f"Saved to {csv_route_path}")
            
            csv_tarif_path = f'{self.output_path}/output_by_tarif.csv'
            aggregated_data['by_tarif'].to_csv(csv_tarif_path, index=False)
            logger.info(f"Saved to {csv_tarif_path}")
            
            logger.info("Loading aggregated data to PostgreSQL...")
            
            aggregated_data['by_card_type'].to_sql(
                'output_by_card_type',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Loaded {len(aggregated_data['by_card_type'])} rows to output_by_card_type")
            
            aggregated_data['by_route'].to_sql(
                'output_by_route',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Loaded {len(aggregated_data['by_route'])} rows to output_by_route")
            
            aggregated_data['by_tarif'].to_sql(
                'output_by_tarif',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Loaded {len(aggregated_data['by_tarif'])} rows to output_by_tarif")
            
            logger.info("=" * 50)
            logger.info("LOAD SUMMARY")
            logger.info("=" * 50)
            logger.info(f"Total records by card type: {len(aggregated_data['by_card_type'])}")
            logger.info(f"Total records by route: {len(aggregated_data['by_route'])}")
            logger.info(f"Total records by tarif: {len(aggregated_data['by_tarif'])}")
            
            logger.info("Data loading completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during data loading: {str(e)}")
            raise


def run_load(**kwargs):
    """Main function untuk Airflow task"""
    db_conn = "postgresql://airflow:airflow@postgres/transjakarta_dwh"
    loader = DataLoader(db_conn)
    
    logger.info("=" * 50)
    logger.info("LOAD TASK STARTED")
    logger.info("=" * 50)
    
    loader.load_aggregated_data()
    
    logger.info("Load task completed successfully")
    return "Load completed"