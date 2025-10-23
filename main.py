
import sys
import os
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


from scripts.build_dim_date import process_dim_date
from scripts.build_dim_channel import process_dim_channel
from scripts.build_dim_customer import process_dim_customer
from scripts.build_dim_product import process_dim_product
from scripts.build_dim_location import process_dim_location
from scripts.build_dim_store import process_dim_store
from scripts.build_fact_order import process_fact_order
from scripts.build_fact_order_item import process_fact_order_item
from scripts.build_fact_payment import process_fact_payment
from scripts.build_fact_shipment import process_fact_shipment
from scripts.build_fact_nps_response import process_fact_nps
from scripts.build_fact_web_session import process_fact_web_session

def run_pipeline():
    """
    Orquestador principal que ejecuta todo el pipeline de ETL.
    """
    print("🚀 Iniciando el pipeline de creación del Data Warehouse...")
    Path("dw").mkdir(exist_ok=True)

    print("\n--- Etapa 1: Procesando Dimensiones ---")
    process_dim_date()
    process_dim_channel()
    process_dim_customer()
    process_dim_product()
    process_dim_location()
    process_dim_store()
    
    print("\n--- Etapa 2: Procesando Tablas de Hechos ---")
    process_fact_order()
    process_fact_order_item()
    process_fact_payment()
    process_fact_shipment()
    process_fact_nps()
    process_fact_web_session()


if __name__ == "__main__":
    run_pipeline()