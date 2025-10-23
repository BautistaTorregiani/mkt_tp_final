# main.py
import sys
from pathlib import Path

# --- CÓDIGO CLAVE para que funcionen las importaciones ---
# Añadir la carpeta raíz al path para que Python encuentre la carpeta 'etl'
sys.path.append(str(Path(__file__).resolve().parent))
# ---------------------------------------------------------

# --- 1. Importar las funciones de cada etapa del ETL ---
from scripts.extract.extract import extract_raw_data
from scripts.load.load import load_data_to_dw

# Importar TODAS las funciones de la carpeta 'transform'
from scripts.transform.build_dim_date import transform_dim_date
from scripts.transform.build_dim_channel import transform_dim_channel
from scripts.transform.build_dim_customer import transform_dim_customer
from scripts.transform.build_dim_product import transform_dim_product
from scripts.transform.build_dim_location import transform_dim_location
from scripts.transform.build_dim_store import transform_dim_store
from scripts.transform.build_fact_order import transform_fact_order
from scripts.transform.build_fact_order_item import transform_fact_order_item
from scripts.transform.build_fact_payment import transform_fact_payment
from scripts.transform.build_fact_shipment import transform_fact_shipment
from scripts.transform.build_fact_nps_response import transform_fact_nps_response
from scripts.transform.build_fact_web_session import transform_fact_web_session

def main():
    """
    Punto de entrada principal que orquesta el pipeline ETL completo.
    """
    print("🚀 Iniciando pipeline de Data Warehouse...")
    
    # --- ETAPA DE EXTRACCIÓN ---
    raw_data = extract_raw_data()
    
    # --- ETAPA DE TRANSFORMACIÓN ---
    print("\n--- 🏁 Iniciando Etapa de Transformación ---")
    
    # Transformar Dimensiones
    dim_date = transform_dim_date(raw_data)
    dim_channel = transform_dim_channel(raw_data)
    dim_customer = transform_dim_customer(raw_data)
    dim_product = transform_dim_product(raw_data)
    dim_location = transform_dim_location(raw_data)
    dim_store = transform_dim_store(raw_data)

    # Agrupar dimensiones en un diccionario para pasarlas a los hechos
    transformed_dims = {
        'dim_date': dim_date,
        'dim_channel': dim_channel,
        'dim_customer': dim_customer,
        'dim_product': dim_product,
        'dim_location': dim_location,
        'dim_store': dim_store
    }
    
    # Transformar Tablas de Hechos
    fact_order = transform_fact_order(raw_data, transformed_dims)
    fact_order_item = transform_fact_order_item(raw_data, transformed_dims)
    fact_payment = transform_fact_payment(raw_data, transformed_dims)
    fact_shipment = transform_fact_shipment(raw_data, transformed_dims)
    fact_nps_response = transform_fact_nps_response(raw_data, transformed_dims)
    fact_web_session = transform_fact_web_session(raw_data, transformed_dims)

    print("--- ✅ Transformación Completada ---")

    # Agrupar todos los DataFrames finales en un solo diccionario
    final_data = {
        'dim_date': dim_date,
        'dim_channel': dim_channel,
        'dim_customer': dim_customer,
        'dim_product': dim_product,
        'dim_location': dim_location,
        'dim_store': dim_store,
        'fact_order': fact_order,
        'fact_order_item': fact_order_item,
        'fact_payment': fact_payment,
        'fact_shipment': fact_shipment,
        'fact_nps_response': fact_nps_response,
        'fact_web_session': fact_web_session
    }
    
    # --- ETAPA DE CARGA ---
    load_data_to_dw(final_data)
    
    print("\n🎉 ¡Pipeline completado con éxito!")

if __name__ == "__main__":
    main()