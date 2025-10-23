import pandas as pd
from pathlib import Path

def extract_raw_data(raw_dir="raw"):
    # Convierte el nombre de la carpeta ("raw") en un objeto de ruta
    raw_path = Path(raw_dir)
    
    # Crea un diccionario vacío donde se guardarán los datos leídos.
    raw_data = {}

    for file_path in raw_path.glob("*.csv"):
        table_name = file_path.stem
        raw_data[table_name] = pd.read_csv(file_path)
        
        print(f"  -> Archivo '{file_path.name}' cargado.")
    return raw_data


if __name__ == "__main__":
    datos_extraidos = extract_raw_data()
    