
from pathlib import Path

def load_data_to_dw(transformed_data, dw_dir="dw"):
    """
    Guarda todos los DataFrames del diccionario 'transformed_data'
    en archivos .csv dentro de la carpeta especificada (por defecto, "dw").
    """
    print("\n--- 🏁 Iniciando Etapa de Carga ---")

    # 1. Prepara el directorio de destino
    dw_path = Path(dw_dir)
    dw_path.mkdir(exist_ok=True) 

    # 2. Recorre y guardar cada tabla
    for table_name, df in transformed_data.items():

        # Crea la ruta completa del archivo de salida, ej: "dw/dim_customer.csv"
        file_path = dw_path / f"{table_name}.csv"

        # Guarda el DataFrame en un archivo .csv.
        # index=False evita que pandas guarde una columna extra con el índice.
        df.to_csv(file_path, index=False)

        print(f"  -> Tabla '{file_path.name}' guardada.")

    print("--- ✅ Carga Completada ---")

    