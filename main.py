# main.py

import sys
from pathlib import Path

# --- CÓDIGO CLAVE para que funcionen las importaciones ---
# Añadir la carpeta raíz al path para que Python encuentre la carpeta 'scripts'
sys.path.append(str(Path(__file__).resolve().parent))
# ---------------------------------------------------------

# --- 1. Importar la función principal del pipeline ---
from scripts.pipeline import run_etl_pipeline

def main():
    """
    Punto de entrada principal que llama al pipeline ETL.
    """
    run_etl_pipeline()

if __name__ == "__main__":
    main()