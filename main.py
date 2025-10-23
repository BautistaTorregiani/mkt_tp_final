import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent))
from scripts.pipeline import run_etl_pipeline

def main():
    run_etl_pipeline()

if __name__ == "__main__":
    main()