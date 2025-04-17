
import os
from pathlib import Path


def create_data_directories():
    """Create necessary data directories if they don't exist."""

    data_root = Path(__file__).resolve().parent.parent / "data"
    directories = [
        f'{data_root}/raw',
        f'{data_root}/datalake',
        f'{data_root}/postgres',
        f'{data_root}/postgres/vol-pgadmin_data',
        f'{data_root}/postgres/vol-pgdata',
    ]
    
    for directory in directories:
        if os.path.exists(directory):
            print(f"Directory already exists: {directory}")
        else:
            os.makedirs(directory)
            print(f"Created directory: {directory}")


if __name__ == "__main__":
   create_data_directories() 