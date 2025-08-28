##raise NotImplementedError

import threading
import time
from concurrent.futures import ThreadPoolExecutor
import requests
import os
import glob
import csv
import typing as t
from utils import read_pokemons

# --- Configuración ---
CSV_FOLDER = "C:/Users/paula/Downloads/concurrent-downloads-master/concurrent-downloads-master/data"
OUTPUT_FOLDER = 'output'
MAX_WORKERS = 10 

## Primer Metodo: Usando Multiprocessing y ThreadPoolExecutor
def read_pokemons(csv_paths):
    """
    Lee múltiples archivos CSV, convierte los nombres a minúsculas y los devuelve.
    """
    for csv_path in csv_paths:
        try:
            with open(csv_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # El test espera que los nombres de carpetas y archivos sean en minúsculas
                    pokemon_name = row.get('Pokemon', '').lower()
                    type_name = row.get('Type1', '').lower()
                    sprite_url = row.get('Sprite', '').strip()

                    if all([pokemon_name, type_name, sprite_url]):
                        yield {
                            'Pokemon': pokemon_name,
                            'Type1': type_name,
                            'Sprite': sprite_url
                        }
        except FileNotFoundError:
            print(f"ADVERTENCIA: No se encontró el archivo {csv_path}. Omitiendo.")
        except Exception as e:
            print(f"Error al leer el archivo {csv_path}: {e}")

def download_image(args):
    """
    Descarga una sola imagen. Adaptada para recibir los argumentos en una tupla.
    """
    output_dir, pokemon_data = args
    pokemon_name = pokemon_data['Pokemon']
    folder_name = pokemon_data['Type1']
    image_url = pokemon_data['Sprite']

    try:
        # Crea la ruta de la carpeta (ej: output/grass)
        folder_path = os.path.join(output_dir, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # Crea la ruta final de la imagen (ej: output/grass/bulbasaur.png)
        image_path = os.path.join(folder_path, f"{pokemon_name}.png")

        if not os.path.exists(image_path):
            response = requests.get(image_url, timeout=15)
            response.raise_for_status()
            with open(image_path, 'wb') as f:
                f.write(response.content)
            print(f"Descargado: {pokemon_name}.png en '{folder_name}'")
        else:
            print(f"Omitido: {pokemon_name}.png ya existe.")
            
    except requests.RequestException as e:
        print(f"❌ Error de red para {pokemon_name}: {e}")
    except Exception as e:
        print(f"❌ Error inesperado procesando a {pokemon_name}: {e}")

def main(output_dir, inputs):
    """
    Función principal que coincide con la firma esperada por el archivo de prueba.
    """
    pokemons_to_download = list(read_pokemons(inputs))
    
    # Preparamos los argumentos para cada llamada a download_image
    # Pasamos el output_dir a cada tarea
    tasks_with_args = [(output_dir, data) for data in pokemons_to_download]
    
    # Usamos un número razonable de hilos
    max_workers = min(32, os.cpu_count() + 4)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(download_image, tasks_with_args)

# Este bloque es opcional, pero te permite ejecutar el script directamente
if __name__ == "__main__":
    # Ejemplo de cómo ejecutarlo directamente
    # Asume que los datos están en una carpeta 'data'
    INPUT_CSVS = [f"data/pokemon-gen{i}-data.csv" for i in range(1, 7)]
    OUTPUT_DIR = "output_threading"
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Iniciando descarga con Threading...")
    main(OUTPUT_DIR, INPUT_CSVS)
    print("¡Proceso con Threading completado!")
