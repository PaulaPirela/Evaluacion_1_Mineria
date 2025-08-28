import threading
import time
from concurrent.futures import ThreadPoolExecutor
import requests
import os
import glob
import csv
import typing as t
from utils import read_pokemons
import asyncio
import datetime

# --- Configuración ---
CSV_FOLDER = "C:/Users/paula/Downloads/concurrent-downloads-master/concurrent-downloads-master/data"
OUTPUT_FOLDER = 'output'
MAX_WORKERS = 10 

# # Segundo Metodo: Threading

def read_pokemons(csv_paths):
    """
    Lee múltiples archivos CSV, convierte los nombres a minúsculas y los devuelve.
    """
    for csv_path in csv_paths:
        try:
            with open(csv_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
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
    Descarga una sola imagen. Será ejecutada por cada hilo del pool.
    """
    output_dir, pokemon_data = args
    pokemon_name = pokemon_data['Pokemon']
    folder_name = pokemon_data['Type1']
    image_url = pokemon_data['Sprite']

    try:
        folder_path = os.path.join(output_dir, folder_name)
        os.makedirs(folder_path, exist_ok=True)
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
        print(f"Error de red para {pokemon_name}: {e}")
    except Exception as e:
        print(f"Error inesperado procesando a {pokemon_name}: {e}")

def main(output_dir, inputs):
    """
    Función principal que coincide con la firma esperada por el archivo de prueba.
    """
    pokemons_to_download = list(read_pokemons(inputs))
    tasks_with_args = [(output_dir, data) for data in pokemons_to_download]
    max_workers = 16

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(download_image, tasks_with_args)


# Este bloque te permite ejecutar el script directamente para probarlo
if __name__ == "__main__":
    INPUT_CSVS = [os.path.join(CSV_FOLDER, f"pokemon-gen{i}-data.csv") for i in range(1, 7)]
    OUTPUT_DIR = OUTPUT_FOLDER
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Iniciando descarga con Threading...")
    
    start_time = time.monotonic()
    
    main(OUTPUT_DIR, INPUT_CSVS)
    
    end_time = time.monotonic()
    total_time = end_time - start_time
    
    # ----> 2. CAPTURAR LA HORA ACTUAL Y FORMATEARLA <----
    end_timestamp = datetime.datetime.now()
    
    print("\n----------------------------------------")
    print("¡Proceso con Threading completado!")
    # ----> 3. IMPRIMIR LA HORA DE FINALIZACIÓN Y LA DURACIÓN <----
    print(f"Hora de finalización: {end_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tiempo total de descarga: {total_time:.2f} segundos.")
    print("----------------------------------------")



# def read_pokemons(csv_paths):
#     """
#     Lee múltiples archivos CSV, convierte los nombres a minúsculas y los devuelve.
#     """
#     start_time = time.monotonic()  # <--- 2. GUARDAR EL TIEMPO DE INICIO
#     for csv_path in csv_paths:
#         try:
#             with open(csv_path, mode='r', encoding='utf-8-sig') as f:
#                 reader = csv.DictReader(f)
#                 for row in reader:
#                     # El test espera que los nombres de carpetas y archivos sean en minúsculas
#                     pokemon_name = row.get('Pokemon', '').lower()
#                     type_name = row.get('Type1', '').lower()
#                     sprite_url = row.get('Sprite', '').strip()

#                     if all([pokemon_name, type_name, sprite_url]):
#                         yield {
#                             'Pokemon': pokemon_name,
#                             'Type1': type_name,
#                             'Sprite': sprite_url
#                         }
#         except FileNotFoundError:
#             print(f"ADVERTENCIA: No se encontró el archivo {csv_path}. Omitiendo.")
#         except Exception as e:
#             print(f"Error al leer el archivo {csv_path}: {e}")

# def download_image(args):
#     """
#     Descarga una sola imagen. Será ejecutada por cada hilo del pool.
#     """
#     output_dir, pokemon_data = args
#     pokemon_name = pokemon_data['Pokemon']
#     folder_name = pokemon_data['Type1']
#     image_url = pokemon_data['Sprite']

#     try:
#         # Crea la ruta de la carpeta (ej: output/grass)
#         folder_path = os.path.join(output_dir, folder_name)
#         os.makedirs(folder_path, exist_ok=True)

#         # Crea la ruta final de la imagen (ej: output/grass/bulbasaur.png)
#         image_path = os.path.join(folder_path, f"{pokemon_name}.png")

#         if not os.path.exists(image_path):
#             response = requests.get(image_url, timeout=15)
#             response.raise_for_status()
#             with open(image_path, 'wb') as f:
#                 f.write(response.content)
#             print(f"Descargado: {pokemon_name}.png en '{folder_name}'")
#         else:
#             print(f"Omitido: {pokemon_name}.png ya existe.")
            
#     except requests.RequestException as e:
#         print(f"Error de red para {pokemon_name}: {e}")
#     except Exception as e:
#         print(f"Error inesperado procesando a {pokemon_name}: {e}")

# def main(output_dir, inputs):
#     """
#     Función principal que coincide con la firma esperada por el archivo de prueba.
#     """
#     pokemons_to_download = list(read_pokemons(inputs))
    
#     # Preparamos los argumentos para cada llamada a download_image,
#     # incluyendo el directorio de salida para cada tarea.
#     tasks_with_args = [(output_dir, data) for data in pokemons_to_download]
    
#     # Usamos un número razonable de hilos para no saturar la red
#     max_workers = 16

#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         # El método 'map' distribuye las tareas entre los hilos del pool
#         executor.map(download_image, tasks_with_args)


# # Este bloque te permite ejecutar el script directamente para probarlo
# if __name__ == "__main__":
#     # Asume que los datos están en una subcarpeta 'data'
#     INPUT_CSVS = [f"data/pokemon-gen{i}-data.csv" for i in range(1, 7)]
#     # OUTPUT_DIR = "output_threading"
#     OUTPUT_DIR = OUTPUT_FOLDER
    
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
    
#     print("Iniciando descarga con Threading...")
#     main(OUTPUT_DIR, INPUT_CSVS)
#     print("¡Proceso con Threading completado!")





# def main():
#     """
#     Función principal para encontrar CSVs, procesar los datos
#     y descargar las imágenes en la estructura de carpetas correcta.
#     """
#     start_time = time.monotonic()  # <--- 2. GUARDAR EL TIEMPO DE INICIO

#     print("Iniciando el proceso de descarga de imágenes de Pokémon...")

#     # 1. Crear la carpeta de salida principal si no existe
#     # (El resto de tu código permanece igual)
#     if not os.path.exists(OUTPUT_FOLDER):
#         os.makedirs(OUTPUT_FOLDER)
#         print(f"Directorio principal '{OUTPUT_FOLDER}' creado.")

#     # 2. Encontrar todos los archivos CSV
#     csv_paths = glob.glob(os.path.join(CSV_FOLDER, '*.csv'))
#     if not csv_paths:
#         print(f"Error: No se encontraron archivos .csv en la carpeta '{CSV_FOLDER}'.")
#         return

#     # 3. Procesar cada Pokémon
#     print(f"Procesando {len(csv_paths)} archivo(s) CSV...")
#     pokemon_generator = read_pokemons(csv_paths)

#     for pokemon in pokemon_generator:
#         try:
#             pokemon_name = pokemon['Pokemon']
#             type_name = pokemon['Type1']
#             image_url = pokemon['Sprite']

#             # ... (Lógica de creación de carpetas y definición de rutas)
#             type_folder_path = os.path.join(OUTPUT_FOLDER, type_name)
#             if not os.path.exists(type_folder_path):
#                 os.makedirs(type_folder_path)
            
#             image_path = os.path.join(type_folder_path, f"{pokemon_name}.png")

#             # 6. Descargar la imagen solo si no existe
#             if not os.path.exists(image_path):
#                 print(f"Descargando {pokemon_name}.png en '{type_folder_path}'...")
#                 response = requests.get(image_url, timeout=10)
                
#                 if response.status_code == 200:
#                     with open(image_path, 'wb') as f:
#                         f.write(response.content)
#                 else:
#                     print(f"Fallo al descargar {pokemon_name} (Código: {response.status_code})")
#             else:
#                 print(f"Omitiendo {pokemon_name}.png, ya existe.")

#         except KeyError as e:
#             print(f"Error de clave en una fila: {e}.")
#         except requests.exceptions.RequestException as e:
#             print(f"Error de red para {pokemon.get('Pokemon', 'un Pokémon')}: {e}")

#     # --- 3. CALCULAR Y MOSTRAR LA DURACIÓN ---
#     end_time = time.monotonic()
#     duration = end_time - start_time

#     print("\n" + "="*40)
#     print("¡Proceso completado!")
#     print(f"Duración total: {duration:.2f} segundos.")
#     print("="*40)

# # Ejecutar la función principal solo cuando el script se corre directamente
# if __name__ == "__main__":
#     main()