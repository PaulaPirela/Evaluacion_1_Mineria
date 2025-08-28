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
import aiohttp  # La versión asíncrona de 'requests'
from utils import read_pokemons
import aiofiles

# --- Configuración ---
CSV_FOLDER = "C:/Users/paula/Downloads/concurrent-downloads-master/concurrent-downloads-master/data"
OUTPUT_FOLDER = 'output'
MAX_WORKERS = 10 


# Tercer metodo: asyncio

# --- Lógica Asíncrona Principal --
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

async def download_image(session, output_dir, pokemon_data):
    """
    Descarga una sola imagen de forma asíncrona.
    """
    pokemon_name = pokemon_data['Pokemon']
    folder_name = pokemon_data['Type1']
    image_url = pokemon_data['Sprite']

    try:
        # Crea la ruta de la carpeta (ej: output/grass)
        folder_path = os.path.join(output_dir, folder_name)
        # os.makedirs es síncrono, pero es rápido y seguro de usar aquí
        os.makedirs(folder_path, exist_ok=True)

        # Crea la ruta final de la imagen (ej: output/grass/bulbasaur.png)
        image_path = os.path.join(folder_path, f"{pokemon_name}.png")

        if not os.path.exists(image_path):
            async with session.get(image_url, timeout=15) as response:
                response.raise_for_status()
                async with aiofiles.open(image_path, 'wb') as f:
                    await f.write(await response.read())
                print(f"Descargado: {pokemon_name}.png en '{folder_name}'")
        else:
            print(f"Omitido: {pokemon_name}.png ya existe.")
            
    except aiohttp.ClientError as e:
        print(f"❌ Error de red para {pokemon_name}: {e}")
    except Exception as e:
        print(f"❌ Error inesperado procesando a {pokemon_name}: {e}")

def main(output_dir, inputs):
    """
    Función principal que coincide con la firma esperada por el archivo de prueba.
    """
    # Creamos una función anidada para poder ejecutar el código asíncrono
    # desde esta función síncrona que llama el test.
    async def async_main():
        pokemons_to_download = list(read_pokemons(inputs))
        
        async with aiohttp.ClientSession() as session:
            # Creamos una lista de tareas (coroutines) para ejecutar
            tasks = [
                download_image(session, output_dir, data) 
                for data in pokemons_to_download
            ]
            # Ejecutamos todas las tareas concurrentemente
            await asyncio.gather(*tasks)

    # Ejecutamos el bucle de eventos de asyncio
    asyncio.run(async_main())

# Este bloque te permite ejecutar el script directamente para probarlo
if __name__ == "__main__":
    # Asume que los datos están en una carpeta 'data'
    INPUT_CSVS = [f"data/pokemon-gen{i}-data.csv" for i in range(1, 7)]
    OUTPUT_DIR = OUTPUT_FOLDER
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Iniciando descarga con Asyncio...")
    main(OUTPUT_DIR, INPUT_CSVS)
    print("¡Proceso con Asyncio completado!")







# def read_pokemons(csv_paths):
#     """
#     Generador que lee los archivos CSV basándose en la estructura real del archivo:
#     - Nombre de imagen de la columna 'Pokemon'.
#     - Nombre de carpeta de la columna 'Type1'.
#     - URL de la imagen de la columna 'Sprite'.
#     """
#     for csv_path in csv_paths:
#         try:
#             with open(csv_path, mode='r', encoding='utf-8-sig') as f:
#                 reader = csv.DictReader(f)
                
#                 # Verificamos que las columnas necesarias existan
#                 headers = [h.strip() for h in reader.fieldnames]
#                 if not all(k in headers for k in ['Pokemon', 'Type1', 'Sprite']):
#                     print(f"ADVERTENCIA: {csv_path} no contiene las columnas 'Pokemon', 'Type1' y 'Sprite'. Omitiendo.")
#                     continue

#                 for row in reader:
#                     # Extraemos los datos usando los nombres de columna correctos
#                     pokemon_name = row.get('Pokemon')
#                     folder_name = row.get('Type1')
#                     image_url = row.get('Sprite', '').strip()

#                     if all([pokemon_name, folder_name, image_url.startswith('http')]):
#                         yield {
#                             'PokemonName': pokemon_name,
#                             'FolderName': folder_name,
#                             'SpriteURL': image_url
#                         }
#         except Exception as e:
#             print(f"Error crítico al procesar el archivo CSV {csv_path}: {e}")


# async def download_image(session, pokemon_data):
#     """
#     Descarga una imagen usando los datos extraídos correctamente.
#     """
#     pokemon_name = pokemon_data.get('PokemonName')
#     folder_name = pokemon_data.get('FolderName')
#     image_url = pokemon_data.get('SpriteURL')

#     try:
#         # Crea la ruta de la carpeta (ej: output/GRASS)
#         valid_folder_name = "".join(c for c in folder_name if c.isalnum() or c in (' ', '_')).rstrip()
#         folder_path = os.path.join(OUTPUT_FOLDER, valid_folder_name)
#         os.makedirs(folder_path, exist_ok=True)

#         # Crea la ruta de la imagen con el nombre del Pokémon (ej: output/GRASS/Chespin.png)
#         valid_pokemon_name = "".join(c for c in pokemon_name if c.isalnum() or c in (' ', '_')).rstrip()
#         image_path = os.path.join(folder_path, f"{valid_pokemon_name}.png")

#         if not os.path.exists(image_path):
#             print(f"Descargando '{pokemon_name}.png' en la carpeta '{valid_folder_name}'...")
#             async with session.get(image_url, timeout=15) as response:
#                 response.raise_for_status()
#                 async with aiofiles.open(image_path, 'wb') as f:
#                     await f.write(await response.read())
#         else:
#             print(f"Omitiendo {pokemon_name}.png, ya existe.")

#     except aiohttp.ClientError as e:
#         print(f"❌ Error de red para {pokemon_name}: {e}")
#     except Exception as e:
#         print(f"❌ Error inesperado procesando a {pokemon_name}: {e}")

# async def main():
#     """
#     Función principal que orquesta todo el proceso.
#     """
#     start_time = time.monotonic()
#     print("Iniciando el proceso de descarga de imágenes de Pokémon...")
#     os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
#     # Busca el archivo CSV específico que subiste
#     csv_paths = glob.glob(os.path.join(CSV_FOLDER, 'pokemon-gen6-data.csv'))
#     if not csv_paths:
#         print(f"Error: No se encontró el archivo 'pokemon-gen6-data.csv' en la carpeta '{CSV_FOLDER}'.")
#         return

#     print(f"Procesando {len(csv_paths)} archivo(s) CSV...")
#     pokemon_generator = read_pokemons(csv_paths)

#     async with aiohttp.ClientSession() as session:
#         tasks = [download_image(session, pokemon) for pokemon in pokemon_generator]
#         await asyncio.gather(*tasks)

#     end_time = time.monotonic()
#     duration = end_time - start_time

#     print("\n" + "="*40)
#     print("¡Proceso completado! ✅")
#     print(f"Duración total: {duration:.2f} segundos.")
#     print("="*40)

# if __name__ == "__main__":
#     asyncio.run(main())