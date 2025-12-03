import requests
import json

import os
from datetime import datetime
from scrapper_log_manager import LogManager
import random
import re
import time
from typing import Dict, List, Any, Optional
from pathlib import Path


def _parse_mana_cost_to_cmc(mana_cost: Optional[str]) -> int:
    """
    Convierte una cadena de coste de maná como "{2}{G}{U}" en su CMC numérico.
    - Los tokens numéricos se toman como su valor entero.
    - Cualquier token no numérico (letras, símbolos híbridos, X, etc.) se cuenta como 1.
    """
    if not mana_cost or not isinstance(mana_cost, str):
        return 0

    tokens = re.findall(r"\{([^}]*)\}", mana_cost)
    total = 0
    for t in tokens:
        t = t.strip()
        if t.isdigit():
            total += int(t)
            continue

        m = re.match(r"^(\d+)", t)
        if m:
            total += int(m.group(1))
            continue

        # cualquier otro símbolo cuenta como 1
        total += 1

    return total

def filtrar_carta(carta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Filtra los campos específicos de una carta según los requerimientos.
    Para cartas de doble cara, devuelve una lista con ambas caras.
    Para cartas normales, devuelve una lista con un solo elemento.
    """
    cartas_resultado = []

    # Verificar si la carta tiene múltiples caras
    if 'card_faces' in carta and len(carta['card_faces']) > 0:
        # Carta de doble cara - crear un registro por cada cara
        for i, face in enumerate(carta['card_faces']):
            # Preferir el campo `face_number` si lo provee la API, si no usar el índice
            face_number = face.get('face_number') if isinstance(face, dict) else None
            if face_number is None:
                face_number = i

            carta_filtrada = {
                # Usar `oracle_id` como identificador principal en lugar del id Scryfall
                "oracle_id": carta.get("oracle_id"),
                # Mantener el id original de Scryfall como referencia si se necesita
                "parent_id": carta.get("id"),
                "face_number": face_number,
                "name": face.get("name"),
                "lang": carta.get("lang"),
                "released_at": carta.get("released_at"),
                "image_png": face.get("image_uris", {}).get("png") if isinstance(face, dict) and "image_uris" in face else None,
                "mana_cost": face.get("mana_cost"),
                # Para cartas de doble cara, usar el CMC calculado desde el coste de cada cara
                "cmc": _parse_mana_cost_to_cmc(face.get("mana_cost")),
                "type_line": face.get("type_line"),
                "oracle_text": face.get("oracle_text"),
                "power": face.get("power"),
                "toughness": face.get("toughness"),
                "colors": face.get("colors", []),
                "color_identity": carta.get("color_identity", []),  # Color identity es de la carta completa
                "keywords": carta.get("keywords", []),  # Keywords de la carta completa
                "produced_mana": face.get("produced_mana", carta.get("produced_mana", [])),
                "commander_legality": carta.get("legalities", {}).get("commander"),
                "game_changer": carta.get("game_changer"),
                "set_name": carta.get("set_name"),
                "rarity": carta.get("rarity"),
                "artist": face.get("artist", carta.get("artist")),
                "full_art": carta.get("full_art"),
                "booster": carta.get("booster"),
                "price_usd": carta.get("prices", {}).get("usd"),
                "price_usd_foil": carta.get("prices", {}).get("usd_foil"),
                "price_usd_etched": carta.get("prices", {}).get("usd_etched"),
                "cardmarket_url": carta.get("purchase_uris", {}).get("cardmarket")
            }
            cartas_resultado.append(carta_filtrada)
    else:
        # Carta de una sola cara
        carta_filtrada = {
            # Usar `oracle_id` como identificador principal
            "oracle_id": carta.get("oracle_id"),
            "parent_id": None,
            # Si la API incluye face_number a nivel de carta, lo usamos, si no None
            "face_number": carta.get("face_number"),
            "name": carta.get("name"),
            "lang": carta.get("lang"),
            "released_at": carta.get("released_at"),
            "image_png": carta.get("image_uris", {}).get("png") if "image_uris" in carta else None,
            "mana_cost": carta.get("mana_cost"),
            "cmc": carta.get("cmc"),
            "type_line": carta.get("type_line"),
            "oracle_text": carta.get("oracle_text"),
            "power": carta.get("power"),
            "toughness": carta.get("toughness"),
            "colors": carta.get("colors", []),
            "color_identity": carta.get("color_identity", []),
            "keywords": carta.get("keywords", []),
            "produced_mana": carta.get("produced_mana", []),
            "commander_legality": carta.get("legalities", {}).get("commander"),
            "game_changer": carta.get("game_changer"),
            "set_name": carta.get("set_name"),
            "rarity": carta.get("rarity"),
            "artist": carta.get("artist"),
            "full_art": carta.get("full_art"),
            "booster": carta.get("booster"),
            "price_usd": carta.get("prices", {}).get("usd"),
            "price_usd_foil": carta.get("prices", {}).get("usd_foil"),
            "price_usd_etched": carta.get("prices", {}).get("usd_etched"),
            "cardmarket_url": carta.get("purchase_uris", {}).get("cardmarket")
        }
        cartas_resultado.append(carta_filtrada)

    return cartas_resultado

def descargar_cartas_scryfall() -> Optional[str]:
    """
    Descarga todas las cartas de Magic: The Gathering desde la API de Scryfall
    y las guarda en un archivo JSON con solo los campos necesarios.
    """
    

    # Definir rutas
    script_dir: Path = Path(__file__).parent
    bulk_data_dir: Path = script_dir / "bulk-data"
    bulk_data_dir.mkdir(exist_ok=True)
    log_file_path: Path = script_dir / "scryfall_scraper.log"

    # Inicializar log manager en la ruta correcta
    logger = LogManager(str(log_file_path))
    logger.inicio_scraping()
    
    print("Iniciando descarga de cartas desde Scryfall...")
    print("=" * 50)
    
    # URL del endpoint de bulk data de Scryfall
    bulk_data_url = "https://api.scryfall.com/bulk-data"
    
    try:
        # Obtener información sobre los datos bulk disponibles
        print("Obteniendo información de bulk data...")
        logger.info("Conectando con la API de Scryfall")
        
        response = requests.get(bulk_data_url)
        response.raise_for_status()
        
        logger.success("Conexión establecida correctamente con la API")
        
        bulk_data = response.json()
        
        # Buscar el archivo de "default_cards" (todas las cartas)
        default_cards = None
        for item in bulk_data['data']:
            if item['type'] == 'default_cards':
                default_cards = item
                break
        
        if not default_cards:
            error_msg = "No se encontró el dataset de cartas predeterminadas"
            print(f"Error: {error_msg}")
            logger.error(error_msg)
            logger.fin_scraping(exitoso=False)
            return None
        
        
        # Mostrar información del dataset
        size_mb = default_cards['size'] / (1024*1024)
        print(f"\nDataset encontrado:")
        print(f"- Nombre: {default_cards['name']}")
        print(f"- Descripción: {default_cards['description']}")
        print(f"- Tamaño: {size_mb:.2f} MB")
        print(f"- Última actualización: {default_cards['updated_at']}")
        
        # Descargar el archivo JSON completo
        download_url = default_cards['download_uri']
        print(f"\nDescargando cartas desde: {download_url}")
        print("Esto puede tardar un momento dependiendo de tu conexión...")
        
        logger.download_progress("Descargando dataset completo...")
        
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        logger.success("Descarga completada")
        logger.download_progress("Parseando JSON...")
        
        # Parsear el JSON
        print("Procesando datos...")
        cartas = response.json()
        
        logger.info(f"JSON parseado correctamente: {len(cartas)} cartas encontradas")
        
        # Filtrar solo los campos necesarios
        print(f"Filtrando campos de {len(cartas)} cartas...")
        logger.download_progress(f"Filtrando {len(cartas)} cartas...")
        
        cartas_filtradas = []
        for carta in cartas:
            cartas_filtradas.extend(filtrar_carta(carta))  # extend porque puede devolver múltiples caras
        
        logger.success(f"Filtrado completado: {len(cartas_filtradas)} registros procesados (incluyendo caras de cartas dobles)")
        #logger.set_cards_count(len(cartas_filtradas))

        # Deduplicar por oracle_id:
        # - Si un oracle_id tiene varias entradas y alguna de ellas tiene `face_number` (cartas doble cara),
        #   conservar todas las caras para ese oracle_id (mantener unidos los registros de la carta).
        # - Si un oracle_id tiene solo registros sin `face_number` (cartas de una sola cara),
        #   conservar únicamente la entrada más barata (empates al azar).
        def _price_value(rec):
            p = rec.get('price_usd')
            try:
                return float(p) if p is not None else float('inf')
            except (ValueError, TypeError):
                return float('inf')

        logger.info("Iniciando deduplicado de cartas...")
        groups = {}
        for rec in cartas_filtradas:
            key = rec.get('oracle_id') or rec.get('parent_id') or rec.get('id')
            if key is None:
                key = f"_no_oracle_{random.getrandbits(64)}"
            groups.setdefault(key, []).append(rec)

        result = []
        for key, items in groups.items():
            # Si hay varias entradas y al menos una tiene face_number, conservar todas las caras
            if any(item.get('face_number') is not None for item in items):
                # Opcional: dentro de las caras con el mismo face_number, elegir la más barata
                by_face = {}
                for item in items:
                    fn = item.get('face_number')
                    # si fn es None lo tratamos como a su propia "cara"
                    face_key = fn if fn is not None else '__none__'
                    if face_key not in by_face:
                        by_face[face_key] = item
                    else:
                        cur = by_face[face_key]
                        cur_price = _price_value(cur)
                        new_price = _price_value(item)
                        if new_price < cur_price:
                            by_face[face_key] = item
                        elif new_price == cur_price:
                            by_face[face_key] = random.choice([cur, item])

                # añadir todas las caras retenidas
                result.extend(by_face.values())
            else:
                # Solo single-face: conservar la más barata
                best = None
                best_price = float('inf')
                for item in items:
                    price = _price_value(item)
                    if price < best_price:
                        best = item
                        best_price = price
                    elif price == best_price:
                        best = random.choice([best, item])
                if best is not None:
                    result.append(best)

        cartas_filtradas = result
        logger.success(f"Deduplicado completado: {len(cartas_filtradas)} registros finales")
        
        # Guardar en archivo local

        nombre_archivo = f"scryfall_cards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_path = bulk_data_dir / nombre_archivo
        print(f"\nGuardando {len(cartas_filtradas)} cartas en '{output_path}'...")

        logger.info(f"Guardando datos en archivo: {output_path}")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(cartas_filtradas, f, ensure_ascii=False, indent=2)

        # Calcular tamaño del archivo
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.file_saved(str(output_path), file_size_mb)

        print("\n" + "=" * 50)
        print("¡Descarga completada exitosamente!")
        print(f"Archivo guardado: {output_path}")
        print(f"Total de cartas descargadas: {len(cartas_filtradas)}")
        print("=" * 50)
        
        # Limpiar archivos antiguos: mantener solo los 3 más recientes
        def _cleanup_old_files(directory: Path, max_files: int = 3) -> None:
            """Elimina archivos JSON antiguos dejando solo los max_files más recientes."""
            if not directory.exists():
                return
            files = sorted(directory.glob("scryfall_cards_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            if len(files) > max_files:
                files_to_delete = files[max_files:]
                for file_path in files_to_delete:
                    try:
                        file_path.unlink()
                        print(f"Eliminado archivo antiguo: {file_path.name}")
                        logger.info(f"Archivo antiguo eliminado: {file_path.name}")
                    except Exception as e:
                        print(f"No se pudo eliminar {file_path.name}: {e}")
                        logger.warning(f"No se pudo eliminar {file_path.name}: {e}")
        
        _cleanup_old_files(bulk_data_dir, max_files=3)
        
        #logger.cards_count(len(cartas_filtradas))
        logger.success("Proceso completado exitosamente sin errores")
        logger.fin_scraping(exitoso=True)
        
        return nombre_archivo
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error al conectar con la API de Scryfall: {e}"
        print(f"\n{error_msg}")
        logger.error(error_msg)
        logger.fin_scraping(exitoso=False)
        return None
        
    except json.JSONDecodeError as e:
        error_msg = f"Error al procesar el JSON: {e}"
        print(f"\n{error_msg}")
        logger.error(error_msg)
        logger.fin_scraping(exitoso=False)
        return None
        
    except Exception as e:
        error_msg = f"Error inesperado: {type(e).__name__}: {e}"
        print(f"\n{error_msg}")
        logger.error(error_msg)
        logger.fin_scraping(exitoso=False)
        return None

def buscar_carta_especifica(nombre_carta: str) -> Optional[Dict[str, Any]]:
    """
    Busca una carta específica por nombre usando la API de Scryfall.
    Útil para búsquedas individuales sin descargar todo el dataset.
    """
    logger = LogManager("scryfall_scraper.log")
    
    url = f"https://api.scryfall.com/cards/named"
    params = {'fuzzy': nombre_carta}
    
    try:
        logger.info(f"Buscando carta: {nombre_carta}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        logger.success(f"Carta encontrada: {nombre_carta}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al buscar carta '{nombre_carta}': {e}")
        return None

if __name__ == "__main__":
    # Ejecutar la descarga completa
    # Modo de ejecución controlado mediante la variable de entorno `SCRAPPER_MODE`:
    # - 'once' -> ejecutar una sola vez
    # - 'loop' -> ejecutar periódicamente cada 10 minutos (comportamiento por defecto)
    mode = os.getenv("SCRAPPER_MODE", "loop").lower()

    if mode == "once":
        archivo = descargar_cartas_scryfall()

        if archivo:
            print(f"\n✓ Los datos están disponibles en: {archivo}")
            print(f"✓ Log guardado en: scrapper/scryfall_scraper.log")
            print("\nPuedes cargar el archivo con:")
            print(f"  with open('{archivo}', 'r', encoding='utf-8') as f:")
            print("      cartas = json.load(f)")
        else:
            print("\n✗ El scraping falló. Revisa el log para más detalles.")
    else:
        print("Iniciando modo loop: ejecutando cada 10 minutos. Ctrl+C para detener.")
        try:
            while True:
                archivo = descargar_cartas_scryfall()
                if archivo:
                    print(f"\n✓ Los datos están disponibles en: {archivo}")
                else:
                    print("\n✗ El scraping falló en esta iteración. Revisa el log.")

                print("Esperando 10 minutos hasta la siguiente ejecución...\n")
                time.sleep(10 * 60)
        except KeyboardInterrupt:
            print("\nEjecución interrumpida por el usuario. Saliendo...")