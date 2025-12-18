from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pathlib import Path
import json
from typing import Any, Dict, List, Optional
import os
from api_log_manager import APILogManager
from datetime import datetime
import re

app = FastAPI(title="Magic Scrapper API", version="0.1")

# Path inside the container where bulk-data will be mounted
BULK_DATA_DIR = Path("/data/bulk-data")

# Inicializar logger de la API (archivo en la carpeta del servicio)
api_log_path = Path(__file__).parent / "api_log" / f"api_{datetime.now().strftime('%Y-%m-%d')}.log"
logger = APILogManager(str(api_log_path))


@app.middleware("http")
async def log_requests_middleware(request: Request, call_next):
    """Middleware que registra cada petición y su respuesta en el log."""
    try:
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8") if body_bytes else ""
    except Exception:
        body_text = "<unreadable>"

    # Query string
    query = request.url.query or ""
    client = request.client.host if request.client else "-"

    response = await call_next(request)

    try:
        status = response.status_code
    except Exception:
        status = 0

    # Registrar usando el logger
    try:
        logger.request(request.method, request.url.path, query, body_text, client, status)
    except Exception:
        pass

    return response


def _latest_json_file(directory: Path) -> Optional[Path]:
    if not directory.exists() or not directory.is_dir():
        return None
    files = list(directory.glob("scryfall_cards_*.json"))
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


@app.get("/cards-data")
async def get_cards_data_json():
    """Devuelve el contenido del JSON más reciente generado por el scrapper."""
    latest = _latest_json_file(BULK_DATA_DIR)
    if latest is None:
        raise HTTPException(status_code=404, detail="No hay archivos de datos disponibles")

    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer el archivo: {e}")

    return JSONResponse(content=data)


@app.delete("/cards-delete")
async def delete_card(name: str = Query(..., description="Nombre exacto de la carta a eliminar")):
    """Elimina todas las entradas cuya clave `name` coincide exactamente con el valor dado
    en el JSON más reciente. Devuelve un mensaje de éxito con el número de elementos eliminados
    o un error 404 si no se encontró ninguna entrada con ese nombre.
    """
    latest = _latest_json_file(BULK_DATA_DIR)
    if latest is None:
        raise HTTPException(status_code=404, detail="No hay archivos de datos disponibles")

    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer el archivo: {e}")

    # Filtrar por nombre exacto
    original_count = len(data)
    filtered = [rec for rec in data if rec.get("name") != name]
    removed = original_count - len(filtered)

    if removed == 0:
        raise HTTPException(status_code=404, detail=f"No se encontró ninguna carta con el nombre exacto '{name}'")

    # Escribir de forma segura el archivo actualizado
    tmp_path = latest.with_suffix(".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)
        # Reemplazo atómico
        os.replace(str(tmp_path), str(latest))
    except Exception as e:
        # Si falla la escritura, intentar eliminar el tmp si existe
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error al actualizar el archivo: {e}")

    return {"status": "success", "removed": removed, "file": latest.name}


@app.post("/cards-update")
async def update_card(
    name: str = Query(..., description="Nombre exacto de la carta a modificar"),
    updates: Dict[str, Any] = Body(..., description="Objeto con los campos a actualizar y sus nuevos valores"),
):
    """Actualiza campos de las entradas cuyo campo `name` coincide exactamente con `name`.
    Body `updates` debe ser un objeto con pares clave:valor para reemplazar en cada registro.
    Devuelve número de registros modificados y el nombre del archivo.
    """
    latest = _latest_json_file(BULK_DATA_DIR)
    if latest is None:
        raise HTTPException(status_code=404, detail="No hay archivos de datos disponibles")

    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer el archivo: {e}")

    updated_count = 0
    for rec in data:
        if rec.get("name") == name:
            for k, v in updates.items():
                rec[k] = v
            updated_count += 1

    if updated_count == 0:
        raise HTTPException(status_code=404, detail=f"No se encontró ninguna carta con el nombre exacto '{name}'")

    tmp_path = latest.with_suffix(".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(str(tmp_path), str(latest))
    except Exception as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error al actualizar el archivo: {e}")

    return {"status": "success", "updated": updated_count, "file": latest.name}

def _latest_log_file(directory: Path) -> Optional[Path]:
    if not directory.exists() or not directory.is_dir():
        return None
    files = list(directory.glob("api_*.log"))
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]

@app.get("/logs-data")
async def get_logs_data():
    """Devuelve el contenido completo del archivo de logs api.log como texto."""
    latest_log_file = _latest_log_file(Path(__file__).parent / "api_log")

    if not latest_log_file.exists():
        raise HTTPException(status_code=404, detail="El archivo de log no existe")
    # Expresión regular para capturar los campos 
    # Quitar espacios y saltos 
    line = line.strip()
    # Separar en bloques 
    timestamp = line.split("]")[0].strip("[") 
    level = line.split("]")[1].strip(" [") 
    rest = line.split("]")[2].strip()

    
    parsed_logs = []
    # Devolver como texto plano
    try:
        with open(latest_log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                match = pattern.match(line)
                if match:
                    log_entry = {
                        "timestamp": match.group("timestamp"),
                        "level": match.group("level"),
                        "ip": match.group("ip"),
                        "method": match.group("method"),
                        "path": match.group("path"),
                        "status": int(match.group("status")),
                    }
                    parsed_logs.append(log_entry)
                    
        return json.dumps(parsed_logs, indent=2, ensure_ascii=False)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer el archivo de log: {e}")

    # Devolver como texto plano
  


@app.get("/health")
async def health():
    return {"status": "ok"}

