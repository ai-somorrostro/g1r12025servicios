from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import JSONResponse
from pathlib import Path
import json
from typing import Any, Dict, List, Optional
import os
from api_log_manager import APILogManager

app = FastAPI(title="Magic Scrapper API", version="0.1")

# Path inside the container where bulk-data will be mounted
BULK_DATA_DIR = Path("/data/bulk-data")

# Inicializar logger de la API (archivo en la carpeta del servicio)
api_log_path = Path(file).parent / "api_log/api.log"
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

@app.get("/health")
async def health():
    return {"status": "ok"}

