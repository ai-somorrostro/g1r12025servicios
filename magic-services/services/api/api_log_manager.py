from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional


class LogLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"


class APILogManager:
    """Gestor de logs simple para la API. Muy parecido al LogManager del scrapper.
    Registra líneas en un archivo de texto con timestamp y nivel.
    """

    def __init__(self, log_file: Optional[str] = None):
        if log_file is None:
            log_file = "api_log/api_" + datetime.now().strftime("%Y-%m-%d") + ".log"

        self.log_file_path = Path(log_file)
        # asegurar directorio
        if not self.log_file_path.parent.exists():
            try:
                self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

    def _write_log(self, level: LogLevel, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] [{level.value}] {message}\n"
        try:
            with open(self.log_file_path, "a", encoding="utf-8") as f:
                f.write(entry)
        except Exception:
            # No raise: logging should never break the app
            pass

    def info(self, message: str) -> None:
        self._write_log(LogLevel.INFO, message)

    def warning(self, message: str) -> None:
        self._write_log(LogLevel.WARNING, message)

    def error(self, message: str) -> None:
        self._write_log(LogLevel.ERROR, message)

    def success(self, message: str) -> None:
        self._write_log(LogLevel.SUCCESS, message)

    def request(self, method: str, path: str, query: str, body: str, client: str, status: int) -> None:
        msg = f"{client} {method} {path} {query} -> {status} | body={body}"
        self.info(msg)

