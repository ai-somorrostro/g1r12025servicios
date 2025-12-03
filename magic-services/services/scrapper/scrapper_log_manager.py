import os
from datetime import datetime
from enum import Enum

class LogLevel(Enum):
    """Niveles de log disponibles"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"

class LogManager:
    """
    Gestor de logs para el scraper de Scryfall.
    Registra todas las operaciones en un archivo de texto.
    """
    
    def __init__(self, log_file="scryfall_scraper.log"):
        """
        Inicializa el gestor de logs.
        
        Args:
            log_file (str): Nombre del archivo de log
        """
        self.log_file = log_file
        self.session_start = None
        self.cards_count = 0
        self.errors = []
        
    def _write_log(self, level, message):
        """
        Escribe una entrada en el archivo de log.
        
        Args:
            level (LogLevel): Nivel del mensaje
            message (str): Mensaje a escribir
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level.value}] {message}\n"
        
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception as e:
            print(f"Error al escribir en el log: {e}")
    
    def inicio_scraping(self):
        """
        Registra el inicio de una sesión de scraping.
        """
        self.session_start = datetime.now()
        separator = "=" * 80
        
        self._write_log(LogLevel.INFO, separator)
        self._write_log(LogLevel.INFO, "INICIO DE SESIÓN DE SCRAPING")
        self._write_log(LogLevel.INFO, f"Fecha y hora de inicio: {self.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        self._write_log(LogLevel.INFO, separator)
    
    def info(self, message):
        """
        Registra un mensaje informativo.
        
        Args:
            message (str): Mensaje informativo
        """
        self._write_log(LogLevel.INFO, message)
    
    def warning(self, message):
        """
        Registra una advertencia.
        
        Args:
            message (str): Mensaje de advertencia
        """
        self._write_log(LogLevel.WARNING, message)
    
    def error(self, message):
        """
        Registra un error.
        
        Args:
            message (str): Mensaje de error
        """
        self.errors.append(message)
        self._write_log(LogLevel.ERROR, message)
    
    def success(self, message):
        """
        Registra un mensaje de éxito.
        
        Args:
            message (str): Mensaje de éxito
        """
        self._write_log(LogLevel.SUCCESS, message)
    
    # def set_cards_count(self, count):
    #     """
    #     Establece el número de cartas recuperadas.
        
    #     Args:
    #         count (int): Número de cartas
    #     """
    #     self.cards_count = count
    #     self._write_log(LogLevel.INFO, f"Total de cartas procesadas: {count}")
    
    # def dataset_info(self, name, description, size_mb, updated_at):
    #     """
    #     Registra información sobre el dataset descargado.
        
    #     Args:
    #         name (str): Nombre del dataset
    #         description (str): Descripción del dataset
    #         size_mb (float): Tamaño en MB
    #         updated_at (str): Fecha de última actualización
    #     """
    #     self._write_log(LogLevel.INFO, f"Dataset: {name}")
    #     self._write_log(LogLevel.INFO, f"Descripción: {description}")
    #     self._write_log(LogLevel.INFO, f"Tamaño: {size_mb:.2f} MB")
    #     self._write_log(LogLevel.INFO, f"Última actualización: {updated_at}")
    
    def download_progress(self, message):
        """
        Registra el progreso de la descarga.
        
        Args:
            message (str): Mensaje de progreso
        """
        self._write_log(LogLevel.INFO, f"Progreso: {message}")
    
    def file_saved(self, filename, file_size_mb=None):
        """
        Registra que un archivo ha sido guardado.
        
        Args:
            filename (str): Nombre del archivo guardado
            file_size_mb (float, optional): Tamaño del archivo en MB
        """
        msg = f"Archivo guardado: {filename}"
        if file_size_mb:
            msg += f" ({file_size_mb:.2f} MB)"
        self._write_log(LogLevel.SUCCESS, msg)
    
    def fin_scraping(self, exitoso=True):
        """
        Registra el final de una sesión de scraping con un resumen.
        
        Args:
            exitoso (bool): Si el scraping fue exitoso o no
        """
        if self.session_start is None:
            self._write_log(LogLevel.WARNING, "No se había iniciado ninguna sesión de scraping")
            return
        
        session_end = datetime.now()
        duration = session_end - self.session_start
        
        separator = "=" * 80
        
        self._write_log(LogLevel.INFO, separator)
        self._write_log(LogLevel.INFO, "FIN DE SESIÓN DE SCRAPING")
        self._write_log(LogLevel.INFO, f"Fecha y hora de fin: {session_end.strftime('%Y-%m-%d %H:%M:%S')}")
        self._write_log(LogLevel.INFO, f"Duración total: {duration}")
        
        if self.errors:
            self._write_log(LogLevel.INFO, "")
            self._write_log(LogLevel.INFO, "ERRORES REGISTRADOS:")
            for i, error in enumerate(self.errors, 1):
                self._write_log(LogLevel.ERROR, f"  {i}. {error}")
        
        # Estado final
        self._write_log(LogLevel.INFO, "")
        if exitoso and len(self.errors) == 0:
            self._write_log(LogLevel.SUCCESS, "SCRAPING COMPLETADO EXITOSAMENTE")
        elif exitoso and len(self.errors) > 0:
            self._write_log(LogLevel.WARNING, "SCRAPING COMPLETADO CON ADVERTENCIAS")
        else:
            self._write_log(LogLevel.ERROR, "SCRAPING FINALIZADO CON ERRORES")
        
        self._write_log(LogLevel.INFO, separator)
        self._write_log(LogLevel.INFO, "")  # Línea en blanco para separar sesiones
        
        # Reset para próxima sesión
        self.session_start = None
        self.cards_count = 0
        self.errors = []
    