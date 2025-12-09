# src/utils.py
import logging
import colorlog
from typing import List, Tuple

def setup_logging(level: str = 'INFO'):
    """Configura logging con colores"""
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    ))
    
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))

def format_bytes(bytes: int) -> str:
    """Formatea bytes a formato legible"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"

class SMBHandler:
    """Handler para SMB (placeholder - implementar si se necesita)"""
    
    def __init__(self, config):
        self.config = config
    
    def list_all_files(self) -> List[Tuple[str, str]]:
        """Lista todos los archivos del share SMB"""
        # TODO: Implementar con smbprotocol si es necesario
        raise NotImplementedError("SMB handler not implemented yet. Use --mock mode.")