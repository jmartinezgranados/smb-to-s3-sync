# src/sync.py
import os
import sys
import logging
from pathlib import Path
from typing import List, Tuple
import boto3
from boto3.s3.transfer import TransferConfig
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
from tqdm import tqdm
import time
import argparse

# Importar config
from config import Config
from utils import setup_logging, format_bytes, SMBHandler

logger = logging.getLogger(__name__)

class OptimizedS3Uploader:
    """Uploader optimizado con boto3 al estilo AWS CLI"""
    
    def __init__(self, config: Config):
        self.config = config
        self.thread_local = local()
        
        # TransferConfig optimizado (como AWS CLI)
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,      # 8MB
            multipart_chunksize=8 * 1024 * 1024,      # 8MB
            max_concurrency=10,
            max_io_queue=1000,
            num_download_attempts=5,
            use_threads=True
        )
        
        # Estadísticas
        self.stats = {
            'success': 0,
            'failed': 0,
            'bytes_transferred': 0,
            'start_time': None
        }
    
    def get_s3_client(self):
        """Cliente S3 por thread"""
        if not hasattr(self.thread_local, 's3_client'):
            self.thread_local.s3_client = boto3.client(
                's3',
                region_name=self.config.aws_region
            )
        return self.thread_local.s3_client
    
    def upload_file(self, file_path: str, s3_key: str) -> Tuple[bool, str, int]:
        """
        Sube un archivo a S3
        Returns: (success, message, bytes_transferred)
        """
        try:
            file_size = os.path.getsize(file_path)
            s3_client = self.get_s3_client()
            
            if self.config.dry_run:
                logger.debug(f"DRY RUN: Would upload {file_path} -> s3://{self.config.s3_bucket}/{s3_key}")
                return True, file_path, file_size
            
            # Upload con config optimizado
            s3_client.upload_file(
                file_path,
                self.config.s3_bucket,
                s3_key,
                Config=self.transfer_config,
                ExtraArgs={'StorageClass': self.config.s3_storage_class}
            )
            
            return True, file_path, file_size
            
        except Exception as e:
            logger.error(f"Error uploading {file_path}: {e}")
            return False, f"{file_path}: {str(e)}", 0
    
    def sync(self, files: List[Tuple[str, str]]):
        """
        Sincroniza lista de archivos a S3
        files: Lista de tuplas (local_path, s3_key)
        """
        if not files:
            logger.warning("No files to sync")
            return
        
        logger.info(f"{'DRY RUN: ' if self.config.dry_run else ''}Syncing {len(files)} files with {self.config.max_workers} workers")
        
        self.stats['start_time'] = time.time()
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit todos los trabajos
            futures = {
                executor.submit(self.upload_file, local_path, s3_key): (local_path, s3_key)
                for local_path, s3_key in files
            }
            
            # Progress bar
            with tqdm(total=len(files), desc="Uploading", unit="files") as pbar:
                for future in as_completed(futures):
                    success, message, bytes_transferred = future.result()
                    
                    if success:
                        self.stats['success'] += 1
                        self.stats['bytes_transferred'] += bytes_transferred
                    else:
                        self.stats['failed'] += 1
                        logger.error(message)
                    
                    pbar.update(1)
                    
                    # Actualizar descripción con stats
                    if self.stats['success'] > 0:
                        elapsed = time.time() - self.stats['start_time']
                        rate = self.stats['success'] / elapsed if elapsed > 0 else 0
                        pbar.set_postfix({
                            'rate': f'{rate:.1f} files/s',
                            'failed': self.stats['failed']
                        })
        
        self.print_summary()
    
    def print_summary(self):
        """Imprime resumen de la sincronización"""
        elapsed = time.time() - self.stats['start_time']
        total = self.stats['success'] + self.stats['failed']
        rate = self.stats['success'] / elapsed if elapsed > 0 else 0
        
        logger.info("="*70)
        logger.info("SYNC SUMMARY")
        logger.info("="*70)
        logger.info(f"  Total files:      {total:,}")
        logger.info(f"  Successful:       {self.stats['success']:,}")
        logger.info(f"  Failed:           {self.stats['failed']:,}")
        logger.info(f"  Data transferred: {format_bytes(self.stats['bytes_transferred'])}")
        logger.info(f"  Time elapsed:     {elapsed:.2f}s")
        logger.info(f"  Upload rate:      {rate:.2f} files/s")
        
        if total > 0:
            # Proyección para 3M archivos
            estimated_hours = (3_000_000 / rate / 3600) if rate > 0 else 0
            logger.info(f"  Estimated for 3M files: {estimated_hours:.2f} hours")
        
        logger.info("="*70)


def create_mock_data(base_path: Path, num_files: int = 1000):
    """Crea datos mock para testing"""
    logger.info(f"Creating {num_files} mock files...")
    
    base_path.mkdir(parents=True, exist_ok=True)
    
    # Crear estructura de directorios
    num_dirs = min(100, num_files // 10)
    files_per_dir = num_files // num_dirs
    
    files = []
    for i in range(num_dirs):
        dir_path = base_path / f"dir_{i:04d}"
        dir_path.mkdir(exist_ok=True)
        
        for j in range(files_per_dir):
            file_path = dir_path / f"file_{j:04d}.txt"
            # Archivos de ~100KB
            file_path.write_bytes(b"x" * (100 * 1024))
            
            # Generar s3_key
            relative = file_path.relative_to(base_path)
            s3_key = str(relative).replace(os.sep, '/')
            files.append((str(file_path), s3_key))
    
    logger.info(f"Created {len(files)} mock files in {base_path}")
    return files


def main():
    parser = argparse.ArgumentParser(description='Optimized SMB to S3 sync')
    parser.add_argument('--mock', action='store_true', help='Use mock data instead of SMB')
    parser.add_argument('--mock-files', type=int, default=100, help='Number of mock files')
    args = parser.parse_args()
    
    # Configuración
    config = Config()
    setup_logging(config.log_level)
    
    logger.info("="*70)
    logger.info("SMB TO S3 SYNC - OPTIMIZED WITH BOTO3")
    logger.info("="*70)
    logger.info(f"Mode: {'MOCK' if args.mock or config.mock_mode else 'PRODUCTION'}")
    logger.info(f"Dry Run: {config.dry_run}")
    logger.info(f"S3 Bucket: {config.s3_bucket}")
    logger.info(f"Max Workers: {config.max_workers}")
    logger.info("="*70)
    
    # Obtener lista de archivos
    if args.mock or config.mock_mode:
        # Modo mock
        mock_path = Path("/app/test-data/mock")
        files = create_mock_data(mock_path, args.mock_files)
    else:
        # Modo producción con SMB
        logger.info("Connecting to SMB share...")
        smb_handler = SMBHandler(config)
        files = smb_handler.list_all_files()
    
    # Sincronizar
    uploader = OptimizedS3Uploader(config)
    uploader.sync(files)
    
    logger.info("✅ Sync completed!")


if __name__ == "__main__":
    main()