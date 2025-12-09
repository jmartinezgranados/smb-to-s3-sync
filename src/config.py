# src/config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    """Configuración centralizada"""
    
    # AWS
    aws_region: str = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    s3_bucket: str = os.getenv('S3_BUCKET', 'mi-bucket-test')
    s3_prefix: str = os.getenv('S3_PREFIX', '')
    s3_storage_class: str = os.getenv('S3_STORAGE_CLASS', 'INTELLIGENT_TIERING')
    
    # SMB
    smb_server: str = os.getenv('SMB_SERVER', '')
    smb_share: str = os.getenv('SMB_SHARE', '')
    smb_user: str = os.getenv('SMB_USER', '')
    smb_password: str = os.getenv('SMB_PASSWORD', '')
    smb_domain: str = os.getenv('SMB_DOMAIN', '')
    
    # Sync
    max_workers: int = int(os.getenv('MAX_WORKERS', '50'))
    dry_run: bool = os.getenv('DRY_RUN', 'true').lower() == 'true'
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Mock
    mock_mode: bool = os.getenv('MOCK_MODE', 'false').lower() == 'true'
    mock_num_files: int = int(os.getenv('MOCK_NUM_FILES', '10'))
    
    def __post_init__(self):
        """Validaciones"""
        if not self.mock_mode:
            if not self.s3_bucket:
                raise ValueError("S3_BUCKET is required")
            if not self.smb_server:
                raise ValueError("SMB_SERVER is required when not in mock mode")