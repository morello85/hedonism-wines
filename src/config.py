from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    local_folder: Path
    local_sales_folder: Path
    api_files_bucket_name: str
    sales_files_bucket_name: str
    db_path: Path


def load_settings() -> Settings:
    return Settings(
        local_folder=Path(_require_env("LOCAL_FOLDER")),
        local_sales_folder=Path(_require_env("LOCAL_SALES_FOLDER")),
        api_files_bucket_name=_require_env("API_FILES_BUCKET_NAME"),
        sales_files_bucket_name=_require_env("SALES_FILES_BUCKET_NAME"),
        db_path=Path(_require_env("DB_PATH")),
    )
