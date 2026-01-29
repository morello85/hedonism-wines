from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

REQUIRED_ENV_VARS = (
    "LOCAL_FOLDER",
    "LOCAL_SALES_FOLDER",
    "API_FILES_BUCKET_NAME",
    "SALES_FILES_BUCKET_NAME",
    "DB_PATH",
)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def missing_env_vars() -> list[str]:
    return [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]


@dataclass(frozen=True)
class Settings:
    local_folder: Path
    local_sales_folder: Path
    api_files_bucket_name: str
    sales_files_bucket_name: str
    db_path: Path


def load_settings(required: bool = True) -> Optional[Settings]:
    missing = missing_env_vars()
    if missing:
        if required:
            missing_list = ", ".join(missing)
            raise ValueError(f"Missing required environment variables: {missing_list}")
        return None
    return Settings(
        local_folder=Path(_require_env("LOCAL_FOLDER")),
        local_sales_folder=Path(_require_env("LOCAL_SALES_FOLDER")),
        api_files_bucket_name=_require_env("API_FILES_BUCKET_NAME"),
        sales_files_bucket_name=_require_env("SALES_FILES_BUCKET_NAME"),
        db_path=Path(_require_env("DB_PATH")),
    )
