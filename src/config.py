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


def missing_env_vars(*, allow_missing_s3: bool = False) -> list[str]:
    required = set(REQUIRED_ENV_VARS)
    if allow_missing_s3:
        required -= {"API_FILES_BUCKET_NAME", "SALES_FILES_BUCKET_NAME"}
    return [name for name in required if not os.getenv(name)]


@dataclass(frozen=True)
class Settings:
    local_folder: Path
    local_sales_folder: Path
    api_files_bucket_name: Optional[str]
    sales_files_bucket_name: Optional[str]
    db_path: Path


def load_settings(
    required: bool = True,
    *,
    allow_missing_s3: bool = False,
) -> Optional[Settings]:
    missing = missing_env_vars(allow_missing_s3=allow_missing_s3)
    if missing:
        if required:
            missing_list = ", ".join(missing)
            raise ValueError(f"Missing required environment variables: {missing_list}")
        return None
    return Settings(
        local_folder=Path(_require_env("LOCAL_FOLDER")),
        local_sales_folder=Path(_require_env("LOCAL_SALES_FOLDER")),
        api_files_bucket_name=os.getenv("API_FILES_BUCKET_NAME"),
        sales_files_bucket_name=os.getenv("SALES_FILES_BUCKET_NAME"),
        db_path=Path(_require_env("DB_PATH")),
    )
