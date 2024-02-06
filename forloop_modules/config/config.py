from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

class OpenAIConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.resolve() / '.env.other', env_file_encoding='utf-8', extra='ignore'
    )

    OPENAI_API_KEY: Optional[str] = None
    
open_ai_config = OpenAIConfig()
