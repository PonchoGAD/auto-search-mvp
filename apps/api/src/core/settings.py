from pydantic import BaseSettings

class Settings(BaseSettings):
    app_name: str = "auto-search-mvp"
    environment: str = "local"

settings = Settings()
