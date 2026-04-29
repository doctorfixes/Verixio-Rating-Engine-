from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://user:password@localhost:5432/verixio"
    denver_open_data_app_token: str = ""

    # Denver Open Data Socrata endpoints
    permits_url: str = (
        "https://data.denvergov.org/resource/6rve-5jzc.json"
    )
    complaints_311_url: str = (
        "https://data.denvergov.org/resource/v5au-5xjb.json"
    )
    crime_url: str = (
        "https://data.denvergov.org/resource/mndn-ipfx.json"
    )
    environmental_url: str = (
        "https://data.denvergov.org/resource/b2jg-d77q.json"
    )
    # Denver Real Property Valuations — used by the parcel seeder
    parcel_seed_url: str = (
        "https://data.denvergov.org/resource/dn8v-f35q.json"
    )


settings = Settings()
