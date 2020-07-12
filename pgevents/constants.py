import pathlib

CORE_MIGRATIONS_LOCATION = pathlib.Path(__file__).parent.absolute() / "migrations"
PENDING = "PENDING"
PROCESSED = "PROCESSED"
