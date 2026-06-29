import os
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()


def ensure_runtime_schema() -> None:
    """Apply small additive schema fixes for installs without Alembic."""
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    with engine.begin() as connection:
        if "urls_extracted" in table_names:
            existing_columns = {
                column["name"] for column in inspector.get_columns("urls_extracted")
            }
            required_columns = {
                "score": "FLOAT",
                "domain": "VARCHAR",
                "final_url": "VARCHAR",
                "http_status": "INTEGER",
                "redirect_count": "INTEGER",
            }
            for name, column_type in required_columns.items():
                if name not in existing_columns:
                    connection.execute(
                        text(f"ALTER TABLE urls_extracted ADD COLUMN {name} {column_type}")
                    )

        if "body_classification" in table_names:
            existing_columns = {
                column["name"] for column in inspector.get_columns("body_classification")
            }
            required_columns = {
                "probabilities": "JSON",
                "class_id": "INTEGER",
            }
            for name, column_type in required_columns.items():
                if name not in existing_columns:
                    connection.execute(
                        text(f"ALTER TABLE body_classification ADD COLUMN {name} {column_type}")
                    )
