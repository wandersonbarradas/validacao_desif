import polars as pl
from app.db import SessionLocal

def criar_df(model) -> pl.DataFrame:
    db = SessionLocal()

    try:
        db_data = db.query(model).all()

        if not db_data:
            return pl.DataFrame([])

        nomes_colunas = model.__table__.columns.keys()
        data = {coluna: [getattr(item, coluna) for item in db_data] for coluna in nomes_colunas}
        df = pl.DataFrame(data)
    finally:
        db.close()

    return df


def search_db(model, prop, value):
    db = SessionLocal()

    try:
        db_data = db.query(model).filter(getattr(model, prop) == value).first()

        if not db_data:
            return None
    finally:
        db.close()

    return db_data
