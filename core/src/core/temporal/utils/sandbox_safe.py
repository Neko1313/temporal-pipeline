"""
Утилиты для безопасной работы в Temporal sandbox
"""

from typing import Any


def safe_polars_import():
    """Безопасный импорт polars в temporal context"""
    try:
        import polars as pl

        return pl
    except Exception:
        # Возвращаем заглушку если polars не может быть импортирован
        return None


def is_polars_dataframe(obj: Any) -> bool:
    """Проверяет является ли объект polars DataFrame без прямого импорта"""
    try:
        pl = safe_polars_import()
        if pl is None:
            return False
        return isinstance(obj, pl.DataFrame)
    except Exception:
        return False


def get_dataframe_info(df: Any) -> dict:
    """Получает информацию о DataFrame безопасным способом"""
    if not is_polars_dataframe(df):
        return {"rows": 0, "columns": 0, "column_names": []}

    try:
        return {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns,
        }
    except Exception:
        return {"rows": 0, "columns": 0, "column_names": []}
