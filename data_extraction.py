import os
import time
import pyodbc

SQL_SERVER = os.getenv("APP_SQL_SERVER", "QIG-WXRELADB501.analytics.moodys.net")


def _autopick_sqlsrv_driver() -> str | None:
    drivers = [d.strip() for d in pyodbc.drivers()]
    if "ODBC Driver 18 for SQL Server" in drivers:
        return "ODBC Driver 18 for SQL Server"
    if "ODBC Driver 17 for SQL Server" in drivers:
        return "ODBC Driver 17 for SQL Server"
    # fallback: n'importe quel driver SQL Server détecté
    for d in reversed(drivers):
        if "sql server" in d.lower():
            return d
    return None


def load_conn_str() -> str:
    try:
        from app.core.config import load_conn_str as _dashboard_conn_str
        return _dashboard_conn_str()
    except Exception:
        pass  # on retombe sur l'auto-détection locale

    drv = _autopick_sqlsrv_driver()
    if not drv:
        raise RuntimeError(
            f"No Microsoft ODBC drivers for SQL Server x64 detected."
            f"Drivers installed: {pyodbc.drivers()}"
        )

    extra = "Encrypt=yes;TrustServerCertificate=yes;" if "18" in drv else ""
    return (
        f"Driver={{{drv}}};"
        f"Server={SQL_SERVER};"
        "Trusted_Connection=yes;"
        f"{extra}"
    )


def load_query(f_name: str) -> str:
    with open(f_name, "r", encoding="utf-8") as f:
        return f.read()


def prepare_query(query: str) -> str:
    return (
        query.replace('IN ("', "IN ('")
             .replace(', "', ", '")
             .replace('")', "')")
             .replace('"))', "'))")
             .replace('",', "',")
             .replace('IN ("', "IN ('")
    )


def connect_db(func):
    def wrapper(*args, **kwargs):
        conn_str = load_conn_str()
        max_retries = 20
        for attempt in range(max_retries):
            try:
                with pyodbc.connect(conn_str, timeout=120) as conn:
                    return func(conn, *args, **kwargs)
            except pyodbc.InterfaceError as e:
                if "IM002" in str(e):
                    raise RuntimeError(
                        "SQL Server ODBC driver not found for this application."
                    "Install the x64 driver (17 or 18) in the “prereqs/” folder."
                    f"Drivers detected: {pyodbc.drivers()}"
                    ) from e
                raise
            except pyodbc.OperationalError as e:
                # Retry réseau/serveur transitoire
                if attempt < max_retries - 1:
                    time.sleep(20)
                    continue
                raise
    return wrapper
