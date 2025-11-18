import yaml
import pyodbc
import os
from pathlib import Path

try:
    import pyodbc
except Exception:
    pyodbc = None


class DatabaseConnector:
    def __init__(self):
        self.cfg = self._load_config()

        def _load_config(self):
            cfg = {
            "server": None,
            "port": "1433",
            "database": None,
            "username": None,
            "password": None,
            "driver": "ODBC Driver 18 for SQL Server",
            "encrypt": "yes",
            "trust_server_certificate": "yes",
            "trusted_connection": None,
        }
        yml = Path(__file__).resolve().parent/"db_cred_sql.yaml"
        if yml.exists():
            try:
                with open(yml, "r", encoding="utf-8") as file:
                    loaded_cfg = yaml.safe_load(file)
                    if isinstance(loaded_cfg, dict):
                        cfg.update({k: v for k, v in loaded_cfg.items() if v is not None})

    def read_yaml(self):
        # Resolve YAML path relative to this file to avoid CWD issues
        base_dir = Path(__file__).resolve().parent
        cred_path = base_dir / 'db_cred.yaml'
        try:
            with open(cred_path, 'r', encoding='utf-8') as file:
                credentials = yaml.safe_load(file)
                if not isinstance(credentials, dict):
                    print(f"Error: db_cred.yaml did not contain a mapping at {cred_path}")
                    return None
                return credentials
        except FileNotFoundError:
            print(f"Error: db_cred.yaml not found at {cred_path}")
            return None
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file at {cred_path}: {e}")
            return None
    
    def create_connection(self):
        credentials = self.read_yaml()
        if credentials is None:
            return None
        
        server = credentials.get("server")
        database = credentials.get("database")
        username = credentials.get("username")
        password = credentials.get("password")

        if not all([server, database, username, password]):
            print("Error: Missing database credentials in YAML file.")
            return None
        try:
            driver = self._select_driver()
            if not driver:
                return None

            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=Yes;"
                f"Connection Timeout=5;"
            )
            connection  = pyodbc.connect(conn_str)
            return connection
        except pyodbc.Error as e:
            print(f"Database connection error: {e}")
            print("Please verify server, database, user, password, and ODBC driver.")
            print(f"Attempted server='{server}', database='{database}', user='{username}', driver='{driver}'")
            return None
