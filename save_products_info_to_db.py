import csv
from pathlib import Path
from db_utils import DatabaseConnector
import pyodbc
from config_utils_fruit import DATA_DIR, resource_path


DEFAULT_PRODUCTS_FILENAME = "product_info.csv"


def _resolve_csv_path(csv_path: str | Path | None) -> Path | None:
    """
    Return a writable CSV path. If no path is provided we copy the packaged
    CSV into DATA_DIR (user-writable) on first run and always return that copy.
    """
    if csv_path:
        return Path(csv_path)

    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    writable_csv = data_dir / DEFAULT_PRODUCTS_FILENAME

    if not writable_csv.exists():
        packaged_csv = Path(resource_path(DEFAULT_PRODUCTS_FILENAME))
        if packaged_csv.exists():
            try:
                writable_csv.write_text(packaged_csv.read_text(encoding="utf-8"), encoding="utf-8")
            except Exception as exc:
                print(f"Warning: could not seed writable CSV: {exc}")
        else:
            return None

    return writable_csv


def save_products_from_csv(csv_path: str | Path | None = None):
    """
    Load product rows from a CSV and upsert them into SQL Server.

    Expected CSV headers: Code, Name
    Creates table [AIProducts] if it doesn't exist.
    """
    db = DatabaseConnector()
    connection = db.create_connection()
    if connection is None:
        print("Warning: Database connection not established. Aborting.")
        return 0, 0

    # Resolve CSV path; default to writable copy under DATA_DIR
    csv_file = _resolve_csv_path(csv_path)
    if not csv_file or not csv_file.exists():
        print(f"CSV not found: {csv_file}")
        connection.close()
        return 0, 0

    inserted = 0
    updated = 0
    cursor = None
    try:
        cursor = connection.cursor()

        # Ensure destination table exists
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'AIProducts'
            )
            BEGIN
                CREATE TABLE AIProducts (
                    Code NVARCHAR(50) NOT NULL PRIMARY KEY,
                    Name NVARCHAR(255) NOT NULL,
                    Barcode NVARCHAR(50) NULL
                )
            END
            """
        )
        connection.commit()

        # Read CSV and upsert rows
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, skipinitialspace=True)

            # Normalize header keys (strip spaces, case-insensitive match)
            fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]
            name_map = {fn.lower(): fn for fn in fieldnames}
            code_key = name_map.get("code")
            name_key = name_map.get("name")
            barcode_key = name_map.get("barcode")

            if code_key is None or name_key is None:
                # Fallback to simple reader by index if headers are unexpected
                f.seek(0)
                next(f, None)  # skip header line
                row_reader = csv.reader(f)
                for row in row_reader:
                    if not row:
                        continue
                    code = (row[0] if len(row) > 0 else "").strip()
                    name = (row[1] if len(row) > 1 else "").strip()
                    barcode = (row.get(barcode_key) or "").strip() if barcode_key else None
                    if not code or not name:
                        continue
                    cursor.execute("UPDATE AIProducts SET Name = ?, Barcode = ? WHERE Code = ?", (name,barcode, code))
                    if cursor.rowcount and cursor.rowcount > 0:
                        updated += 1
                    else:
                        cursor.execute("INSERT INTO AIProducts (Code, Name, Barcode) VALUES (?, ?, ?)", (code, name, barcode))
                        inserted += 1
            else:
                for row in reader:
                    code = (row.get(code_key) or "").strip()
                    name = (row.get(name_key) or "").strip()
                    if not code or not name:
                        continue
                    cursor.execute("UPDATE AIProducts SET Name = ?, Barcode = ? WHERE Code = ?", (name, code, barcode))
                    if cursor.rowcount and cursor.rowcount > 0:
                        updated += 1
                    else:
                        cursor.execute("INSERT INTO AIProducts (Code, Name, Barcode) VALUES (?, ?, ?)", (code, name, barcode))
                        inserted += 1

        connection.commit()
        print(f"Products saved. Inserted: {inserted}, Updated: {updated}")
        return inserted, updated
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        try:
            connection.rollback()
        except Exception:
            pass
        return inserted, updated
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        try:
            connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    save_products_from_csv()
