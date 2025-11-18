import csv
from pathlib import Path
from db_utils import DatabaseConnector
import pyodbc


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

    # Resolve CSV path; default to repo-local product_info.csv
    csv_file = Path(csv_path) if csv_path else Path(__file__).resolve().parent / "product_info.csv"
    if not csv_file.exists():
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
                    Name NVARCHAR(255) NOT NULL
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
                    if not code or not name:
                        continue
                    cursor.execute("UPDATE AIProducts SET Name = ? WHERE Code = ?", (name, code))
                    if cursor.rowcount and cursor.rowcount > 0:
                        updated += 1
                    else:
                        cursor.execute("INSERT INTO AIProducts (Code, Name) VALUES (?, ?)", (code, name))
                        inserted += 1
            else:
                for row in reader:
                    code = (row.get(code_key) or "").strip()
                    name = (row.get(name_key) or "").strip()
                    if not code or not name:
                        continue
                    cursor.execute("UPDATE AIProducts SET Name = ? WHERE Code = ?", (name, code))
                    if cursor.rowcount and cursor.rowcount > 0:
                        updated += 1
                    else:
                        cursor.execute("INSERT INTO AIProducts (Code, Name) VALUES (?, ?)", (code, name))
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
