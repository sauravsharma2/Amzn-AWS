import pandas as pd

from src.db_connexion.data_extraction import connect_db, load_query


@connect_db
def get_cases(conn) -> pd.DataFrame:
    """
    Loads candidate cases from Companies using queries/get_cases.sql.
    SQL selects columns [Id, Name, Person, Branch, Foreign] with conditions:
    - Id LIKE 'BEC*%' → direct entries,
    - Id LIKE other listed patterns (e.g., 'BEPE%', 'BESCRL%', 'BECSTAT%', 'BE%L', 'BEC[1-4]-%', …) → semi-direct entries.
    Additional filters:
    - Branch IN (0,3) OR NULL,
    - Person NOT IN ('I','D'),
    - [Foreign] NOT IN (1,2) OR NULL.
    Adds CountryCode = first two characters of Id.
    """

    query = load_query("queries/get_cases.sql")
    cursor = conn.cursor()
    cursor.execute(query)
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[col[0] for col in cursor.description])

    if "CountryCode" not in df.columns:
        df["CountryCode"] = df["Id"].astype(str).str[:2]

    return df


@connect_db
def get_cases_from_file(conn, file_path: str = "cases.txt") -> pd.DataFrame:
    """
    Optimized bulk load from file with temporary table strategy.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        # MODIFICATION : Insérer 'C' en 3ème position (index 2)
        ids = [line.strip()[:2] + '' + line.strip()[2:] for line in f if line.strip()]

    if not ids:
        print("[WARN] Fichier vide ou non trouvé")
        return pd.DataFrame(columns=['Id', 'Name', 'Person', 'Branch', 'Foreign', 'CountryCode'])

    cursor = conn.cursor()

    try:
        cursor.execute("""
            IF OBJECT_ID('tempdb..#TempIds') IS NOT NULL
                DROP TABLE #TempIds
            CREATE TABLE #TempIds (Id VARCHAR(50))
        """)

        batch_size = 1000
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            values = ','.join([f"('{id.replace(chr(39), chr(39) + chr(39))}')" for id in batch_ids])
            cursor.execute(f"INSERT INTO #TempIds (Id) VALUES {values}")

        conn.commit()

        query = """
            SELECT c.Id, c.Name, c.Person, c.Branch, c.[Foreign]
            FROM bvdaffils_yesterday.dbo.Companies c
            INNER JOIN #TempIds t ON c.Id = t.Id
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print(f"[WARN] Aucun ID trouvé dans la base parmi les {len(ids)} IDs du fichier")
            sample_ids = ids[:5]
            print(f"[DEBUG] Exemples d'IDs recherchés (avec C ajouté) : {sample_ids}")
            return pd.DataFrame(columns=['Id', 'Name', 'Person', 'Branch', 'Foreign', 'CountryCode'])

        df = pd.DataFrame.from_records(results, columns=['Id', 'Name', 'Person', 'Branch', 'Foreign'])
        df["CountryCode"] = df["Id"].astype(str).str[:2]

        print(f"[INFO] Trouvé {len(df)}/{len(ids)} IDs dans la base ({len(df) / len(ids) * 100:.1f}%)")

        return df

    finally:
        try:
            cursor.execute("IF OBJECT_ID('tempdb..#TempIds') IS NOT NULL DROP TABLE #TempIds")
            conn.commit()
        except Exception as cleanup_error:
            print(f"[ERROR] Cleanup: {cleanup_error}")




