"""
investigate_gbg_threshold.py
----------------------------
Find where GBG stores its face match score and threshold in the database.

We know:
  - SELF_ONBOARDING_TRACKER_KYC.HIGH_LEVEL_RESULT = GBG's final verdict ('Passed')
  - SELF_ONBOARDING_TRACKER_MAIN.JOURNEY_ID        = GBG's journey reference ID

Goal: Find the numeric face match score GBG used to arrive at 'Passed'.
"""

import oracledb
import json

oracle_config = {
    'user':     'MA',
    'password': 'wU8n1av8U$#OLt7pRePrOd',
    'dsn':      'copkdresb-scan:1561/MONAPREPROD'
}

def run(sql, label="", params=None):
    conn = oracledb.connect(**oracle_config)
    cur  = conn.cursor()
    try:
        print(f"\n{'='*70}")
        print(f"QUERY: {label}")
        print(f"{'='*70}")
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"Columns: {cols}")
        print(f"Rows:    {len(rows)}")
        if rows:
            for row in rows[:20]:
                print(dict(zip(cols, row)))
        return rows, cols
    except Exception as e:
        print(f"ERROR: {e}")
        return [], []
    finally:
        cur.close()
        conn.close()


def run_raw(sql, label=""):
    conn = oracledb.connect(**oracle_config)
    cur  = conn.cursor()
    try:
        print(f"\n{'='*70}")
        print(f"QUERY: {label}")
        print(f"{'='*70}")
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows[:30]:
            print(row)
        return rows
    except Exception as e:
        print(f"ERROR: {e}")
        return []
    finally:
        cur.close()
        conn.close()


# ============================================================
# 1. What columns does SELF_ONBOARDING_TRACKER_KYC have?
#    Looking for: score, confidence, face, match, similarity, threshold, gbg
# ============================================================
run_raw("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = 'SELF_ONBOARDING_TRACKER_KYC'
      AND OWNER = 'MA'
    ORDER BY COLUMN_ID
""", "KYC table - ALL columns")


# ============================================================
# 2. What other GBG-related tables exist in schema MA?
#    GBG typically stores detailed response in a separate table
# ============================================================
run_raw("""
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'MA'
      AND (
           UPPER(TABLE_NAME) LIKE '%GBG%'
        OR UPPER(TABLE_NAME) LIKE '%JOURNEY%'
        OR UPPER(TABLE_NAME) LIKE '%BIOMETRIC%'
        OR UPPER(TABLE_NAME) LIKE '%FACE%'
        OR UPPER(TABLE_NAME) LIKE '%LIVENESS%'
        OR UPPER(TABLE_NAME) LIKE '%IDENTITY%'
        OR UPPER(TABLE_NAME) LIKE '%VERIFICATION%'
        OR UPPER(TABLE_NAME) LIKE '%KYC%'
        OR UPPER(TABLE_NAME) LIKE '%ONBOARD%'
      )
    ORDER BY TABLE_NAME
""", "Tables with GBG/journey/face/biometric in name")


# ============================================================
# 3. Look at the full KYC record for a known GBG-passed session
#    to see all populated fields
# ============================================================
run_raw("""
    SELECT k.*, sm.JOURNEY_ID, sm.IS_GBG_JOURNEY_ID_CAPTURED
    FROM MA.SELF_ONBOARDING_TRACKER_KYC k
    JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm ON sm.ID = k.SESSION_ID
    WHERE k.HIGH_LEVEL_RESULT = 'Passed'
      AND sm.LIVELINESS_CHECK = 1
      AND ROWNUM <= 3
""", "Full KYC row for a GBG-passed record")


# ============================================================
# 4. Are there any VARCHAR/CLOB columns in the KYC table
#    that might hold raw GBG JSON response?
# ============================================================
run_raw("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = 'SELF_ONBOARDING_TRACKER_KYC'
      AND OWNER = 'MA'
      AND DATA_TYPE IN ('CLOB', 'VARCHAR2', 'NVARCHAR2', 'XMLTYPE')
    ORDER BY COLUMN_ID
""", "KYC table - CLOB/VARCHAR columns (may hold raw GBG response)")


# ============================================================
# 5. Check if SELF_ONBOARDING_TRACKER_MAIN has any score fields
# ============================================================
run_raw("""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = 'SELF_ONBOARDING_TRACKER_MAIN'
      AND OWNER = 'MA'
    ORDER BY COLUMN_ID
""", "MAIN table - ALL columns")


# ============================================================
# 6. Check all OTHER SELF_ONBOARDING_TRACKER_* tables in the schema
# ============================================================
run_raw("""
    SELECT TABLE_NAME
    FROM ALL_TABLES
    WHERE OWNER = 'MA'
      AND TABLE_NAME LIKE 'SELF_ONBOARDING_TRACKER%'
    ORDER BY TABLE_NAME
""", "All SELF_ONBOARDING_TRACKER_* tables")


# ============================================================
# 7. For each sibling tracker table, show its columns
#    so we can spot score/threshold/face fields
# ============================================================
conn = oracledb.connect(**oracle_config)
cur  = conn.cursor()
cur.execute("""
    SELECT TABLE_NAME FROM ALL_TABLES
    WHERE OWNER = 'MA'
      AND TABLE_NAME LIKE 'SELF_ONBOARDING_TRACKER%'
    ORDER BY TABLE_NAME
""")
tracker_tables = [r[0] for r in cur.fetchall()]
cur.close()
conn.close()

for tbl in tracker_tables:
    run_raw(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = '{tbl}'
          AND OWNER = 'MA'
          AND (
               UPPER(COLUMN_NAME) LIKE '%SCORE%'
            OR UPPER(COLUMN_NAME) LIKE '%CONF%'
            OR UPPER(COLUMN_NAME) LIKE '%FACE%'
            OR UPPER(COLUMN_NAME) LIKE '%MATCH%'
            OR UPPER(COLUMN_NAME) LIKE '%THRESH%'
            OR UPPER(COLUMN_NAME) LIKE '%SIMILAR%'
            OR UPPER(COLUMN_NAME) LIKE '%GBG%'
            OR UPPER(COLUMN_NAME) LIKE '%BIOM%'
            OR UPPER(COLUMN_NAME) LIKE '%IDENT%'
            OR UPPER(COLUMN_NAME) LIKE '%RESULT%'
            OR UPPER(COLUMN_NAME) LIKE '%STATUS%'
            OR UPPER(COLUMN_NAME) LIKE '%LIVE%'
          )
        ORDER BY COLUMN_ID
    """, f"{tbl} — score/face/threshold columns")


# ============================================================
# 8. Check if HIGH_LEVEL_RESULT has other values besides 'Passed'
#    and look for a numeric companion field (face score)
# ============================================================
run_raw("""
    SELECT HIGH_LEVEL_RESULT, COUNT(*) as CNT
    FROM MA.SELF_ONBOARDING_TRACKER_KYC
    GROUP BY HIGH_LEVEL_RESULT
    ORDER BY CNT DESC
""", "KYC - ALL HIGH_LEVEL_RESULT values")


# ============================================================
# 9. GBG API responses often contain a CLOB with JSON.
#    If any CLOB column exists, peek at first 500 chars
# ============================================================
conn = oracledb.connect(**oracle_config)
cur  = conn.cursor()
cur.execute("""
    SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = 'SELF_ONBOARDING_TRACKER_KYC'
      AND OWNER = 'MA'
      AND DATA_TYPE = 'CLOB'
""")
clob_cols = [r[0] for r in cur.fetchall()]
cur.close()
conn.close()

for col in clob_cols:
    if 'IMAGE' in col.upper() or 'PHOTO' in col.upper() or 'CAPTURE' in col.upper():
        print(f"\nSkipping image CLOB column: {col}")
        continue
    run_raw(f"""
        SELECT SUBSTR(DBMS_LOB.SUBSTR({col}, 1000, 1), 1, 1000) as PREVIEW
        FROM MA.SELF_ONBOARDING_TRACKER_KYC
        WHERE {col} IS NOT NULL
          AND HIGH_LEVEL_RESULT = 'Passed'
          AND ROWNUM <= 2
    """, f"KYC.{col} — CLOB preview (looking for GBG face score JSON)")


print("\n" + "="*70)
print("INVESTIGATION COMPLETE")
print("="*70)
print("Review output above to identify:")
print("  1. Which table/column stores GBG face match score")
print("  2. Whether a raw GBG JSON response is stored")
print("  3. What threshold GBG used for face matching")



