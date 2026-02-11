"""
investigate_gbg_threshold_3.py
--------------------------------
The OCR tracker has 222,774 records but doesn't join on SESSION_ID to KYC.
Goal:
  1. Find the correct join key between OCR tracker and KYC/MAIN tables
  2. Read RAW_RESPONSE CLOB - it likely contains the full GBG JSON with face score
  3. Find GBG's numeric face match score and threshold
"""

import oracledb
import json

oracle_config = {
    'user':     'MA',
    'password': 'wU8n1av8U$#OLt7pRePrOd',
    'dsn':      'copkdresb-scan:1561/MONAPREPROD'
}

def run(sql, label=""):
    conn = oracledb.connect(**oracle_config)
    cur  = conn.cursor()
    try:
        print(f"\n{'='*70}")
        print(f"QUERY: {label}")
        print(f"{'='*70}")
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"Columns: {cols}")
        print(f"Rows: {len(rows)}")
        for row in rows[:20]:
            print(dict(zip(cols, row)))
        return rows, cols
    except Exception as e:
        print(f"ERROR: {e}")
        return [], []
    finally:
        cur.close()
        conn.close()


# ============================================================
# 1. What does the OCR tracker's ID column look like?
#    Is it a UUID? Does it match JOURNEY_ID in MAIN?
# ============================================================
run("""
    SELECT ID, DEVICE_ID, SESSION_ID, STATUS,
           CONFIDENCE_SCORE, LIVELINESS_CONFIDENCE_SCORE
    FROM MA.SELF_ONBOARDING_TRACKER_OCR
    WHERE ROWNUM <= 10
    ORDER BY DATE_CREATED DESC
""", "OCR tracker - raw sample rows (check ID format)")


# ============================================================
# 2. Does OCR.ID match MAIN.JOURNEY_ID?
#    MAIN.JOURNEY_ID is the GBG journey reference
# ============================================================
run("""
    SELECT COUNT(*) AS matches
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm
        ON sm.JOURNEY_ID = o.ID
    WHERE sm.LIVELINESS_CHECK = 1
""", "OCR.ID joins to MAIN.JOURNEY_ID? (count matches)")


# ============================================================
# 3. Does OCR.SESSION_ID match MAIN.ID?
# ============================================================
run("""
    SELECT COUNT(*) AS matches
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm
        ON sm.ID = o.SESSION_ID
    WHERE sm.LIVELINESS_CHECK = 1
""", "OCR.SESSION_ID joins to MAIN.ID? (count matches)")


# ============================================================
# 4. Does OCR.ID match MAIN.ID directly?
# ============================================================
run("""
    SELECT COUNT(*) AS matches
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm
        ON sm.ID = o.ID
    WHERE sm.LIVELINESS_CHECK = 1
""", "OCR.ID joins to MAIN.ID? (count matches)")


# ============================================================
# 5. Does OCR.DEVICE_ID match MAIN.DEVICE_ID?
# ============================================================
run("""
    SELECT COUNT(*) AS matches
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm
        ON sm.DEVICE_ID = o.DEVICE_ID
    WHERE sm.LIVELINESS_CHECK = 1
""", "OCR.DEVICE_ID joins to MAIN.DEVICE_ID? (count matches)")


# ============================================================
# 6. Peek at MAIN.JOURNEY_ID to compare format with OCR.ID
# ============================================================
run("""
    SELECT ID, JOURNEY_ID, LIVELINESS_CHECK, IS_GBG_JOURNEY_ID_CAPTURED
    FROM MA.SELF_ONBOARDING_TRACKER_MAIN
    WHERE LIVELINESS_CHECK = 1
      AND JOURNEY_ID IS NOT NULL
    FETCH FIRST 10 ROWS ONLY
""", "MAIN - sample JOURNEY_ID values (compare format to OCR.ID)")


# ============================================================
# 7. Read RAW_RESPONSE CLOB from OCR tracker
#    This likely contains the full GBG API JSON response
#    including the face match score and threshold
# ============================================================
run("""
    SELECT
        o.ID,
        o.SESSION_ID,
        o.CONFIDENCE_SCORE,
        o.LIVELINESS_CONFIDENCE_SCORE,
        o.STATUS,
        DBMS_LOB.SUBSTR(o.RAW_RESPONSE, 2000, 1) AS raw_response_preview
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    WHERE o.RAW_RESPONSE IS NOT NULL
      AND o.CONFIDENCE_SCORE IS NOT NULL
    FETCH FIRST 5 ROWS ONLY
""", "OCR RAW_RESPONSE CLOB preview - GBG JSON response with face score")


# ============================================================
# 8. Read RAW_RESPONSE for high confidence records
#    (to see what a passing GBG response looks like)
# ============================================================
run("""
    SELECT
        o.CONFIDENCE_SCORE,
        o.LIVELINESS_CONFIDENCE_SCORE,
        o.STATUS,
        DBMS_LOB.SUBSTR(o.RAW_RESPONSE, 3000, 1) AS raw_response_preview
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    WHERE o.RAW_RESPONSE IS NOT NULL
      AND o.CONFIDENCE_SCORE >= 80
    FETCH FIRST 3 ROWS ONLY
""", "OCR RAW_RESPONSE for HIGH confidence records (>= 80%)")


# ============================================================
# 9. Read RAW_RESPONSE for LOW confidence records
#    (to see what a failing GBG response looks like)
# ============================================================
run("""
    SELECT
        o.CONFIDENCE_SCORE,
        o.LIVELINESS_CONFIDENCE_SCORE,
        o.STATUS,
        DBMS_LOB.SUBSTR(o.RAW_RESPONSE, 3000, 1) AS raw_response_preview
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    WHERE o.RAW_RESPONSE IS NOT NULL
      AND o.CONFIDENCE_SCORE < 70
    FETCH FIRST 3 ROWS ONLY
""", "OCR RAW_RESPONSE for LOW confidence records (< 70%)")


# ============================================================
# 10. Check what the AWS_IMAGE and ID_PHOTO BLOBs in OCR contain
#     Are these the same images stored in KYC/AWS tables?
# ============================================================
run("""
    SELECT
        o.ID,
        o.SESSION_ID,
        CASE WHEN o.AWS_IMAGE IS NOT NULL
             THEN DBMS_LOB.GETLENGTH(o.AWS_IMAGE) ELSE 0 END AS face_image_bytes,
        CASE WHEN o.ID_PHOTO IS NOT NULL
             THEN DBMS_LOB.GETLENGTH(o.ID_PHOTO) ELSE 0 END AS card_image_bytes,
        o.CONFIDENCE_SCORE,
        o.STATUS
    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
    WHERE o.AWS_IMAGE IS NOT NULL
       OR o.ID_PHOTO IS NOT NULL
    FETCH FIRST 10 ROWS ONLY
""", "OCR - image sizes (AWS_IMAGE = selfie, ID_PHOTO = card)")


# ============================================================
# 11. Verify: does the OCR tracker actually store GBG data
#     or is it storing the OLD OCR/AWS pipeline data?
#     Check date range of records
# ============================================================
run("""
    SELECT
        TO_CHAR(MIN(DATE_CREATED), 'YYYY-MM-DD') AS earliest,
        TO_CHAR(MAX(DATE_CREATED), 'YYYY-MM-DD') AS latest,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN CONFIDENCE_SCORE IS NOT NULL THEN 1 END) AS with_score,
        COUNT(CASE WHEN RAW_RESPONSE IS NOT NULL THEN 1 END) AS with_raw_response,
        COUNT(CASE WHEN AWS_IMAGE IS NOT NULL THEN 1 END) AS with_face_image,
        COUNT(CASE WHEN ID_PHOTO IS NOT NULL THEN 1 END) AS with_card_image
    FROM MA.SELF_ONBOARDING_TRACKER_OCR
""", "OCR tracker - date range and data completeness")


print("\n" + "="*70)
print("INVESTIGATION 3 COMPLETE")
print("="*70)