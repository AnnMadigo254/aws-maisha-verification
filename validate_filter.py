import oracledb

oracle_config = {
    'user': 'MA',
    'password': 'wU8n1av8U$#OLt7pRePrOd',
    'dsn': 'copkdresb-scan:1561/MONAPREPROD'
}

def validate_correct_filter():
    connection = oracledb.connect(**oracle_config)
    cursor = connection.cursor()

    print("="*70)
    print("VALIDATION - CORRECT FILTER FOR AWS vs GBG COMPARISON")
    print("="*70)

    # -------------------------------------------------------
    # How many records with LIVELINESS_CHECK=1
    # split by HIGH_LEVEL_RESULT
    # -------------------------------------------------------
    print("\n[1] HIGH_LEVEL_RESULT distribution for LIVELINESS_CHECK=1 records:")
    print("-"*60)
    cursor.execute("""
        SELECT 
            k.HIGH_LEVEL_RESULT,
            COUNT(*) as COUNT
        FROM MA.SELF_ONBOARDING_TRACKER_AWS o
        JOIN MA.SELF_ONBOARDING_TRACKER_KYC k ON o.SESSION_ID = k.SESSION_ID
        JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm ON sm.ID = k.SESSION_ID
        WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
            AND o.AWS_IMAGE IS NOT NULL
            AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
            AND sm.IS_GBG_JOURNEY_ID_CAPTURED = 1
            AND sm.IS_RETRIEVED = 1
            AND sm.LIVELINESS_CHECK = 1
        GROUP BY k.HIGH_LEVEL_RESULT
        ORDER BY COUNT(*) DESC
    """)
    rows = cursor.fetchall()
    total = sum(r[1] for r in rows)
    print(f"\n  {'HIGH_LEVEL_RESULT':25s} | {'COUNT':8s} | {'%':6s} | USE FOR COMPARISON?")
    print(f"  {'-'*25}-+-{'-'*8}-+-{'-'*6}-+-{'-'*20}")
    for row in rows:
        pct = (row[1] / total * 100) if total > 0 else 0
        use = "✅ YES - GBG Approved" if row[0] == 'Passed' else "❌ NO - GBG Rejected/Unknown"
        print(f"  {str(row[0]):25s} | {row[1]:8,d} | {pct:5.1f}% | {use}")

    # -------------------------------------------------------
    # Check if there are any non-Passed HIGH_LEVEL_RESULT
    # values we should also test
    # -------------------------------------------------------
    print("\n[2] ALL UNIQUE HIGH_LEVEL_RESULT values in KYC table:")
    print("-"*60)
    cursor.execute("""
        SELECT DISTINCT HIGH_LEVEL_RESULT, COUNT(*) as COUNT
        FROM MA.SELF_ONBOARDING_TRACKER_KYC
        GROUP BY HIGH_LEVEL_RESULT
        ORDER BY COUNT(*) DESC
    """)
    rows = cursor.fetchall()
    for row in rows:
        print(f"  '{str(row[0])}' → {row[1]:,} records")

    # -------------------------------------------------------
    # Final count - how many records available for 
    # proper comparison
    # -------------------------------------------------------
    print("\n[3] FINAL RECORD COUNTS FOR COMPARISON:")
    print("-"*60)

    # GBG Passed
    cursor.execute("""
        SELECT COUNT(*)
        FROM MA.SELF_ONBOARDING_TRACKER_AWS o
        JOIN MA.SELF_ONBOARDING_TRACKER_KYC k ON o.SESSION_ID = k.SESSION_ID
        JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm ON sm.ID = k.SESSION_ID
        WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
            AND o.AWS_IMAGE IS NOT NULL
            AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
            AND sm.IS_GBG_JOURNEY_ID_CAPTURED = 1
            AND sm.IS_RETRIEVED = 1
            AND sm.LIVELINESS_CHECK = 1
            AND k.HIGH_LEVEL_RESULT = 'Passed'
    """)
    passed = cursor.fetchone()[0]

    # GBG Failed/Other
    cursor.execute("""
        SELECT COUNT(*)
        FROM MA.SELF_ONBOARDING_TRACKER_AWS o
        JOIN MA.SELF_ONBOARDING_TRACKER_KYC k ON o.SESSION_ID = k.SESSION_ID
        JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm ON sm.ID = k.SESSION_ID
        WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
            AND o.AWS_IMAGE IS NOT NULL
            AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
            AND sm.IS_GBG_JOURNEY_ID_CAPTURED = 1
            AND sm.IS_RETRIEVED = 1
            AND sm.LIVELINESS_CHECK = 1
            AND (k.HIGH_LEVEL_RESULT != 'Passed' OR k.HIGH_LEVEL_RESULT IS NULL)
    """)
    failed = cursor.fetchone()[0]

    print(f"\n  ✅ GBG PASSED (HIGH_LEVEL_RESULT='Passed'): {passed:,} records")
    print(f"  ❌ GBG FAILED/OTHER:                         {failed:,} records")
    print(f"  📊 TOTAL VALID FOR COMPARISON:               {passed + failed:,} records")
    print(f"\n  Previous approach (OCR_CHECK_MISMATCH=2):    3,687 records")
    print(f"  Correct approach (LIVELINESS_CHECK=1):       {passed + failed:,} records")
    print(f"  Improvement:                                 {passed + failed - 3687:,} more records!")

    print("\n" + "="*70)
    print("CONCLUSION:")
    print("="*70)
    print("""
  OLD (WRONG) FILTER:
    WHERE OCR_CHECK_MISMATCH = 2
    → Checks document text match (OCR vs IPRS)
    → Has NOTHING to do with face verification
    → Only 3,687 records

  NEW (CORRECT) FILTER:
    WHERE LIVELINESS_CHECK = 1
      AND HIGH_LEVEL_RESULT = 'Passed'  (for GBG-approved)
    → Checks actual face/liveness verification
    → Direct comparison with Maisha face matching
    → Many more valid records

  GBG VERDICT FIELD: k.HIGH_LEVEL_RESULT
    'Passed'  = GBG approved the face verification ✅
    anything else = GBG rejected ❌
    """)

    cursor.close()
    connection.close()

if __name__ == "__main__":
    validate_correct_filter()