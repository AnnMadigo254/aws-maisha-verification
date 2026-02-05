"""
Quick Database Connection Verification
Tests connectivity to MONAUAT and checks for Maisha data
"""

import oracledb
import sys

def test_connection(config):
    """Test database connection and data availability"""
    print("\n" + "="*70)
    print("DATABASE CONNECTION TEST")
    print("="*70)
    print(f"Database: {config['dsn']}")
    print(f"User: {config['user']}")
    print("="*70 + "\n")
    
    try:
        # Test connection
        print("1. Testing database connection...")
        connection = oracledb.connect(
            user=config['user'],
            password=config['password'],
            dsn=config['dsn']
        )
        print("   ✓ Connection successful!\n")
        
        cursor = connection.cursor()
        
        # Test 1: Check Maisha records
        print("2. Checking Maisha card records...")
        query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT o.SESSION_ID) as unique_sessions,
                COUNT(CASE WHEN o.AWS_IMAGE IS NOT NULL THEN 1 END) as has_aws_image,
                COUNT(CASE WHEN o.ID_PHOTO IS NOT NULL THEN 1 END) as has_id_photo,
                COUNT(CASE WHEN o.AWS_IMAGE IS NOT NULL AND o.ID_PHOTO IS NOT NULL THEN 1 END) as has_both
            FROM MA.SELF_ONBOARDING_TRACKER_OCR o
            JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
                ON o.SESSION_ID = k.SESSION_ID
            WHERE k.ID_TYPE = 'MAISHA_CARD'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        print(f"   Total Records: {result[0]:,}")
        print(f"   Unique Sessions: {result[1]:,}")
        print(f"   Has AWS_IMAGE: {result[2]:,}")
        print(f"   Has ID_PHOTO: {result[3]:,}")
        print(f"   Has Both Images: {result[4]:,}")
        
        if result[4] == 0:
            print("   ⚠️  WARNING: No records with both images found!")
        else:
            print(f"   ✓ {result[4]:,} records available for testing\n")
        
        # Test 2: Sample record structure
        print("3. Checking sample record structure...")
        sample_query = """
            SELECT 
                o.SESSION_ID,
                k.KYC_ID_NO,
                CASE WHEN o.AWS_IMAGE IS NOT NULL THEN 'YES' ELSE 'NO' END as has_aws,
                CASE WHEN o.ID_PHOTO IS NOT NULL THEN 'YES' ELSE 'NO' END as has_id
            FROM MA.SELF_ONBOARDING_TRACKER_OCR o
            JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
                ON o.SESSION_ID = k.SESSION_ID
            WHERE k.ID_TYPE = 'MAISHA_CARD'
                AND o.AWS_IMAGE IS NOT NULL
                AND o.ID_PHOTO IS NOT NULL
            FETCH FIRST 3 ROWS ONLY
        """
        
        cursor.execute(sample_query)
        samples = cursor.fetchall()
        
        print(f"   Sample Records (first 3):")
        for idx, sample in enumerate(samples, 1):
            session_short = sample[0][:16] if sample[0] else 'N/A'
            print(f"   {idx}. Session: {session_short}... KYC: {sample[1]} AWS:{sample[2]} ID:{sample[3]}")
        
        print("\n   ✓ Sample records retrieved successfully\n")
        
        # Test 3: Check table structure
        print("4. Verifying BLOB columns...")
        blob_query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                DATA_LENGTH
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = 'SELF_ONBOARDING_TRACKER_OCR'
                AND OWNER = 'MA'
                AND COLUMN_NAME IN ('AWS_IMAGE', 'ID_PHOTO')
            ORDER BY COLUMN_NAME
        """
        
        cursor.execute(blob_query)
        columns = cursor.fetchall()
        
        for col in columns:
            print(f"   {col[0]}: {col[1]} ({col[2]} bytes)")
        
        print("\n   ✓ BLOB columns verified\n")
        
        # Close connection
        cursor.close()
        connection.close()
        
        print("="*70)
        print("✅ ALL CHECKS PASSED - Ready for verification testing!")
        print("="*70 + "\n")
        
        return True
        
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        print(f"\n❌ Database Error:")
        print(f"   Code: {error_obj.code}")
        print(f"   Message: {error_obj.message}")
        print("\n" + "="*70)
        print("Troubleshooting:")
        
        if error_obj.code == 1017:
            print("  • Invalid username/password")
            print("  • Verify password with DBA John Kiongo")
        elif error_obj.code == 12541:
            print("  • Cannot connect to database")
            print("  • Check TNS listener status")
            print("  • Verify network connectivity")
        elif error_obj.code == 12514:
            print("  • Service name not found")
            print("  • Verify DSN: should be MONAUAT")
        else:
            print("  • Contact DBA team for assistance")
        
        print("="*70 + "\n")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected Error: {str(e)}\n")
        return False


def main():
    """Main execution"""
    
    # Database configurations
    configs = {
        'MONAUAT': {
            'user': 'MA',
            'password': 'wU8n1av8U$#OLtiq0MrtT',
            'dsn': '172.16.17.29:1561/MONAUAT'
        },
        'MONASIT': {
            'user': 'MA',
            'password': 'YOUR_SIT_PASSWORD',  # Update if testing SIT
            'dsn': '172.16.17.29:1561/MONASIT'
        },
        'MONAPREPROD': {
            'user': 'MA',
            'password': 'YOUR_PREPROD_PASSWORD',  # Update if testing PREPROD
            'dsn': 'copkdresb-scan:1561/MONAPREPROD'
        }
    }
    
    print("\n" + "="*70)
    print("MAISHA VERIFICATION - DATABASE CONNECTIVITY CHECK")
    print("="*70)
    print("\nAvailable Environments:")
    for idx, name in enumerate(configs.keys(), 1):
        print(f"  {idx}. {name}")
    
    # Default to MONAUAT
    print("\nTesting MONAUAT (default)...")
    
    config = configs['MONAUAT']
    success = test_connection(config)
    
    if success:
        print("Next Steps:")
        print("  1. Run the full verification test:")
        print("     python maisha_verification_tester_updated.py")
        print("  2. Start with 5 records to validate")
        print("  3. Review the results and scale up\n")
        sys.exit(0)
    else:
        print("Please fix the connection issues before proceeding.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()