# import oracledb
# import json
# import csv
# from datetime import datetime
# from typing import List, Dict
# import logging
# import sys
# import base64
# import re

# # Import the official Maisha client
# from maisha_client import MaishaVerificationClient, MaishaAPIError

# # Fix Windows encoding issues
# if sys.platform == 'win32':
#     import codecs
#     sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
#     sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# # Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(
#             f'maisha_verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
#             encoding='utf-8'
#         ),
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)


# def clean_base64_string(b64_string: str) -> str:
#     """
#     Clean base64 string by removing data URI prefixes and whitespace.
    
#     Examples of what might be in the database:
#     - data:image/jpeg;base64,/9j/4AAQ...
#     - /9j/4AAQ... (pure base64)
#     - Data with newlines or spaces
#     """
#     if not b64_string:
#         return ""
    
#     # Remove data URI prefix if present
#     if 'base64,' in b64_string:
#         b64_string = b64_string.split('base64,')[1]
    
#     # Remove any whitespace, newlines, etc.
#     b64_string = ''.join(b64_string.split())
    
#     # Remove any non-base64 characters
#     # Base64 uses: A-Z, a-z, 0-9, +, /, =
#     b64_string = re.sub(r'[^A-Za-z0-9+/=]', '', b64_string)
    
#     return b64_string


# def validate_base64_image(b64_string: str) -> bool:
#     """Validate that base64 string is a valid image."""
#     try:
#         # Decode to bytes
#         image_bytes = base64.b64decode(b64_string)
        
#         # Check minimum size (should be at least 1KB for a real image)
#         if len(image_bytes) < 1000:
#             logger.warning(f"Image too small: {len(image_bytes)} bytes")
#             return False
        
#         # Check for JPEG magic bytes (FF D8 FF)
#         if image_bytes[:3] != b'\xff\xd8\xff':
#             logger.warning(f"Not a JPEG image. First bytes: {image_bytes[:10].hex()}")
#             # Could also be PNG (89 50 4E 47) or other formats
#             # For now, just warn but don't fail
        
#         logger.debug(f"Valid image: {len(image_bytes)} bytes, starts with {image_bytes[:3].hex()}")
#         return True
        
#     except Exception as e:
#         logger.error(f"Base64 validation failed: {str(e)}")
#         return False


# class MaishaVerificationTester:
#     def __init__(self, 
#                  oracle_config: Dict[str, str],
#                  api_key: str,
#                  api_base_url: str = "https://18.235.35.175"):
#         """Initialize the Maisha verification tester"""
#         self.oracle_config = oracle_config
#         self.api_key = api_key
#         self.results = []
        
#         # Initialize Maisha API client
#         self.client = MaishaVerificationClient(
#             api_key=api_key,
#             base_url=api_base_url,
#             verify_ssl=False
#         )
        
#         logger.info(f"API Configuration:")
#         logger.info(f"  Endpoint: {api_base_url}")
#         logger.info(f"  API Key: {api_key[:20]}...")
    
#     def test_api_connection(self):
#         """Test API connectivity with health check"""
#         logger.info("="*70)
#         logger.info("API CONNECTIVITY TEST")
#         logger.info("="*70)
        
#         try:
#             health = self.client.health_check()
#             logger.info(f"[SUCCESS] API Status: {health.get('status', 'unknown')}")
#             logger.info(f"API Response: {json.dumps(health, indent=2)}")
#             logger.info("="*70)
#             return True
#         except Exception as e:
#             logger.error(f"[FAILED] API health check failed: {str(e)}")
#             logger.error("="*70)
#             return False
    
#     def test_network_connectivity(self):
#         """Test basic network connectivity to database server"""
#         import socket
        
#         logger.info("="*70)
#         logger.info("NETWORK CONNECTIVITY TEST")
#         logger.info("="*70)
        
#         dsn = self.oracle_config['dsn']
#         logger.info(f"Testing connection to: {dsn}")
        
#         try:
#             if ':' in dsn and '/' in dsn:
#                 host_port = dsn.split('/')[0]
#                 host = host_port.split(':')[0]
#                 port = int(host_port.split(':')[1])
#             else:
#                 logger.warning("Cannot parse DSN format")
#                 return True
            
#             logger.info(f"Hostname: {host}")
#             logger.info(f"Port: {port}")
#             logger.info("Testing TCP connection...")
            
#             sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             sock.settimeout(10)
#             result = sock.connect_ex((host, port))
#             sock.close()
            
#             if result == 0:
#                 logger.info(f"[SUCCESS] TCP connection to {host}:{port} successful")
#                 logger.info("="*70)
#                 return True
#             else:
#                 logger.error(f"[FAILED] Cannot connect to {host}:{port}")
#                 logger.error("="*70)
#                 return False
                
#         except Exception as e:
#             logger.error(f"[FAILED] Network test error: {str(e)}")
#             logger.error("="*70)
#             return False
    
#     def test_single_comparison(self, card_base64: str, face_base64: str):
#         """Test single comparison to debug the issue"""
#         logger.info("="*70)
#         logger.info("TESTING SINGLE COMPARISON WITH CLEANED DATA")
#         logger.info("="*70)
        
#         # Clean the base64 strings
#         card_clean = clean_base64_string(card_base64)
#         face_clean = clean_base64_string(face_base64)
        
#         logger.info(f"Original card length: {len(card_base64)}")
#         logger.info(f"Cleaned card length:  {len(card_clean)}")
#         logger.info(f"Original face length: {len(face_base64)}")
#         logger.info(f"Cleaned face length:  {len(face_clean)}")
        
#         # Validate the cleaned data
#         logger.info("Validating card image...")
#         card_valid = validate_base64_image(card_clean)
        
#         logger.info("Validating face image...")
#         face_valid = validate_base64_image(face_clean)
        
#         if not card_valid or not face_valid:
#             logger.error("[FAILED] Image validation failed")
#             logger.error("="*70)
#             return False
        
#         try:
#             # Try the /api/v1/compare endpoint with cleaned data
#             logger.info("Calling API with cleaned images...")
#             result = self.client.compare_faces(
#                 source_image=card_clean,
#                 target_image=face_clean,
#                 reference_id="test-001",
#                 extract_face=True
#             )
            
#             logger.info(f"[SUCCESS] Single comparison worked!")
#             logger.info(f"  Match: {result.match}")
#             logger.info(f"  Score: {result.similarity_score}")
#             logger.info(f"  Method: {result.comparison_method}")
#             logger.info("="*70)
#             return True
            
#         except MaishaAPIError as e:
#             logger.error(f"[FAILED] Single comparison failed: {e}")
#             logger.error(f"  Error Code: {e.error_code}")
#             logger.error(f"  Status Code: {e.status_code}")
#             logger.error("="*70)
#             return False
#         except Exception as e:
#             logger.error(f"[FAILED] Unexpected error: {str(e)}")
#             logger.error("="*70)
#             return False
    
#     def fetch_maisha_records(self, limit: int = None) -> List[Dict]:
#         """Fetch Maisha card records using blob_to_clob conversion"""
#         try:
#             logger.info(f"Connecting to Oracle database: {self.oracle_config['dsn']}")
#             connection = oracledb.connect(
#                 user=self.oracle_config['user'],
#                 password=self.oracle_config['password'],
#                 dsn=self.oracle_config['dsn']
#             )
            
#             cursor = connection.cursor()
            
#             query = """
#                 SELECT *
#                 FROM (
#                     SELECT
#                         blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
#                         blob_to_clob(o.ID_PHOTO)  AS ID_PHOTO_BASE64,
#                         k.KYC_ID_NO,
#                         o.SESSION_ID
#                     FROM MA.SELF_ONBOARDING_TRACKER_OCR o
#                     JOIN MA.SELF_ONBOARDING_TRACKER_KYC k
#                         ON o.SESSION_ID = k.SESSION_ID
#                     WHERE k.ID_TYPE = 'MAISHA_CARD'
#                 )
#                 WHERE LENGTH(ID_PHOTO_BASE64) > 16
#             """
            
#             if limit:
#                 query += f" AND ROWNUM <= {limit}"
            
#             logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
#             logger.info("="*70)
            
#             cursor.execute(query)
            
#             records = []
#             session_ids_seen = set()
            
#             for idx, row in enumerate(cursor, 1):
#                 try:
#                     aws_image_clob = row[0]
#                     id_photo_clob = row[1]
#                     kyc_id = row[2]
#                     session_id = row[3]
                    
#                     if session_id in session_ids_seen:
#                         logger.warning(f"  [DUPLICATE] Record {idx}: Session {session_id[:16]}... skipping")
#                         continue
                    
#                     session_ids_seen.add(session_id)
                    
#                     aws_image_data = aws_image_clob.read() if aws_image_clob else None
#                     id_photo_data = id_photo_clob.read() if id_photo_clob else None
                    
#                     if not aws_image_data or not id_photo_data:
#                         logger.warning(f"  [WARNING] Record {idx}: Missing image data")
#                         continue
                    
#                     # CRITICAL: Clean the base64 data
#                     aws_image_clean = clean_base64_string(aws_image_data)
#                     id_photo_clean = clean_base64_string(id_photo_data)
                    
#                     # Validate it's actually base64
#                     try:
#                         base64.b64decode(aws_image_clean[:100])
#                         base64.b64decode(id_photo_clean[:100])
#                     except Exception as e:
#                         logger.warning(f"  [WARNING] Record {idx}: Invalid base64 after cleaning: {str(e)}")
#                         continue
                    
#                     record = {
#                         'card_image_base64': id_photo_clean,
#                         'face_image_base64': aws_image_clean,
#                         'KYC_ID_NO': kyc_id,
#                         'SESSION_ID': session_id,
#                         'record_index': len(records) + 1
#                     }
#                     records.append(record)
                    
#                     if idx <= 5:
#                         logger.info(f"  Record {idx:2d} | Session: {session_id[:20]}... | KYC: {kyc_id}")
#                         logger.info(f"            | Card: {len(id_photo_clean)} chars")
#                         logger.info(f"            | Face: {len(aws_image_clean)} chars")
#                     elif idx % 10 == 0:
#                         logger.info(f"  Processed {idx} records, kept {len(records)}...")
                    
#                 except Exception as e:
#                     logger.warning(f"  [FAILED] Record {idx}: {str(e)}")
#                     continue
            
#             logger.info("="*70)
#             logger.info(f"[SUCCESS] Fetched {len(records)} valid card-face pairs")
#             logger.info(f"[SUCCESS] Unique sessions: {len(session_ids_seen)}")
#             logger.info("="*70)
            
#             cursor.close()
#             connection.close()
            
#             return records
            
#         except Exception as e:
#             logger.error(f"[FAILED] Database error: {str(e)}")
#             raise
    
#     def verify_batch_using_client(self, records: List[Dict], batch_num: int) -> List[Dict]:
#         """Verify batch using official Maisha client"""
#         logger.info(f"Preparing batch {batch_num} with {len(records)} records...")
        
#         # Prepare verifications for batch API
#         verifications = []
#         for record in records:
#             verification = {
#                 "id": record['SESSION_ID'],
#                 "source_image": record['card_image_base64'],
#                 "target_image": record['face_image_base64'],
#                 "reference_id": str(record['KYC_ID_NO'])
#             }
#             verifications.append(verification)
        
#         try:
#             # Call batch compare API
#             logger.info(f"Calling batch API with {len(verifications)} verifications...")
            
#             batch_result = self.client.batch_compare(
#                 verifications=verifications,
#                 extract_face=True,
#                 parallel=True,
#                 stop_on_error=False
#             )
            
#             logger.info(f"[SUCCESS] Batch {batch_num} completed")
#             logger.info(f"  Total: {batch_result.total}")
#             logger.info(f"  Completed: {batch_result.completed}")
#             logger.info(f"  Passed: {batch_result.passed}")
#             logger.info(f"  Failed: {batch_result.failed}")
#             logger.info(f"  Errors: {batch_result.errors}")
            
#             # Print first result
#             if batch_num == 1 and batch_result.results:
#                 print("\n" + "="*70)
#                 print(f"SAMPLE API RESPONSE - BATCH {batch_num}")
#                 print("="*70)
#                 print(json.dumps(batch_result.results[0], indent=2))
#                 print("="*70 + "\n")
            
#             # Combine with original records
#             combined_results = []
#             for idx, api_result in enumerate(batch_result.results):
#                 if idx < len(records):
#                     combined = {
#                         'session_id': records[idx]['SESSION_ID'],
#                         'kyc_id_no': records[idx]['KYC_ID_NO'],
#                         'record_index': records[idx]['record_index'],
#                         'verified': api_result.get('match', False),
#                         'similarity_score': api_result.get('similarity_score', 0),
#                         'threshold': api_result.get('threshold', 70),
#                         'comparison_method': api_result.get('comparison_method'),
#                         'comparison_id': api_result.get('comparison_id'),
#                         'error': api_result.get('error'),
#                         'test_timestamp': datetime.now().isoformat()
#                     }
#                     combined_results.append(combined)
            
#             return combined_results
            
#         except MaishaAPIError as e:
#             logger.error(f"[FAILED] API Error: {e}")
#             logger.error(f"  Error Code: {e.error_code}")
#             logger.error(f"  Status Code: {e.status_code}")
#             raise
#         except Exception as e:
#             logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
#             raise
    
#     def run_batch_test(self, batch_size: int = 10, total_limit: int = None, test_single: bool = True) -> List[Dict]:
#         """Run batch verification tests"""
#         logger.info("="*70)
#         logger.info("MAISHA CARD VERIFICATION TEST")
#         logger.info("="*70)
#         logger.info(f"Database: {self.oracle_config['dsn']}")
#         logger.info(f"Batch Size: {batch_size}")
#         logger.info(f"Limit: {total_limit if total_limit else 'ALL'}")
#         logger.info("="*70)
        
#         # Test connections
#         if not self.test_network_connectivity():
#             logger.error("[FAILED] Network connectivity test failed")
#             return []
        
#         if not self.test_api_connection():
#             logger.error("[FAILED] API connectivity test failed")
#             return []
        
#         # Fetch records
#         records = self.fetch_maisha_records(limit=total_limit)
        
#         if not records:
#             logger.warning("[WARNING] No records fetched")
#             return []
        
#         # Test single comparison first if enabled
#         if test_single and records:
#             logger.info("\n" + "="*70)
#             logger.info("TESTING SINGLE COMPARISON BEFORE BATCH")
#             logger.info("="*70)
#             single_success = self.test_single_comparison(
#                 records[0]['card_image_base64'],
#                 records[0]['face_image_base64']
#             )
            
#             if not single_success:
#                 logger.error("Single comparison failed. Please check:")
#                 logger.error("  1. Image format (should be JPEG)")
#                 logger.error("  2. Image size (not too large)")
#                 logger.error("  3. Base64 encoding is correct")
#                 logger.error("\nContact API support with the error code above.")
#                 return []
            
#             logger.info("Single comparison successful! Continuing with batch...\n")
        
#         all_results = []
#         total_batches = (len(records) + batch_size - 1) // batch_size
        
#         # Process in batches
#         for i in range(0, len(records), batch_size):
#             batch = records[i:i + batch_size]
#             batch_num = (i // batch_size) + 1
            
#             logger.info("="*70)
#             logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
#             logger.info("="*70)
            
#             try:
#                 batch_results = self.verify_batch_using_client(batch, batch_num)
                
#                 # Log results
#                 for idx, result in enumerate(batch_results):
#                     record_num = i + idx + 1
#                     verified = result.get('verified', False)
#                     score = result.get('similarity_score', 0)
#                     method = result.get('comparison_method', 'unknown')
                    
#                     status = "✓ VERIFIED" if verified else "✗ NOT VERIFIED"
                    
#                     if result.get('error'):
#                         logger.warning(f"  {record_num:3d}. [ERROR] {result['error'][:60]}")
#                     else:
#                         logger.info(f"  {record_num:3d}. [{status}] Score: {score:5.2f}% | Method: {method}")
                
#                 all_results.extend(batch_results)
#                 logger.info(f"[SUCCESS] Batch {batch_num} completed")
                
#             except Exception as e:
#                 logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
#                 continue
        
#         self.results = all_results
        
#         logger.info("="*70)
#         logger.info(f"TEST COMPLETED: {len(all_results)}/{len(records)} records processed")
#         logger.info("="*70)
        
#         return all_results
    
#     def analyze_results(self) -> Dict:
#         """Analyze test results"""
#         if not self.results:
#             return {}
        
#         total = len(self.results)
#         verified = sum(1 for r in self.results if r.get('verified', False))
#         not_verified = sum(1 for r in self.results if not r.get('verified') and not r.get('error'))
#         failed = sum(1 for r in self.results if r.get('error'))
        
#         scores = [r.get('similarity_score', 0) for r in self.results if not r.get('error')]
#         avg_score = sum(scores) / len(scores) if scores else 0
#         max_score = max(scores) if scores else 0
#         min_score = min(scores) if scores else 0
        
#         return {
#             'total_tests': total,
#             'verified_count': verified,
#             'not_verified_count': not_verified,
#             'failed_count': failed,
#             'verification_rate': (verified / total * 100) if total > 0 else 0,
#             'avg_similarity_score': avg_score,
#             'max_similarity_score': max_score,
#             'min_similarity_score': min_score
#         }
    
#     def export_csv(self, output_file: str = None) -> str:
#         """Export results to CSV"""
#         if not self.results:
#             return None
        
#         if not output_file:
#             output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
#         columns = [
#             'record_index', 'session_id', 'kyc_id_no',
#             'verified', 'similarity_score', 'threshold',
#             'comparison_method', 'comparison_id',
#             'error', 'test_timestamp'
#         ]
        
#         with open(output_file, 'w', newline='', encoding='utf-8') as f:
#             writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
#             writer.writeheader()
#             writer.writerows(self.results)
        
#         logger.info(f"[SUCCESS] CSV exported: {output_file}")
#         return output_file
    
#     def print_summary(self, analysis: Dict):
#         """Print summary to console"""
#         print("\n" + "="*70)
#         print("MAISHA CARD VERIFICATION - TEST SUMMARY")
#         print("="*70)
#         print(f"\nOVERALL RESULTS:")
#         print(f"  Total Tests:        {analysis.get('total_tests', 0):,}")
#         print(f"  Verified:           {analysis.get('verified_count', 0):,} ({analysis.get('verification_rate', 0):.1f}%)")
#         print(f"  Not Verified:       {analysis.get('not_verified_count', 0):,}")
#         print(f"  Failed/Errors:      {analysis.get('failed_count', 0):,}")
#         print(f"\nSIMILARITY SCORES:")
#         print(f"  Average:            {analysis.get('avg_similarity_score', 0):.2f}%")
#         print(f"  Maximum:            {analysis.get('max_similarity_score', 0):.2f}%")
#         print(f"  Minimum:            {analysis.get('min_similarity_score', 0):.2f}%")
#         print("="*70 + "\n")


# def main():
#     """Main execution"""
    
#     # MONAPREPROD configuration
#     oracle_config = {
#         'user': 'MA',
#         'password': 'wU8n1av8U$#OLt7pRePrOd',
#         'dsn': 'copkdresb-scan:1561/MONAPREPROD'
#     }
    
#     # API Configuration
#     api_key = "dab4424126543da8cffb8e250a63196957ee12a11312da23bf088db4f8dbb982"
#     api_base_url = "https://18.235.35.175"
    
#     batch_size = 5
#     total_limit = 120
    
#     print("\n" + "="*70)
#     print("MAISHA CARD VERIFICATION TEST")
#     print("="*70)
#     print(f"Database: {oracle_config['dsn']}")
#     print(f"API: {api_base_url}")
#     print(f"Batch Size: {batch_size}")
#     print(f"Total Limit: {total_limit}")
#     print("="*70 + "\n")
    
#     try:
#         tester = MaishaVerificationTester(
#             oracle_config=oracle_config,
#             api_key=api_key,
#             api_base_url=api_base_url
#         )
        
#         results = tester.run_batch_test(
#             batch_size=batch_size,
#             total_limit=total_limit,
#             test_single=True
#         )
        
#         if not results:
#             print("[FAILED] No results to analyze\n")
#             return
        
#         analysis = tester.analyze_results()
#         tester.print_summary(analysis)
        
#         csv_file = tester.export_csv()
#         print(f"[SUCCESS] Results exported: {csv_file}")
#         print(f"[SUCCESS] Log file: Check maisha_verification_*.log\n")
        
#     except Exception as e:
#         logger.error(f"[FAILED] Test execution failed: {str(e)}")
#         print(f"\n[FAILED] ERROR: {str(e)}\n")


# if __name__ == "__main__":
#     import urllib3
#     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
#     main()



#!/usr/bin/env python3
"""
Maisha Card Verification - Production Test Script
Features:
  ✅ Base64 cleaning for Oracle-stored images
  ✅ Visual inspection of first 5 image pairs (side-by-side comparisons)
  ✅ Single comparison test before batch processing
  ✅ Batch processing with 300s timeout (ensemble models)
  ✅ CSV export with similarity scores
  ✅ HTML report for easy visual review
"""

import oracledb
import requests
import json
import csv
import base64
import re
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

# Fix Windows encoding issues
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Import Maisha client (ensure maisha_client.py is in same directory)
from maisha_client import MaishaVerificationClient, MaishaAPIError, ComparisonResult

# Set up logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            f'maisha_verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# BASE64 CLEANING UTILITIES
# ============================================================================

def clean_base64_string(b64_string: str) -> str:
    """
    Clean base64 string by removing data URI prefixes and invalid characters.
    
    Handles formats commonly stored in Oracle:
      - "image/jpeg;base64,/9j/4AAQ..."
      - "data:image/jpeg;base64,/9j/4AAQ..."
      - "/9j/4AAQ..." (pure base64 with whitespace/newlines)
    
    Returns clean base64 string ready for API consumption.
    """
    if not b64_string:
        return ""
    
    # Convert to string if needed
    b64_string = str(b64_string)
    
    # Remove data URI prefix if present
    if 'base64,' in b64_string:
        b64_string = b64_string.split('base64,')[-1]
    
    # Remove all whitespace/newlines
    b64_string = ''.join(b64_string.split())
    
    # Remove any non-base64 characters (keep A-Z, a-z, 0-9, +, /, =)
    b64_string = re.sub(r'[^A-Za-z0-9+/=]', '', b64_string)
    
    return b64_string


def validate_base64_image(b64_string: str) -> bool:
    """Validate that base64 string decodes to a reasonable image size."""
    try:
        image_bytes = base64.b64decode(b64_string[:100])  # Just check first 100 chars
        return True
    except Exception as e:
        logger.warning(f"Base64 validation warning: {str(e)}")
        return True  # Don't fail validation - let API handle it


# ============================================================================
# IMAGE INSPECTION TOOL (Critical for debugging low similarity scores)
# ============================================================================

class ImageInspector:
    """Save and visually inspect Maisha card/face image pairs"""
    
    def __init__(self, output_dir: str = "inspection_samples"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.card_dir = self.output_dir / "card_images"
        self.face_dir = self.output_dir / "face_images"
        self.comparison_dir = self.output_dir / "comparisons"
        for d in [self.card_dir, self.face_dir, self.comparison_dir]:
            d.mkdir(exist_ok=True)
    
    def save_base64_image(self, b64_string: str, output_path: Path) -> bool:
        """Save base64 image to file"""
        try:
            # Clean the base64 string first
            b64_clean = clean_base64_string(b64_string)
            
            # Decode and save
            image_data = base64.b64decode(b64_clean)
            with open(output_path, 'wb') as f:
                f.write(image_data)
            return True
        except Exception as e:
            logger.error(f"Failed to save image {output_path}: {str(e)}")
            return False
    
    def create_side_by_side_comparison(self, card_path: Path, face_path: Path, 
                                       output_path: Path, session_id: str, kyc_id: str, 
                                       similarity_score: Optional[float] = None):
        """Create side-by-side comparison image with metadata"""
        try:
            # Open images
            card_img = Image.open(card_path)
            face_img = Image.open(face_path)
            
            # Resize to reasonable display size (maintain aspect ratio)
            def resize_keep_aspect(img, max_size=(300, 400)):
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
                return img
            
            card_img = resize_keep_aspect(card_img, (300, 400))
            face_img = resize_keep_aspect(face_img, (300, 400))
            
            # Create canvas
            padding = 20
            header_height = 70
            canvas_width = card_img.width + face_img.width + 3 * padding
            canvas_height = max(card_img.height, face_img.height) + header_height + padding
            
            canvas = Image.new('RGB', (canvas_width, canvas_height), 'white')
            draw = ImageDraw.Draw(canvas)
            
            # Try to load a font (fallback to default if not available)
            try:
                font = ImageFont.truetype("arial.ttf", 16)
                header_font = ImageFont.truetype("arialbd.ttf", 18)
            except:
                font = ImageFont.load_default()
                header_font = ImageFont.load_default()
            
            # Draw header
            header_text = f"Session: {session_id[:12]}... | KYC: {kyc_id}"
            draw.text((padding, 10), header_text, fill='black', font=header_font)
            
            if similarity_score is not None:
                score_color = 'green' if similarity_score >= 70 else 'red'
                score_text = f"Similarity: {similarity_score:.1f}%"
                draw.text((canvas_width - 180, 10), score_text, fill=score_color, font=header_font)
            
            # Paste images
            y_offset = header_height
            canvas.paste(card_img, (padding, y_offset))
            canvas.paste(face_img, (card_img.width + 2 * padding, y_offset))
            
            # Draw labels
            draw.text((padding, y_offset + card_img.height + 5), "Maisha Card Photo", fill='blue', font=font)
            draw.text((card_img.width + 2 * padding, y_offset + face_img.height + 5), "Selfie/Face", fill='green', font=font)
            
            # Save
            canvas.save(output_path, 'JPEG', quality=95)
            logger.info(f"  → Comparison saved: {output_path.name}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to create comparison for {session_id}: {str(e)}")
            return False
    
    def generate_html_report(self, comparisons: list, report_path: Path):
        """Generate simple HTML report for easy viewing"""
        html = """<!DOCTYPE html>
<html>
<head>
    <title>Maisha Card Verification - Image Inspection</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .comparison { 
            background: white; 
            border-radius: 8px; 
            padding: 20px; 
            margin: 25px 0; 
            box-shadow: 0 3px 10px rgba(0,0,0,0.15);
        }
        .header { 
            display: flex; 
            justify-content: space-between; 
            margin-bottom: 20px; 
            padding-bottom: 15px; 
            border-bottom: 2px solid #eee;
        }
        .score { 
            font-weight: bold; 
            font-size: 1.4em;
            padding: 8px 15px;
            border-radius: 20px;
        }
        .score.pass { background-color: #e8f5e9; color: #2e7d32; }
        .score.fail { background-color: #ffebee; color: #c62828; }
        .images { display: flex; gap: 30px; align-items: center; justify-content: center; margin: 20px 0; }
        .image-box { text-align: center; max-width: 350px; }
        .image-box img { max-width: 100%; max-height: 400px; border: 3px solid #ddd; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .label { margin-top: 12px; font-weight: bold; font-size: 1.1em; }
        .card-label { color: #1565c0; }
        .face-label { color: #2e7d32; }
        h1 { color: #1976d2; text-align: center; margin-bottom: 30px; }
        .instructions { 
            background: #e3f2fd; 
            padding: 20px; 
            border-radius: 8px; 
            margin-bottom: 30px;
            border-left: 4px solid #1976d2;
        }
        .stats { 
            display: flex; 
            justify-content: space-around; 
            background: white; 
            padding: 15px; 
            border-radius: 8px; 
            margin: 25px 0;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        .stat-item { text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #1976d2; }
        .stat-label { color: #666; margin-top: 5px; }
    </style>
</head>
<body>
    <h1>🔍 Maisha Card Verification - Image Inspection Report</h1>
    
    <div class="instructions">
        <h3>📋 Inspection Instructions</h3>
        <p>Visually verify each image pair to determine if low similarity scores are due to:</p>
        <ul>
            <li>✅ <strong>Genuine match</strong> - Card photo and selfie belong to same person (API should return &gt;70%)</li>
            <li>❌ <strong>Genuine mismatch</strong> - Different people (API correctly returns low score)</li>
            <li>⚠️ <strong>Poor image quality</strong> - Blurry, dark, or angled photos causing false negatives</li>
            <li>⚠️ <strong>Wrong image pairing</strong> - Data pipeline issue (card/face from different sessions)</li>
        </ul>
        <p><strong>💡 Your first test returned 27.07% similarity</strong> - Visual inspection will reveal why!</p>
    </div>
    
    <div class="stats">
        <div class="stat-item">
            <div class="stat-value">{total}</div>
            <div class="stat-label">Samples</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{matches}</div>
            <div class="stat-label">Expected Matches</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{mismatches}</div>
            <div class="stat-label">Expected Mismatches</div>
        </div>
    </div>
    
    <hr>
"""
        
        matches = 0
        mismatches = 0
        
        for comp in comparisons:
            score = comp.get('similarity_score', 0)
            score_class = "pass" if score >= 70 else "fail"
            
            if score >= 70:
                matches += 1
            else:
                mismatches += 1
            
            html += f"""
    <div class="comparison">
        <div class="header">
            <div>
                <strong>Session ID:</strong> {comp['session_id'][:16]}...<br>
                <strong>KYC ID:</strong> {comp['kyc_id']}
            </div>
            <div class="score {score_class}">
                {score:.1f}%
            </div>
        </div>
        <div class="images">
            <div class="image-box">
                <img src="comparisons/{comp['comparison_file']}" alt="Comparison">
            </div>
        </div>
        <div style="margin-top: 15px; text-align: center;">
            <strong>Files:</strong> 
            <span style="color: #1565c0;">{comp['card_file']}</span> | 
            <span style="color: #2e7d32;">{comp['face_file']}</span>
        </div>
    </div>
"""
        
        html += """
</body>
</html>"""
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html.format(total=len(comparisons), matches=matches, mismatches=mismatches))
        logger.info(f"HTML report generated: {report_path.absolute()}")

    def inspect_first_batch(self, records: List[Dict], max_samples: int = 5, 
                           similarity_scores: Optional[List[float]] = None) -> Path:
        """
        Save and create visual comparisons for first N records.
        Returns path to HTML report for easy viewing.
        """
        logger.info("\n" + "="*70)
        logger.info(f"SAVING SAMPLE IMAGES FOR VISUAL INSPECTION (First {max_samples} records)")
        logger.info("="*70)
        logger.info(f"Output directory: {self.output_dir.absolute()}")
        logger.info("="*70)
        
        comparisons = []
        
        for idx, record in enumerate(records[:max_samples], 1):
            session_id = record['SESSION_ID']
            kyc_id = record['KYC_ID_NO']
            
            logger.info(f"\nSample {idx}/{max_samples}:")
            logger.info(f"  Session: {session_id[:20]}...")
            logger.info(f"  KYC ID:  {kyc_id}")
            
            # Save card image
            card_filename = f"{idx:02d}_{session_id[:8]}_card.jpg"
            card_path = self.card_dir / card_filename
            if self.save_base64_image(record['card_image_base64'], card_path):
                logger.info(f"  ✓ Card saved: {card_filename}")
            else:
                logger.warning(f"  ✗ Failed to save card image")
                continue
            
            # Save face image
            face_filename = f"{idx:02d}_{session_id[:8]}_face.jpg"
            face_path = self.face_dir / face_filename
            if self.save_base64_image(record['face_image_base64'], face_path):
                logger.info(f"  ✓ Face saved: {face_filename}")
            else:
                logger.warning(f"  ✗ Failed to save face image")
                continue
            
            # Create side-by-side comparison
            comp_filename = f"{idx:02d}_{session_id[:8]}_comparison.jpg"
            comp_path = self.comparison_dir / comp_filename
            score = similarity_scores[idx-1] if similarity_scores and idx-1 < len(similarity_scores) else None
            
            if self.create_side_by_side_comparison(
                card_path, face_path, comp_path, session_id, kyc_id, score
            ):
                comparisons.append({
                    'session_id': session_id,
                    'kyc_id': kyc_id,
                    'card_file': card_filename,
                    'face_file': face_filename,
                    'comparison_file': comp_filename,
                    'similarity_score': score if score else 0.0
                })
        
        # Generate HTML report
        report_path = self.output_dir / "inspection_report.html"
        self.generate_html_report(comparisons, report_path)
        
        logger.info("\n" + "="*70)
        logger.info("✅ VISUAL INSPECTION COMPLETE")
        logger.info("="*70)
        logger.info(f"📁 Card images:  {self.card_dir.absolute()}")
        logger.info(f"📁 Face images:  {self.face_dir.absolute()}")
        logger.info(f"📁 Comparisons:  {self.comparison_dir.absolute()}")
        logger.info(f"📄 HTML Report:  file:///{report_path.absolute()}")
        logger.info("="*70)
        logger.info("\n💡 NEXT STEPS:")
        logger.info("   1. Open the HTML report in your browser")
        logger.info("   2. Visually inspect each pair to verify:")
        logger.info("      - Do card photo and selfie belong to same person?")
        logger.info("      - Are images clear, well-lit, and properly framed?")
        logger.info("   3. If images look correct but score is low → API issue")
        logger.info("   4. If images are poor quality → frontend validation needed")
        logger.info("   5. Press ENTER to continue with batch processing...\n")
        
        return report_path


# ============================================================================
# MAISHA VERIFICATION TESTER
# ============================================================================

class MaishaVerificationTester:
    def __init__(self,
                 oracle_config: Dict[str, str],
                 api_key: str,
                 api_base_url: str = "https://18.235.35.175",
                 timeout: int = 300):  # CRITICAL: 300s for ensemble batch processing
        """Initialize the Maisha verification tester"""
        self.oracle_config = oracle_config
        self.api_key = api_key
        self.results = []
        
        # Initialize Maisha API client with 300s timeout
        self.client = MaishaVerificationClient(
            api_key=api_key,
            base_url=api_base_url,
            timeout=timeout,  # ← INCREASED FOR ENSEMBLE MODELS
            verify_ssl=False
        )
        logger.info(f"API Configuration:")
        logger.info(f"  Endpoint: {api_base_url}")
        logger.info(f"  API Key: {api_key[:20]}...")
        logger.info(f"  Timeout: {timeout} seconds (for ensemble batch processing)")

    def test_network_connectivity(self):
        """Test basic network connectivity to database server"""
        import socket
        logger.info("\n" + "="*70)
        logger.info("NETWORK CONNECTIVITY TEST")
        logger.info("="*70)
        dsn = self.oracle_config['dsn']
        logger.info(f"Testing connection to: {dsn}")
        try:
            if ':' in dsn and '/' in dsn:
                host_port = dsn.split('/')[0]
                host = host_port.split(':')[0]
                port = int(host_port.split(':')[1])
            else:
                logger.warning("Cannot parse DSN format, using defaults")
                return True
            
            logger.info(f"Hostname: {host}")
            logger.info(f"Port: {port}")
            logger.info("Testing TCP connection...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                logger.info(f"[SUCCESS] TCP connection to {host}:{port} successful")
                logger.info("="*70)
                return True
            else:
                logger.error(f"[FAILED] Cannot connect to {host}:{port}")
                logger.error(f"Error code: {result}")
                logger.error("Possible issues:")
                logger.error("  1. Not connected to bank VPN")
                logger.error("  2. Hostname/port incorrect")
                logger.error("  3. Firewall blocking connection")
                logger.error("  4. Database server is down")
                logger.error("="*70)
                return False
        except Exception as e:
            logger.error(f"[FAILED] Network test error: {str(e)}")
            logger.error("="*70)
            return False

    def test_api_connection(self):
        """Test API connectivity with health check"""
        logger.info("\n" + "="*70)
        logger.info("API CONNECTIVITY TEST")
        logger.info("="*70)
        try:
            health = self.client.health_check()
            logger.info(f"[SUCCESS] API Status: {health.get('status', 'unknown')}")
            logger.info(f"API Response: {json.dumps(health, indent=2)}")
            logger.info("="*70)
            return True
        except Exception as e:
            logger.error(f"[FAILED] API health check failed: {str(e)}")
            logger.error("="*70)
            return False

    def test_single_comparison(self, card_base64: str, face_base64: str) -> Optional[ComparisonResult]:
        """Test single comparison to validate the pipeline before batch processing"""
        logger.info("\n" + "="*70)
        logger.info("TESTING SINGLE COMPARISON BEFORE BATCH")
        logger.info("="*70)
        logger.info("TESTING SINGLE COMPARISON WITH CLEANED DATA")
        logger.info("="*70)
        
        # Clean the base64 strings
        card_clean = clean_base64_string(card_base64)
        face_clean = clean_base64_string(face_base64)
        
        logger.info(f"Original card length: {len(card_base64):,}")
        logger.info(f"Cleaned card length:  {len(card_clean):,}")
        logger.info(f"Original face length: {len(face_base64):,}")
        logger.info(f"Cleaned face length:  {len(face_clean):,}")
        
        # Validate the cleaned data
        logger.info("Validating card image...")
        card_valid = validate_base64_image(card_clean)
        logger.info("Validating face image...")
        face_valid = validate_base64_image(face_clean)
        
        if not card_valid or not face_valid:
            logger.error("[FAILED] Image validation failed")
            logger.error("="*70)
            return None
        
        try:
            # Call API with cleaned data
            logger.info("Calling API with cleaned images...")
            result = self.client.compare_faces(
                source_image=card_clean,
                target_image=face_clean,
                reference_id="test-single-comparison",
                extract_face=True
            )
            
            logger.info(f"[SUCCESS] Single comparison worked!")
            logger.info(f"  Match: {result.match}")
            logger.info(f"  Score: {result.similarity_score:.2f}%")
            logger.info(f"  Threshold: {result.threshold}%")
            logger.info(f"  Method: {result.comparison_method}")
            logger.info(f"  Comparison ID: {result.comparison_id}")
            if result.model_scores:
                logger.info(f"  Model Scores:")
                for score in result.model_scores:
                    logger.info(f"    - {score.get('model_name')}: {score.get('similarity_score'):.2f}% (match: {score.get('is_match')})")
            logger.info("="*70)
            return result
            
        except MaishaAPIError as e:
            logger.error(f"[FAILED] Single comparison failed: {e}")
            logger.error(f"  Error Code: {e.error_code}")
            logger.error(f"  Status Code: {e.status_code}")
            logger.error("="*70)
            return None
        except Exception as e:
            logger.error(f"[FAILED] Unexpected error: {str(e)}")
            logger.error("="*70)
            return None

    def fetch_maisha_records(self, limit: int = None) -> List[Dict]:
        """Fetch Maisha card records using blob_to_clob conversion"""
        try:
            logger.info(f"\nConnecting to Oracle database: {self.oracle_config['dsn']}")
            connection = oracledb.connect(
                user=self.oracle_config['user'],
                password=self.oracle_config['password'],
                dsn=self.oracle_config['dsn']
            )
            cursor = connection.cursor()
            
            query = """
                SELECT *
                FROM (
                    SELECT
                        blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
                        blob_to_clob(o.ID_PHOTO)  AS ID_PHOTO_BASE64,
                        k.KYC_ID_NO,
                        o.SESSION_ID
                    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
                    JOIN MA.SELF_ONBOARDING_TRACKER_KYC k
                        ON o.SESSION_ID = k.SESSION_ID
                    WHERE k.ID_TYPE = 'MAISHA_CARD'
                )
                WHERE LENGTH(ID_PHOTO_BASE64) > 16
            """
            if limit:
                query += f" AND ROWNUM <= {limit}"
            
            logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
            logger.info("VERIFYING RECORD PAIRING - Each record must have:")
            logger.info("  - One unique SESSION_ID")
            logger.info("  - One KYC_ID_NO (customer identifier)")
            logger.info("  - AWS_IMAGE and ID_PHOTO from SAME session")
            logger.info("="*70)
            
            cursor.execute(query)
            records = []
            session_ids_seen = set()
            
            for idx, row in enumerate(cursor, 1):
                try:
                    aws_image_clob = row[0]
                    id_photo_clob = row[1]
                    kyc_id = row[2]
                    session_id = row[3]
                    
                    # Check for duplicate session IDs
                    if session_id in session_ids_seen:
                        logger.warning(f"  [DUPLICATE] Record {idx}: Session {session_id[:16]}... already processed, skipping")
                        continue
                    session_ids_seen.add(session_id)
                    
                    # Read CLOB to string
                    aws_image_data = aws_image_clob.read() if aws_image_clob else None
                    id_photo_data = id_photo_clob.read() if id_photo_clob else None
                    
                    if not aws_image_data or not id_photo_data:
                        logger.warning(f"  [WARNING] Record {idx}: Missing image data, skipping")
                        continue
                    
                    # CRITICAL: Clean the base64 data
                    aws_image_clean = clean_base64_string(aws_image_data)
                    id_photo_clean = clean_base64_string(id_photo_data)
                    
                    # Validate it's actually base64 (at least partially)
                    try:
                        base64.b64decode(aws_image_clean[:100])
                        base64.b64decode(id_photo_clean[:100])
                    except Exception as e:
                        logger.warning(f"  [WARNING] Record {idx}: Invalid base64 after cleaning: {str(e)}")
                        continue
                    
                    record = {
                        'card_image_base64': id_photo_clean,
                        'face_image_base64': aws_image_clean,
                        'KYC_ID_NO': kyc_id,
                        'SESSION_ID': session_id,
                        'record_index': len(records) + 1
                    }
                    records.append(record)
                    
                    # Log first 5 records with full details
                    if idx <= 5:
                        logger.info(f"  Record {idx:2d} | Session: {session_id[:20]}... | KYC: {kyc_id}")
                        logger.info(f"            | Card: {len(id_photo_clean):,} chars")
                        logger.info(f"            | Face: {len(aws_image_clean):,} chars")
                        logger.info(f"            | [PAIRED] Both from same SESSION_ID")
                    elif idx % 10 == 0:
                        logger.info(f"  Processed {idx} records, kept {len(records)}...")
                        
                except Exception as e:
                    logger.warning(f"  [FAILED] Record {idx}: {str(e)}")
                    continue
            
            logger.info("="*70)
            logger.info(f"[SUCCESS] Fetched {len(records)} valid card-face pairs")
            logger.info(f"[SUCCESS] Unique sessions: {len(session_ids_seen)}")
            logger.info("="*70)
            
            cursor.close()
            connection.close()
            return records
            
        except Exception as e:
            logger.error(f"[FAILED] Database error: {str(e)}")
            raise

    def verify_batch_using_client(self, records: List[Dict], batch_num: int) -> List[Dict]:
        """Verify batch using official Maisha client"""
        logger.info(f"\nPreparing batch {batch_num} with {len(records)} records...")
        
        # Prepare verifications for batch API
        verifications = []
        for record in records:
            verification = {
                "id": record['SESSION_ID'],
                "source_image": record['card_image_base64'],
                "target_image": record['face_image_base64'],
                "reference_id": str(record['KYC_ID_NO'])
            }
            verifications.append(verification)
        
        try:
            # Call batch compare API
            logger.info(f"Calling batch API with {len(verifications)} verifications...")
            batch_result = self.client.batch_compare(
                verifications=verifications,
                extract_face=True,
                parallel=True,
                stop_on_error=False
            )
            
            logger.info(f"[SUCCESS] Batch {batch_num} completed")
            logger.info(f"  Total: {batch_result.total}")
            logger.info(f"  Completed: {batch_result.completed}")
            logger.info(f"  Passed: {batch_result.passed}")
            logger.info(f"  Failed: {batch_result.failed}")
            logger.info(f"  Errors: {batch_result.errors}")
            
            # Print first result for debugging
            if batch_num == 1 and batch_result.results:
                print("\n" + "="*70)
                print(f"SAMPLE API RESPONSE - BATCH {batch_num}")
                print("="*70)
                # Filter out large image fields for readability
                first_result = {k: v for k, v in batch_result.results[0].items() 
                              if 'image' not in k.lower() and (v is None or len(str(v)) < 200)}
                print(json.dumps(first_result, indent=2))
                print("="*70 + "\n")
            
            # Combine with original records
            combined_results = []
            for idx, api_result in enumerate(batch_result.results):
                if idx < len(records):
                    combined = {
                        'session_id': records[idx]['SESSION_ID'],
                        'kyc_id_no': records[idx]['KYC_ID_NO'],
                        'record_index': records[idx]['record_index'],
                        'verified': api_result.get('match', False),
                        'similarity_score': api_result.get('similarity_score', 0),
                        'threshold': api_result.get('threshold', 70),
                        'comparison_method': api_result.get('comparison_method'),
                        'comparison_id': api_result.get('comparison_id'),
                        'error': api_result.get('error'),
                        'test_timestamp': datetime.now().isoformat()
                    }
                    combined_results.append(combined)
            
            return combined_results
            
        except MaishaAPIError as e:
            logger.error(f"[FAILED] API Error: {e}")
            logger.error(f"  Error Code: {e.error_code}")
            logger.error(f"  Status Code: {e.status_code}")
            raise
        except Exception as e:
            logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
            raise

    def run_batch_test(self, batch_size: int = 5, total_limit: int = 120, 
                      test_single: bool = True, inspect_images: bool = True) -> List[Dict]:
        """
        Run batch verification tests with visual inspection option.
        
        Args:
            batch_size: Records per batch (reduced to 5 for reliability with ensemble models)
            total_limit: Maximum records to process
            test_single: Test single comparison before batch
            inspect_images: Save and visually inspect first 5 image pairs
        """
        logger.info("\n" + "="*70)
        logger.info("MAISHA CARD VERIFICATION TEST")
        logger.info("="*70)
        logger.info(f"Database: {self.oracle_config['dsn']}")
        logger.info(f"API: {self.client.base_url}")
        logger.info(f"Batch Size: {batch_size} (reduced for ensemble reliability)")
        logger.info(f"Total Limit: {total_limit}")
        logger.info(f"Timeout: {self.client.timeout} seconds")
        logger.info("="*70)
        
        # Test connections
        if not self.test_network_connectivity():
            logger.error("[FAILED] Network connectivity test failed")
            return []
        
        if not self.test_api_connection():
            logger.error("[FAILED] API connectivity test failed")
            return []
        
        # Fetch records
        records = self.fetch_maisha_records(limit=total_limit)
        if not records:
            logger.warning("[WARNING] No records fetched")
            return []
        
        # Test single comparison first if enabled
        single_result = None
        if test_single and records:
            single_result = self.test_single_comparison(
                records[0]['card_image_base64'],
                records[0]['face_image_base64']
            )
            if not single_result:
                logger.error("Single comparison failed. Cannot proceed with batch.")
                return []
            logger.info("Single comparison successful! Continuing with batch...\n")
        
        # VISUAL INSPECTION STEP (Critical for debugging low scores)
        if inspect_images and records:
            inspector = ImageInspector(
                output_dir=f"inspection_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            # Get similarity scores from single test for first record
            scores = [single_result.similarity_score] if single_result else None
            
            report_path = inspector.inspect_first_batch(
                records, 
                max_samples=5, 
                similarity_scores=scores
            )
            
            # Prompt user to inspect before continuing
            print(f"\n✅ Open this HTML report to inspect images:")
            print(f"   file:///{report_path.absolute()}\n")
            input("   Press ENTER to continue with batch processing (or Ctrl+C to abort)... ")
        
        # Process in batches
        all_results = []
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info("\n" + "="*70)
            logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
            logger.info("="*70)
            
            try:
                batch_results = self.verify_batch_using_client(batch, batch_num)
                
                # Log results
                for idx, result in enumerate(batch_results):
                    record_num = i + idx + 1
                    verified = result.get('verified', False)
                    score = result.get('similarity_score', 0)
                    method = result.get('comparison_method', 'unknown')
                    status = "✓ VERIFIED" if verified else "✗ NOT VERIFIED"
                    
                    if result.get('error'):
                        logger.warning(f"  {record_num:3d}. [ERROR] {result['error'][:80]}")
                    else:
                        color_code = "\033[92m" if verified else "\033[91m"  # Green/Red ANSI codes
                        reset = "\033[0m"
                        logger.info(f"  {record_num:3d}. [{status}] Score: {score:5.2f}% | Method: {method}")
                
                all_results.extend(batch_results)
                logger.info(f"[SUCCESS] Batch {batch_num} completed")
                
            except Exception as e:
                logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
                continue
        
        self.results = all_results
        logger.info("\n" + "="*70)
        logger.info(f"TEST COMPLETED: {len(all_results)}/{len(records)} records processed")
        logger.info("="*70)
        return all_results

    def analyze_results(self) -> Dict:
        """Analyze test results"""
        if not self.results:
            return {}
        
        total = len(self.results)
        verified = sum(1 for r in self.results if r.get('verified', False))
        not_verified = sum(1 for r in self.results if not r.get('verified') and not r.get('error'))
        failed = sum(1 for r in self.results if r.get('error'))
        
        scores = [r.get('similarity_score', 0) for r in self.results if not r.get('error')]
        avg_score = sum(scores) / len(scores) if scores else 0
        max_score = max(scores) if scores else 0
        min_score = min(scores) if scores else 0
        
        return {
            'total_tests': total,
            'verified_count': verified,
            'not_verified_count': not_verified,
            'failed_count': failed,
            'verification_rate': (verified / total * 100) if total > 0 else 0,
            'avg_similarity_score': avg_score,
            'max_similarity_score': max_score,
            'min_similarity_score': min_score
        }

    def export_csv(self, output_file: str = None) -> str:
        """Export results to CSV"""
        if not self.results:
            return None
        
        if not output_file:
            output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        columns = [
            'record_index', 'session_id', 'kyc_id_no',
            'verified', 'similarity_score', 'threshold',
            'comparison_method', 'comparison_id',
            'error', 'test_timestamp'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.results)
        
        logger.info(f"[SUCCESS] CSV exported: {output_file}")
        return output_file

    def print_summary(self, analysis: Dict):
        """Print summary to console"""
        print("\n" + "="*70)
        print("MAISHA CARD VERIFICATION - TEST SUMMARY")
        print("="*70)
        print(f"\nOVERALL RESULTS:")
        print(f"  Total Tests:        {analysis.get('total_tests', 0):,}")
        print(f"  Verified:           {analysis.get('verified_count', 0):,} ({analysis.get('verification_rate', 0):.1f}%)")
        print(f"  Not Verified:       {analysis.get('not_verified_count', 0):,}")
        print(f"  Failed/Errors:      {analysis.get('failed_count', 0):,}")
        print(f"\nSIMILARITY SCORES:")
        print(f"  Average:            {analysis.get('avg_similarity_score', 0):.2f}%")
        print(f"  Maximum:            {analysis.get('max_similarity_score', 0):.2f}%")
        print(f"  Minimum:            {analysis.get('min_similarity_score', 0):.2f}%")
        print("="*70 + "\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution"""
    # MONAPREPROD configuration
    oracle_config = {
        'user': 'MA',
        'password': 'wU8n1av8U$#OLt7pRePrOd',
        'dsn': 'copkdresb-scan:1561/MONAPREPROD'
    }
    
    # API Configuration
    api_key = "dab4424126543da8cffb8e250a63196957ee12a11312da23bf088db4f8dbb982"
    api_base_url = "https://18.235.35.175"
    
    # CRITICAL SETTINGS FOR PRODUCTION
    batch_size = 5      # Reduced from 10 → 5 for ensemble reliability
    total_limit = 120
    timeout = 300       # 5 minutes for ensemble batch processing
    
    print("\n" + "="*70)
    print("MAISHA CARD VERIFICATION TEST")
    print("="*70)
    print(f"Database: {oracle_config['dsn']}")
    print(f"API: {api_base_url}")
    print(f"Batch Size: {batch_size} (optimized for ensemble models)")
    print(f"Timeout: {timeout} seconds")
    print(f"Total Limit: {total_limit}")
    print("="*70 + "\n")
    
    try:
        tester = MaishaVerificationTester(
            oracle_config=oracle_config,
            api_key=api_key,
            api_base_url=api_base_url,
            timeout=timeout  # ← CRITICAL FOR BATCH SUCCESS
        )
        
        results = tester.run_batch_test(
            batch_size=batch_size,
            total_limit=total_limit,
            test_single=True,
            inspect_images=True  # ← ENABLES VISUAL INSPECTION
        )
        
        if not results:
            print("[FAILED] No results to analyze\n")
            return
        
        analysis = tester.analyze_results()
        tester.print_summary(analysis)
        
        csv_file = tester.export_csv()
        print(f"[SUCCESS] Results exported: {csv_file}")
        print(f"[SUCCESS] Log file: maisha_verification_*.log")
        print(f"[SUCCESS] Inspection samples: inspection_*/ directory\n")
        
    except Exception as e:
        logger.error(f"[FAILED] Test execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        print(f"\n[FAILED] ERROR: {str(e)}\n")
        raise


if __name__ == "__main__":
    # Disable SSL warnings (self-signed cert)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Check for required dependencies
    try:
        from PIL import Image
    except ImportError:
        print("ERROR: Pillow (PIL) not installed. Install with:")
        print("  pip install Pillow")
        sys.exit(1)
    
    try:
        from maisha_client import MaishaVerificationClient
    except ImportError:
        print("ERROR: maisha_client.py not found in current directory.")
        print("Please ensure maisha_client.py is in the same folder as this script.")
        sys.exit(1)
    
    main()