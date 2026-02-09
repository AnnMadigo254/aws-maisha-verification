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

# # CLIENT-SIDE THRESHOLD - Fallback threshold (matching our GBG client-side threshold)
# CLIENT_THRESHOLD = 60.0


# def clean_base64_string(b64_string: str) -> str:
#     """Clean base64 string by removing data URI prefixes and whitespace."""
#     if not b64_string:
#         return ""
    
#     if 'base64,' in b64_string:
#         b64_string = b64_string.split('base64,')[1]
    
#     b64_string = ''.join(b64_string.split())
#     b64_string = re.sub(r'[^A-Za-z0-9+/=]', '', b64_string)
    
#     return b64_string


# def validate_base64_image(b64_string: str) -> bool:
#     """Validate that base64 string is a valid image."""
#     try:
#         image_bytes = base64.b64decode(b64_string)
        
#         if len(image_bytes) < 1000:
#             logger.warning(f"Image too small: {len(image_bytes)} bytes")
#             return False
        
#         if image_bytes[:3] != b'\xff\xd8\xff':
#             logger.warning(f"Not a JPEG image. First bytes: {image_bytes[:10].hex()}")
        
#         logger.debug(f"Valid image: {len(image_bytes)} bytes")
#         return True
        
#     except Exception as e:
#         logger.error(f"Base64 validation failed: {str(e)}")
#         return False


# class MaishaVerificationTester:
#     def __init__(self, 
#                  oracle_config: Dict[str, str],
#                  api_key: str,
#                  api_base_url: str = "https://18.235.35.175",
#                  compare_with_gbg: bool = False,
#                  client_threshold: float = CLIENT_THRESHOLD):
#         """Initialize the Maisha verification tester"""
#         self.oracle_config = oracle_config
#         self.api_key = api_key
#         self.results = []
#         self.compare_with_gbg = compare_with_gbg
#         self.client_threshold = client_threshold
        
#         # Initialize Maisha API client
#         self.client = MaishaVerificationClient(
#             api_key=api_key,
#             base_url=api_base_url,
#             verify_ssl=False
#         )
        
#         logger.info(f"API Configuration:")
#         logger.info(f"  Endpoint: {api_base_url}")
#         logger.info(f"  API Key: {api_key[:20]}...")
#         logger.info(f"  GBG Comparison: {'ENABLED' if compare_with_gbg else 'DISABLED'}")
#         logger.info(f"  Client-Side Threshold: {client_threshold}% (fallback threshold)")
    
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
    
#     def fetch_maisha_records(self, limit: int = None, gbg_status: str = None) -> List[Dict]:
#         """
#         Fetch Maisha card records with GBG journey information
        
#         Args:
#             limit: Number of records to fetch
#             gbg_status: 'APPROVED' for GBG-approved (OCR_CHECK=2), 
#                        'REJECTED' for GBG-rejected (OCR_CHECK!=2), 
#                        or None for all GBG-processed records
#         """
#         try:
#             logger.info(f"Connecting to Oracle database: {self.oracle_config['dsn']}")
#             connection = oracledb.connect(
#                 user=self.oracle_config['user'],
#                 password=self.oracle_config['password'],
#                 dsn=self.oracle_config['dsn']
#             )
            
#             cursor = connection.cursor()
            
#             # Build query
#             if self.compare_with_gbg:
#                 query = """
#                     SELECT
#                         blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
#                         blob_to_clob(k.KYC_IDFRONT_CAPTURE) AS ID_PHOTO_BASE64,
#                         k.KYC_ID_NO,
#                         k.SESSION_ID,
#                         sm.JOURNEY_ID as GBG_JOURNEY_ID,
#                         sm.IS_GBG_JOURNEY_ID_CAPTURED as GBG_CAPTURED,
#                         sm.IS_RETRIEVED as GBG_RETRIEVED,
#                         sm.OCR_CHECK_MISMATCH as GBG_OCR_CHECK,
#                         sm.CREATED_DATE as ONBOARDING_DATE
#                     FROM MA.SELF_ONBOARDING_TRACKER_AWS o
#                     JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
#                         ON o.SESSION_ID = k.SESSION_ID
#                     JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm 
#                         ON sm.ID = k.SESSION_ID
#                     WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
#                         AND o.AWS_IMAGE IS NOT NULL
#                         AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
#                         AND sm.IS_GBG_JOURNEY_ID_CAPTURED = 1
#                         AND sm.IS_RETRIEVED = 1
#                 """
                
#                 # Filter by OCR_CHECK_MISMATCH status
#                 if gbg_status == 'APPROVED':
#                     # GBG APPROVED records (OCR_CHECK_MISMATCH = 2)
#                     query += " AND sm.OCR_CHECK_MISMATCH = 2"
#                 elif gbg_status == 'REJECTED':
#                     # GBG REJECTED records (OCR_CHECK_MISMATCH != 2)
#                     query += " AND sm.OCR_CHECK_MISMATCH != 2"
#                 # else: all GBG-processed records (no additional filter)
                    
#                 query += " ORDER BY sm.CREATED_DATE DESC"
#             else:
#                 query = """
#                     SELECT
#                         blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
#                         blob_to_clob(k.KYC_IDFRONT_CAPTURE) AS ID_PHOTO_BASE64,
#                         k.KYC_ID_NO,
#                         k.SESSION_ID
#                     FROM MA.SELF_ONBOARDING_TRACKER_AWS o
#                     JOIN MA.SELF_ONBOARDING_TRACKER_KYC k
#                         ON o.SESSION_ID = k.SESSION_ID
#                     WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
#                         AND o.AWS_IMAGE IS NOT NULL
#                         AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
#                     ORDER BY k.CREATED_ON DESC
#                 """
            
#             if limit:
#                 query += f" FETCH FIRST {limit} ROWS ONLY"
            
#             logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
#             if self.compare_with_gbg:
#                 if gbg_status == 'APPROVED':
#                     logger.info(f"  GBG Filter: GBG-APPROVED only (OCR_CHECK_MISMATCH=2)")
#                 elif gbg_status == 'REJECTED':
#                     logger.info(f"  GBG Filter: GBG-REJECTED only (OCR_CHECK_MISMATCH!=2)")
#                 else:
#                     logger.info(f"  GBG Filter: ALL GBG-processed records (both approved & rejected)")
#             logger.info("="*70)
            
#             cursor.execute(query)
            
#             records = []
#             session_ids_seen = set()
#             gbg_stats = {'GBG_APPROVED': 0, 'GBG_REJECTED': 0, 'GBG_UNKNOWN': 0} if self.compare_with_gbg else None
            
#             for idx, row in enumerate(cursor, 1):
#                 try:
#                     aws_image_clob = row[0]
#                     id_photo_clob = row[1]
#                     kyc_id = row[2]
#                     session_id = row[3]
                    
#                     if session_id in session_ids_seen:
#                         continue
                    
#                     session_ids_seen.add(session_id)
                    
#                     aws_image_data = aws_image_clob.read() if aws_image_clob else None
#                     id_photo_data = id_photo_clob.read() if id_photo_clob else None
                    
#                     if not aws_image_data or not id_photo_data:
#                         continue
                    
#                     aws_image_clean = clean_base64_string(aws_image_data)
#                     id_photo_clean = clean_base64_string(id_photo_data)
                    
#                     try:
#                         base64.b64decode(aws_image_clean[:100])
#                         base64.b64decode(id_photo_clean[:100])
#                     except:
#                         continue
                    
#                     record = {
#                         'card_image_base64': id_photo_clean,
#                         'face_image_base64': aws_image_clean,
#                         'KYC_ID_NO': kyc_id,
#                         'SESSION_ID': session_id,
#                         'record_index': len(records) + 1
#                     }
                    
#                     if self.compare_with_gbg and len(row) > 4:
#                         record['GBG_JOURNEY_ID'] = row[4]
#                         record['GBG_CAPTURED'] = row[5]
#                         record['GBG_RETRIEVED'] = row[6]
#                         record['GBG_OCR_CHECK'] = row[7]
#                         record['ONBOARDING_DATE'] = row[8] if len(row) > 8 else None
                        
#                         # Use OCR_CHECK_MISMATCH to determine GBG verification
#                         ocr_check = row[7]
#                         if ocr_check == 2:
#                             gbg_stats['GBG_APPROVED'] += 1
#                         elif ocr_check is not None and ocr_check != 2:
#                             gbg_stats['GBG_REJECTED'] += 1
#                         else:
#                             gbg_stats['GBG_UNKNOWN'] += 1
                    
#                     records.append(record)
                    
#                     if idx <= 5:
#                         logger.info(f"  Record {idx:2d} | Session: {session_id[:20]}... | KYC: {kyc_id}")
#                         if self.compare_with_gbg:
#                             ocr_check = record.get('GBG_OCR_CHECK')
#                             ocr_status = "GBG-APPROVED" if ocr_check == 2 else "GBG-REJECTED"
#                             logger.info(f"            | Status: {ocr_status} (OCR_CHECK={ocr_check})")
#                         logger.info(f"            | Card: {len(id_photo_clean)} chars | Face: {len(aws_image_clean)} chars")
#                     elif idx % 20 == 0:
#                         logger.info(f"  Processed {idx} records, kept {len(records)}...")
                    
#                 except Exception as e:
#                     logger.debug(f"  Skipped record {idx}: {str(e)}")
#                     continue
            
#             logger.info("="*70)
#             logger.info(f"[SUCCESS] Fetched {len(records)} valid card-face pairs")
            
#             if self.compare_with_gbg and gbg_stats:
#                 logger.info(f"[SUCCESS] GBG Status Breakdown:")
#                 logger.info(f"           GBG-Approved (OCR=2):  {gbg_stats['GBG_APPROVED']}")
#                 logger.info(f"           GBG-Rejected (OCR!=2): {gbg_stats['GBG_REJECTED']}")
#                 if gbg_stats['GBG_UNKNOWN'] > 0:
#                     logger.info(f"           Unknown (OCR=NULL):   {gbg_stats['GBG_UNKNOWN']}")
            
#             logger.info("="*70)
            
#             cursor.close()
#             connection.close()
            
#             return records
            
#         except Exception as e:
#             logger.error(f"[FAILED] Database error: {str(e)}")
#             import traceback
#             logger.error(traceback.format_exc())
#             raise
    
#     def verify_batch_using_client(self, records: List[Dict], batch_num: int) -> List[Dict]:
#         """Verify batch using official Maisha client with CLIENT-SIDE THRESHOLD and API diagnostics"""
#         logger.info(f"Preparing batch {batch_num} with {len(records)} records...")
        
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
            
#             # Calculate using CLIENT threshold
#             client_passed = sum(1 for r in batch_result.results 
#                               if r.get('similarity_score', 0) >= self.client_threshold)
#             api_passed = sum(1 for r in batch_result.results 
#                             if r.get('match', False))
            
#             logger.info(f"  Passed (client {self.client_threshold}%): {client_passed}")
#             logger.info(f"  API 'match' field says: {api_passed}")
            
#             # DIAGNOSTIC: Print detailed API response for first batch
#             if batch_num == 1 and batch_result.results:
#                 print("\n" + "="*70)
#                 print(f"DIAGNOSTIC - API RESPONSE ANALYSIS (BATCH {batch_num})")
#                 print("="*70)
                
#                 # Show first 3 responses
#                 for idx, api_result in enumerate(batch_result.results[:3], 1):
#                     print(f"\n--- Response {idx} ---")
#                     print(json.dumps(api_result, indent=2))
                    
#                     # Detailed field analysis
#                     score = api_result.get('similarity_score', 0)
#                     match = api_result.get('match', False)
#                     threshold_in_response = api_result.get('threshold')
                    
#                     print(f"\nDIAGNOSTIC CHECKS for Response {idx}:")
#                     print(f"  Similarity Score: {score:.2f}%")
#                     print(f"  API 'match' field: {match}")
#                     print(f"  Threshold in response: {threshold_in_response}")
#                     print(f"  Expected match (score >= 70): {score >= 70}")
#                     print(f"  Actual match from API: {match}")
                    
#                     # Threshold issue detection
#                     if score >= 70 and not match:
#                         print(f"  🚨 THRESHOLD ISSUE: Score {score:.2f}% but match=False")
#                         print(f"     API is NOT using 70% threshold")
#                     elif score < 70 and match:
#                         print(f"  ⚠️  UNEXPECTED: Score {score:.2f}% but match=True")
#                         print(f"     API threshold may be lower than 70%")
#                     else:
#                         print(f"  ✓ Match field aligns with expected 70% threshold")
                
#                 # Summary of threshold issue
#                 print("\n" + "-"*70)
#                 print("THRESHOLD ISSUE SUMMARY:")
#                 print("-"*70)
                
#                 high_score_rejected = sum(1 for r in batch_result.results 
#                                          if r.get('similarity_score', 0) >= 70 
#                                          and not r.get('match', False))
#                 low_score_approved = sum(1 for r in batch_result.results 
#                                         if r.get('similarity_score', 0) < 70 
#                                         and r.get('match', False))
                
#                 if high_score_rejected > 0:
#                     print(f"❌ THRESHOLD BUG STILL PRESENT:")
#                     print(f"   {high_score_rejected} records with score ≥70% rejected by API")
#                     print(f"   The API 'match' field is NOT using the documented 70% threshold")
                    
#                     # Find the actual threshold being used
#                     rejected_scores = sorted([r.get('similarity_score', 0) 
#                                             for r in batch_result.results 
#                                             if not r.get('match', False)], reverse=True)
#                     if rejected_scores:
#                         highest_rejected = rejected_scores[0]
#                         print(f"   Highest rejected score: {highest_rejected:.2f}%")
#                         print(f"   Estimated API threshold: >{highest_rejected:.2f}%")
                    
#                     print(f"\n   ✓ Using client-side {self.client_threshold}% threshold as fallback")
#                 else:
#                     print(f"✓ THRESHOLD APPEARS FIXED:")
#                     print(f"   API 'match' field aligns with 70% threshold")
#                     print(f"   No high-scoring rejections detected")
                
#                 if low_score_approved > 0:
#                     print(f"\n⚠️  {low_score_approved} records with score <70% approved by API")
#                     print(f"   API threshold may be lower than expected")
                
#                 print("="*70 + "\n")
            
#             combined_results = []
#             for idx, api_result in enumerate(batch_result.results):
#                 if idx < len(records):
#                     similarity_score = api_result.get('similarity_score', 0)
                    
#                     # APPLY CLIENT-SIDE THRESHOLD
#                     client_verified = similarity_score >= self.client_threshold
#                     api_match = api_result.get('match', False)
                    
#                     combined = {
#                         'record_index': records[idx]['record_index'],
#                         'session_id': records[idx]['SESSION_ID'],
#                         'kyc_id_no': records[idx]['KYC_ID_NO'],
                        
#                         # Use client-side threshold instead of API's broken match field
#                         'aws_verified': client_verified,
#                         'aws_api_match': api_match,  # Keep for comparison
#                         'threshold_mismatch': (similarity_score >= 70 and not api_match) or (similarity_score < 70 and api_match),
                        
#                         'aws_similarity_score': similarity_score,
#                         'aws_threshold': self.client_threshold,
#                         'aws_api_threshold': api_result.get('threshold'),  # What API reports
#                         'aws_comparison_method': api_result.get('comparison_method'),
#                         'aws_comparison_id': api_result.get('comparison_id'),
#                         'aws_error': api_result.get('error'),
#                         'test_timestamp': datetime.now().isoformat()
#                     }
                    
#                     if self.compare_with_gbg:
#                         combined['gbg_journey_id'] = records[idx].get('GBG_JOURNEY_ID')
#                         combined['gbg_captured'] = records[idx].get('GBG_CAPTURED')
#                         combined['gbg_retrieved'] = records[idx].get('GBG_RETRIEVED')
#                         combined['gbg_ocr_check'] = records[idx].get('GBG_OCR_CHECK')
                        
#                         # GBG verified = OCR_CHECK_MISMATCH = 2 (no mismatch, approved)
#                         gbg_verified = (records[idx].get('GBG_OCR_CHECK') == 2)
                        
#                         combined['both_verified'] = (gbg_verified and client_verified)
#                         combined['gbg_verified'] = gbg_verified
#                         combined['agreement'] = (gbg_verified == client_verified)
                    
#                     combined_results.append(combined)
            
#             return combined_results
            
#         except MaishaAPIError as e:
#             logger.error(f"[FAILED] API Error: {e}")
#             raise
#         except Exception as e:
#             logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
#             raise
    
#     def run_batch_test(self, batch_size: int = 10, total_limit: int = None, 
#                        test_single: bool = False, gbg_filter: str = None) -> List[Dict]:
#         """Run batch verification tests (skip single test - it times out)"""
#         logger.info("="*70)
#         if self.compare_with_gbg:
#             logger.info("MAISHA CARD VERIFICATION TEST - WITH GBG COMPARISON")
#         else:
#             logger.info("MAISHA CARD VERIFICATION TEST")
#         logger.info("="*70)
#         logger.info(f"Database: {self.oracle_config['dsn']}")
#         logger.info(f"Batch Size: {batch_size}")
#         logger.info(f"Limit: {total_limit if total_limit else 'ALL'}")
#         if self.compare_with_gbg:
#             logger.info(f"GBG Filter: {gbg_filter if gbg_filter else 'ALL (approved + rejected)'}")
#         logger.info(f"Client Threshold: {self.client_threshold}% (fallback threshold)")
#         logger.info("="*70)
        
#         if not self.test_network_connectivity():
#             return []
        
#         if not self.test_api_connection():
#             return []
        
#         records = self.fetch_maisha_records(limit=total_limit, gbg_status=gbg_filter)
        
#         if not records:
#             logger.warning("[WARNING] No records fetched")
#             return []
        
#         all_results = []
#         total_batches = (len(records) + batch_size - 1) // batch_size
        
#         for i in range(0, len(records), batch_size):
#             batch = records[i:i + batch_size]
#             batch_num = (i // batch_size) + 1
            
#             logger.info("="*70)
#             logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
#             logger.info("="*70)
            
#             try:
#                 batch_results = self.verify_batch_using_client(batch, batch_num)
                
#                 for idx, result in enumerate(batch_results):
#                     record_num = i + idx + 1
#                     aws_verified = result.get('aws_verified', False)
#                     score = result.get('aws_similarity_score', 0)
                    
#                     if self.compare_with_gbg:
#                         gbg_verified = result.get('gbg_verified', False)
#                         agreement = result.get('agreement', False)
#                         logger.info(f"  {record_num:3d}. GBG:{'✓' if gbg_verified else '✗'} "
#                                   f"AWS:{'✓' if aws_verified else '✗'} "
#                                   f"Agree:{'✓' if agreement else '✗'} | Score: {score:5.2f}%")
#                     else:
#                         logger.info(f"  {record_num:3d}. [{'✓' if aws_verified else '✗'}] Score: {score:5.2f}%")
                
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
#         """Analyze test results with threshold issue tracking"""
#         if not self.results:
#             return {}
        
#         total = len(self.results)
#         aws_verified = sum(1 for r in self.results if r.get('aws_verified', False))
#         aws_not_verified = total - aws_verified
        
#         # Track threshold mismatches
#         threshold_mismatches = sum(1 for r in self.results if r.get('threshold_mismatch', False))
#         high_score_rejected = sum(1 for r in self.results 
#                                  if r.get('aws_similarity_score', 0) >= 70 
#                                  and not r.get('aws_api_match', False))
        
#         scores = [r.get('aws_similarity_score', 0) for r in self.results]
#         avg_score = sum(scores) / len(scores) if scores else 0
#         max_score = max(scores) if scores else 0
#         min_score = min(scores) if scores else 0
        
#         analysis = {
#             'total_tests': total,
#             'aws_verified_count': aws_verified,
#             'aws_not_verified_count': aws_not_verified,
#             'aws_verification_rate': (aws_verified / total * 100) if total > 0 else 0,
#             'avg_similarity_score': avg_score,
#             'max_similarity_score': max_score,
#             'min_similarity_score': min_score,
            
#             # Threshold issue tracking
#             'threshold_mismatches': threshold_mismatches,
#             'threshold_mismatch_rate': (threshold_mismatches / total * 100) if total > 0 else 0,
#             'high_score_rejected_count': high_score_rejected,
#             'api_threshold_issue': high_score_rejected > 0
#         }
        
#         if self.compare_with_gbg:
#             gbg_verified = sum(1 for r in self.results if r.get('gbg_verified', False))
#             agreement = sum(1 for r in self.results if r.get('agreement', False))
#             both_verified = sum(1 for r in self.results if r.get('both_verified', False))
#             gbg_yes_aws_no = sum(1 for r in self.results if r.get('gbg_verified') and not r.get('aws_verified'))
#             gbg_no_aws_yes = sum(1 for r in self.results if not r.get('gbg_verified') and r.get('aws_verified'))
#             both_no = sum(1 for r in self.results if not r.get('gbg_verified') and not r.get('aws_verified'))
            
#             # Analyze AWS scores for GBG-verified vs GBG-rejected
#             gbg_verified_scores = [r.get('aws_similarity_score', 0) for r in self.results if r.get('gbg_verified')]
#             gbg_rejected_scores = [r.get('aws_similarity_score', 0) for r in self.results if not r.get('gbg_verified')]
            
#             # Score distribution for GBG-verified customers
#             score_ranges = {
#                 '90-100%': sum(1 for s in gbg_verified_scores if s >= 90),
#                 '80-89%': sum(1 for s in gbg_verified_scores if 80 <= s < 90),
#                 '70-79%': sum(1 for s in gbg_verified_scores if 70 <= s < 80),
#                 '60-69%': sum(1 for s in gbg_verified_scores if 60 <= s < 70),
#                 'Below 60%': sum(1 for s in gbg_verified_scores if s < 60)
#             }
            
#             # Score distribution for GBG-rejected customers
#             rejected_score_ranges = {
#                 '90-100%': sum(1 for s in gbg_rejected_scores if s >= 90),
#                 '80-89%': sum(1 for s in gbg_rejected_scores if 80 <= s < 90),
#                 '70-79%': sum(1 for s in gbg_rejected_scores if 70 <= s < 80),
#                 '60-69%': sum(1 for s in gbg_rejected_scores if 60 <= s < 70),
#                 'Below 60%': sum(1 for s in gbg_rejected_scores if s < 60)
#             }
            
#             # Find problematic cases
#             low_score_gbg_verified = [r for r in self.results 
#                                      if r.get('gbg_verified') and r.get('aws_similarity_score', 0) < 60]
#             high_score_gbg_rejected = [r for r in self.results 
#                                       if not r.get('gbg_verified') and r.get('aws_similarity_score', 0) >= 80]
            
#             analysis.update({
#                 'gbg_verified_count': gbg_verified,
#                 'gbg_not_verified_count': total - gbg_verified,
#                 'gbg_verification_rate': (gbg_verified / total * 100) if total > 0 else 0,
#                 'agreement_count': agreement,
#                 'agreement_rate': (agreement / total * 100) if total > 0 else 0,
#                 'both_verified_count': both_verified,
#                 'both_no': both_no,
#                 'gbg_yes_aws_no': gbg_yes_aws_no,
#                 'gbg_no_aws_yes': gbg_no_aws_yes,
                
#                 # GBG-verified customer score analysis
#                 'gbg_verified_avg_score': sum(gbg_verified_scores) / len(gbg_verified_scores) if gbg_verified_scores else 0,
#                 'gbg_verified_max_score': max(gbg_verified_scores) if gbg_verified_scores else 0,
#                 'gbg_verified_min_score': min(gbg_verified_scores) if gbg_verified_scores else 0,
#                 'gbg_verified_score_distribution': score_ranges,
                
#                 # GBG-rejected customer score analysis  
#                 'gbg_rejected_avg_score': sum(gbg_rejected_scores) / len(gbg_rejected_scores) if gbg_rejected_scores else 0,
#                 'gbg_rejected_max_score': max(gbg_rejected_scores) if gbg_rejected_scores else 0,
#                 'gbg_rejected_min_score': min(gbg_rejected_scores) if gbg_rejected_scores else 0,
#                 'gbg_rejected_score_distribution': rejected_score_ranges,
                
#                 # Problematic cases
#                 'low_score_gbg_verified_count': len(low_score_gbg_verified),
#                 'high_score_gbg_rejected_count': len(high_score_gbg_rejected),
                
#                 # False rates
#                 'false_negative_rate': (gbg_yes_aws_no / gbg_verified * 100) if gbg_verified > 0 else 0,
#                 'false_positive_rate': (gbg_no_aws_yes / (total - gbg_verified) * 100) if (total - gbg_verified) > 0 else 0
#             })
        
#         return analysis
    
#     def export_csv(self, output_file: str = None) -> str:
#         """Export results to CSV"""
#         if not self.results:
#             return None
        
#         if not output_file:
#             output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
#         columns = list(self.results[0].keys()) if self.results else []
        
#         with open(output_file, 'w', newline='', encoding='utf-8') as f:
#             writer = csv.DictWriter(f, fieldnames=columns)
#             writer.writeheader()
#             writer.writerows(self.results)
        
#         logger.info(f"[SUCCESS] CSV exported: {output_file}")
#         return output_file
    
#     def print_summary(self, analysis: Dict):
#         """Print enhanced summary with threshold issue status"""
#         print("\n" + "="*70)
#         if self.compare_with_gbg:
#             print("MAISHA CARD VERIFICATION - GBG vs AWS COMPARISON")
#         else:
#             print("MAISHA CARD VERIFICATION - TEST SUMMARY")
#         print("="*70)
        
#         # Threshold Issue Status
#         if analysis.get('api_threshold_issue', False):
#             print("\n🚨 API THRESHOLD ISSUE DETECTED:")
#             print(f"  {analysis.get('high_score_rejected_count', 0)} records with score ≥70% were rejected by API")
#             print(f"  The API 'match' field is NOT using the documented 70% threshold")
#             print(f"  ✓ Using client-side {self.client_threshold}% threshold for accurate results")
#         else:
#             print("\n✓ API THRESHOLD STATUS:")
#             print(f"  API 'match' field appears to align with 70% threshold")
#             print(f"  No threshold issue detected in this test")
        
#         if analysis.get('threshold_mismatch_rate', 0) > 0:
#             print(f"\nThreshold Mismatches: {analysis.get('threshold_mismatches', 0)} ({analysis.get('threshold_mismatch_rate', 0):.1f}%)")
        
#         print()
        
#         if self.compare_with_gbg:
#             print(f"TOTAL RECORDS: {analysis.get('total_tests', 0):,}")
            
#             print(f"\nGBG RESULTS:")
#             print(f"  ✓ Approved (OCR=2):     {analysis.get('gbg_verified_count', 0):,} ({analysis.get('gbg_verification_rate', 0):.1f}%)")
#             print(f"  ✗ Rejected (OCR!=2):    {analysis.get('gbg_not_verified_count', 0):,}")
            
#             print(f"\nAWS RESULTS (with {self.client_threshold}% threshold):")
#             print(f"  ✓ Verified:     {analysis.get('aws_verified_count', 0):,} ({analysis.get('aws_verification_rate', 0):.1f}%)")
#             print(f"  ✗ Not Verified: {analysis.get('aws_not_verified_count', 0):,}")
            
#             print(f"\nAGREEMENT ANALYSIS:")
#             print(f"  Overall Agreement:     {analysis.get('agreement_rate', 0):.1f}%")
#             print(f"  ✓✓ Both Verified:      {analysis.get('both_verified_count', 0):,}")
#             print(f"  ✗✗ Both Rejected:      {analysis.get('both_no', 0):,}")
#             print(f"  ✓✗ GBG Approved, AWS Rejected:    {analysis.get('gbg_yes_aws_no', 0):,} (False Negatives: {analysis.get('false_negative_rate', 0):.1f}%)")
#             print(f"  ✗✓ GBG Rejected, AWS Approved:    {analysis.get('gbg_no_aws_yes', 0):,} (False Positives: {analysis.get('false_positive_rate', 0):.1f}%)")
            
#             print(f"\nAWS SCORES FOR GBG-APPROVED CUSTOMERS:")
#             print(f"  Average:  {analysis.get('gbg_verified_avg_score', 0):.2f}%")
#             print(f"  Maximum:  {analysis.get('gbg_verified_max_score', 0):.2f}%")
#             print(f"  Minimum:  {analysis.get('gbg_verified_min_score', 0):.2f}%")
            
#             print(f"\n  Score Distribution (GBG-approved customers):")
#             dist = analysis.get('gbg_verified_score_distribution', {})
#             for range_name, count in dist.items():
#                 pct = (count / analysis.get('gbg_verified_count', 1) * 100) if analysis.get('gbg_verified_count', 0) > 0 else 0
#                 print(f"    {range_name:12s}: {count:3d} customers ({pct:5.1f}%)")
            
#             if analysis.get('gbg_rejected_avg_score', 0) >= 0:
#                 print(f"\nAWS SCORES FOR GBG-REJECTED CUSTOMERS:")
#                 print(f"  Average:  {analysis.get('gbg_rejected_avg_score', 0):.2f}%")
#                 print(f"  Maximum:  {analysis.get('gbg_rejected_max_score', 0):.2f}%")
#                 print(f"  Minimum:  {analysis.get('gbg_rejected_min_score', 0):.2f}%")
                
#                 print(f"\n  Score Distribution (GBG-rejected customers):")
#                 rej_dist = analysis.get('gbg_rejected_score_distribution', {})
#                 for range_name, count in rej_dist.items():
#                     rej_count = analysis.get('gbg_not_verified_count', 1)
#                     pct = (count / rej_count * 100) if rej_count > 0 else 0
#                     print(f"    {range_name:12s}: {count:3d} customers ({pct:5.1f}%)")
            
#             print(f"\nPROBLEMATIC CASES:")
#             print(f"  GBG✓ but AWS score <60%:  {analysis.get('low_score_gbg_verified_count', 0):,} cases")
#             print(f"  GBG✗ but AWS score ≥80%:  {analysis.get('high_score_gbg_rejected_count', 0):,} cases")
            
#             if analysis.get('low_score_gbg_verified_count', 0) > 0:
#                 print(f"\n  ⚠️  WARNING: {analysis.get('low_score_gbg_verified_count', 0)} GBG-approved customers have AWS scores <60%")
#                 print(f"      Possible causes: poor image quality, different angles, or GBG false positives")
            
#             if analysis.get('high_score_gbg_rejected_count', 0) > 0:
#                 print(f"\n  ⚠️  CONCERN: {analysis.get('high_score_gbg_rejected_count', 0)} GBG-rejected customers have AWS scores ≥80%")
#                 print(f"      Possible causes: sophisticated fraud or GBG false negatives")
            
#         else:
#             print(f"\nTotal: {analysis.get('total_tests', 0):,}")
#             print(f"Verified: {analysis.get('aws_verified_count', 0):,} ({analysis.get('aws_verification_rate', 0):.1f}%)")
        
#         print(f"\nOVERALL AWS SIMILARITY SCORES:")
#         print(f"  Average:  {analysis.get('avg_similarity_score', 0):.2f}%")
#         print(f"  Maximum:  {analysis.get('max_similarity_score', 0):.2f}%")
#         print(f"  Minimum:  {analysis.get('min_similarity_score', 0):.2f}%")
#         print("="*70 + "\n")


# def main():
#     oracle_config = {
#         'user': 'MA',
#         'password': 'wU8n1av8U$#OLt7pRePrOd',
#         'dsn': 'copkdresb-scan:1561/MONAPREPROD'
#     }
    
#     api_key = "dab4424126543da8cffb8e250a63196957ee12a11312da23bf088db4f8dbb982"
#     api_base_url = "https://18.235.35.175"
    
#     compare_with_gbg = True
#     gbg_filter = None  # Test BOTH approved AND rejected (set to None for all)
#     batch_size = 5
#     total_limit = 200  # 200 records total (mix of approved and rejected)
    
#     print("\n" + "="*70)
#     print("MAISHA VERIFICATION - COMPREHENSIVE GBG COMPARISON")
#     print("="*70)
#     print(f"Database: {oracle_config['dsn']}")
#     print(f"Testing: {total_limit} GBG-processed records (BOTH approved & rejected)")
#     print(f"Threshold: {CLIENT_THRESHOLD}% (client-side fallback)")
#     print(f"Note: OCR_CHECK_MISMATCH=2 → GBG approved, !=2 → GBG rejected")
#     print("="*70 + "\n")
    
#     try:
#         tester = MaishaVerificationTester(
#             oracle_config=oracle_config,
#             api_key=api_key,
#             api_base_url=api_base_url,
#             compare_with_gbg=compare_with_gbg,
#             client_threshold=CLIENT_THRESHOLD
#         )
        
#         results = tester.run_batch_test(
#             batch_size=batch_size,
#             total_limit=total_limit,
#             test_single=False,
#             gbg_filter=gbg_filter  # None = all records (both approved and rejected)
#         )
        
#         if not results:
#             print("[FAILED] No results\n")
#             return
        
#         analysis = tester.analyze_results()
#         tester.print_summary(analysis)
        
#         csv_file = tester.export_csv()
#         print(f"[SUCCESS] Results: {csv_file}")
#         print(f"[SUCCESS] Check CSV columns: gbg_ocr_check, gbg_verified, aws_verified, agreement\n")
        
#     except Exception as e:
#         logger.error(f"[FAILED] {str(e)}")


# if __name__ == "__main__":
#     import urllib3
#     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#     main()



#  == MAISH VERIFICATION TESTER WITH GBG COMPARISON - OFFICIAL CLIENT IMPLEMENTATION ========================
import oracledb
import json
import csv
from datetime import datetime
from typing import List, Dict
import logging
import sys
import base64
import re
from pathlib import Path

# Import the official Maisha client
from maisha_client import MaishaVerificationClient, MaishaAPIError

# Fix Windows encoding issues
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Set up logging
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

# CLIENT-SIDE THRESHOLD - Fallback threshold (matching our GBG client-side threshold)
CLIENT_THRESHOLD = 60.0


def clean_base64_string(b64_string: str) -> str:
    """Clean base64 string by removing data URI prefixes and whitespace."""
    if not b64_string:
        return ""
    
    if 'base64,' in b64_string:
        b64_string = b64_string.split('base64,')[1]
    
    b64_string = ''.join(b64_string.split())
    b64_string = re.sub(r'[^A-Za-z0-9+/=]', '', b64_string)
    
    return b64_string


def validate_base64_image(b64_string: str) -> bool:
    """Validate that base64 string is a valid image."""
    try:
        image_bytes = base64.b64decode(b64_string)
        
        if len(image_bytes) < 1000:
            logger.warning(f"Image too small: {len(image_bytes)} bytes")
            return False
        
        if image_bytes[:3] != b'\xff\xd8\xff':
            logger.warning(f"Not a JPEG image. First bytes: {image_bytes[:10].hex()}")
        
        logger.debug(f"Valid image: {len(image_bytes)} bytes")
        return True
        
    except Exception as e:
        logger.error(f"Base64 validation failed: {str(e)}")
        return False


class MaishaVerificationTester:
    def __init__(self, 
                 oracle_config: Dict[str, str],
                 api_key: str,
                 api_base_url: str = "https://18.235.35.175",
                 compare_with_gbg: bool = False,
                 client_threshold: float = CLIENT_THRESHOLD,
                 save_disagreement_images: bool = True):
        """Initialize the Maisha verification tester"""
        self.oracle_config = oracle_config
        self.api_key = api_key
        self.results = []
        self.compare_with_gbg = compare_with_gbg
        self.client_threshold = client_threshold
        self.save_disagreement_images = save_disagreement_images
        
        # Create directories for saving disagreement images
        if self.save_disagreement_images:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.images_dir = Path(f"disagreement_images_{timestamp}")
            self.images_dir.mkdir(exist_ok=True)
            
            # Create subdirectories for different disagreement types
            self.gbg_approved_aws_rejected_dir = self.images_dir / "1_GBG_APPROVED_AWS_REJECTED"
            self.gbg_approved_aws_rejected_dir.mkdir(exist_ok=True)
            
            self.gbg_rejected_aws_approved_dir = self.images_dir / "2_GBG_REJECTED_AWS_APPROVED"
            self.gbg_rejected_aws_approved_dir.mkdir(exist_ok=True)
            
            # Also save some agreement cases for comparison
            self.both_approved_dir = self.images_dir / "3_BOTH_APPROVED_samples"
            self.both_approved_dir.mkdir(exist_ok=True)
            
            self.both_rejected_dir = self.images_dir / "4_BOTH_REJECTED_samples"
            self.both_rejected_dir.mkdir(exist_ok=True)
            
            logger.info(f"Image save directories created: {self.images_dir}")
        
        # Initialize Maisha API client
        self.client = MaishaVerificationClient(
            api_key=api_key,
            base_url=api_base_url,
            verify_ssl=False
        )
        
        logger.info(f"API Configuration:")
        logger.info(f"  Endpoint: {api_base_url}")
        logger.info(f"  API Key: {api_key[:20]}...")
        logger.info(f"  GBG Comparison: {'ENABLED' if compare_with_gbg else 'DISABLED'}")
        logger.info(f"  Client-Side Threshold: {client_threshold}% (fallback threshold)")
        logger.info(f"  Save Disagreement Images: {'YES' if save_disagreement_images else 'NO'}")
    
    def save_image_pair(self, card_base64: str, face_base64: str, 
                       filename_prefix: str, directory: Path, 
                       metadata: Dict = None):
        """Save a card-face image pair with metadata"""
        try:
            # Decode and save card image
            card_data = base64.b64decode(card_base64)
            card_path = directory / f"{filename_prefix}_CARD.jpg"
            with open(card_path, 'wb') as f:
                f.write(card_data)
            
            # Decode and save face image
            face_data = base64.b64decode(face_base64)
            face_path = directory / f"{filename_prefix}_FACE.jpg"
            with open(face_path, 'wb') as f:
                f.write(face_data)
            
            # Save metadata if provided
            if metadata:
                meta_path = directory / f"{filename_prefix}_INFO.json"
                with open(meta_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, default=str)
            
            logger.debug(f"Saved image pair: {filename_prefix}")
            return True
        except Exception as e:
            logger.warning(f"Failed to save image pair {filename_prefix}: {str(e)}")
            return False
    
    def test_api_connection(self):
        """Test API connectivity with health check"""
        logger.info("="*70)
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
    
    def test_network_connectivity(self):
        """Test basic network connectivity to database server"""
        import socket
        
        logger.info("="*70)
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
                logger.warning("Cannot parse DSN format")
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
                logger.error("="*70)
                return False
                
        except Exception as e:
            logger.error(f"[FAILED] Network test error: {str(e)}")
            logger.error("="*70)
            return False
    
    def fetch_maisha_records(self, limit: int = None, gbg_status: str = None) -> List[Dict]:
        """
        Fetch Maisha card records with GBG journey information
        
        Args:
            limit: Number of records to fetch
            gbg_status: 'APPROVED' for GBG-approved (OCR_CHECK=2), 
                       'REJECTED' for GBG-rejected (OCR_CHECK!=2), 
                       or None for all GBG-processed records
        """
        try:
            logger.info(f"Connecting to Oracle database: {self.oracle_config['dsn']}")
            connection = oracledb.connect(
                user=self.oracle_config['user'],
                password=self.oracle_config['password'],
                dsn=self.oracle_config['dsn']
            )
            
            cursor = connection.cursor()
            
            # Build query
            if self.compare_with_gbg:
                query = """
                    SELECT
                        blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
                        blob_to_clob(k.KYC_IDFRONT_CAPTURE) AS ID_PHOTO_BASE64,
                        k.KYC_ID_NO,
                        k.SESSION_ID,
                        sm.JOURNEY_ID as GBG_JOURNEY_ID,
                        sm.IS_GBG_JOURNEY_ID_CAPTURED as GBG_CAPTURED,
                        sm.IS_RETRIEVED as GBG_RETRIEVED,
                        sm.OCR_CHECK_MISMATCH as GBG_OCR_CHECK,
                        sm.CREATED_DATE as ONBOARDING_DATE
                    FROM MA.SELF_ONBOARDING_TRACKER_AWS o
                    JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
                        ON o.SESSION_ID = k.SESSION_ID
                    JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm 
                        ON sm.ID = k.SESSION_ID
                    WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
                        AND o.AWS_IMAGE IS NOT NULL
                        AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
                        AND sm.IS_GBG_JOURNEY_ID_CAPTURED = 1
                        AND sm.IS_RETRIEVED = 1
                """
                
                # Filter by OCR_CHECK_MISMATCH status
                if gbg_status == 'APPROVED':
                    query += " AND sm.OCR_CHECK_MISMATCH = 2"
                elif gbg_status == 'REJECTED':
                    query += " AND sm.OCR_CHECK_MISMATCH != 2"
                    
                query += " ORDER BY sm.CREATED_DATE DESC"
            else:
                query = """
                    SELECT
                        blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
                        blob_to_clob(k.KYC_IDFRONT_CAPTURE) AS ID_PHOTO_BASE64,
                        k.KYC_ID_NO,
                        k.SESSION_ID
                    FROM MA.SELF_ONBOARDING_TRACKER_AWS o
                    JOIN MA.SELF_ONBOARDING_TRACKER_KYC k
                        ON o.SESSION_ID = k.SESSION_ID
                    WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
                        AND o.AWS_IMAGE IS NOT NULL
                        AND k.KYC_IDFRONT_CAPTURE IS NOT NULL
                    ORDER BY k.CREATED_ON DESC
                """
            
            if limit:
                query += f" FETCH FIRST {limit} ROWS ONLY"
            
            logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
            if self.compare_with_gbg:
                if gbg_status == 'APPROVED':
                    logger.info(f"  GBG Filter: GBG-APPROVED only (OCR_CHECK_MISMATCH=2)")
                elif gbg_status == 'REJECTED':
                    logger.info(f"  GBG Filter: GBG-REJECTED only (OCR_CHECK_MISMATCH!=2)")
                else:
                    logger.info(f"  GBG Filter: ALL GBG-processed records (both approved & rejected)")
            logger.info("="*70)
            
            cursor.execute(query)
            
            records = []
            session_ids_seen = set()
            gbg_stats = {'GBG_APPROVED': 0, 'GBG_REJECTED': 0, 'GBG_UNKNOWN': 0} if self.compare_with_gbg else None
            
            for idx, row in enumerate(cursor, 1):
                try:
                    aws_image_clob = row[0]
                    id_photo_clob = row[1]
                    kyc_id = row[2]
                    session_id = row[3]
                    
                    if session_id in session_ids_seen:
                        continue
                    
                    session_ids_seen.add(session_id)
                    
                    aws_image_data = aws_image_clob.read() if aws_image_clob else None
                    id_photo_data = id_photo_clob.read() if id_photo_clob else None
                    
                    if not aws_image_data or not id_photo_data:
                        continue
                    
                    aws_image_clean = clean_base64_string(aws_image_data)
                    id_photo_clean = clean_base64_string(id_photo_data)
                    
                    try:
                        base64.b64decode(aws_image_clean[:100])
                        base64.b64decode(id_photo_clean[:100])
                    except:
                        continue
                    
                    record = {
                        'card_image_base64': id_photo_clean,
                        'face_image_base64': aws_image_clean,
                        'KYC_ID_NO': kyc_id,
                        'SESSION_ID': session_id,
                        'record_index': len(records) + 1
                    }
                    
                    if self.compare_with_gbg and len(row) > 4:
                        record['GBG_JOURNEY_ID'] = row[4]
                        record['GBG_CAPTURED'] = row[5]
                        record['GBG_RETRIEVED'] = row[6]
                        record['GBG_OCR_CHECK'] = row[7]
                        record['ONBOARDING_DATE'] = row[8] if len(row) > 8 else None
                        
                        # Use OCR_CHECK_MISMATCH to determine GBG verification
                        ocr_check = row[7]
                        if ocr_check == 2:
                            gbg_stats['GBG_APPROVED'] += 1
                        elif ocr_check is not None and ocr_check != 2:
                            gbg_stats['GBG_REJECTED'] += 1
                        else:
                            gbg_stats['GBG_UNKNOWN'] += 1
                    
                    records.append(record)
                    
                    if idx <= 5:
                        logger.info(f"  Record {idx:2d} | Session: {session_id[:20]}... | KYC: {kyc_id}")
                        if self.compare_with_gbg:
                            ocr_check = record.get('GBG_OCR_CHECK')
                            ocr_status = "GBG-APPROVED" if ocr_check == 2 else "GBG-REJECTED"
                            logger.info(f"            | Status: {ocr_status} (OCR_CHECK={ocr_check})")
                        logger.info(f"            | Card: {len(id_photo_clean)} chars | Face: {len(aws_image_clean)} chars")
                    elif idx % 20 == 0:
                        logger.info(f"  Processed {idx} records, kept {len(records)}...")
                    
                except Exception as e:
                    logger.debug(f"  Skipped record {idx}: {str(e)}")
                    continue
            
            logger.info("="*70)
            logger.info(f"[SUCCESS] Fetched {len(records)} valid card-face pairs")
            
            if self.compare_with_gbg and gbg_stats:
                logger.info(f"[SUCCESS] GBG Status Breakdown:")
                logger.info(f"           GBG-Approved (OCR=2):  {gbg_stats['GBG_APPROVED']}")
                logger.info(f"           GBG-Rejected (OCR!=2): {gbg_stats['GBG_REJECTED']}")
                if gbg_stats['GBG_UNKNOWN'] > 0:
                    logger.info(f"           Unknown (OCR=NULL):   {gbg_stats['GBG_UNKNOWN']}")
            
            logger.info("="*70)
            
            cursor.close()
            connection.close()
            
            return records
            
        except Exception as e:
            logger.error(f"[FAILED] Database error: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def verify_batch_using_client(self, records: List[Dict], batch_num: int) -> List[Dict]:
        """Verify batch using official Maisha client with CLIENT-SIDE THRESHOLD and API diagnostics"""
        logger.info(f"Preparing batch {batch_num} with {len(records)} records...")
        
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
            
            # Calculate using CLIENT threshold
            client_passed = sum(1 for r in batch_result.results 
                              if r.get('similarity_score', 0) >= self.client_threshold)
            api_passed = sum(1 for r in batch_result.results 
                            if r.get('match', False))
            
            logger.info(f"  Passed (client {self.client_threshold}%): {client_passed}")
            logger.info(f"  API 'match' field says: {api_passed}")
            
            # DIAGNOSTIC: Print detailed API response for first batch
            if batch_num == 1 and batch_result.results:
                print("\n" + "="*70)
                print(f"DIAGNOSTIC - API RESPONSE ANALYSIS (BATCH {batch_num})")
                print("="*70)
                
                # Show first 3 responses
                for idx, api_result in enumerate(batch_result.results[:3], 1):
                    print(f"\n--- Response {idx} ---")
                    print(json.dumps(api_result, indent=2))
                    
                    # Detailed field analysis
                    score = api_result.get('similarity_score', 0)
                    match = api_result.get('match', False)
                    threshold_in_response = api_result.get('threshold')
                    
                    print(f"\nDIAGNOSTIC CHECKS for Response {idx}:")
                    print(f"  Similarity Score: {score:.2f}%")
                    print(f"  API 'match' field: {match}")
                    print(f"  Threshold in response: {threshold_in_response}")
                    print(f"  Expected match (score >= 70): {score >= 70}")
                    print(f"  Actual match from API: {match}")
                    
                    # Threshold issue detection
                    if score >= 70 and not match:
                        print(f"  🚨 THRESHOLD ISSUE: Score {score:.2f}% but match=False")
                        print(f"     API is NOT using 70% threshold")
                    elif score < 70 and match:
                        print(f"  ⚠️  UNEXPECTED: Score {score:.2f}% but match=True")
                        print(f"     API threshold may be lower than 70%")
                    else:
                        print(f"  ✓ Match field aligns with expected 70% threshold")
                
                # Summary of threshold issue
                print("\n" + "-"*70)
                print("THRESHOLD ISSUE SUMMARY:")
                print("-"*70)
                
                high_score_rejected = sum(1 for r in batch_result.results 
                                         if r.get('similarity_score', 0) >= 70 
                                         and not r.get('match', False))
                low_score_approved = sum(1 for r in batch_result.results 
                                        if r.get('similarity_score', 0) < 70 
                                        and r.get('match', False))
                
                if high_score_rejected > 0:
                    print(f"❌ THRESHOLD BUG STILL PRESENT:")
                    print(f"   {high_score_rejected} records with score ≥70% rejected by API")
                    print(f"   The API 'match' field is NOT using the documented 70% threshold")
                    
                    # Find the actual threshold being used
                    rejected_scores = sorted([r.get('similarity_score', 0) 
                                            for r in batch_result.results 
                                            if not r.get('match', False)], reverse=True)
                    if rejected_scores:
                        highest_rejected = rejected_scores[0]
                        print(f"   Highest rejected score: {highest_rejected:.2f}%")
                        print(f"   Estimated API threshold: >{highest_rejected:.2f}%")
                    
                    print(f"\n   ✓ Using client-side {self.client_threshold}% threshold as fallback")
                else:
                    print(f"✓ THRESHOLD APPEARS FIXED:")
                    print(f"   API 'match' field aligns with 70% threshold")
                    print(f"   No high-scoring rejections detected")
                
                if low_score_approved > 0:
                    print(f"\n⚠️  {low_score_approved} records with score <70% approved by API")
                    print(f"   API threshold may be lower than expected")
                
                print("="*70 + "\n")
            
            # Track counters for saving sample images
            both_approved_count = 0
            both_rejected_count = 0
            
            combined_results = []
            for idx, api_result in enumerate(batch_result.results):
                if idx < len(records):
                    similarity_score = api_result.get('similarity_score', 0)
                    
                    # APPLY CLIENT-SIDE THRESHOLD
                    client_verified = similarity_score >= self.client_threshold
                    api_match = api_result.get('match', False)
                    
                    combined = {
                        'record_index': records[idx]['record_index'],
                        'session_id': records[idx]['SESSION_ID'],
                        'kyc_id_no': records[idx]['KYC_ID_NO'],
                        
                        # Use client-side threshold instead of API's broken match field
                        'aws_verified': client_verified,
                        'aws_api_match': api_match,
                        'threshold_mismatch': (similarity_score >= 70 and not api_match) or (similarity_score < 70 and api_match),
                        
                        'aws_similarity_score': similarity_score,
                        'aws_threshold': self.client_threshold,
                        'aws_api_threshold': api_result.get('threshold'),
                        'aws_comparison_method': api_result.get('comparison_method'),
                        'aws_comparison_id': api_result.get('comparison_id'),
                        'aws_error': api_result.get('error'),
                        'test_timestamp': datetime.now().isoformat()
                    }
                    
                    if self.compare_with_gbg:
                        combined['gbg_journey_id'] = records[idx].get('GBG_JOURNEY_ID')
                        combined['gbg_captured'] = records[idx].get('GBG_CAPTURED')
                        combined['gbg_retrieved'] = records[idx].get('GBG_RETRIEVED')
                        combined['gbg_ocr_check'] = records[idx].get('GBG_OCR_CHECK')
                        
                        # GBG verified = OCR_CHECK_MISMATCH = 2 (no mismatch, approved)
                        gbg_verified = (records[idx].get('GBG_OCR_CHECK') == 2)
                        
                        combined['both_verified'] = (gbg_verified and client_verified)
                        combined['gbg_verified'] = gbg_verified
                        combined['agreement'] = (gbg_verified == client_verified)
                        
                        # SAVE IMAGES FOR DISAGREEMENT CASES
                        if self.save_disagreement_images:
                            session_short = records[idx]['SESSION_ID'][:8]
                            
                            # Prepare metadata
                            metadata = {
                                'session_id': records[idx]['SESSION_ID'],
                                'kyc_id': records[idx]['KYC_ID_NO'],
                                'gbg_journey_id': records[idx].get('GBG_JOURNEY_ID'),
                                'gbg_ocr_check': records[idx].get('GBG_OCR_CHECK'),
                                'gbg_verified': gbg_verified,
                                'aws_verified': client_verified,
                                'aws_score': similarity_score,
                                'aws_threshold': self.client_threshold,
                                'agreement': combined['agreement'],
                                'onboarding_date': str(records[idx].get('ONBOARDING_DATE'))
                            }
                            
                            # Category 1: GBG APPROVED, AWS REJECTED (False Negatives)
                            if gbg_verified and not client_verified:
                                filename = f"FN_{similarity_score:.1f}pct_{session_short}"
                                self.save_image_pair(
                                    records[idx]['card_image_base64'],
                                    records[idx]['face_image_base64'],
                                    filename,
                                    self.gbg_approved_aws_rejected_dir,
                                    metadata
                                )
                            
                            # Category 2: GBG REJECTED, AWS APPROVED (False Positives)
                            elif not gbg_verified and client_verified:
                                filename = f"FP_{similarity_score:.1f}pct_{session_short}"
                                self.save_image_pair(
                                    records[idx]['card_image_base64'],
                                    records[idx]['face_image_base64'],
                                    filename,
                                    self.gbg_rejected_aws_approved_dir,
                                    metadata
                                )
                            
                            # Category 3: BOTH APPROVED (save first 10 samples)
                            elif gbg_verified and client_verified and both_approved_count < 10:
                                both_approved_count += 1
                                filename = f"PASS_{similarity_score:.1f}pct_{session_short}"
                                self.save_image_pair(
                                    records[idx]['card_image_base64'],
                                    records[idx]['face_image_base64'],
                                    filename,
                                    self.both_approved_dir,
                                    metadata
                                )
                            
                            # Category 4: BOTH REJECTED (save first 10 samples)
                            elif not gbg_verified and not client_verified and both_rejected_count < 10:
                                both_rejected_count += 1
                                filename = f"FAIL_{similarity_score:.1f}pct_{session_short}"
                                self.save_image_pair(
                                    records[idx]['card_image_base64'],
                                    records[idx]['face_image_base64'],
                                    filename,
                                    self.both_rejected_dir,
                                    metadata
                                )
                    
                    combined_results.append(combined)
            
            return combined_results
            
        except MaishaAPIError as e:
            logger.error(f"[FAILED] API Error: {e}")
            raise
        except Exception as e:
            logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
            raise
    
    def run_batch_test(self, batch_size: int = 10, total_limit: int = None, 
                       test_single: bool = False, gbg_filter: str = None) -> List[Dict]:
        """Run batch verification tests"""
        logger.info("="*70)
        if self.compare_with_gbg:
            logger.info("MAISHA CARD VERIFICATION TEST - WITH GBG COMPARISON")
        else:
            logger.info("MAISHA CARD VERIFICATION TEST")
        logger.info("="*70)
        logger.info(f"Database: {self.oracle_config['dsn']}")
        logger.info(f"Batch Size: {batch_size}")
        logger.info(f"Limit: {total_limit if total_limit else 'ALL'}")
        if self.compare_with_gbg:
            logger.info(f"GBG Filter: {gbg_filter if gbg_filter else 'ALL (approved + rejected)'}")
        logger.info(f"Client Threshold: {self.client_threshold}% (fallback threshold)")
        logger.info("="*70)
        
        if not self.test_network_connectivity():
            return []
        
        if not self.test_api_connection():
            return []
        
        records = self.fetch_maisha_records(limit=total_limit, gbg_status=gbg_filter)
        
        if not records:
            logger.warning("[WARNING] No records fetched")
            return []
        
        all_results = []
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info("="*70)
            logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
            logger.info("="*70)
            
            try:
                batch_results = self.verify_batch_using_client(batch, batch_num)
                
                for idx, result in enumerate(batch_results):
                    record_num = i + idx + 1
                    aws_verified = result.get('aws_verified', False)
                    score = result.get('aws_similarity_score', 0)
                    
                    if self.compare_with_gbg:
                        gbg_verified = result.get('gbg_verified', False)
                        agreement = result.get('agreement', False)
                        logger.info(f"  {record_num:3d}. GBG:{'✓' if gbg_verified else '✗'} "
                                  f"AWS:{'✓' if aws_verified else '✗'} "
                                  f"Agree:{'✓' if agreement else '✗'} | Score: {score:5.2f}%")
                    else:
                        logger.info(f"  {record_num:3d}. [{'✓' if aws_verified else '✗'}] Score: {score:5.2f}%")
                
                all_results.extend(batch_results)
                logger.info(f"[SUCCESS] Batch {batch_num} completed")
                
            except Exception as e:
                logger.error(f"[FAILED] Batch {batch_num} failed: {str(e)}")
                continue
        
        self.results = all_results
        
        logger.info("="*70)
        logger.info(f"TEST COMPLETED: {len(all_results)}/{len(records)} records processed")
        logger.info("="*70)
        
        return all_results
    
    def analyze_results(self) -> Dict:
        """Analyze test results with threshold issue tracking"""
        if not self.results:
            return {}
        
        total = len(self.results)
        aws_verified = sum(1 for r in self.results if r.get('aws_verified', False))
        aws_not_verified = total - aws_verified
        
        # Track threshold mismatches
        threshold_mismatches = sum(1 for r in self.results if r.get('threshold_mismatch', False))
        high_score_rejected = sum(1 for r in self.results 
                                 if r.get('aws_similarity_score', 0) >= 70 
                                 and not r.get('aws_api_match', False))
        
        scores = [r.get('aws_similarity_score', 0) for r in self.results]
        avg_score = sum(scores) / len(scores) if scores else 0
        max_score = max(scores) if scores else 0
        min_score = min(scores) if scores else 0
        
        analysis = {
            'total_tests': total,
            'aws_verified_count': aws_verified,
            'aws_not_verified_count': aws_not_verified,
            'aws_verification_rate': (aws_verified / total * 100) if total > 0 else 0,
            'avg_similarity_score': avg_score,
            'max_similarity_score': max_score,
            'min_similarity_score': min_score,
            
            # Threshold issue tracking
            'threshold_mismatches': threshold_mismatches,
            'threshold_mismatch_rate': (threshold_mismatches / total * 100) if total > 0 else 0,
            'high_score_rejected_count': high_score_rejected,
            'api_threshold_issue': high_score_rejected > 0
        }
        
        if self.compare_with_gbg:
            gbg_verified = sum(1 for r in self.results if r.get('gbg_verified', False))
            agreement = sum(1 for r in self.results if r.get('agreement', False))
            both_verified = sum(1 for r in self.results if r.get('both_verified', False))
            gbg_yes_aws_no = sum(1 for r in self.results if r.get('gbg_verified') and not r.get('aws_verified'))
            gbg_no_aws_yes = sum(1 for r in self.results if not r.get('gbg_verified') and r.get('aws_verified'))
            both_no = sum(1 for r in self.results if not r.get('gbg_verified') and not r.get('aws_verified'))
            
            # Analyze AWS scores for GBG-verified vs GBG-rejected
            gbg_verified_scores = [r.get('aws_similarity_score', 0) for r in self.results if r.get('gbg_verified')]
            gbg_rejected_scores = [r.get('aws_similarity_score', 0) for r in self.results if not r.get('gbg_verified')]
            
            # Score distribution for GBG-verified customers
            score_ranges = {
                '90-100%': sum(1 for s in gbg_verified_scores if s >= 90),
                '80-89%': sum(1 for s in gbg_verified_scores if 80 <= s < 90),
                '70-79%': sum(1 for s in gbg_verified_scores if 70 <= s < 80),
                '60-69%': sum(1 for s in gbg_verified_scores if 60 <= s < 70),
                'Below 60%': sum(1 for s in gbg_verified_scores if s < 60)
            }
            
            # Score distribution for GBG-rejected customers
            rejected_score_ranges = {
                '90-100%': sum(1 for s in gbg_rejected_scores if s >= 90),
                '80-89%': sum(1 for s in gbg_rejected_scores if 80 <= s < 90),
                '70-79%': sum(1 for s in gbg_rejected_scores if 70 <= s < 80),
                '60-69%': sum(1 for s in gbg_rejected_scores if 60 <= s < 70),
                'Below 60%': sum(1 for s in gbg_rejected_scores if s < 60)
            }
            
            # Find problematic cases
            low_score_gbg_verified = [r for r in self.results 
                                     if r.get('gbg_verified') and r.get('aws_similarity_score', 0) < 60]
            high_score_gbg_rejected = [r for r in self.results 
                                      if not r.get('gbg_verified') and r.get('aws_similarity_score', 0) >= 80]
            
            analysis.update({
                'gbg_verified_count': gbg_verified,
                'gbg_not_verified_count': total - gbg_verified,
                'gbg_verification_rate': (gbg_verified / total * 100) if total > 0 else 0,
                'agreement_count': agreement,
                'agreement_rate': (agreement / total * 100) if total > 0 else 0,
                'both_verified_count': both_verified,
                'both_no': both_no,
                'gbg_yes_aws_no': gbg_yes_aws_no,
                'gbg_no_aws_yes': gbg_no_aws_yes,
                
                # GBG-verified customer score analysis
                'gbg_verified_avg_score': sum(gbg_verified_scores) / len(gbg_verified_scores) if gbg_verified_scores else 0,
                'gbg_verified_max_score': max(gbg_verified_scores) if gbg_verified_scores else 0,
                'gbg_verified_min_score': min(gbg_verified_scores) if gbg_verified_scores else 0,
                'gbg_verified_score_distribution': score_ranges,
                
                # GBG-rejected customer score analysis  
                'gbg_rejected_avg_score': sum(gbg_rejected_scores) / len(gbg_rejected_scores) if gbg_rejected_scores else 0,
                'gbg_rejected_max_score': max(gbg_rejected_scores) if gbg_rejected_scores else 0,
                'gbg_rejected_min_score': min(gbg_rejected_scores) if gbg_rejected_scores else 0,
                'gbg_rejected_score_distribution': rejected_score_ranges,
                
                # Problematic cases
                'low_score_gbg_verified_count': len(low_score_gbg_verified),
                'high_score_gbg_rejected_count': len(high_score_gbg_rejected),
                
                # False rates
                'false_negative_rate': (gbg_yes_aws_no / gbg_verified * 100) if gbg_verified > 0 else 0,
                'false_positive_rate': (gbg_no_aws_yes / (total - gbg_verified) * 100) if (total - gbg_verified) > 0 else 0
            })
        
        return analysis
    
    def export_csv(self, output_file: str = None) -> str:
        """Export results to CSV"""
        if not self.results:
            return None
        
        if not output_file:
            output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        columns = list(self.results[0].keys()) if self.results else []
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.results)
        
        logger.info(f"[SUCCESS] CSV exported: {output_file}")
        return output_file
    
    def print_summary(self, analysis: Dict):
        """Print enhanced summary with threshold issue status and image save locations"""
        print("\n" + "="*70)
        if self.compare_with_gbg:
            print("MAISHA CARD VERIFICATION - GBG vs AWS COMPARISON")
        else:
            print("MAISHA CARD VERIFICATION - TEST SUMMARY")
        print("="*70)
        
        # Threshold Issue Status
        if analysis.get('api_threshold_issue', False):
            print("\n🚨 API THRESHOLD ISSUE DETECTED:")
            print(f"  {analysis.get('high_score_rejected_count', 0)} records with score ≥70% were rejected by API")
            print(f"  The API 'match' field is NOT using the documented 70% threshold")
            print(f"  ✓ Using client-side {self.client_threshold}% threshold for accurate results")
        else:
            print("\n✓ API THRESHOLD STATUS:")
            print(f"  API 'match' field appears to align with 70% threshold")
            print(f"  No threshold issue detected in this test")
        
        if analysis.get('threshold_mismatch_rate', 0) > 0:
            print(f"\nThreshold Mismatches: {analysis.get('threshold_mismatches', 0)} ({analysis.get('threshold_mismatch_rate', 0):.1f}%)")
        
        print()
        
        if self.compare_with_gbg:
            print(f"TOTAL RECORDS: {analysis.get('total_tests', 0):,}")
            
            print(f"\nGBG RESULTS:")
            print(f"  ✓ Approved (OCR=2):     {analysis.get('gbg_verified_count', 0):,} ({analysis.get('gbg_verification_rate', 0):.1f}%)")
            print(f"  ✗ Rejected (OCR!=2):    {analysis.get('gbg_not_verified_count', 0):,}")
            
            print(f"\nAWS RESULTS (with {self.client_threshold}% threshold):")
            print(f"  ✓ Verified:     {analysis.get('aws_verified_count', 0):,} ({analysis.get('aws_verification_rate', 0):.1f}%)")
            print(f"  ✗ Not Verified: {analysis.get('aws_not_verified_count', 0):,}")
            
            print(f"\nAGREEMENT ANALYSIS:")
            print(f"  Overall Agreement:     {analysis.get('agreement_rate', 0):.1f}%")
            print(f"  ✓✓ Both Verified:      {analysis.get('both_verified_count', 0):,}")
            print(f"  ✗✗ Both Rejected:      {analysis.get('both_no', 0):,}")
            print(f"  ✓✗ GBG Approved, AWS Rejected:    {analysis.get('gbg_yes_aws_no', 0):,} (False Negatives: {analysis.get('false_negative_rate', 0):.1f}%)")
            print(f"  ✗✓ GBG Rejected, AWS Approved:    {analysis.get('gbg_no_aws_yes', 0):,} (False Positives: {analysis.get('false_positive_rate', 0):.1f}%)")
            
            print(f"\nAWS SCORES FOR GBG-APPROVED CUSTOMERS:")
            print(f"  Average:  {analysis.get('gbg_verified_avg_score', 0):.2f}%")
            print(f"  Maximum:  {analysis.get('gbg_verified_max_score', 0):.2f}%")
            print(f"  Minimum:  {analysis.get('gbg_verified_min_score', 0):.2f}%")
            
            print(f"\n  Score Distribution (GBG-approved customers):")
            dist = analysis.get('gbg_verified_score_distribution', {})
            for range_name, count in dist.items():
                pct = (count / analysis.get('gbg_verified_count', 1) * 100) if analysis.get('gbg_verified_count', 0) > 0 else 0
                print(f"    {range_name:12s}: {count:3d} customers ({pct:5.1f}%)")
            
            if analysis.get('gbg_rejected_avg_score', 0) >= 0:
                print(f"\nAWS SCORES FOR GBG-REJECTED CUSTOMERS:")
                print(f"  Average:  {analysis.get('gbg_rejected_avg_score', 0):.2f}%")
                print(f"  Maximum:  {analysis.get('gbg_rejected_max_score', 0):.2f}%")
                print(f"  Minimum:  {analysis.get('gbg_rejected_min_score', 0):.2f}%")
                
                print(f"\n  Score Distribution (GBG-rejected customers):")
                rej_dist = analysis.get('gbg_rejected_score_distribution', {})
                for range_name, count in rej_dist.items():
                    rej_count = analysis.get('gbg_not_verified_count', 1)
                    pct = (count / rej_count * 100) if rej_count > 0 else 0
                    print(f"    {range_name:12s}: {count:3d} customers ({pct:5.1f}%)")
            
            print(f"\nPROBLEMATIC CASES:")
            print(f"  GBG✓ but AWS score <60%:  {analysis.get('low_score_gbg_verified_count', 0):,} cases")
            print(f"  GBG✗ but AWS score ≥80%:  {analysis.get('high_score_gbg_rejected_count', 0):,} cases")
            
        else:
            print(f"\nTotal: {analysis.get('total_tests', 0):,}")
            print(f"Verified: {analysis.get('aws_verified_count', 0):,} ({analysis.get('aws_verification_rate', 0):.1f}%)")
        
        print(f"\nOVERALL AWS SIMILARITY SCORES:")
        print(f"  Average:  {analysis.get('avg_similarity_score', 0):.2f}%")
        print(f"  Maximum:  {analysis.get('max_similarity_score', 0):.2f}%")
        print(f"  Minimum:  {analysis.get('min_similarity_score', 0):.2f}%")
        
        # Image save locations
        if self.save_disagreement_images:
            print(f"\nIMAGES SAVED TO: {self.images_dir}/")
            print(f"  1. GBG Approved, AWS Rejected: {self.gbg_approved_aws_rejected_dir.name}/")
            print(f"  2. GBG Rejected, AWS Approved: {self.gbg_rejected_aws_approved_dir.name}/")
            print(f"  3. Both Approved (samples):    {self.both_approved_dir.name}/")
            print(f"  4. Both Rejected (samples):    {self.both_rejected_dir.name}/")
            print(f"\nEach folder contains:")
            print(f"  - *_CARD.jpg  (ID card image)")
            print(f"  - *_FACE.jpg  (Selfie image)")
            print(f"  - *_INFO.json (Metadata with scores and IDs)")
        
        print("="*70 + "\n")


def main():
    oracle_config = {
        'user': 'MA',
        'password': 'wU8n1av8U$#OLt7pRePrOd',
        'dsn': 'copkdresb-scan:1561/MONAPREPROD'
    }
    
    api_key = "dab4424126543da8cffb8e250a63196957ee12a11312da23bf088db4f8dbb982"
    api_base_url = "https://18.235.35.175"
    
    compare_with_gbg = True
    gbg_filter = None  # Test BOTH approved AND rejected (set to None for all)
    batch_size = 5
    total_limit = 200  # 200 records total (mix of approved and rejected)
    save_images = True  # Save disagreement images
    
    print("\n" + "="*70)
    print("MAISHA VERIFICATION - COMPREHENSIVE GBG COMPARISON WITH IMAGE EXPORT")
    print("="*70)
    print(f"Database: {oracle_config['dsn']}")
    print(f"Testing: {total_limit} GBG-processed records (BOTH approved & rejected)")
    print(f"Threshold: {CLIENT_THRESHOLD}% (client-side fallback)")
    print(f"Save Images: YES - All disagreements + samples")
    print(f"Note: OCR_CHECK_MISMATCH=2 → GBG approved, !=2 → GBG rejected")
    print("="*70 + "\n")
    
    try:
        tester = MaishaVerificationTester(
            oracle_config=oracle_config,
            api_key=api_key,
            api_base_url=api_base_url,
            compare_with_gbg=compare_with_gbg,
            client_threshold=CLIENT_THRESHOLD,
            save_disagreement_images=save_images
        )
        
        results = tester.run_batch_test(
            batch_size=batch_size,
            total_limit=total_limit,
            test_single=False,
            gbg_filter=gbg_filter
        )
        
        if not results:
            print("[FAILED] No results\n")
            return
        
        analysis = tester.analyze_results()
        tester.print_summary(analysis)
        
        csv_file = tester.export_csv()
        print(f"\n[SUCCESS] CSV Results: {csv_file}")
        
        if save_images:
            print(f"\n[SUCCESS] Images saved to: {tester.images_dir}/")
            print(f"\nTO REVIEW WITH YOUR BOSS:")
            print(f"  1. Open: {tester.images_dir}/")
            print(f"  2. Check folder: 1_GBG_APPROVED_AWS_REJECTED/")
            print(f"     - These are FALSE NEGATIVES (legitimate customers AWS rejected)")
            print(f"  3. Check folder: 2_GBG_REJECTED_AWS_APPROVED/")
            print(f"     - These are FALSE POSITIVES (suspicious cases AWS approved)")
            print(f"  4. Each image pair has:")
            print(f"     - *_CARD.jpg  (ID card)")
            print(f"     - *_FACE.jpg  (Selfie)")
            print(f"     - *_INFO.json (Scores, IDs, timestamps)")
        
        print("\n" + "="*70)
        print("TEST COMPLETE - Ready for review!")
        print("="*70 + "\n")
        
    except Exception as e:
        logger.error(f"[FAILED] {str(e)}")


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()