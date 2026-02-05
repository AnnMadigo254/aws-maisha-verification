# import oracledb
# import requests
# import json
# import base64
# import csv
# from datetime import datetime
# from typing import List, Dict
# import logging
# from pathlib import Path

# # Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(f'maisha_verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)

# class MaishaVerificationTester:
#     def __init__(self, 
#                  oracle_config: Dict[str, str],
#                  api_base_url: str = "https://18.235.35.175",
#                  use_local: bool = False,
#                  save_sample_images: bool = False):
#         """Initialize the Maisha verification tester"""
#         self.oracle_config = oracle_config
#         self.api_base_url = "http://localhost:8000" if use_local else api_base_url
#         self.use_local = use_local
#         self.token = None
#         self.results = []
#         self.save_sample_images = save_sample_images
        
#         if self.save_sample_images:
#             self.images_dir = Path(f"sample_images_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
#             self.images_dir.mkdir(exist_ok=True)
#             logger.info(f"Sample images will be saved to: {self.images_dir}")
    
#     def save_image_from_base64(self, base64_str: str, filename: str) -> str:
#         """Save a base64 string as an image file"""
#         try:
#             image_data = base64.b64decode(base64_str)
#             filepath = self.images_dir / filename
#             with open(filepath, 'wb') as f:
#                 f.write(image_data)
#             file_size = len(image_data) / 1024
#             logger.info(f"    ✓ {filename} ({file_size:.1f}KB)")
#             return str(filepath)
#         except Exception as e:
#             logger.error(f"    ❌ Failed to save {filename}: {str(e)}")
#             return None
    
#     def get_auth_token(self) -> str:
#         """Get JWT authentication token"""
#         try:
#             url = f"{self.api_base_url}/auth/token"
#             response = requests.post(
#                 url,
#                 headers={"Content-Type": "application/json"},
#                 verify=False if not self.use_local else True,
#                 timeout=30
#             )
#             response.raise_for_status()
#             self.token = response.json()['token']
#             logger.info("✓ Authentication successful")
#             return self.token
#         except Exception as e:
#             logger.error(f"❌ Failed to get auth token: {str(e)}")
#             raise
    
#     def fetch_maisha_records(self, limit: int = None) -> List[Dict]:
#         """
#         Fetch Maisha card records from Oracle database
        
#         IMPORTANT: Checking if AWS_IMAGE and ID_PHOTO are swapped!
#         """
#         try:
#             logger.info("Connecting to Oracle database...")
#             connection = oracledb.connect(
#                 user=self.oracle_config['user'],
#                 password=self.oracle_config['password'],
#                 dsn=self.oracle_config['dsn']
#             )
            
#             cursor = connection.cursor()
            
#             query = """
#                 SELECT 
#                     o.AWS_IMAGE,
#                     o.ID_PHOTO,
#                     k.KYC_ID_NO,
#                     o.SESSION_ID
#                 FROM MA.SELF_ONBOARDING_TRACKER_OCR o
#                 JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
#                     ON o.SESSION_ID = k.SESSION_ID
#                 WHERE k.ID_TYPE = 'MAISHA_CARD'
#                     AND o.AWS_IMAGE IS NOT NULL
#                     AND o.ID_PHOTO IS NOT NULL
#                 ORDER BY o.SESSION_ID
#             """
            
#             if limit:
#                 query += f" FETCH FIRST {limit} ROWS ONLY"
            
#             logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
#             cursor.execute(query)
            
#             records = []
#             session_ids_seen = set()
            
#             for idx, row in enumerate(cursor, 1):
#                 try:
#                     session_id = row[3]
#                     kyc_id = row[2]
                    
#                     if session_id in session_ids_seen:
#                         continue
                    
#                     session_ids_seen.add(session_id)
                    
#                     # Read BLOB data
#                     aws_image_blob = row[0].read()  # AWS_IMAGE column
#                     id_photo_blob = row[1].read()   # ID_PHOTO column
                    
#                     # Convert bytes to string
#                     aws_image_base64 = aws_image_blob.decode('utf-8')
#                     id_photo_base64 = id_photo_blob.decode('utf-8')
                    
#                     # SWAP THEM! Based on your observation:
#                     # AWS_IMAGE appears to be the FACE (selfie)
#                     # ID_PHOTO appears to be the CARD
#                     record = {
#                         'card_image_base64': id_photo_base64,    # ID_PHOTO → card
#                         'face_image_base64': aws_image_base64,   # AWS_IMAGE → face
#                         'KYC_ID_NO': kyc_id,
#                         'SESSION_ID': session_id,
#                         'record_index': len(records) + 1
#                     }
#                     records.append(record)
                    
#                     # Save sample images with SWAPPED labels
#                     if self.save_sample_images and idx <= 5:
#                         logger.info(f"\n  Record {idx}: Session {session_id[:16]}... KYC: {kyc_id}")
#                         logger.info(f"  NOTE: AWS_IMAGE → face, ID_PHOTO → card (SWAPPED!)")
#                         short_id = session_id[:8]
                        
#                         # Save what AWS_IMAGE contains (face)
#                         face_filename = f"{idx:02d}_AWS_IMAGE_(face)_{short_id}.jpg"
#                         self.save_image_from_base64(aws_image_base64, face_filename)
                        
#                         # Save what ID_PHOTO contains (card)
#                         card_filename = f"{idx:02d}_ID_PHOTO_(card)_{short_id}.jpg"
#                         self.save_image_from_base64(id_photo_base64, card_filename)
                    
#                     if idx % 10 == 0 and not self.save_sample_images:
#                         logger.info(f"  Processed {idx} records...")
                    
#                 except Exception as e:
#                     logger.warning(f"  ❌ Failed record {idx}: {str(e)}")
#                     continue
            
#             logger.info(f"\n✓ Fetched {len(records)} unique card-face pairs")
#             logger.info(f"⚠️  SWAPPED: AWS_IMAGE→face, ID_PHOTO→card\n")
            
#             cursor.close()
#             connection.close()
            
#             return records
            
#         except Exception as e:
#             logger.error(f"❌ Database error: {str(e)}")
#             raise
    
#     def call_batch_verify(self, records: List[Dict], batch_num: int) -> List[Dict]:
#         """Call batch verify API"""
#         if not self.token:
#             self.get_auth_token()
        
#         url = f"{self.api_base_url}/test/batch-verify"
        
#         # Build payload with CORRECTED mapping
#         verifications = []
#         for r in records:
#             verification = {
#                 "card_image_key": r['card_image_base64'],  # Now correctly the card
#                 "face_image_key": r['face_image_base64']   # Now correctly the face
#             }
#             verifications.append(verification)
        
#         payload = {"verifications": verifications}
        
#         payload_size_mb = len(json.dumps(payload)) / (1024 * 1024)
#         logger.info(f"Sending batch {batch_num}: {len(records)} records, {payload_size_mb:.2f}MB")
        
#         try:
#             response = requests.post(
#                 url,
#                 headers={
#                     "Content-Type": "application/json",
#                     "Authorization": f"Bearer {self.token}"
#                 },
#                 json=payload,
#                 verify=False if not self.use_local else True,
#                 timeout=180
#             )
            
#             response.raise_for_status()
#             result = response.json()
            
#             logger.info(f"✓ Batch {batch_num} completed")
            
#             # Combine results
#             combined_results = []
#             api_results = result.get('results', [])
            
#             for idx, api_result in enumerate(api_results):
#                 combined = {
#                     'session_id': records[idx]['SESSION_ID'],
#                     'kyc_id_no': records[idx]['KYC_ID_NO'],
#                     'record_index': records[idx]['record_index'],
#                     'verified': api_result.get('verified', False),
#                     'confidence': api_result.get('confidence'),
#                     'deepface_verified': api_result.get('deepface_verified'),
#                     'deepface_distance': api_result.get('deepface_distance'),
#                     'aws_verified': api_result.get('aws_verified'),
#                     'rekognition_confidence': api_result.get('rekognition_confidence'),
#                     'similarity_score': api_result.get('similarity_score'),
#                     'quorum_agreement': api_result.get('quorum_agreement'),
#                     'error': api_result.get('error'),
#                     'verification_id': api_result.get('verification_id'),
#                     'test_timestamp': datetime.now().isoformat()
#                 }
#                 combined_results.append(combined)
            
#             return combined_results
            
#         except Exception as e:
#             logger.error(f"❌ Batch {batch_num} failed: {str(e)}")
#             raise
    
#     def run_batch_test(self, batch_size: int = 5, total_limit: int = None) -> List[Dict]:
#         """Run batch verification tests"""
#         logger.info("="*70)
#         logger.info("MAISHA CARD VERIFICATION TEST - WITH CORRECTED MAPPING")
#         logger.info("="*70)
#         logger.info(f"API Endpoint: {self.api_base_url}")
#         logger.info(f"Batch Size: {batch_size}")
#         logger.info(f"Limit: {total_limit if total_limit else 'ALL'}")
#         logger.info(f"FIX: Swapping AWS_IMAGE→face and ID_PHOTO→card")
#         if self.save_sample_images:
#             logger.info(f"Save Images: Yes (first 5 records)")
#         logger.info("="*70 + "\n")
        
#         # Fetch records
#         records = self.fetch_maisha_records(limit=total_limit)
        
#         if not records:
#             logger.warning("No records found")
#             return []
        
#         all_results = []
#         total_batches = (len(records) + batch_size - 1) // batch_size
        
#         # Process in batches
#         for i in range(0, len(records), batch_size):
#             batch = records[i:i + batch_size]
#             batch_num = (i // batch_size) + 1
            
#             logger.info(f"\n{'='*70}")
#             logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
#             logger.info("="*70)
            
#             try:
#                 batch_results = self.call_batch_verify(batch, batch_num)
                
#                 # Log results
#                 for idx, result in enumerate(batch_results):
#                     record_num = i + idx + 1
#                     verified = result['verified']
#                     confidence = result.get('confidence', 0) or 0
                    
#                     deepface = "✓" if result.get('deepface_verified') else "✗"
#                     aws = "✓" if result.get('aws_verified') else "✗"
                    
#                     status = "✓ VERIFIED" if verified else "✗ NOT VERIFIED"
#                     logger.info(f"  {record_num}. {status} (conf: {confidence:.2f}) [DF:{deepface} AWS:{aws}]")
                
#                 all_results.extend(batch_results)
#                 logger.info(f"✓ Batch {batch_num} completed\n")
                
#             except Exception as e:
#                 logger.error(f"❌ Batch {batch_num} failed: {str(e)}\n")
#                 continue
        
#         self.results = all_results
        
#         logger.info(f"{'='*70}")
#         logger.info(f"TEST COMPLETED: {len(all_results)}/{len(records)} records")
#         logger.info("="*70 + "\n")
        
#         return all_results
    
#     def analyze_results(self) -> Dict:
#         """Analyze test results"""
#         if not self.results:
#             return {}
        
#         total = len(self.results)
#         verified = sum(1 for r in self.results if r.get('verified', False))
#         not_verified = sum(1 for r in self.results if r.get('verified') == False and not r.get('error'))
#         failed = sum(1 for r in self.results if r.get('error'))
        
#         deepface_verified = sum(1 for r in self.results if r.get('deepface_verified', False))
#         aws_verified = sum(1 for r in self.results if r.get('aws_verified', False))
#         both_verified = sum(1 for r in self.results 
#                           if r.get('deepface_verified', False) and r.get('aws_verified', False))
        
#         return {
#             'total_tests': total,
#             'verified_count': verified,
#             'not_verified_count': not_verified,
#             'failed_count': failed,
#             'verification_rate': (verified / total * 100) if total > 0 else 0,
#             'deepface_verified': deepface_verified,
#             'aws_verified': aws_verified,
#             'both_verified': both_verified,
#         }
    
#     def export_csv(self, output_file: str = None) -> str:
#         """Export results to CSV"""
#         if not self.results:
#             return None
        
#         if not output_file:
#             output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
#         columns = [
#             'record_index', 'session_id', 'kyc_id_no', 'verified', 'confidence',
#             'deepface_verified', 'deepface_distance', 'aws_verified',
#             'rekognition_confidence', 'similarity_score', 'quorum_agreement',
#             'error', 'verification_id', 'test_timestamp'
#         ]
        
#         with open(output_file, 'w', newline='', encoding='utf-8') as f:
#             writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
#             writer.writeheader()
#             writer.writerows(self.results)
        
#         logger.info(f"✓ CSV exported: {output_file}")
#         return output_file
    
#     def print_summary(self, analysis: Dict):
#         """Print summary to console"""
#         print("\n" + "="*70)
#         print("MAISHA CARD VERIFICATION - TEST SUMMARY")
#         print("="*70)
#         print(f"\nOVERALL RESULTS:")
#         print(f"  Total Tests:       {analysis.get('total_tests', 0)}")
#         print(f"  ✓ Verified:        {analysis.get('verified_count', 0)} ({analysis.get('verification_rate', 0):.1f}%)")
#         print(f"  ✗ Not Verified:    {analysis.get('not_verified_count', 0)}")
#         print(f"  ❌ Failed/Errors:  {analysis.get('failed_count', 0)}")
        
#         print(f"\nVERIFICATION METHODS:")
#         print(f"  DeepFace Verified: {analysis.get('deepface_verified', 0)}")
#         print(f"  AWS Verified:      {analysis.get('aws_verified', 0)}")
#         print(f"  Both Verified:     {analysis.get('both_verified', 0)}")
#         print("="*70 + "\n")


# def main():
#     """Main execution"""
    
#     oracle_config = {
#         'user': 'MA',
#         'password': 'wU8n1av8U$#OLtiq0MrtT',
#         'dsn': '172.16.17.29:1561/MONA'
#     }
    
#     # Configuration
#     use_local = False
#     batch_size = 5
#     save_sample_images = True   # Save to verify the swap is correct
#     total_limit = 20            # Test 20 records
    
#     # Initialize tester
#     tester = MaishaVerificationTester(
#         oracle_config=oracle_config,
#         use_local=use_local,
#         save_sample_images=save_sample_images
#     )
    
#     # Run test
#     results = tester.run_batch_test(
#         batch_size=batch_size,
#         total_limit=total_limit
#     )
    
#     if not results:
#         return
    
#     # Analyze and print results
#     analysis = tester.analyze_results()
#     tester.print_summary(analysis)
    
#     # Export results
#     csv_file = tester.export_csv()
#     print(f"📊 Results: {csv_file}")
#     if save_sample_images:
#         print(f"🖼️  Images: {tester.images_dir}/")
#         print(f"   Check filenames - they now show what each column contains!\n")


# if __name__ == "__main__":
#     import urllib3
#     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
#     main()









# # import oracledb
# # import requests
# # import json
# # import base64
# # import csv
# # from datetime import datetime
# # from typing import List, Dict
# # import logging
# # from pathlib import Path

# # # Set up logging
# # logging.basicConfig(
# #     level=logging.DEBUG,  # Changed to DEBUG for more detail
# #     format='%(asctime)s - %(levelname)s - %(message)s',
# #     handlers=[
# #         logging.FileHandler(f'maisha_verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
# #         logging.StreamHandler()
# #     ]
# # )
# # logger = logging.getLogger(__name__)

# # class MaishaVerificationTester:
# #     def __init__(self, 
# #                  oracle_config: Dict[str, str],
# #                  api_base_url: str = "https://18.235.35.175",
# #                  use_local: bool = False):
# #         """Initialize the Maisha verification tester"""
# #         self.oracle_config = oracle_config
# #         self.api_base_url = "http://localhost:8000" if use_local else api_base_url
# #         self.use_local = use_local
# #         self.token = None
# #         self.results = []
    
# #     def get_auth_token(self) -> str:
# #         """Get JWT authentication token"""
# #         try:
# #             url = f"{self.api_base_url}/auth/token"
# #             response = requests.post(
# #                 url,
# #                 headers={"Content-Type": "application/json"},
# #                 verify=False if not self.use_local else True,
# #                 timeout=30
# #             )
# #             response.raise_for_status()
# #             self.token = response.json()['token']
# #             logger.info("✓ Authentication successful")
# #             return self.token
# #         except Exception as e:
# #             logger.error(f"❌ Failed to get auth token: {str(e)}")
# #             raise
    
# #     def fetch_maisha_records(self, limit: int = None) -> List[Dict]:
# #         """Fetch Maisha card records - SWAP BACK to original"""
# #         try:
# #             logger.info("Connecting to Oracle database...")
# #             connection = oracledb.connect(
# #                 user=self.oracle_config['user'],
# #                 password=self.oracle_config['password'],
# #                 dsn=self.oracle_config['dsn']
# #             )
            
# #             cursor = connection.cursor()
            
# #             query = """
# #                 SELECT 
# #                     o.AWS_IMAGE,
# #                     o.ID_PHOTO,
# #                     k.KYC_ID_NO,
# #                     o.SESSION_ID
# #                 FROM MA.SELF_ONBOARDING_TRACKER_OCR o
# #                 JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
# #                     ON o.SESSION_ID = k.SESSION_ID
# #                 WHERE k.ID_TYPE = 'MAISHA_CARD'
# #                     AND o.AWS_IMAGE IS NOT NULL
# #                     AND o.ID_PHOTO IS NOT NULL
# #                 ORDER BY o.SESSION_ID
# #             """
            
# #             if limit:
# #                 query += f" FETCH FIRST {limit} ROWS ONLY"
            
# #             logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
# #             cursor.execute(query)
            
# #             records = []
# #             session_ids_seen = set()
            
# #             for idx, row in enumerate(cursor, 1):
# #                 try:
# #                     session_id = row[3]
# #                     kyc_id = row[2]
                    
# #                     if session_id in session_ids_seen:
# #                         continue
                    
# #                     session_ids_seen.add(session_id)
                    
# #                     # Read BLOB data - USE ORIGINAL MAPPING
# #                     card_blob = row[0].read()  # AWS_IMAGE
# #                     face_blob = row[1].read()  # ID_PHOTO
                    
# #                     # Convert to base64 strings
# #                     card_base64 = card_blob.decode('utf-8')
# #                     face_base64 = face_blob.decode('utf-8')
                    
# #                     record = {
# #                         'card_image_base64': card_base64,
# #                         'face_image_base64': face_base64,
# #                         'KYC_ID_NO': kyc_id,
# #                         'SESSION_ID': session_id,
# #                         'record_index': len(records) + 1
# #                     }
# #                     records.append(record)
                    
# #                     if idx % 10 == 0:
# #                         logger.info(f"  Processed {idx} records...")
                    
# #                 except Exception as e:
# #                     logger.warning(f"  ❌ Failed record {idx}: {str(e)}")
# #                     continue
            
# #             logger.info(f"\n✓ Fetched {len(records)} unique card-face pairs\n")
            
# #             cursor.close()
# #             connection.close()
            
# #             return records
            
# #         except Exception as e:
# #             logger.error(f"❌ Database error: {str(e)}")
# #             raise
    
# #     def call_batch_verify(self, records: List[Dict], batch_num: int) -> List[Dict]:
# #         """Call batch verify API with FULL RESPONSE LOGGING"""
# #         if not self.token:
# #             self.get_auth_token()
        
# #         url = f"{self.api_base_url}/test/batch-verify"
        
# #         # Build payload
# #         verifications = []
# #         for r in records:
# #             verification = {
# #                 "card_image_key": r['card_image_base64'],
# #                 "face_image_key": r['face_image_base64']
# #             }
# #             verifications.append(verification)
        
# #         payload = {"verifications": verifications}
        
# #         payload_size_mb = len(json.dumps(payload)) / (1024 * 1024)
# #         logger.info(f"Sending batch {batch_num}: {len(records)} records, {payload_size_mb:.2f}MB")
        
# #         try:
# #             response = requests.post(
# #                 url,
# #                 headers={
# #                     "Content-Type": "application/json",
# #                     "Authorization": f"Bearer {self.token}"
# #                 },
# #                 json=payload,
# #                 verify=False if not self.use_local else True,
# #                 timeout=180
# #             )
            
# #             response.raise_for_status()
# #             result = response.json()
            
# #             logger.info(f"✓ Batch {batch_num} completed")
            
# #             # PRINT FULL API RESPONSE
# #             print("\n" + "="*70)
# #             print(f"FULL API RESPONSE - BATCH {batch_num}")
# #             print("="*70)
# #             print(json.dumps(result, indent=2))
# #             print("="*70 + "\n")
            
# #             # Combine results
# #             combined_results = []
# #             api_results = result.get('results', [])
            
# #             for idx, api_result in enumerate(api_results):
# #                 combined = {
# #                     'session_id': records[idx]['SESSION_ID'],
# #                     'kyc_id_no': records[idx]['KYC_ID_NO'],
# #                     'record_index': records[idx]['record_index'],
# #                     'verified': api_result.get('verified', False),
# #                     'confidence': api_result.get('confidence'),
# #                     'deepface_verified': api_result.get('deepface_verified'),
# #                     'deepface_distance': api_result.get('deepface_distance'),
# #                     'aws_verified': api_result.get('aws_verified'),
# #                     'rekognition_confidence': api_result.get('rekognition_confidence'),
# #                     'similarity_score': api_result.get('similarity_score'),
# #                     'quorum_agreement': api_result.get('quorum_agreement'),
# #                     'error': api_result.get('error'),
# #                     'message': api_result.get('message'),
# #                     'verification_id': api_result.get('verification_id'),
# #                     'test_timestamp': datetime.now().isoformat()
# #                 }
# #                 combined_results.append(combined)
            
# #             return combined_results
            
# #         except Exception as e:
# #             logger.error(f"❌ Batch {batch_num} failed: {str(e)}")
# #             raise
    
# #     def run_batch_test(self, batch_size: int = 3, total_limit: int = 3) -> List[Dict]:
# #         """Run test with JUST 3 RECORDS to see detailed response"""
# #         logger.info("="*70)
# #         logger.info("MAISHA VERIFICATION - DIAGNOSTIC TEST")
# #         logger.info("="*70)
# #         logger.info(f"API Endpoint: {self.api_base_url}")
# #         logger.info(f"Testing {total_limit} records to see full API response")
# #         logger.info("="*70 + "\n")
        
# #         # Fetch records
# #         records = self.fetch_maisha_records(limit=total_limit)
        
# #         if not records:
# #             logger.warning("No records found")
# #             return []
        
# #         # Process ONE batch to see full response
# #         logger.info(f"\n{'='*70}")
# #         logger.info(f"PROCESSING {len(records)} RECORDS")
# #         logger.info("="*70)
        
# #         try:
# #             batch_results = self.call_batch_verify(records, 1)
# #             self.results = batch_results
# #             return batch_results
# #         except Exception as e:
# #             logger.error(f"❌ Test failed: {str(e)}")
# #             return []
    
# #     def export_csv(self, output_file: str = None) -> str:
# #         """Export results to CSV"""
# #         if not self.results:
# #             return None
        
# #         if not output_file:
# #             output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
# #         # Get all keys from results
# #         all_keys = set()
# #         for r in self.results:
# #             all_keys.update(r.keys())
        
# #         columns = sorted(all_keys)
        
# #         with open(output_file, 'w', newline='', encoding='utf-8') as f:
# #             writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
# #             writer.writeheader()
# #             writer.writerows(self.results)
        
# #         logger.info(f"✓ CSV exported: {output_file}")
# #         return output_file


# # def main():
# #     """Main execution"""
    
# #     oracle_config = {
# #         'user': 'MA',
# #         'password': 'wU8n1av8U$#OLtiq0MrtT',
# #         'dsn': '172.16.17.29:1561/MONA'
# #     }
    
# #     # Test with JUST 3 RECORDS to see full API response
# #     tester = MaishaVerificationTester(
# #         oracle_config=oracle_config,
# #         use_local=False
# #     )
    
# #     # Run diagnostic test
# #     results = tester.run_batch_test(batch_size=3, total_limit=3)
    
# #     if results:
# #         csv_file = tester.export_csv()
# #         print(f"\n📊 Results exported: {csv_file}\n")
# #         print("CHECK THE FULL API RESPONSE ABOVE ☝️")
# #         print("Look for 'error', 'message', or other fields that explain why verification is failing\n")


# # if __name__ == "__main__":
# #     import urllib3
# #     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
# #     main()


import oracledb
import requests
import json
import csv
from datetime import datetime
from typing import List, Dict
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'maisha_verification_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MaishaVerificationTester:
    def __init__(self, 
                 oracle_config: Dict[str, str],
                 api_base_url: str = "https://18.235.35.175",
                 use_local: bool = False):
        """Initialize the Maisha verification tester"""
        self.oracle_config = oracle_config
        self.api_base_url = "http://localhost:8000" if use_local else api_base_url
        self.use_local = use_local
        self.token = None
        self.results = []
    
    def get_auth_token(self) -> str:
        """Get JWT authentication token"""
        try:
            url = f"{self.api_base_url}/auth/token"
            response = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                verify=False if not self.use_local else True,
                timeout=30
            )
            response.raise_for_status()
            self.token = response.json()['token']
            logger.info("✓ Authentication successful")
            return self.token
        except Exception as e:
            logger.error(f"❌ Failed to get auth token: {str(e)}")
            raise
    
    def fetch_maisha_records(self, limit: int = None) -> List[Dict]:
        """
        Fetch Maisha card records using blob_to_clob conversion
        This gets data that's already base64 encoded
        """
        try:
            logger.info(f"Connecting to Oracle database: {self.oracle_config['dsn']}")
            connection = oracledb.connect(
                user=self.oracle_config['user'],
                password=self.oracle_config['password'],
                dsn=self.oracle_config['dsn']
            )
            
            cursor = connection.cursor()
            
            # Updated query using blob_to_clob (data is already base64)
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
            cursor.execute(query)
            
            records = []
            
            for idx, row in enumerate(cursor, 1):
                try:
                    # Data is already base64 encoded - just read the CLOB
                    aws_image_clob = row[0]
                    id_photo_clob = row[1]
                    kyc_id = row[2]
                    session_id = row[3]
                    
                    # Read CLOB to string (already base64)
                    aws_image_base64 = aws_image_clob.read() if aws_image_clob else None
                    id_photo_base64 = id_photo_clob.read() if id_photo_clob else None
                    
                    # Skip if either image is missing
                    if not aws_image_base64 or not id_photo_base64:
                        logger.warning(f"  ⚠️  Record {idx}: Missing image data, skipping")
                        continue
                    
                    record = {
                        'card_image_base64': aws_image_base64,  # Already base64
                        'face_image_base64': id_photo_base64,   # Already base64
                        'KYC_ID_NO': kyc_id,
                        'SESSION_ID': session_id,
                        'record_index': len(records) + 1
                    }
                    records.append(record)
                    
                    if idx % 10 == 0:
                        logger.info(f"  Processed {idx} records, kept {len(records)}...")
                    
                except Exception as e:
                    logger.warning(f"  ❌ Failed record {idx}: {str(e)}")
                    continue
            
            logger.info(f"\n✓ Fetched {len(records)} valid card-face pairs\n")
            
            cursor.close()
            connection.close()
            
            return records
            
        except Exception as e:
            logger.error(f"❌ Database error: {str(e)}")
            raise
    
    def verify_data_exists(self) -> Dict:
        """Quick verification of data availability"""
        try:
            logger.info("Verifying Maisha data availability...")
            connection = oracledb.connect(
                user=self.oracle_config['user'],
                password=self.oracle_config['password'],
                dsn=self.oracle_config['dsn']
            )
            
            cursor = connection.cursor()
            
            # Quick count using same logic as fetch
            query = """
                SELECT COUNT(*) as total_testable
                FROM (
                    SELECT
                        blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
                        blob_to_clob(o.ID_PHOTO)  AS ID_PHOTO_BASE64
                    FROM MA.SELF_ONBOARDING_TRACKER_OCR o
                    JOIN MA.SELF_ONBOARDING_TRACKER_KYC k
                        ON o.SESSION_ID = k.SESSION_ID
                    WHERE k.ID_TYPE = 'MAISHA_CARD'
                )
                WHERE LENGTH(ID_PHOTO_BASE64) > 16
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            data_info = {
                'total_testable': result[0],
                'has_data': result[0] > 0
            }
            
            logger.info(f"\n{'='*70}")
            logger.info(f"DATABASE DATA VERIFICATION")
            logger.info(f"{'='*70}")
            logger.info(f"Database: {self.oracle_config['dsn']}")
            logger.info(f"Testable Maisha Records: {data_info['total_testable']:,}")
            
            if data_info['has_data']:
                logger.info(f"✓ Maisha data is available")
            else:
                logger.warning(f"⚠️  No Maisha data found!")
            logger.info(f"{'='*70}\n")
            
            cursor.close()
            connection.close()
            
            return data_info
            
        except Exception as e:
            logger.error(f"❌ Data verification failed: {str(e)}")
            raise
    
    def call_batch_verify(self, records: List[Dict], batch_num: int) -> List[Dict]:
        """Call batch verify API"""
        if not self.token:
            self.get_auth_token()
        
        url = f"{self.api_base_url}/test/batch-verify"
        
        # Build payload - data is already base64
        verifications = []
        for r in records:
            verification = {
                "card_image_key": r['card_image_base64'],  # Already base64
                "face_image_key": r['face_image_base64']   # Already base64
            }
            verifications.append(verification)
        
        payload = {"verifications": verifications}
        
        payload_size_mb = len(json.dumps(payload)) / (1024 * 1024)
        logger.info(f"Sending batch {batch_num}: {len(records)} records, {payload_size_mb:.2f}MB")
        
        try:
            response = requests.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token}"
                },
                json=payload,
                verify=False if not self.use_local else True,
                timeout=180
            )
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"✓ Batch {batch_num} completed")
            
            # Print full API response for first batch
            if batch_num == 1:
                print("\n" + "="*70)
                print(f"SAMPLE API RESPONSE - BATCH {batch_num}")
                print("="*70)
                print(json.dumps(result, indent=2))
                print("="*70 + "\n")
            
            # Combine results with metadata
            combined_results = []
            api_results = result.get('results', [])
            
            for idx, api_result in enumerate(api_results):
                combined = {
                    'session_id': records[idx]['SESSION_ID'],
                    'kyc_id_no': records[idx]['KYC_ID_NO'],
                    'record_index': records[idx]['record_index'],
                    'verified': api_result.get('verified', False),
                    'confidence': api_result.get('confidence'),
                    'deepface_verified': api_result.get('deepface_verified'),
                    'deepface_distance': api_result.get('deepface_distance'),
                    'aws_verified': api_result.get('aws_verified'),
                    'rekognition_confidence': api_result.get('rekognition_confidence'),
                    'similarity_score': api_result.get('similarity_score'),
                    'quorum_agreement': api_result.get('quorum_agreement'),
                    'error': api_result.get('error'),
                    'message': api_result.get('message'),
                    'verification_id': api_result.get('verification_id'),
                    'test_timestamp': datetime.now().isoformat()
                }
                combined_results.append(combined)
            
            return combined_results
            
        except Exception as e:
            logger.error(f"❌ Batch {batch_num} failed: {str(e)}")
            raise
    
    def run_batch_test(self, batch_size: int = 5, total_limit: int = None) -> List[Dict]:
        """Run batch verification tests"""
        logger.info("="*70)
        logger.info("MAISHA CARD VERIFICATION TEST - OPTIMIZED QUERY")
        logger.info("="*70)
        logger.info(f"Database: {self.oracle_config['dsn']}")
        logger.info(f"API Endpoint: {self.api_base_url}")
        logger.info(f"Batch Size: {batch_size}")
        logger.info(f"Limit: {total_limit if total_limit else 'ALL'}")
        logger.info(f"Using: blob_to_clob conversion (already base64)")
        logger.info("="*70 + "\n")
        
        # Verify data exists
        data_info = self.verify_data_exists()
        if not data_info['has_data']:
            logger.error("❌ No Maisha data available for testing!")
            return []
        
        # Fetch records
        records = self.fetch_maisha_records(limit=total_limit)
        
        if not records:
            logger.warning("No records fetched")
            return []
        
        all_results = []
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        # Process in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info(f"\n{'='*70}")
            logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
            logger.info("="*70)
            
            try:
                batch_results = self.call_batch_verify(batch, batch_num)
                
                # Log results
                for idx, result in enumerate(batch_results):
                    record_num = i + idx + 1
                    verified = result['verified']
                    confidence = result.get('confidence', 0) or 0
                    
                    deepface = "✓" if result.get('deepface_verified') else "✗"
                    aws = "✓" if result.get('aws_verified') else "✗"
                    
                    status = "✓ VERIFIED" if verified else "✗ NOT VERIFIED"
                    logger.info(f"  {record_num}. {status} (conf: {confidence:.2f}) [DF:{deepface} AWS:{aws}]")
                
                all_results.extend(batch_results)
                logger.info(f"✓ Batch {batch_num} completed\n")
                
            except Exception as e:
                logger.error(f"❌ Batch {batch_num} failed: {str(e)}\n")
                continue
        
        self.results = all_results
        
        logger.info(f"{'='*70}")
        logger.info(f"TEST COMPLETED: {len(all_results)}/{len(records)} records processed")
        logger.info("="*70 + "\n")
        
        return all_results
    
    def analyze_results(self) -> Dict:
        """Analyze test results"""
        if not self.results:
            return {}
        
        total = len(self.results)
        verified = sum(1 for r in self.results if r.get('verified', False))
        not_verified = sum(1 for r in self.results if r.get('verified') == False and not r.get('error'))
        failed = sum(1 for r in self.results if r.get('error'))
        
        deepface_verified = sum(1 for r in self.results if r.get('deepface_verified', False))
        aws_verified = sum(1 for r in self.results if r.get('aws_verified', False))
        both_verified = sum(1 for r in self.results 
                          if r.get('deepface_verified', False) and r.get('aws_verified', False))
        
        # Calculate confidence statistics
        confidences = [r.get('confidence', 0) or 0 for r in self.results if not r.get('error')]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        return {
            'total_tests': total,
            'verified_count': verified,
            'not_verified_count': not_verified,
            'failed_count': failed,
            'verification_rate': (verified / total * 100) if total > 0 else 0,
            'deepface_verified': deepface_verified,
            'aws_verified': aws_verified,
            'both_verified': both_verified,
            'avg_confidence': avg_confidence
        }
    
    def export_csv(self, output_file: str = None) -> str:
        """Export results to CSV"""
        if not self.results:
            return None
        
        if not output_file:
            output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        columns = [
            'record_index', 'session_id', 'kyc_id_no', 'verified', 'confidence',
            'deepface_verified', 'deepface_distance', 'aws_verified',
            'rekognition_confidence', 'similarity_score', 'quorum_agreement',
            'error', 'message', 'verification_id', 'test_timestamp'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.results)
        
        logger.info(f"✓ CSV exported: {output_file}")
        return output_file
    
    def print_summary(self, analysis: Dict):
        """Print summary to console"""
        print("\n" + "="*70)
        print("MAISHA CARD VERIFICATION - TEST SUMMARY")
        print("="*70)
        print(f"\nOVERALL RESULTS:")
        print(f"  Total Tests:       {analysis.get('total_tests', 0)}")
        print(f"  ✓ Verified:        {analysis.get('verified_count', 0)} ({analysis.get('verification_rate', 0):.1f}%)")
        print(f"  ✗ Not Verified:    {analysis.get('not_verified_count', 0)}")
        print(f"  ❌ Failed/Errors:  {analysis.get('failed_count', 0)}")
        print(f"  Avg Confidence:    {analysis.get('avg_confidence', 0):.2f}")
        
        print(f"\nVERIFICATION METHODS:")
        print(f"  DeepFace Verified: {analysis.get('deepface_verified', 0)}")
        print(f"  AWS Verified:      {analysis.get('aws_verified', 0)}")
        print(f"  Both Verified:     {analysis.get('both_verified', 0)}")
        print("="*70 + "\n")


def main():
    """Main execution"""
    
    # =========================================================================
    # DATABASE CONFIGURATION
    # =========================================================================
    
    # MONAPREPROD (production data on preprod server)
    oracle_config = {
        'user': 'MA',
        'password': 'wU8n1av8U$#OLt7pRePrOd',  # Update if password is different
        'dsn': '172.16.17.29:1561/MONAPREPROD'
    }
    
    # =========================================================================
    # TEST CONFIGURATION
    # =========================================================================
    use_local = False  # Set to True if testing against localhost:8000
    
    # Choose your test scenario:
    
    # Scenario 1: Quick validation (5 records)
    batch_size = 5
    total_limit = 5
    
    # Scenario 2: Small test (20 records)
    # batch_size = 5
    # total_limit = 20
    
    # Scenario 3: Medium test (100 records)
    # batch_size = 10
    # total_limit = 100
    
    # Scenario 4: Full test (120 records - as per your query)
    # batch_size = 10
    # total_limit = 120
    
    # Scenario 5: All available records
    # batch_size = 20
    # total_limit = None  # No limit
    
    # =========================================================================
    # RUN TEST
    # =========================================================================
    print("\n" + "="*70)
    print("MAISHA CARD VERIFICATION TEST")
    print("="*70)
    print(f"Database: {oracle_config['dsn']}")
    print(f"API: {'http://localhost:8000' if use_local else 'https://18.235.35.175'}")
    print(f"Batch Size: {batch_size}")
    print(f"Total Limit: {total_limit if total_limit else 'ALL'}")
    print(f"Query: Using blob_to_clob (optimized)")
    print("="*70 + "\n")
    
    try:
        # Initialize tester
        tester = MaishaVerificationTester(
            oracle_config=oracle_config,
            use_local=use_local
        )
        
        # Run test
        results = tester.run_batch_test(
            batch_size=batch_size,
            total_limit=total_limit
        )
        
        if not results:
            print("❌ No results to analyze\n")
            return
        
        # Analyze and print results
        analysis = tester.analyze_results()
        tester.print_summary(analysis)
        
        # Export results
        csv_file = tester.export_csv()
        print(f"📊 Results exported: {csv_file}")
        print(f"📝 Log file: Check maisha_verification_*.log\n")
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {str(e)}")
        print(f"\n❌ ERROR: {str(e)}\n")
        raise


if __name__ == "__main__":
    import urllib3
    # Disable SSL warnings for testing
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    main()

