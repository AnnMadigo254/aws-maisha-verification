import oracledb
import json
import csv
from datetime import datetime
from typing import List, Dict
import logging
import sys
import base64
import re

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


def clean_base64_string(b64_string: str) -> str:
    """
    Clean base64 string by removing data URI prefixes and whitespace.
    
    Examples of what might be in the database:
    - data:image/jpeg;base64,/9j/4AAQ...
    - /9j/4AAQ... (pure base64)
    - Data with newlines or spaces
    """
    if not b64_string:
        return ""
    
    # Remove data URI prefix if present
    if 'base64,' in b64_string:
        b64_string = b64_string.split('base64,')[1]
    
    # Remove any whitespace, newlines, etc.
    b64_string = ''.join(b64_string.split())
    
    # Remove any non-base64 characters
    # Base64 uses: A-Z, a-z, 0-9, +, /, =
    b64_string = re.sub(r'[^A-Za-z0-9+/=]', '', b64_string)
    
    return b64_string


def validate_base64_image(b64_string: str) -> bool:
    """Validate that base64 string is a valid image."""
    try:
        # Decode to bytes
        image_bytes = base64.b64decode(b64_string)
        
        # Check minimum size (should be at least 1KB for a real image)
        if len(image_bytes) < 1000:
            logger.warning(f"Image too small: {len(image_bytes)} bytes")
            return False
        
        # Check for JPEG magic bytes (FF D8 FF)
        if image_bytes[:3] != b'\xff\xd8\xff':
            logger.warning(f"Not a JPEG image. First bytes: {image_bytes[:10].hex()}")
            # Could also be PNG (89 50 4E 47) or other formats
            # For now, just warn but don't fail
        
        logger.debug(f"Valid image: {len(image_bytes)} bytes, starts with {image_bytes[:3].hex()}")
        return True
        
    except Exception as e:
        logger.error(f"Base64 validation failed: {str(e)}")
        return False


class MaishaVerificationTester:
    def __init__(self, 
                 oracle_config: Dict[str, str],
                 api_key: str,
                 api_base_url: str = "https://18.235.35.175",
                 compare_with_gbg: bool = False):
        """Initialize the Maisha verification tester"""
        self.oracle_config = oracle_config
        self.api_key = api_key
        self.results = []
        self.compare_with_gbg = compare_with_gbg
        
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
    
    def test_single_comparison(self, card_base64: str, face_base64: str):
        """Test single comparison to debug the issue"""
        logger.info("="*70)
        logger.info("TESTING SINGLE COMPARISON WITH CLEANED DATA")
        logger.info("="*70)
        
        # Clean the base64 strings
        card_clean = clean_base64_string(card_base64)
        face_clean = clean_base64_string(face_base64)
        
        logger.info(f"Original card length: {len(card_base64)}")
        logger.info(f"Cleaned card length:  {len(card_clean)}")
        logger.info(f"Original face length: {len(face_base64)}")
        logger.info(f"Cleaned face length:  {len(face_clean)}")
        
        # Validate the cleaned data
        logger.info("Validating card image...")
        card_valid = validate_base64_image(card_clean)
        
        logger.info("Validating face image...")
        face_valid = validate_base64_image(face_clean)
        
        if not card_valid or not face_valid:
            logger.error("[FAILED] Image validation failed")
            logger.error("="*70)
            return False
        
        try:
            # Try the /api/v1/compare endpoint with cleaned data
            logger.info("Calling API with cleaned images...")
            result = self.client.compare_faces(
                source_image=card_clean,
                target_image=face_clean,
                reference_id="test-001",
                extract_face=True
            )
            
            logger.info(f"[SUCCESS] Single comparison worked!")
            logger.info(f"  Match: {result.match}")
            logger.info(f"  Score: {result.similarity_score}")
            logger.info(f"  Method: {result.comparison_method}")
            logger.info("="*70)
            return True
            
        except MaishaAPIError as e:
            logger.error(f"[FAILED] Single comparison failed: {e}")
            logger.error(f"  Error Code: {e.error_code}")
            logger.error(f"  Status Code: {e.status_code}")
            logger.error("="*70)
            return False
        except Exception as e:
            logger.error(f"[FAILED] Unexpected error: {str(e)}")
            logger.error("="*70)
            return False
    
    def fetch_maisha_records(self, limit: int = None, gbg_status: str = None) -> List[Dict]:
        """
        Fetch Maisha card records with GBG journey information
        
        Args:
            limit: Number of records to fetch
            gbg_status: 'CAPTURED' for GBG-verified records, 'NOT_CAPTURED' for non-GBG, or None for all
        """
        try:
            logger.info(f"Connecting to Oracle database: {self.oracle_config['dsn']}")
            connection = oracledb.connect(
                user=self.oracle_config['user'],
                password=self.oracle_config['password'],
                dsn=self.oracle_config['dsn']
            )
            
            cursor = connection.cursor()
            
            # Build query based on whether GBG comparison is enabled
            if self.compare_with_gbg:
                query = """
                    SELECT *
                    FROM (
                        SELECT
                            blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
                            blob_to_clob(k.KYC_IDFRONT_CAPTURE) AS ID_PHOTO_BASE64,
                            k.KYC_ID_NO,
                            k.SESSION_ID,
                            sm.JOURNEY_ID as GBG_JOURNEY_ID,
                            sm.IS_GBG_JOURNEY_ID_CAPTURED as GBG_CAPTURED,
                            sm.IS_RETRIEVED as GBG_RETRIEVED,
                            sm.CREATED_DATE as ONBOARDING_DATE
                        FROM MA.SELF_ONBOARDING_TRACKER_AWS o
                        JOIN MA.SELF_ONBOARDING_TRACKER_KYC k 
                            ON o.SESSION_ID = k.SESSION_ID
                        JOIN MA.SELF_ONBOARDING_TRACKER_MAIN sm 
                            ON sm.ID = k.SESSION_ID
                        WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
                """
                
                # Add GBG status filter if specified
                if gbg_status == 'CAPTURED':
                    query += " AND sm.IS_GBG_JOURNEY_ID_CAPTURED = 1 AND sm.IS_RETRIEVED = 1"
                elif gbg_status == 'NOT_CAPTURED':
                    query += " AND (sm.IS_GBG_JOURNEY_ID_CAPTURED = 0 OR sm.IS_GBG_JOURNEY_ID_CAPTURED IS NULL)"
                    
                query += """
                        ORDER BY sm.CREATED_DATE DESC
                    )
                    WHERE LENGTH(ID_PHOTO_BASE64) > 16
                """
            else:
                query = """
                    SELECT *
                    FROM (
                        SELECT
                            blob_to_clob(o.AWS_IMAGE) AS AWS_IMAGE_BASE64,
                            blob_to_clob(k.KYC_IDFRONT_CAPTURE) AS ID_PHOTO_BASE64,
                            k.KYC_ID_NO,
                            k.SESSION_ID
                        FROM MA.SELF_ONBOARDING_TRACKER_AWS o
                        JOIN MA.SELF_ONBOARDING_TRACKER_KYC k
                            ON o.SESSION_ID = k.SESSION_ID
                        WHERE k.ID_TYPE = 'Kenya - National Identification Card - Front - 2024'
                        ORDER BY k.CREATED_ON DESC
                    )
                    WHERE LENGTH(ID_PHOTO_BASE64) > 16
                """
            
            if limit:
                query += f" AND ROWNUM <= {limit}"
            
            logger.info(f"Fetching records (limit: {limit if limit else 'all'})...")
            if self.compare_with_gbg:
                if gbg_status == 'CAPTURED':
                    logger.info(f"  GBG Filter: Only GBG-verified records")
                elif gbg_status == 'NOT_CAPTURED':
                    logger.info(f"  GBG Filter: Only non-GBG records")
                else:
                    logger.info(f"  GBG Filter: ALL records")
            logger.info("="*70)
            
            cursor.execute(query)
            
            records = []
            session_ids_seen = set()
            gbg_stats = {'GBG_CAPTURED': 0, 'NOT_CAPTURED': 0} if self.compare_with_gbg else None
            
            for idx, row in enumerate(cursor, 1):
                try:
                    aws_image_clob = row[0]
                    id_photo_clob = row[1]
                    kyc_id = row[2]
                    session_id = row[3]
                    
                    if session_id in session_ids_seen:
                        logger.warning(f"  [DUPLICATE] Record {idx}: Session {session_id[:16]}... skipping")
                        continue
                    
                    session_ids_seen.add(session_id)
                    
                    aws_image_data = aws_image_clob.read() if aws_image_clob else None
                    id_photo_data = id_photo_clob.read() if id_photo_clob else None
                    
                    if not aws_image_data or not id_photo_data:
                        logger.warning(f"  [WARNING] Record {idx}: Missing image data")
                        continue
                    
                    # CRITICAL: Clean the base64 data
                    aws_image_clean = clean_base64_string(aws_image_data)
                    id_photo_clean = clean_base64_string(id_photo_data)
                    
                    # Validate it's actually base64
                    try:
                        base64.b64decode(aws_image_clean[:100])
                        base64.b64decode(id_photo_clean[:100])
                    except Exception as e:
                        logger.warning(f"  [WARNING] Record {idx}: Invalid base64 after cleaning: {str(e)}")
                        continue
                    
                    record = {
                        'card_image_base64': id_photo_clean,      # KYC_IDFRONT_CAPTURE = ID card
                        'face_image_base64': aws_image_clean,     # AWS_IMAGE = selfie/face
                        'KYC_ID_NO': kyc_id,
                        'SESSION_ID': session_id,
                        'record_index': len(records) + 1
                    }
                    
                    # Add GBG data if comparison is enabled
                    if self.compare_with_gbg and len(row) > 4:
                        gbg_journey_id = row[4]
                        gbg_captured = row[5]
                        gbg_retrieved = row[6]
                        
                        record['GBG_JOURNEY_ID'] = gbg_journey_id
                        record['GBG_CAPTURED'] = gbg_captured
                        record['GBG_RETRIEVED'] = gbg_retrieved
                        record['ONBOARDING_DATE'] = row[7] if len(row) > 7 else None
                        
                        if gbg_captured == 1 and gbg_retrieved == 1:
                            gbg_stats['GBG_CAPTURED'] += 1
                        else:
                            gbg_stats['NOT_CAPTURED'] += 1
                    
                    records.append(record)
                    
                    if idx <= 5:
                        logger.info(f"  Record {idx:2d} | Session: {session_id[:20]}... | KYC: {kyc_id}")
                        if self.compare_with_gbg:
                            gbg_status_str = "GBG-VERIFIED" if record.get('GBG_CAPTURED') == 1 else "NO-GBG"
                            logger.info(f"            | Status: {gbg_status_str}")
                            if record.get('GBG_JOURNEY_ID'):
                                logger.info(f"            | Journey: {record['GBG_JOURNEY_ID']}")
                        logger.info(f"            | Card: {len(id_photo_clean)} chars")
                        logger.info(f"            | Face: {len(aws_image_clean)} chars")
                    elif idx % 10 == 0:
                        logger.info(f"  Processed {idx} records, kept {len(records)}...")
                    
                except Exception as e:
                    logger.warning(f"  [FAILED] Record {idx}: {str(e)}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    continue
            
            logger.info("="*70)
            logger.info(f"[SUCCESS] Fetched {len(records)} valid card-face pairs")
            logger.info(f"[SUCCESS] Unique sessions: {len(session_ids_seen)}")
            
            if self.compare_with_gbg and gbg_stats:
                logger.info(f"[SUCCESS] GBG Status Breakdown:")
                logger.info(f"           GBG-Verified:  {gbg_stats['GBG_CAPTURED']}")
                logger.info(f"           Not Verified:  {gbg_stats['NOT_CAPTURED']}")
            
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
        """Verify batch using official Maisha client"""
        logger.info(f"Preparing batch {batch_num} with {len(records)} records...")
        
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
            
            # Print first result
            if batch_num == 1 and batch_result.results:
                print("\n" + "="*70)
                print(f"SAMPLE API RESPONSE - BATCH {batch_num}")
                print("="*70)
                print(json.dumps(batch_result.results[0], indent=2))
                print("="*70 + "\n")
            
            # Combine with original records
            combined_results = []
            for idx, api_result in enumerate(batch_result.results):
                if idx < len(records):
                    combined = {
                        'record_index': records[idx]['record_index'],
                        'session_id': records[idx]['SESSION_ID'],
                        'kyc_id_no': records[idx]['KYC_ID_NO'],
                        'aws_verified': api_result.get('match', False),
                        'aws_similarity_score': api_result.get('similarity_score', 0),
                        'aws_threshold': api_result.get('threshold', 70),
                        'aws_comparison_method': api_result.get('comparison_method'),
                        'aws_comparison_id': api_result.get('comparison_id'),
                        'aws_error': api_result.get('error'),
                        'test_timestamp': datetime.now().isoformat()
                    }
                    
                    # Add GBG data if comparison is enabled
                    if self.compare_with_gbg:
                        combined['gbg_journey_id'] = records[idx].get('GBG_JOURNEY_ID')
                        combined['gbg_captured'] = records[idx].get('GBG_CAPTURED')
                        combined['gbg_retrieved'] = records[idx].get('GBG_RETRIEVED')
                        
                        # Calculate agreement: both verified by GBG and AWS
                        gbg_verified = (records[idx].get('GBG_CAPTURED') == 1 and 
                                       records[idx].get('GBG_RETRIEVED') == 1)
                        aws_verified = api_result.get('match', False)
                        combined['both_verified'] = (gbg_verified and aws_verified)
                        combined['gbg_verified'] = gbg_verified
                        combined['agreement'] = (gbg_verified == aws_verified)
                    
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
    
    def run_batch_test(self, batch_size: int = 10, total_limit: int = None, 
                       test_single: bool = True, gbg_filter: str = None) -> List[Dict]:
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
            logger.info(f"GBG Filter: {gbg_filter if gbg_filter else 'ALL'}")
        logger.info("="*70)
        
        # Test connections
        if not self.test_network_connectivity():
            logger.error("[FAILED] Network connectivity test failed")
            return []
        
        if not self.test_api_connection():
            logger.error("[FAILED] API connectivity test failed")
            return []
        
        # Fetch records
        records = self.fetch_maisha_records(limit=total_limit, gbg_status=gbg_filter)
        
        if not records:
            logger.warning("[WARNING] No records fetched")
            return []
        
        # Test single comparison first if enabled
        if test_single and records:
            logger.info("\n" + "="*70)
            logger.info("TESTING SINGLE COMPARISON BEFORE BATCH")
            logger.info("="*70)
            single_success = self.test_single_comparison(
                records[0]['card_image_base64'],
                records[0]['face_image_base64']
            )
            
            if not single_success:
                logger.error("Single comparison failed. Please check:")
                logger.error("  1. Image format (should be JPEG)")
                logger.error("  2. Image size (not too large)")
                logger.error("  3. Base64 encoding is correct")
                logger.error("\nContact API support with the error code above.")
                return []
            
            logger.info("Single comparison successful! Continuing with batch...\n")
        
        all_results = []
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        # Process in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info("="*70)
            logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} records)")
            logger.info("="*70)
            
            try:
                batch_results = self.verify_batch_using_client(batch, batch_num)
                
                # Log results
                for idx, result in enumerate(batch_results):
                    record_num = i + idx + 1
                    aws_verified = result.get('aws_verified', False)
                    score = result.get('aws_similarity_score', 0)
                    method = result.get('aws_comparison_method', 'unknown')
                    
                    if self.compare_with_gbg:
                        gbg_verified = result.get('gbg_verified', False)
                        agreement = result.get('agreement', False)
                        gbg_icon = "✓" if gbg_verified else "✗"
                        aws_icon = "✓" if aws_verified else "✗"
                        agree_icon = "✓" if agreement else "✗"
                        
                        if result.get('aws_error'):
                            logger.warning(f"  {record_num:3d}. [ERROR] {result['aws_error'][:60]}")
                        else:
                            logger.info(f"  {record_num:3d}. GBG:{gbg_icon} AWS:{aws_icon} Agree:{agree_icon} | Score: {score:5.2f}%")
                    else:
                        status = "✓ VERIFIED" if aws_verified else "✗ NOT VERIFIED"
                        
                        if result.get('aws_error'):
                            logger.warning(f"  {record_num:3d}. [ERROR] {result['aws_error'][:60]}")
                        else:
                            logger.info(f"  {record_num:3d}. [{status}] Score: {score:5.2f}% | Method: {method}")
                
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
        """Analyze test results"""
        if not self.results:
            return {}
        
        total = len(self.results)
        aws_verified = sum(1 for r in self.results if r.get('aws_verified', False))
        aws_not_verified = sum(1 for r in self.results if not r.get('aws_verified') and not r.get('aws_error'))
        failed = sum(1 for r in self.results if r.get('aws_error'))
        
        scores = [r.get('aws_similarity_score', 0) for r in self.results if not r.get('aws_error')]
        avg_score = sum(scores) / len(scores) if scores else 0
        max_score = max(scores) if scores else 0
        min_score = min(scores) if scores else 0
        
        analysis = {
            'total_tests': total,
            'aws_verified_count': aws_verified,
            'aws_not_verified_count': aws_not_verified,
            'failed_count': failed,
            'aws_verification_rate': (aws_verified / total * 100) if total > 0 else 0,
            'avg_similarity_score': avg_score,
            'max_similarity_score': max_score,
            'min_similarity_score': min_score
        }
        
        # Add GBG comparison stats if enabled
        if self.compare_with_gbg:
            gbg_verified = sum(1 for r in self.results if r.get('gbg_verified', False))
            not_gbg_verified = total - gbg_verified
            both_verified = sum(1 for r in self.results if r.get('both_verified', False))
            agreement = sum(1 for r in self.results if r.get('agreement', False))
            
            gbg_yes_aws_no = sum(1 for r in self.results if r.get('gbg_verified') and not r.get('aws_verified'))
            gbg_no_aws_yes = sum(1 for r in self.results if not r.get('gbg_verified') and r.get('aws_verified'))
            both_no = sum(1 for r in self.results if not r.get('gbg_verified') and not r.get('aws_verified'))
            
            analysis.update({
                'gbg_verified_count': gbg_verified,
                'gbg_not_verified_count': not_gbg_verified,
                'gbg_verification_rate': (gbg_verified / total * 100) if total > 0 else 0,
                'both_verified_count': both_verified,
                'agreement_count': agreement,
                'agreement_rate': (agreement / total * 100) if total > 0 else 0,
                'gbg_yes_aws_no': gbg_yes_aws_no,
                'gbg_no_aws_yes': gbg_no_aws_yes,
                'both_no': both_no
            })
        
        return analysis
    
    def export_csv(self, output_file: str = None) -> str:
        """Export results to CSV"""
        if not self.results:
            return None
        
        if not output_file:
            if self.compare_with_gbg:
                output_file = f"maisha_gbg_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            else:
                output_file = f"maisha_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if self.compare_with_gbg:
            columns = [
                'record_index', 'session_id', 'kyc_id_no',
                'gbg_journey_id', 'gbg_captured', 'gbg_retrieved', 'gbg_verified',
                'aws_verified', 'aws_similarity_score', 'aws_threshold',
                'aws_comparison_method', 'aws_comparison_id',
                'both_verified', 'agreement', 'aws_error', 'test_timestamp'
            ]
        else:
            columns = [
                'record_index', 'session_id', 'kyc_id_no',
                'aws_verified', 'aws_similarity_score', 'aws_threshold',
                'aws_comparison_method', 'aws_comparison_id',
                'aws_error', 'test_timestamp'
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
        if self.compare_with_gbg:
            print("MAISHA CARD VERIFICATION - GBG vs AWS COMPARISON SUMMARY")
        else:
            print("MAISHA CARD VERIFICATION - TEST SUMMARY")
        print("="*70)
        
        if self.compare_with_gbg:
            print(f"\nTOTAL RECORDS: {analysis.get('total_tests', 0):,}")
            
            print(f"\nGBG RESULTS:")
            print(f"  ✓ Verified:     {analysis.get('gbg_verified_count', 0):,} ({analysis.get('gbg_verification_rate', 0):.1f}%)")
            print(f"  ✗ Not Verified: {analysis.get('gbg_not_verified_count', 0):,}")
            
            print(f"\nAWS RESULTS:")
            print(f"  ✓ Verified:     {analysis.get('aws_verified_count', 0):,} ({analysis.get('aws_verification_rate', 0):.1f}%)")
            print(f"  ✗ Not Verified: {analysis.get('aws_not_verified_count', 0):,}")
            print(f"  ❌ Errors:      {analysis.get('failed_count', 0):,}")
            
            print(f"\nCOMPARISON:")
            print(f"  Agreement Rate:        {analysis.get('agreement_rate', 0):.1f}%")
            print(f"  ✓✓ Both Verified:      {analysis.get('both_verified_count', 0):,}")
            print(f"  ✗✗ Both Not Verified:  {analysis.get('both_no', 0):,}")
            print(f"  ✓✗ GBG Yes, AWS No:    {analysis.get('gbg_yes_aws_no', 0):,}")
            print(f"  ✗✓ GBG No, AWS Yes:    {analysis.get('gbg_no_aws_yes', 0):,}")
        else:
            print(f"\nOVERALL RESULTS:")
            print(f"  Total Tests:        {analysis.get('total_tests', 0):,}")
            print(f"  Verified:           {analysis.get('aws_verified_count', 0):,} ({analysis.get('aws_verification_rate', 0):.1f}%)")
            print(f"  Not Verified:       {analysis.get('aws_not_verified_count', 0):,}")
            print(f"  Failed/Errors:      {analysis.get('failed_count', 0):,}")
        
        print(f"\nSIMILARITY SCORES:")
        print(f"  Average:            {analysis.get('avg_similarity_score', 0):.2f}%")
        print(f"  Maximum:            {analysis.get('max_similarity_score', 0):.2f}%")
        print(f"  Minimum:            {analysis.get('min_similarity_score', 0):.2f}%")
        print("="*70 + "\n")


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
    
    # TEST CONFIGURATION
    compare_with_gbg = True      # Enable GBG comparison
    gbg_filter = 'CAPTURED'      # Options: 'CAPTURED', 'NOT_CAPTURED', or None for all
    batch_size = 5
    total_limit = 100            # Test 100 records
    
    print("\n" + "="*70)
    if compare_with_gbg:
        print("MAISHA CARD VERIFICATION TEST - WITH GBG COMPARISON")
    else:
        print("MAISHA CARD VERIFICATION TEST")
    print("="*70)
    print(f"Database: {oracle_config['dsn']}")
    print(f"API: {api_base_url}")
    print(f"Batch Size: {batch_size}")
    print(f"Total Limit: {total_limit}")
    if compare_with_gbg:
        print(f"GBG Filter: {gbg_filter if gbg_filter else 'ALL'}")
    print("="*70 + "\n")
    
    try:
        tester = MaishaVerificationTester(
            oracle_config=oracle_config,
            api_key=api_key,
            api_base_url=api_base_url,
            compare_with_gbg=compare_with_gbg
        )
        
        results = tester.run_batch_test(
            batch_size=batch_size,
            total_limit=total_limit,
            test_single=True,
            gbg_filter=gbg_filter
        )
        
        if not results:
            print("[FAILED] No results to analyze\n")
            return
        
        analysis = tester.analyze_results()
        tester.print_summary(analysis)
        
        csv_file = tester.export_csv()
        print(f"[SUCCESS] Results exported: {csv_file}")
        print(f"[SUCCESS] Log file: Check maisha_verification_*.log\n")
        
    except Exception as e:
        logger.error(f"[FAILED] Test execution failed: {str(e)}")
        print(f"\n[FAILED] ERROR: {str(e)}\n")


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    main()


