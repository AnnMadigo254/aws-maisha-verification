# #!/usr/bin/env python3
# """
# Maisha Card Verification API Client

# A Python client for the Maisha Card Face Verification API.
# Supports direct comparison, batch processing, and database integration.

# Installation:
#     pip install requests

# Usage:
#     from maisha_client import MaishaVerificationClient
    
#     client = MaishaVerificationClient(api_key="your-key")
#     result = client.compare_faces("card.jpg", "selfie.jpg")
#     print(f"Match: {result.match}, Score: {result.similarity_score}")

# Author: Maisha Verification Team
# Version: 1.0.0
# """

# import requests
# import base64
# import json
# from pathlib import Path
# from typing import Dict, Any, Optional, List, Union
# from dataclasses import dataclass
# from enum import Enum


# class VerificationStatus(Enum):
#     """Verification result statuses."""
#     APPROVED = "APPROVED"
#     REVIEW_NEEDED = "REVIEW NEEDED"
#     FACE_MISMATCH = "FACE MISMATCH"
#     LIVENESS_FAILED = "LIVENESS FAILED"
#     NO_FACE_DETECTED = "NO FACE DETECTED"
#     PENDING = "PENDING"
#     INITIATED = "INITIATED"


# @dataclass
# class ComparisonResult:
#     """Result of a face comparison."""
#     success: bool
#     match: bool
#     similarity_score: float
#     threshold: float
#     comparison_method: str
#     comparison_id: str
#     error: Optional[str] = None
    
#     @property
#     def passed(self) -> bool:
#         return self.match and self.similarity_score >= self.threshold


# @dataclass
# class BatchResult:
#     """Result of a batch verification."""
#     success: bool
#     batch_id: str
#     total: int
#     completed: int
#     passed: int
#     failed: int
#     errors: int
#     results: List[Dict[str, Any]]


# class MaishaAPIError(Exception):
#     """API error with error code."""
#     def __init__(self, message: str, error_code: str = None, status_code: int = None):
#         super().__init__(message)
#         self.error_code = error_code
#         self.status_code = status_code


# class MaishaVerificationClient:
#     """
#     Client for Maisha Card Face Verification API.
    
#     Args:
#         api_key: Your API key
#         base_url: API base URL (default: https://18.235.35.175)
#         timeout: Request timeout in seconds (default: 60)
#         verify_ssl: Verify SSL certificates (default: False for self-signed)
    
#     Example:
#         client = MaishaVerificationClient(api_key="your-key")
#         result = client.compare_faces("id_card.jpg", "selfie.jpg")
#         if result.match:
#             print(f"Verified! Score: {result.similarity_score}")
#     """
    
#     def __init__(
#         self,
#         api_key: str,
#         base_url: str = "https://18.235.35.175",
#         timeout: int = 60,
#         verify_ssl: bool = False
#     ):
#         self.api_key = api_key
#         self.base_url = base_url.rstrip("/")
#         self.timeout = timeout
#         self.verify_ssl = verify_ssl
#         self.session = requests.Session()
#         self.session.headers.update({
#             "X-API-Key": api_key,
#             "Content-Type": "application/json"
#         })

#     def _request(
#         self,
#         method: str,
#         endpoint: str,
#         data: Dict = None,
#         params: Dict = None
#     ) -> Dict[str, Any]:
#         """Make API request."""
#         url = f"{self.base_url}{endpoint}"
        
#         try:
#             response = self.session.request(
#                 method=method,
#                 url=url,
#                 json=data,
#                 params=params,
#                 timeout=self.timeout,
#                 verify=self.verify_ssl
#             )
            
#             result = response.json()
            
#             if response.status_code >= 400:
#                 raise MaishaAPIError(
#                     message=result.get("error", "Unknown error"),
#                     error_code=result.get("error_code"),
#                     status_code=response.status_code
#                 )
            
#             return result
            
#         except requests.exceptions.RequestException as e:
#             raise MaishaAPIError(f"Request failed: {e}")
    
#     def _encode_image(self, image: Union[str, bytes, Path]) -> str:
#         """Encode image to base64."""
#         if isinstance(image, bytes):
#             return base64.b64encode(image).decode("utf-8")
        
#         path = Path(image)
#         if path.exists():
#             return base64.b64encode(path.read_bytes()).decode("utf-8")
        
#         # Assume it's already base64 or an S3 key
#         return str(image)
    
#     def health_check(self) -> Dict[str, Any]:
#         """Check API health status."""
#         return self._request("GET", "/api/v1/health")

#     def compare_faces(
#         self,
#         source_image: Union[str, bytes, Path],
#         target_image: Union[str, bytes, Path],
#         reference_id: str = None,
#         extract_face: bool = True
#     ) -> ComparisonResult:
#         """
#         Compare two face images directly.
        
#         Args:
#             source_image: ID card image (file path, bytes, or base64)
#             target_image: Selfie/face image (file path, bytes, or base64)
#             reference_id: Your internal reference ID
#             extract_face: Extract face from source image (default: True)
        
#         Returns:
#             ComparisonResult with match status and similarity score
#         """
#         data = {
#             "source_image": self._encode_image(source_image),
#             "target_image": self._encode_image(target_image),
#             "source_type": "base64",
#             "target_type": "base64",
#             "extract_face": extract_face
#         }
        
#         if reference_id:
#             data["reference_id"] = reference_id
        
#         result = self._request("POST", "/api/v1/compare", data)
        
#         return ComparisonResult(
#             success=result.get("success", False),
#             match=result.get("match", False),
#             similarity_score=result.get("similarity_score", 0),
#             threshold=result.get("threshold", 70),
#             comparison_method=result.get("comparison_method", "unknown"),
#             comparison_id=result.get("comparison_id", ""),
#             error=result.get("error")
#         )

#     def batch_compare(
#         self,
#         verifications: List[Dict[str, Any]],
#         extract_face: bool = True,
#         parallel: bool = True,
#         stop_on_error: bool = False
#     ) -> BatchResult:
#         """
#         Process multiple face comparisons in a single request.
        
#         Args:
#             verifications: List of verification items, each with:
#                 - id: Optional item identifier
#                 - source_image: ID card image (path, bytes, or base64)
#                 - target_image: Face image (path, bytes, or base64)
#                 - reference_id: Optional reference
#             extract_face: Extract face from source images
#             parallel: Process in parallel (faster)
#             stop_on_error: Stop on first error
        
#         Returns:
#             BatchResult with results for each item
        
#         Example:
#             result = client.batch_compare([
#                 {"id": "user-1", "source_image": "card1.jpg", "target_image": "face1.jpg"},
#                 {"id": "user-2", "source_image": "card2.jpg", "target_image": "face2.jpg"}
#             ])
#             for item in result.results:
#                 print(f"{item['id']}: {'Match' if item['match'] else 'No match'}")
#         """
#         items = []
#         for v in verifications:
#             item = {
#                 "id": v.get("id"),
#                 "source_image": self._encode_image(v["source_image"]),
#                 "target_image": self._encode_image(v["target_image"]),
#                 "source_type": "base64",
#                 "target_type": "base64"
#             }
#             if v.get("reference_id"):
#                 item["reference_id"] = v["reference_id"]
#             items.append(item)
        
#         data = {
#             "verifications": items,
#             "options": {
#                 "extract_face": extract_face,
#                 "parallel": parallel,
#                 "stop_on_error": stop_on_error
#             }
#         }
        
#         result = self._request("POST", "/api/v1/verify/batch", data)
        
#         return BatchResult(
#             success=result.get("success", False),
#             batch_id=result.get("batch_id", ""),
#             total=result.get("total", 0),
#             completed=result.get("completed", 0),
#             passed=result.get("passed", 0),
#             failed=result.get("failed", 0),
#             errors=result.get("errors", 0),
#             results=result.get("results", [])
#         )

#     def initiate_verification(
#         self,
#         reference_id: str = None,
#         metadata: Dict = None
#     ) -> Dict[str, Any]:
#         """
#         Initiate a liveness verification session.
        
#         Returns session_id and upload_url for card image.
#         Use this for full liveness verification flow.
#         """
#         data = {}
#         if reference_id:
#             data["reference_id"] = reference_id
#         if metadata:
#             data["metadata"] = metadata
        
#         return self._request("POST", "/api/v1/verify/initiate", data)
    
#     def complete_verification(
#         self,
#         verification_id: str,
#         session_id: str = None,
#         card_image_key: str = None
#     ) -> Dict[str, Any]:
#         """Complete verification after liveness check."""
#         data = {"verification_id": verification_id}
#         if session_id:
#             data["session_id"] = session_id
#         if card_image_key:
#             data["card_image_key"] = card_image_key
        
#         return self._request("POST", "/api/v1/verify/complete", data)
    
#     def get_verification_status(self, verification_id: str) -> Dict[str, Any]:
#         """Get status of a verification."""
#         return self._request(
#             "GET",
#             "/api/v1/verify/status",
#             params={"verification_id": verification_id}
#         )


# # Convenience function for quick usage
# def verify_faces(
#     api_key: str,
#     source_image: Union[str, bytes, Path],
#     target_image: Union[str, bytes, Path],
#     base_url: str = "https://18.235.35.175"
# ) -> ComparisonResult:
#     """
#     Quick face verification.
    
#     Example:
#         from maisha_client import verify_faces
#         result = verify_faces("your-key", "card.jpg", "selfie.jpg")
#         print(f"Match: {result.match}")
#     """
#     client = MaishaVerificationClient(api_key=api_key, base_url=base_url)
#     return client.compare_faces(source_image, target_image)


# if __name__ == "__main__":
#     # Example usage
#     import sys
    
#     if len(sys.argv) < 4:
#         print("Usage: python maisha_client.py <api_key> <card_image> <selfie_image>")
#         sys.exit(1)
    
#     api_key, card_image, selfie_image = sys.argv[1:4]
    
#     client = MaishaVerificationClient(api_key=api_key)
    
#     # Health check
#     health = client.health_check()
#     print(f"API Status: {health['status']}")
    
#     # Compare faces
#     result = client.compare_faces(card_image, selfie_image)
#     print(f"Match: {result.match}")
#     print(f"Similarity: {result.similarity_score:.1f}%")
#     print(f"Method: {result.comparison_method}")

#!/usr/bin/env python3
"""
Maisha Card Verification API Client

A Python client for the Maisha Card Face Verification API.
Supports direct comparison, batch processing, and database integration.

Installation:
    pip install requests

Usage:
    from maisha_client import MaishaVerificationClient
    
    client = MaishaVerificationClient(api_key="your-key")
    result = client.compare_faces("card.jpg", "selfie.jpg")
    print(f"Match: {result.match}, Score: {result.similarity_score}")

Author: Maisha Verification Team
Version: 1.1.0 (Enhanced with base64 cleaning & quorum support)
"""

import requests
import base64
import re
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from enum import Enum


class VerificationStatus(Enum):
    """Verification result statuses."""
    APPROVED = "APPROVED"
    REVIEW_NEEDED = "REVIEW NEEDED"
    FACE_MISMATCH = "FACE MISMATCH"
    LIVENESS_FAILED = "LIVENESS FAILED"
    NO_FACE_DETECTED = "NO FACE DETECTED"
    PENDING = "PENDING"
    INITIATED = "INITIATED"


@dataclass
class ComparisonResult:
    """Result of a face comparison."""
    success: bool
    match: bool
    similarity_score: float
    threshold: float
    comparison_method: str
    comparison_id: str
    error: Optional[str] = None
    model_scores: Optional[List[Dict[str, Any]]] = None
    quorum_agreement: Optional[bool] = None
    
    @property
    def passed(self) -> bool:
        """Check if verification passed (match=True or high similarity)."""
        return self.match or self.similarity_score >= self.threshold
    
    @property
    def rekognition_match(self) -> bool:
        """Check if Rekognition specifically matched (useful when quorum fails)."""
        if self.model_scores:
            for score in self.model_scores:
                if score.get("model_name") == "rekognition":
                    return score.get("is_match", False)
        return self.match


@dataclass
class BatchResult:
    """Result of a batch verification."""
    success: bool
    batch_id: str
    total: int
    completed: int
    passed: int
    failed: int
    errors: int
    results: List[Dict[str, Any]]


class MaishaAPIError(Exception):
    """API error with error code."""
    def __init__(self, message: str, error_code: str = None, status_code: int = None):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code


class MaishaVerificationClient:
    """
    Client for Maisha Card Face Verification API.
    
    Args:
        api_key: Your API key
        base_url: API base URL (default: https://18.235.35.175)
        timeout: Request timeout in seconds (default: 120)
        verify_ssl: Verify SSL certificates (default: False for self-signed certs)
    
    Example:
        client = MaishaVerificationClient(api_key="your-key")
        result = client.compare_faces("id_card.jpg", "selfie.jpg")
        if result.match:
            print(f"Verified! Score: {result.similarity_score}")
    """
    
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://18.235.35.175",
        timeout: int = 300,
        verify_ssl: bool = False
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        })

    def _clean_base64(self, data: str) -> str:
        """
        Clean base64 string by removing data URI prefixes and invalid characters.
        
        Handles formats commonly stored in Oracle:
          - "data:image/jpeg;base64,/9j/4AAQ..."
          - "image/jpeg;base64,/9j/4AAQ..."
          - "/9j/4AAQ..." (pure base64 with whitespace)
        
        Returns clean base64 string ready for API consumption.
        """
        if not isinstance(data, str):
            return str(data)
        
        # Remove data URI prefix if present
        if 'base64,' in data:
            data = data.split('base64,')[-1]
        
        # Remove all whitespace/newlines
        data = ''.join(data.split())
        
        # Remove any non-base64 characters (keep A-Z, a-z, 0-9, +, /, =)
        data = re.sub(r'[^A-Za-z0-9+/=]', '', data)
        
        return data

    def _encode_image(self, image: Union[str, bytes, Path]) -> str:
        """Encode image to clean base64 string."""
        if isinstance(image, bytes):
            return base64.b64encode(image).decode("utf-8")
        
        path = Path(image)
        if path.exists():
            return base64.b64encode(path.read_bytes()).decode("utf-8")
        
        # CRITICAL: Clean base64 strings from database
        return self._clean_base64(str(image))
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Dict = None,
        params: Dict = None
    ) -> Dict[str, Any]:
        """Make API request."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            
            result = response.json()
            
            if response.status_code >= 400:
                raise MaishaAPIError(
                    message=result.get("error", result.get("message", "Unknown error")),
                    error_code=result.get("error_code"),
                    status_code=response.status_code
                )
            
            return result
            
        except requests.exceptions.RequestException as e:
            raise MaishaAPIError(f"Request failed: {e}")
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health status."""
        return self._request("GET", "/api/v1/health")

    def compare_faces(
        self,
        source_image: Union[str, bytes, Path],
        target_image: Union[str, bytes, Path],
        reference_id: str = None,
        extract_face: bool = True
    ) -> ComparisonResult:
        """
        Compare two face images directly.
        
        FIXED: Includes source_type/target_type="base64" to prevent S3 key misinterpretation.
        FIXED: Automatically cleans base64 strings with data URI prefixes.
        
        Args:
            source_image: ID card image (file path, bytes, or base64)
            target_image: Selfie/face image (file path, bytes, or base64)
            reference_id: Your internal reference ID
            extract_face: Extract face from source image (default: True)
        
        Returns:
            ComparisonResult with match status and similarity score
        """
        data = {
            "source_image": self._encode_image(source_image),
            "target_image": self._encode_image(target_image),
            "source_type": "base64",   # CRITICAL: Prevents S3 key misinterpretation
            "target_type": "base64",   # CRITICAL: Prevents S3 key misinterpretation
            "extract_face": extract_face
        }
        
        if reference_id:
            data["reference_id"] = reference_id
        
        result = self._request("POST", "/api/v1/compare", data)
        
        return ComparisonResult(
            success=result.get("success", False),
            match=result.get("match", False),
            similarity_score=result.get("similarity_score", 0),
            threshold=result.get("threshold", 70),
            comparison_method=result.get("comparison_method", "unknown"),
            comparison_id=result.get("comparison_id", ""),
            error=result.get("error"),
            model_scores=result.get("model_scores"),
            quorum_agreement=result.get("quorum_agreement")
        )

    def batch_compare(
        self,
        verifications: List[Dict[str, Any]],
        extract_face: bool = True,
        parallel: bool = True,
        stop_on_error: bool = False
    ) -> BatchResult:
        """
        Process multiple face comparisons in a single request.
        
        FIXED: Each item includes source_type/target_type="base64".
        FIXED: Automatically cleans base64 strings.
        
        Args:
            verifications: List of verification items, each with:
                - id: Optional item identifier
                - source_image: ID card image (path, bytes, or base64)
                - target_image: Face image (path, bytes, or base64)
                - reference_id: Optional reference
            extract_face: Extract face from source images
            parallel: Process in parallel (faster)
            stop_on_error: Stop on first error
        
        Returns:
            BatchResult with results for each item
        """
        items = []
        for v in verifications:
            item = {
                "id": v.get("id"),
                "source_image": self._encode_image(v["source_image"]),
                "target_image": self._encode_image(v["target_image"]),
                "source_type": "base64",   # CRITICAL FIX
                "target_type": "base64",   # CRITICAL FIX
            }
            if v.get("reference_id"):
                item["reference_id"] = v["reference_id"]
            items.append(item)
        
        data = {
            "verifications": items,
            "options": {
                "extract_face": extract_face,
                "parallel": parallel,
                "stop_on_error": stop_on_error
            }
        }
        
        result = self._request("POST", "/api/v1/verify/batch", data)
        
        return BatchResult(
            success=result.get("success", False),
            batch_id=result.get("batch_id", ""),
            total=result.get("total", 0),
            completed=result.get("completed", 0),
            passed=result.get("passed", 0),
            failed=result.get("failed", 0),
            errors=result.get("errors", 0),
            results=result.get("results", [])
        )

    def initiate_verification(
        self,
        reference_id: str = None,
        metadata: Dict = None
    ) -> Dict[str, Any]:
        """
        Initiate a liveness verification session.
        
        Returns session_id and upload_url for card image.
        Use this for full liveness verification flow.
        """
        data = {}
        if reference_id:
            data["reference_id"] = reference_id
        if metadata:
            data["metadata"] = metadata
        
        return self._request("POST", "/api/v1/verify/initiate", data)
    
    def complete_verification(
        self,
        verification_id: str,
        session_id: str = None,
        card_image_key: str = None
    ) -> Dict[str, Any]:
        """Complete verification after liveness check."""
        data = {"verification_id": verification_id}
        if session_id:
            data["session_id"] = session_id
        if card_image_key:
            data["card_image_key"] = card_image_key
        
        return self._request("POST", "/api/v1/verify/complete", data)
    
    def get_verification_status(self, verification_id: str) -> Dict[str, Any]:
        """Get status of a verification."""
        return self._request(
            "GET",
            "/api/v1/verify/status",
            params={"verification_id": verification_id}
        )


# Convenience function for quick usage
def verify_faces(
    api_key: str,
    source_image: Union[str, bytes, Path],
    target_image: Union[str, bytes, Path],
    base_url: str = "https://18.235.35.175"
) -> ComparisonResult:
    """
    Quick face verification.
    
    Example:
        from maisha_client import verify_faces
        result = verify_faces("your-key", "card.jpg", "selfie.jpg")
        print(f"Match: {result.match}")
    """
    client = MaishaVerificationClient(api_key=api_key, base_url=base_url)
    return client.compare_faces(source_image, target_image)


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python maisha_client.py <api_key> <card_image> <selfie_image>")
        sys.exit(1)
    
    api_key, card_image, selfie_image = sys.argv[1:4]
    
    client = MaishaVerificationClient(api_key=api_key)
    
    # Health check
    health = client.health_check()
    print(f"API Status: {health['status']}")
    
    # Compare faces
    result = client.compare_faces(card_image, selfie_image)
    print(f"Match: {result.match}")
    print(f"Similarity: {result.similarity_score:.1f}%")
    print(f"Method: {result.comparison_method}")
    if result.quorum_agreement is not None:
        print(f"Quorum Agreement: {result.quorum_agreement}")
    if result.model_scores:
        print("Model Scores:")
        for score in result.model_scores:
            print(f"  - {score.get('model_name')}: {score.get('similarity_score'):.1f}% (match: {score.get('is_match')})")