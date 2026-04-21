import os
import logging
import requests

# Configure logging to see the payload and responses
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_api_request(strategy: dict, raw_lead: dict, counselor_name: str) -> dict:
    """
    Formats the raw CRM lead data and Decision Engine strategy into the 
    exact nested JSON structure required by the WhatsApp Message Engine API.
    
    Args:
        strategy (dict): Output from determine_message_strategy()
        raw_lead (dict): Full lead record from the CRM
        counselor_name (str): Assigned counselor's name

    Returns:
        dict: The formatted payload ready for the API.
    """
    
    # --- 1. Map Core Lead Data with Default Fallbacks ---
    
    # Handle firstName mapping and ensure it's not empty
    raw_name = raw_lead.get("name") or raw_lead.get("first_name") or ""
    # Extract the first word safely
    first_name = str(raw_name).strip().split(" ")[0] if str(raw_name).strip() else ""
    if not first_name:
        first_name = "Student"  # Fallback per requirements
        
    # Handle course interest 
    raw_course = raw_lead.get("course") or raw_lead.get("course_interest") or ""
    course_interest = str(raw_course).strip()
    if not course_interest:
        course_interest = "your chosen course"  # Fallback per requirements
        
    # Handle Trigger Type validation
    trigger_type = strategy.get("triggerType", "")
    if not isinstance(trigger_type, str) or not trigger_type.strip():
        trigger_type = "general_follow_up" # Emergency fallback
        
    # --- 2. Construct the Payload ---
    payload = {
        "leadData": {
            "firstName": first_name,
            "courseInterest": course_interest,
            "city": raw_lead.get("city", "Not specified"),
            "source": raw_lead.get("source", "")
        },
        "triggerType": trigger_type,
        "telecallerName": counselor_name,
        "applicationData": {
            "status": raw_lead.get("application_status", "pending"),
            "institutionName": raw_lead.get("institution_name", ""),
            "totalFee": raw_lead.get("total_fee", 0),
            "paidAmount": raw_lead.get("paid_amount", 0),
            "discountPercent": raw_lead.get("discount_percent", 0),
            "discountType": raw_lead.get("discount_type", "none"),
            "seatBlockAmount": raw_lead.get("seat_block_amount", 0),
            "seatBlockPaid": raw_lead.get("seat_block_paid", False),
            "nextInstallmentAmount": raw_lead.get("next_installment_amount", 0),
            "nextInstallmentDueDate": raw_lead.get("next_installment_due_date", ""),
            "hostelIncluded": raw_lead.get("hostel_included", False)
        },
        "docStats": {
            "verified": raw_lead.get("doc_verified", 0),
            "total": raw_lead.get("doc_total", 0),
            "pendingNames": raw_lead.get("doc_pending_names", [])
        },
        "callRemarks": raw_lead.get("call_remarks", []),
        "recentMessages": raw_lead.get("recent_messages", []),
        "lastContactDaysAgo": raw_lead.get("last_contact_days_ago", 0)
    }
    
    return payload


def send_to_api(request_payload: dict) -> dict:
    """
    POSTs the formatted request payload to the Message Engine API.
    Handles timeouts, HTTP errors, and logs interactions for debugging.
    
    Args:
        request_payload (dict): The formatted payload from build_api_request()
        
    Returns:
        dict: The JSON response from the API, or an error dictionary if failed.
    """
    url = os.getenv("WHATSAPP_API_URL")
    
    # Log the outgoing payload
    logger.info(f"Sending Request to API Payload: {request_payload}")
    
    try:
        # Perform POST using requests with a 30 sec timeout
        response = requests.post(url, json=request_payload, timeout=30)
        
        # Raise an exception for HTTP errors (4xx, 5xx)
        response.raise_for_status() 
        
        # Parse the JSON response
        response_data = response.json()
        logger.info(f"API Request Successful - Response: {response_data}")
        return response_data
        
    except requests.exceptions.Timeout:
        logger.error("API Request Failed - Reason: Timeout after 30 seconds.")
        return {"error": "Request timed out"}
        
    except requests.exceptions.HTTPError as http_err:
        # The endpoint returned a server-side or client-side HTTP error code
        logger.error(f"API Request Failed - HTTP Error: {http_err}")
        logger.error(f"Response Body: {response.text}")
        return {"error": str(http_err), "details": response.text}
        
    except requests.exceptions.RequestException as req_err:
        # A catastrophic failure like DNS failure, connection refused, etc. 
        logger.error(f"API Request Failed - Unexpected Error: {req_err}")
        return {"error": str(req_err)}

if __name__ == "__main__":
    # Test Data 
    sample_strategy = {"triggerType": "application_abandoned"}
    sample_lead = {
        "name": "Jane Doe",
        "course_interest": "B.Tech Computer Science",
        "doc_total": 4,
        "doc_verified": 2,
        "doc_pending_names": ["Aadhaar", "12th Marksheet"],
        "call_remarks": [
            {"remark": "Wants hostel", "disposition": "Positive", "daysAgo": 1}
        ]
    }
    
    # 1. Build Payload
    payload = build_api_request(sample_strategy, sample_lead, "Counselor Rohit")
    
    # Optional testing logic - will be fully activated on deployment
    import json
    print("\n--- Built Payload Structure ---")
    print(json.dumps(payload, indent=2))
