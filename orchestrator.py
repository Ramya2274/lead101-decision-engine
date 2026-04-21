import logging
import traceback
import time
from datetime import datetime, timezone

# Import the existing modules built for this project
from scoring_engine import calculate_lead_score
from decision_engine import determine_message_strategy
from api_client import build_api_request, send_to_api

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_pipeline(raw_lead: dict, counselor_name: str) -> dict:
    """
    Executes the entire end-to-end messaging pipeline for a single lead.
    
    Steps:
      1. Scores the lead based on behavior.
      2. Determines the best message strategy based on stage and score.
      3. Skips the execution if the strategy blocks sending.
      4. Formats the data into the downstream API payload.
      5. Connects to the WhatsApp generation API endpoint.
      
    Args:
        raw_lead (dict): The complete raw data from the CRM
        counselor_name (str): Assigned active counselor
        
    Returns:
        dict: High-level metric overview of execution status.
    """
    
    # Safely extract basic identifiers
    lead_id = str(raw_lead.get("id", "unknown"))
    raw_name = raw_lead.get("name") or raw_lead.get("first_name") or "Student"
    lead_name = str(raw_name).strip().split(" ")[0] if str(raw_name).strip() else "Student"
    
    # Initialize the result container
    result = {
        "status": "failed",
        "lead_id": lead_id,
        "lead_name": lead_name,
        "lead_score": 0,
        "stage": raw_lead.get("stage", "unknown"),
        "trigger_type": "",
        "channel": "",
        "urgency_level": "",
        "reasoning": "",
        "api_response": None,
        "generated_message": "",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "errors": []
    }
    
    # STEP 1: SCORE LEAD
    try:
        score = calculate_lead_score(raw_lead)
        result["lead_score"] = score
        logger.info(f"[Lead {lead_id}] Score computed: {score}")
    except Exception as e:
        logger.error(f"[Lead {lead_id}] Scoring Error: {str(e)}\n{traceback.format_exc()}")
        result["errors"].append(f"Scoring Error: {str(e)}")
        # Pipeline resilience: proceed with a default neutral score
        score = 50
        result["lead_score"] = score
        logger.info(f"[Lead {lead_id}] Using default score of 50 due to initial failure.")

    # STEP 2: DETERMINE STRATEGY
    try:
        strategy = determine_message_strategy(score, raw_lead)
        
        result["trigger_type"] = strategy.get("triggerType", "")
        result["channel"] = strategy.get("channel", "")
        result["urgency_level"] = strategy.get("urgency_level", "")
        result["reasoning"] = strategy.get("reasoning", "")
        
        # Log decision logic
        logger.info(f"[Lead {lead_id}] Strategy -> Send: {strategy.get('should_send')}, Trigger: {result['trigger_type']}")
        
        # Hard check to abort the pipeline appropriately 
        if not strategy.get("should_send"):
            logger.warning(f"[Lead {lead_id}] Send Skipped. Reason: {result['reasoning']}")
            result["status"] = "skipped"
            return result
            
    except Exception as e:
        logger.error(f"[Lead {lead_id}] Strategy Calculation Error: {str(e)}\n{traceback.format_exc()}")
        result["errors"].append(f"Strategy Error: {str(e)}")
        result["status"] = "failed"
        return result

    # STEP 3: BUILD REQUEST PAYLOAD
    try:
        payload = build_api_request(strategy, raw_lead, counselor_name)
    except Exception as e:
        logger.error(f"[Lead {lead_id}] Request Payload Build Error: {str(e)}\n{traceback.format_exc()}")
        result["errors"].append(f"Payload Formatting Error: {str(e)}")
        result["status"] = "failed"
        return result
        
    # STEP 4: SEND TO GENERATION API
    try:
        api_resp = send_to_api(payload)
        result["api_response"] = api_resp
        
        # Intercept if our send_to_api module returned a predefined error dict mapping.
        if isinstance(api_resp, dict) and "error" in api_resp:
            logger.error(f"[Lead {lead_id}] API Rejection / Failure: {api_resp['error']}")
            result["errors"].append(f"API Error Code: {api_resp['error']}")
            result["status"] = "failed"
            return result
            
        result["status"] = "sent"
        
        # Extract Actual Output Text Data (trying standard API response architectures)
        if isinstance(api_resp, dict):
            # Probe common expected dictionary keys
            if "data" in api_resp and "message" in api_resp["data"]:
                result["generated_message"] = api_resp["data"]["message"]
            elif "message" in api_resp:
                result["generated_message"] = api_resp["message"]
            elif "response" in api_resp:
                 result["generated_message"] = api_resp["response"]
            else:
                 # Fallback to pure dump
                 result["generated_message"] = str(api_resp)
        else:
            result["generated_message"] = str(api_resp)
            
    except Exception as e:
        logger.error(f"[Lead {lead_id}] Send Execution Error: {str(e)}\n{traceback.format_exc()}")
        result["errors"].append(f"Transport layer error: {str(e)}")
        result["status"] = "failed"
        return result
        
    return result


def run_pipeline_for_all(leads: list, counselor_map: dict) -> list:
    """
    Executes the entire pipeline across an array of leads, adding anti-rate limiting 
    delays and compiling the results into a high-level summary overview.
    
    Args:
        leads (list): Total Array of input CRM raw lead dictionaries
        counselor_map (dict): Mapping between lead_id -> string assigned Counselor
        
    Returns:
        list: Aggregated pipeline array containing the full 'run_pipeline()' output for each.
    """
    results = []
    
    total = len(leads)
    sent = 0
    skipped = 0
    failed = 0
    
    logger.info(f"--- INIT BATCH PIPELINE FOR {total} LEADS ---")
    
    for count_index, lead in enumerate(leads):
        lead_id = lead.get("id", "unknown")
        
        # Fallback to unassigned "System Default" mapping if key map is missing
        counselor_name = counselor_map.get(lead_id, "Assigning Officer")
        
        # Execute Full Pipeline
        lead_result = run_pipeline(lead, counselor_name)
        results.append(lead_result)
        
        # Tabulate Metrics
        status = lead_result["status"]
        if status == "sent":
            sent += 1
        elif status == "skipped":
            skipped += 1
        else:
            failed += 1
            
        # Throttling Logic - Prevent downstream API 429 Too Many Requests errors
        # Avoid putting the delay on the final iterated element
        if count_index < total - 1:
            time.sleep(1)
            
    # Print and Log System Wide Overview 
    summary_msg = f"Batch Complete Overview -> Total: {total} | Sent: {sent} | Skipped: {skipped} | Failed: {failed}"
    logger.info(summary_msg)
    print(f"\n====================== BATCH SUMMARY ======================\n{summary_msg}\n===========================================================\n")
    
    return results

if __name__ == "__main__":
    # Test batch list integration
    mock_leads = [
        # Lead 1: Extremely active engagement. Should succeed sending
        {
             "id": "1001",
             "name": "Arjun Kumar",
             "stage": "application",
             "source": "referral",
             "response_time_hours": 1,
             "form_completion_percentage": 25, # < 50 => trigger: application_incomplete
             "days_since_last_contact": 3,
             "course_interest": "B.Tech Computer Science"
        },
        # Lead 2: Rate limited by 'Don't send if previous message >= 3' rule
        {
             "id": "1002",
             "name": "Sarah Connor",
             "stage": "inquiry",
             "previous_message_count_today": 4, 
             "days_since_last_contact": 0
        }
    ]
    
    mock_counselors = {
        "1001": "Rahul Sharma",
        "1002": "Priya Verma"
    }
    
    final_output = run_pipeline_for_all(mock_leads, mock_counselors)
    
    import json
    print("OUTPUT ARRAY:")
    print(json.dumps(final_output, indent=2))
