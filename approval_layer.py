import json
import os
import uuid
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json

from scoring_engine import calculate_lead_score
from decision_engine import determine_message_strategy
from api_client import build_api_request, send_to_api
from db import get_connection, release_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 1. APPROVAL RULES EVALUATOR
# ---------------------------------------------------------
def should_require_approval(pipeline_result: dict) -> bool:
    """
    Evaluates whether a generated message pipeline action requires human oversight.
    
    APPROVAL RULES SUMMARY:
    NEEDS APPROVAL:
      - Any high urgency message
      - Payment and admission stage messages
      - Re-engagement messages (sensitive intent)
      - Very long messages (> 300 chars, if already generated)
      - Any message pipeline execution containing caught errors

    AUTO-SEND (No approval needed):
      - Low urgency welcome and follow-up messages
      - Score < 60 in early stages (Inquiry)
    """
    urgency = pipeline_result.get("urgency_level")
    trigger = pipeline_result.get("trigger_type")
    score = pipeline_result.get("lead_score", 0)
    stage = pipeline_result.get("stage")
    msg = pipeline_result.get("generated_message", "")
    errors = pipeline_result.get("errors", [])

    # Check "Needs Approval" (True) conditions first
    if urgency == "high":
        return True
    if trigger in ["OFFER_LETTER_SENT", "FEES_DISCUSSION", "COLD_REENGAGEMENT", "SEAT_BLOCK_PENDING"]:
        return True
    if score >= 86:
        return True
    if stage in ["admission", "payment"]:
        return True
    if msg and len(msg) > 300:
        return True
    if errors and len(errors) > 0:
        return True

    # Check Explicit "Auto-Send" (False) constraints
    if urgency == "low":
        return False
    if trigger in ["NEW_LEAD_ASSIGNED", "PIPELINE_STAGE_CHANGE"]:
        return False
    if score < 60 and stage == "inquiry":
        return False

    return False

# ---------------------------------------------------------
# 2. APPROVAL QUEUE STORAGE MANAGER
# ---------------------------------------------------------
class ApprovalQueue:
    """
    Manages the persistent layer for pending counselor message approvals.
    Backed by PostgreSQL database for transactional scale.
    """
    def add_to_queue(self, pipeline_result: dict) -> str:
        """Adds a pipeline payload to the pending PostgreSQL queue awaiting review."""
        app_id = str(uuid.uuid4())
        
        conn = get_connection()
        if not conn:
            logger.error("DB Connection failed")
            return app_id

        try:
            with conn.cursor() as cur:
                query = """
                INSERT INTO approval_queue (
                    approval_id, lead_id, lead_name, lead_score, trigger_type, 
                    stage, urgency_level, reasoning, generated_message, status, 
                    created_at, full_pipeline_result
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW(), %s)
                """
                cur.execute(query, (
                    app_id,
                    str(pipeline_result.get("lead_id", "")),
                    pipeline_result.get("lead_name", ""),
                    pipeline_result.get("lead_score", 0),
                    pipeline_result.get("trigger_type", ""),
                    pipeline_result.get("stage", ""),
                    pipeline_result.get("urgency_level", ""),
                    pipeline_result.get("reasoning", ""),
                    pipeline_result.get("generated_message", ""),
                    Json(pipeline_result)
                ))
            conn.commit()
        except Exception as e:
            logger.error(f"DB Insert Error: {e}")
            conn.rollback()
        finally:
            release_connection(conn)
            
        return app_id

    def get_pending(self) -> list:
        """Retrieves and sorts all pending queue items logically directly from SQL."""
        conn = get_connection()
        if not conn:
            return []
            
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                SELECT * FROM approval_queue 
                WHERE status = 'pending'
                ORDER BY 
                    CASE urgency_level
                        WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 3
                        ELSE 4
                    END,
                    created_at ASC
                """
                cur.execute(query)
                rows = cur.fetchall()
                results = []
                for row in rows:
                    r = dict(row)
                    if hasattr(r['created_at'], 'isoformat'):
                        r['created_at'] = r['created_at'].isoformat()
                    results.append(r)
                return results
        except Exception as e:
            logger.error(f"DB Select Error: {e}")
            return []
        finally:
            release_connection(conn)

    def approve(self, approval_id: str, edited_message: str = None) -> dict:
        """Marks a queue document as approved and injects human edits."""
        conn = get_connection()
        if not conn:
            return None
            
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM approval_queue WHERE approval_id = %s", (approval_id,))
                row = cur.fetchone()
                if not row:
                    return None
                    
                final_msg = edited_message if edited_message is not None else row.get("generated_message", "")
                
                query = """
                UPDATE approval_queue
                SET status = 'approved',
                    final_message = %s,
                    approved_at = NOW()
                WHERE approval_id = %s
                RETURNING *
                """
                cur.execute(query, (final_msg, approval_id))
                updated_row = cur.fetchone()
            conn.commit()
            
            if updated_row:
                res = dict(updated_row)
                if hasattr(res['created_at'], 'isoformat'): res['created_at'] = res['created_at'].isoformat()
                if res.get('approved_at') and hasattr(res['approved_at'], 'isoformat'): res['approved_at'] = res['approved_at'].isoformat()
                return res
        except Exception as e:
            logger.error(f"DB Update Error: {e}")
            conn.rollback()
            return None
        finally:
            release_connection(conn)

    def reject(self, approval_id: str, reason: str = "") -> dict:
        """Marks a queue document as rejected completely."""
        conn = get_connection()
        if not conn:
            return None
            
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                UPDATE approval_queue
                SET status = 'rejected',
                    rejection_reason = %s,
                    rejected_at = NOW()
                WHERE approval_id = %s
                RETURNING *
                """
                cur.execute(query, (reason, approval_id))
                updated_row = cur.fetchone()
            conn.commit()
            
            if updated_row:
                res = dict(updated_row)
                if hasattr(res['created_at'], 'isoformat'): res['created_at'] = res['created_at'].isoformat()
                if res.get('rejected_at') and hasattr(res['rejected_at'], 'isoformat'): res['rejected_at'] = res['rejected_at'].isoformat()
                return res
        except Exception as e:
            logger.error(f"DB Update Error: {e}")
            conn.rollback()
            return None
        finally:
            release_connection(conn)

    def get_summary(self) -> dict:
        """Calculates analytic insights and response speeds via PostgreSQL aggregations."""
        conn = get_connection()
        default_res = {
            "total": 0, "pending": 0, "approved": 0, "rejected": 0, "avg_approval_time_minutes": 0.0
        }
        if not conn:
            return default_res
            
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT status, COUNT(*) as cnt FROM approval_queue GROUP BY status")
                rows = cur.fetchall()
                total = pending = approved = rejected = 0
                for r in rows:
                    if r['status'] == 'pending': pending = r['cnt']
                    elif r['status'] == 'approved': approved = r['cnt']
                    elif r['status'] == 'rejected': rejected = r['cnt']
                    total += r['cnt']
                
                cur.execute("""
                SELECT AVG(EXTRACT(EPOCH FROM (approved_at - created_at))/60) as avg_mins
                FROM approval_queue
                WHERE status = 'approved' AND approved_at IS NOT NULL AND created_at IS NOT NULL
                """)
                avg_row = cur.fetchone()
                avg_mins = avg_row['avg_mins'] if (avg_row and avg_row['avg_mins']) else 0.0
                
            return {
                "total": total,
                "pending": pending,
                "approved": approved,
                "rejected": rejected,
                "avg_approval_time_minutes": round(float(avg_mins), 2)
            }
        except Exception as e:
            logger.error(f"DB Summary Error: {e}")
            return default_res
        finally:
            release_connection(conn)

# ---------------------------------------------------------
# 3. MODIFIED PIPELINE ARCHITECTURE (PAUSE & WAIT MODE)
# ---------------------------------------------------------
def run_pipeline_with_approval(raw_lead: dict, counselor_name: str, auto_send_if_approved: bool = False) -> dict:
    """
    Executes scoring and preparation steps but pauses the network call
    to API if the message strategy qualifies for human oversight limits.
    """
    lead_id = str(raw_lead.get("id", "unknown"))
    raw_name = raw_lead.get("name") or raw_lead.get("first_name") or "Student"
    lead_name = str(raw_name).strip().split(" ")[0] if str(raw_name).strip() else "Student"
    
    stage = raw_lead.get("stage", "unknown")
    
    result = {
        "status": "failed",
        "lead_id": lead_id,
        "lead_name": lead_name,
        "lead_score": 0,
        "stage": stage,
        "trigger_type": "",
        "channel": "",
        "urgency_level": "",
        "reasoning": "",
        "api_response": None,
        "generated_message": "",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "errors": []
    }
    
    # 1. Scoring
    score = 50
    try:
        score = calculate_lead_score(raw_lead)
        result["lead_score"] = score
    except Exception as e:
        result["errors"].append(str(e))
        result["lead_score"] = score

    # 2. Strategy Mapping
    try:
        strategy = determine_message_strategy(score, raw_lead)
        result["trigger_type"] = strategy.get("triggerType", "")
        result["channel"] = strategy.get("channel", "")
        result["urgency_level"] = strategy.get("urgency_level", "")
        result["reasoning"] = strategy.get("reasoning", "")
        
        if not strategy.get("should_send"):
            result["status"] = "skipped"
            return result
    except Exception as e:
        result["errors"].append(str(e))
        result["status"] = "failed"
        return result

    # 3. Build Formatting
    try:
        payload = build_api_request(strategy, raw_lead, counselor_name)
    except Exception as e:
        result["errors"].append(str(e))
        result["status"] = "failed"
        return result
        
    # Inject prepared payload to save it if we must queue this
    result["prepared_payload"] = payload
        
    # Check Approval
    needs_approval = should_require_approval(result)
    
    if not needs_approval:
        # Pipeline proceeds autonomously
        api_resp = send_to_api(payload)
        result["api_response"] = api_resp
        result["status"] = "sent"
        result["approval_required"] = False
        
        # Extract output
        if isinstance(api_resp, dict):
            if "data" in api_resp and "message" in api_resp["data"]:
                result["generated_message"] = api_resp["data"]["message"]
            elif "message" in api_resp:
                result["generated_message"] = api_resp["message"]
            else:
                 result["generated_message"] = str(api_resp)
        else:
             result["generated_message"] = str(api_resp)
        return result
    else:
        # Halt API execution, store in Queue
        q = ApprovalQueue()
        app_id = q.add_to_queue(result)
        
        result["status"] = "pending_approval"
        result["approval_id"] = app_id
        result["approval_required"] = True
        return result

# ---------------------------------------------------------
# 4. REVIEW PROCESSOR DISPATCH
# ---------------------------------------------------------
def process_approval_decision(approval_id: str, decision: str, edited_message: str = None, rejection_reason: str = "") -> dict:
    """
    Takes the human action input, updates the persistent queue, and optionally
    dispatches the approved transmission immediately onto the network stack.
    """
    q = ApprovalQueue()
    
    if decision == "approve":
        approved_item = q.approve(approval_id, edited_message)
        if not approved_item:
            return {"status": "error", "message": "Invalid approval_id"}
            
        # Network Execution Post-Approval
        orig_result = approved_item.get("full_pipeline_result", {})
        payload = orig_result.get("prepared_payload")
        
        if not payload:
            return {"status": "error", "message": "Missing API payload in stored data."}
            
        # (If human edits took place, we could inject them into the payload here if the upstream
        # API takes overrides. As requested, we call the native send_to_api function)
        api_resp = send_to_api(payload)
        
        return {
            "status": "sent", 
            "approval_id": approval_id, 
            "api_response": api_resp,
            "final_message": approved_item.get("final_message")
        }
        
    elif decision == "reject":
        rejected_item = q.reject(approval_id, rejection_reason)
        if not rejected_item:
            return {"status": "error", "message": "Invalid approval_id"}
            
        return {
            "status": "rejected", 
            "approval_id": approval_id, 
            "reason": rejection_reason
        }

    return {"status": "error", "message": "Invalid decision type"}
