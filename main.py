import os
from dotenv import load_dotenv

load_dotenv()
import logging
from datetime import datetime, timezone
import time
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Core AI Components
from scoring_engine import calculate_lead_score
from decision_engine import determine_message_strategy
from api_client import build_api_request, send_to_api
from orchestrator import run_pipeline, run_pipeline_for_all

# Approval & Oversight Components
from approval_layer import (
    should_require_approval,
    ApprovalQueue,
    run_pipeline_with_approval,
    process_approval_decision
)

# Tracking & ML Feedback Components
from tracking_layer import (
    MessageTracker,
    FeedbackLoop,
    get_enriched_lead_score
)

from db import init_db

# ==============================================================================
# CONFIGURATION & SETUP
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fastapi_app")

app = FastAPI(
    title="Lead101 Decision Engine & Orchestrator API",
    description="The complete AI Messaging Pipeline, Scoring loop, and Feedback integration exposed as REST endpoints.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    init_db()

@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Global request tracker to ensure high observability."""
    start_time = datetime.now(timezone.utc)
    logger.info(f"Incoming Request: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Internal Pipeline Crash on {request.url.path} -> {e}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )

# ==============================================================================
# PYDANTIC MODEL SCHEMAS
# ==============================================================================
class ScoreRequest(BaseModel):
    lead_data: Dict[str, Any] = Field(..., description="Raw dictionary containing lead behaviors and traits")
    include_engagement: bool = Field(False, description="Flag to inject message interaction history into the score")

class BatchScoreRequest(BaseModel):
    leads: List[Dict[str, Any]] = Field(..., description="Array of leads to score")

class StrategyRequest(BaseModel):
    lead_data: Dict[str, Any] = Field(..., description="Lead traits and context variables")
    lead_score: Optional[int] = Field(None, description="Optional pre-computed score; calculates autonomously if missing")

class PipelineRunRequest(BaseModel):
    lead_data: Dict[str, Any] = Field(..., description="Input Lead context")
    counselor_name: str = Field(..., description="Assigned counselor for proxy payload signature")
    require_approval: bool = Field(False, description="Whether to route generated intents through human review queue")

class BatchPipelineRunRequest(BaseModel):
    leads: List[Dict[str, Any]] = Field(..., description="An array of CRM queued leads")
    counselor_map: Dict[str, str] = Field(..., description="Mapping Dictionary between lead_id and assigned agent name")
    require_approval: bool = Field(False, description="Require manual override verification (Bulk Mode)")

class ApprovalDecisionRequest(BaseModel):
    decision: str = Field(..., description="'approve' or 'reject'")
    edited_message: Optional[str] = Field(None, description="Counselor's human edited syntax correction")
    rejection_reason: Optional[str] = Field(None, description="Analytics metadata regarding why this was paused")

class TrackDeliveryRequest(BaseModel):
    message_id: str = Field(..., description="Originating message tracking UUID")
    delivered: bool = Field(..., description="Webhook delivery success status")
    read: bool = Field(False, description="Client read-receipt acknowledgment status")

class TrackReplyRequest(BaseModel):
    message_id: str = Field(..., description="Origin Tracking ID")
    reply_text: str = Field(..., description="Raw string input from the lead reply")

class TrackConversionRequest(BaseModel):
    message_id: str = Field(..., description="Origin Tracking ID")
    conversion_type: str = Field(..., description="Event Type: form_submitted, payment_made, etc.")


# ==============================================================================
# SECTION 1: SCORING ENDPOINTS
# ==============================================================================
@app.post("/score", tags=["Scoring"], response_model=Dict[str, Any])
def score_lead(req: ScoreRequest):
    """Calculates instantaneous lead conversion intent probabilities."""
    try:
        lead_id = str(req.lead_data.get("id", "unknown"))
        if req.include_engagement:
            res = get_enriched_lead_score(req.lead_data)
            score = res.get("final_score", 0)
            bonus = res.get("engagement_bonus", 0)
        else:
            score = calculate_lead_score(req.lead_data)
            base_score = score
            bonus = 0

        # Define categorical tag
        category = "cold"
        if score > 85: category = "red_hot"
        elif score > 60: category = "hot"
        elif score > 30: category = "warm"

        return {
            "lead_id": lead_id,
            "base_score": score - bonus if req.include_engagement else score,
            "engagement_bonus": bonus if req.include_engagement else None,
            "final_score": score,
            "score_category": category,
            "should_prioritize": score >= 70
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/score/batch", tags=["Scoring"], response_model=Dict[str, Any])
def score_batch(req: BatchScoreRequest):
    """Optimized sequence calculation over large arrays of CRM leads."""
    results = []
    for lead in req.leads:
        try:
            score = calculate_lead_score(lead)
            results.append({"lead_id": str(lead.get("id")), "final_score": score})
        except Exception:
            pass # Failsafes skip broken leads locally
            
    # Ordered descent priority output
    results = sorted(results, key=lambda x: x["final_score"], reverse=True)
    return {"total": len(results), "results": results}


# ==============================================================================
# SECTION 2: STRATEGY ENDPOINTS
# ==============================================================================
@app.post("/strategy", tags=["Strategy"], response_model=Dict[str, Any])
def resolve_strategy(req: StrategyRequest):
    """Routes pipeline strategy matching constraints autonomously."""
    try:
        score = req.lead_score
        if score is None:
            score = calculate_lead_score(req.lead_data)
            
        strategy_response = determine_message_strategy(score, req.lead_data)
        strategy_response["_lead_score_used"] = score
        return strategy_response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ==============================================================================
# SECTION 3: PIPELINE ENDPOINTS
# ==============================================================================
@app.post("/pipeline/run", tags=["Pipeline"])
def run_single_pipeline(req: PipelineRunRequest):
    """Fires end-to-end processing execution unit with optional human staging layer."""
    try:
        if req.require_approval:
            result = run_pipeline_with_approval(req.lead_data, req.counselor_name, auto_send_if_approved=False)
        else:
            result = run_pipeline(req.lead_data, req.counselor_name)
            
        logger.info(f"Pipeline executed for lead {req.lead_data.get('id')}. Status: {result.get('status')}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipeline/run-batch", tags=["Pipeline"])
def run_batch_processor(req: BatchPipelineRunRequest):
    """Bulk Orchestrator dispatcher with rate throttling controls embedded."""
    total, sent, skipped, pending, failed = len(req.leads), 0, 0, 0, 0
    results = []
    
    try:
        for index, lead in enumerate(req.leads):
            counselor_name = req.counselor_map.get(str(lead.get("id")), "System Coordinator")
            
            if req.require_approval:
                res = run_pipeline_with_approval(lead, counselor_name)
            else:
                res = run_pipeline(lead, counselor_name)
                
            results.append(res)
            st = res.get("status")
            
            if st == "sent": sent += 1
            elif st == "skipped": skipped += 1
            elif st == "pending_approval": pending += 1
            else: failed += 1
            
            # Rate limiting logic locally enforced to prevent downstream crash
            if index < total - 1:
                time.sleep(1)
                
        return {
            "total": total, "sent": sent, "skipped": skipped, 
            "pending_approval": pending, "failed": failed, 
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# SECTION 4: APPROVAL ENDPOINTS
# ==============================================================================
@app.get("/approvals/pending", tags=["Approvals"])
def get_pending_queue():
    """Retrieves current prioritized review backlog for counselor dashboard."""
    q = ApprovalQueue()
    items = q.get_pending()
    return {"total_pending": len(items), "items": items}

@app.post("/approvals/{approval_id}/decide", tags=["Approvals"])
def submit_approval_override(approval_id: str, req: ApprovalDecisionRequest):
    """Executes network proxy post counselor UI verification."""
    result = process_approval_decision(
        approval_id, 
        req.decision, 
        req.edited_message, 
        req.rejection_reason
    )
    if result.get("status") == "error":
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result

@app.get("/approvals/summary", tags=["Approvals"])
def review_approval_kpi():
    """Calculates agent dashboard operational insights."""
    q = ApprovalQueue()
    return q.get_summary()


# ==============================================================================
# SECTION 5: TRACKING ENDPOINTS
# ==============================================================================
@app.post("/track/delivery", tags=["Tracking"])
def telemetry_delivery(req: TrackDeliveryRequest):
    """Listens to Webhook callbacks tracking network layer propagation rules."""
    tk = MessageTracker()
    res = tk.log_delivery_status(req.message_id, req.delivered, req.read)
    if not res: raise HTTPException(status_code=404, detail="Message Origin Not Found")
    return res

@app.post("/track/reply", tags=["Tracking"])
def telemetry_reply(req: TrackReplyRequest):
    """Ingests remote replies into internal classification intent structures."""
    tk = MessageTracker()
    res = tk.log_reply(req.message_id, req.reply_text)
    if not res: raise HTTPException(status_code=404, detail="Message Origin Not Found")
    return res

@app.post("/track/conversion", tags=["Tracking"])
def telemetry_conversion(req: TrackConversionRequest):
    """Binds backend CRM business changes successfully generated by direct messaging."""
    tk = MessageTracker()
    res = tk.log_conversion(req.message_id, req.conversion_type)
    if not res: raise HTTPException(status_code=404, detail="Message Origin Not Found")
    return res

@app.get("/track/lead/{lead_id}", tags=["Tracking"])
def telemetry_student_aggregate(lead_id: str):
    """Bundles entire CRM profile mapping lifecycle with sentiment adjustments."""
    tk = MessageTracker()
    msgs = tk.get_message_outcomes(lead_id)
    eng = FeedbackLoop().get_lead_engagement_score(lead_id)
    return {
        "lead_id": lead_id,
        "total_messages": len(msgs),
        "messages": msgs,
        "engagement_score": eng
    }

@app.get("/track/stats", tags=["Tracking"])
def telemetry_holistic_kpi():
    """Top level macro analysis array calculation routines."""
    tk = MessageTracker()
    return tk.get_performance_stats()


# ==============================================================================
# SECTION 6: FEEDBACK ENDPOINTS
# ==============================================================================
@app.get("/feedback/report", tags=["Feedback"])
def analyze_campaign_feedback():
    """Generates deep insight human reviewable document metrics and adjustments."""
    loop = FeedbackLoop()
    rpt = loop.generate_feedback_report()
    adj = loop.calculate_score_adjustments().get("adjustments", [])
    return {
        "report_text": rpt,
        "adjustments": adj,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

@app.get("/feedback/adjustments", tags=["Feedback"])
def ai_machine_read_feedback():
    """Strict JSON raw data analysis arrays for ML consumption architectures."""
    loop = FeedbackLoop()
    return loop.calculate_score_adjustments()


# ==============================================================================
# SECTION 7: HEALTH & INFO
# ==============================================================================
@app.get("/", tags=["Health"])
def engine_versioning():
    return {
        "service": "Lead101 Decision Engine",
        "version": "1.0.0",
        "status": "running",
        "endpoints": len(app.routes),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/health", tags=["Health"])
def engine_heartbeat():
    q = ApprovalQueue()
    tk = MessageTracker()
    try:
        stats = tk.get_performance_stats()
        msg_count = stats.get("total_sent", 0)
    except:
        msg_count = 0
        
    return {
        "status": "healthy",
        "approval_queue": q.get_summary().get("pending", 0),
        "messages_tracked": msg_count,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    # Standalone execution binding directly out to deployment hosts
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
