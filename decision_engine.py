import math

def determine_message_strategy(lead_score: int, lead_data: dict) -> dict:
    """
    Determines the messaging strategy for a lead by evaluating scoring logic
    and pipeline state, preparing the payload for the WhatsApp Message API.

    Decision Matrix Rules Applied:
    1. Send blocking / Rate limiting (based on prior contact & time of day).
    2. Stage progression triggering (maps to prompt types).
    3. Urgency evaluation (based on deadlines and scores).
    4. Delays & Timing optimization.
    5. Channel preference determination.

    Args:
        lead_score (int): Normalized score from 0-100 indicating intent.
        lead_data (dict): All available attributes of the lead.

    Returns:
        dict: A strategy payload containing should_send, timing, channel, trigger and reasoning.
    """

    # Extract commonly used variables with defensive defaults
    stage = str(lead_data.get("stage", "")).lower()
    days_since_last_contact = lead_data.get("days_since_last_contact", 0)
    current_hour = lead_data.get("current_hour", 12)
    previous_msgs = lead_data.get("previous_message_count_today", 0)
    deadline_days = lead_data.get("deadline_days")
    app_data = lead_data.get("applicationData", {})
    doc_stats = lead_data.get("docStats", {})

    # 1. INITIALIZE DEFAULTS
    strategy = {
        "should_send": True,
        "timing_delay_hours": 0,
        "triggerType": "NEW_LEAD_ASSIGNED",
        "channel": "whatsapp",
        "urgency_level": "low",
        "reasoning": "Standard send criteria met."
    }

    # 2. DETERMINE URGENCY LEVEL
    # High: Red-hot score, or impending deadline, or overdue payments
    is_payment_overdue = (stage == "payment" and app_data.get("isOverdue", False))
    if lead_score >= 85 or (deadline_days is not None and deadline_days <= 2) or is_payment_overdue:
        strategy["urgency_level"] = "high"
    elif (50 <= lead_score <= 84) or (deadline_days is not None and 3 <= deadline_days <= 7):
        strategy["urgency_level"] = "medium"
    else:
        strategy["urgency_level"] = "low"

    # 3. DETERMINE CHANNEL PREFERENCE
    # Payment urgency supersedes everything for WhatsApp
    if stage == "payment" and strategy["urgency_level"] == "high":
        strategy["channel"] = "whatsapp"
    elif lead_score >= 70:
        strategy["channel"] = "whatsapp"
    elif lead_score < 30:
        strategy["channel"] = "email"
    else:
        # Fallback to last interaction channel if valid, else standard whatsapp
        last_inter = str(lead_data.get("last_interaction_type", "")).lower()
        if last_inter in ["whatsapp", "email", "sms"]:
            strategy["channel"] = last_inter
        else:
            strategy["channel"] = "whatsapp"

    # 4. TRIGGER TYPE MAPPING
    # This determines the prompt injected into the LLM logic
    if stage == "inquiry":
        if lead_score >= 70:
            strategy["triggerType"] = "NEW_LEAD_ASSIGNED"
        elif lead_score < 70 and days_since_last_contact > 7:
            strategy["triggerType"] = "COLD_REENGAGEMENT"
        else:
            strategy["triggerType"] = "NEW_LEAD_ASSIGNED"
            
    elif stage == "engagement":
        call_remarks = lead_data.get("callRemarks", [])
        has_positive = False
        # Look for positive interest signals in previous calls
        for remark in call_remarks:
            text_to_check = ""
            if isinstance(remark, str):
                text_to_check = remark.lower()
            elif isinstance(remark, dict):
                text_to_check = str(remark.get("disposition", "")).lower() + " " + str(remark.get("note", "")).lower()
                
            if "positive" in text_to_check or "interest" in text_to_check or "good" in text_to_check:
                has_positive = True
                break
                
        if has_positive:
            strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"
        else:
            strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"
            
    elif stage == "application":
        form_completion = lead_data.get("form_completion_percentage", 0)
        days_in_stage = lead_data.get("days_since_stage_entry", 0)
        
        if days_in_stage > 5:
            strategy["triggerType"] = "COLD_REENGAGEMENT"
        elif form_completion < 50:
            strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"
        else:
            strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"
            
    elif stage == "verification":
        verified = doc_stats.get("verified", 0)
        total = doc_stats.get("total", 0)
        
        if total > 0 and verified < total:
            strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"
        elif total > 0 and verified == total:
            strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"
            
    elif stage == "admission":
        if deadline_days is not None and 0 < deadline_days <= 3:
            strategy["triggerType"] = "OFFER_LETTER_SENT"
        elif deadline_days is not None and deadline_days > 3:
            strategy["triggerType"] = "OFFER_LETTER_SENT"
            
    elif stage == "payment":
        seat_block_paid = app_data.get("seatBlockPaid")
        days_to_installment = app_data.get("daysToNextInstallment", 999) # Default large number
        
        if seat_block_paid is False:
            strategy["triggerType"] = "SEAT_BLOCK_PENDING"
        elif days_to_installment <= 3:
            strategy["triggerType"] = "FEES_DISCUSSION"
        else:
            strategy["triggerType"] = "FEES_DISCUSSION"
            
    elif stage == "enrollment":
        strategy["triggerType"] = "PIPELINE_STAGE_CHANGE"


    # 5. BLOCKING & TIMING LOGIC
    # Priority 1: Check Hard Blocks (Rule 1)
    if previous_msgs >= 3:
        strategy["should_send"] = False
        strategy["timing_delay_hours"] = 24
        strategy["reasoning"] = "Max daily message limit reached (>=3)."
        
    elif current_hour < 9 or current_hour > 21:
        strategy["should_send"] = False
        if current_hour < 9:
            strategy["timing_delay_hours"] = 9 - current_hour
        else:
            strategy["timing_delay_hours"] = (24 - current_hour) + 9
        strategy["reasoning"] = f"Outside operating hours ({current_hour}:00 is not between 9 AM - 9 PM). Delaying to 9 AM."
        
    elif days_since_last_contact < 1 and lead_score < 85:
        # Prevent badgering non-hot leads
        strategy["should_send"] = False
        strategy["timing_delay_hours"] = 4
        strategy["reasoning"] = "Contacted < 1 day ago and lead score is not 'Red-hot'. Padding spacing by 4 hours."
        
    # Priority 2: Apply Smart Timing (Rule 4) if not blocked
    else:
        # Convert < 2 hours logic to days logic for comparison 
        # (days_since_last_contact < 2/24) means less than 2 hours ago
        less_than_two_hours = (days_since_last_contact < (2.0 / 24.0)) 

        if lead_score >= 86 and strategy["urgency_level"] == "high":
            strategy["timing_delay_hours"] = 0
            strategy["reasoning"] = "Red-hot lead + High urgency. Sending immediately."
        elif 61 <= lead_score <= 85 and less_than_two_hours:
            strategy["timing_delay_hours"] = 2
            strategy["reasoning"] = "Hot lead, but contacted < 2 hours ago. Delaying message by 2 hours."
        elif 31 <= lead_score <= 60:
            strategy["timing_delay_hours"] = 4
            strategy["reasoning"] = "Warm lead profile. Imposed generic 4-hour delay strategy."
        elif lead_score <= 30 and days_since_last_contact > 14:
            strategy["timing_delay_hours"] = 24
            strategy["reasoning"] = "Cold lead with stale engagement (>14 days). Batching for 24-hr delay."
            
    return strategy

if __name__ == "__main__":
    # Test cases to validate decision engine
    def print_plan(name, score, data):
        print(f"\n--- Strategy for: {name} ---")
        strategy = determine_message_strategy(score, data)
        for k, v in strategy.items():
            print(f"  {k}: {v}")

    # Case 1: High intent application incomplete
    print_plan("Alice", 88, {
        "stage": "application",
        "current_hour": 14,
        "form_completion_percentage": 20,
        "days_since_last_contact": 2,
        "deadline_days": 1, 
        "previous_message_count_today": 0
    })

    # Case 2: Max messages sent, blocked
    print_plan("Bob", 65, {
        "stage": "inquiry",
        "current_hour": 10,
        "previous_message_count_today": 3,
        "days_since_last_contact": 0.5
    })

    # Case 3: Middle of the night
    print_plan("Charlie", 95, {
        "stage": "admission",
        "current_hour": 2,
        "deadline_days": 2,
        "previous_message_count_today": 0
    })

    # Case 4: Engagement with positive remarks
    print_plan("Diana", 62, {
        "stage": "engagement",
        "current_hour": 15,
        "callRemarks": [{"disposition": "Positive", "note": "Wants MBA"}],
        "days_since_last_contact": 3
    })
