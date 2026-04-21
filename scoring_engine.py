import math

def calculate_lead_score(lead_data: dict) -> int:
    """
    Calculates a dynamic lead score for the Lead101 CRM based on behavioral and historical data.
    
    The score ranges from 0 to 100, categorized as:
    - 0-30: Cold lead (Needs nurturing)
    - 31-60: Warm lead (Needs follow-up)
    - 61-85: Hot lead (High intent)
    - 86-100: Red-hot lead (Urgent conversion)

    Args:
        lead_data (dict): Dictionary containing lead attributes.
        
    Returns:
        int: Normalized lead score between 0 and 100.
    """
    
    # Initialize base score
    # Note: We allow the base score to exceed 100 initially because the final
    # multiplier and normalization will bring it into the 0-100 range.
    raw_score = 0
    
    # --- 1. SOURCE QUALITY (Max: 25 pts) ---
    # Higher intent sources get more weight.
    source_weights = {
        "referral": 25,
        "google_ads": 20,
        "facebook": 15,
        "website_form": 15,
        "walk_in": 10
    }
    source = str(lead_data.get("source", "")).lower()
    raw_score += source_weights.get(source, 0)
    
    # --- 2. STAGE PROGRESSION (Max: 40 pts) ---
    # Scoring leads based on their position in the funnel.
    stage_weights = {
        "inquiry": 5,        # Just started
        "engagement": 10,     # Interacted with content
        "application": 20,    # Documented intent
        "verification": 25,   # Background check in progress
        "admission": 30,      # Offer extended
        "payment": 35,        # Remittance pending
        "enrollment": 40      # Finalizing seat
    }
    stage = str(lead_data.get("stage", "")).lower()
    raw_score += stage_weights.get(stage, 0)
    
    # --- 3. ENGAGEMENT SIGNALS (Max: 45 pts) ---
    # Direct actions taken by the lead.
    if lead_data.get("email_opened", False):
        raw_score += 10
        
    if lead_data.get("whatsapp_replied", False):
        raw_score += 15
        
    # Form completion is proportional to the percentage finished.
    # Maxes at +20 pts.
    form_completion = lead_data.get("form_completion_percentage", 0)
    if isinstance(form_completion, (int, float)):
        raw_score += (min(max(form_completion, 0), 100) / 100) * 20
        
    # --- 4. RECENCY & CHURN RISK (Penalty) ---
    # If the lead hasn't been contacted in a week, they are cooling off.
    days_since_last_contact = lead_data.get("days_since_last_contact")
    if days_since_last_contact is not None and days_since_last_contact > 7:
        # Penalty increases the longer they are left unattended (damping factor)
        penalty = 10 + min(days_since_last_contact - 7, 10) 
        raw_score -= penalty
        
    # --- 5. RESPONSE SPEED (Max: 15 pts) ---
    # Fast replies indicate high urgency/intent.
    response_speed = lead_data.get("response_time_hours")
    if response_speed is not None and response_speed < 2:
        raw_score += 15
        
    # --- 6. HISTORICAL CONVERSION MULTIPLIER ---
    # This factor adjusts the score based on how similar profiles converted in the past.
    # It acts as a probability weight.
    conversion_rate = lead_data.get("previous_year_conversion_rate_for_similar_profile", 1.0)
    
    # Handle cases where conversion_rate might be None or invalid
    if conversion_rate is None or not isinstance(conversion_rate, (int, float)):
        conversion_rate = 0.5  # Default to neutral if data is missing
        
    # Calculate final score
    final_score = raw_score * conversion_rate
    
    # Final Normalization: Ensure score is between 0 and 100
    # We round to the nearest integer as requested.
    return int(max(0, min(100, final_score)))

# Example Lead Scenarios for Testing
if __name__ == "__main__":
    test_leads = [
        {
            "name": "High Intent Referral",
            "source": "referral",
            "stage": "application",
            "days_since_last_contact": 1,
            "email_opened": True,
            "whatsapp_replied": True,
            "form_completion_percentage": 90,
            "response_time_hours": 0.5,
            "previous_year_conversion_rate_for_similar_profile": 0.9
        },
        {
            "name": "Cold Facebook Lead",
            "source": "facebook",
            "stage": "inquiry",
            "days_since_last_contact": 15,
            "email_opened": False,
            "whatsapp_replied": False,
            "form_completion_percentage": 10,
            "response_time_hours": 48,
            "previous_year_conversion_rate_for_similar_profile": 0.2
        }
    ]
    
    print("--- Lead101 Scoring Test ---")
    for lead in test_leads:
        score = calculate_lead_score(lead)
        status = "Cold"
        if score > 85: status = "Red-Hot"
        elif score > 60: status = "Hot"
        elif score > 30: status = "Warm"
        
        print(f"Lead: {lead['name']} | Score: {score} | Status: {status}")
