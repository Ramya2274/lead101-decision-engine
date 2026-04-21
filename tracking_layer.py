import json
import os
import uuid
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json

from scoring_engine import calculate_lead_score
from db import get_connection, release_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 1. MESSAGE TRACKING DATABASE INTERFACE
# ---------------------------------------------------------
class MessageTracker:
    """
    Manages the persistent layer for tracking all sent messages,
    deliveries, replies, and subsequent conversion events backing into PostgreSQL.
    """
    def log_sent_message(self, pipeline_result: dict, counselor_name: str = "") -> str:
        msg_id = str(uuid.uuid4())
        
        conn = get_connection()
        if not conn:
            return msg_id
            
        try:
            with conn.cursor() as cur:
                query = """
                INSERT INTO message_tracking (
                    message_id, lead_id, lead_name, lead_score, stage, trigger_type, 
                    channel, urgency_level, generated_message, counselor_name, reasoning,
                    sent_at, delivered, read, replied, converted
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), FALSE, FALSE, FALSE, FALSE)
                """
                cur.execute(query, (
                    msg_id,
                    str(pipeline_result.get("lead_id", "")),
                    pipeline_result.get("lead_name", ""),
                    pipeline_result.get("lead_score", 0),
                    pipeline_result.get("stage", ""),
                    pipeline_result.get("trigger_type", ""),
                    pipeline_result.get("channel", ""),
                    pipeline_result.get("urgency_level", ""),
                    pipeline_result.get("generated_message", ""),
                    counselor_name,
                    pipeline_result.get("reasoning", "")
                ))
            conn.commit()
        except Exception as e:
            logger.error(f"DB Insert Error: {e}")
            conn.rollback()
        finally:
            release_connection(conn)
            
        return msg_id
        
    def log_delivery_status(self, message_id: str, delivered: bool, read: bool = False) -> dict:
        conn = get_connection()
        if not conn: return None
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                update_fields = []
                if delivered:
                    update_fields.append("delivered = TRUE, delivered_at = COALESCE(delivered_at, NOW())")
                if read:
                    update_fields.append("read = TRUE, read_at = COALESCE(read_at, NOW())")
                    
                if not update_fields: return None
                
                query = f"""
                UPDATE message_tracking
                SET {', '.join(update_fields)}
                WHERE message_id = %s
                RETURNING *
                """
                cur.execute(query, (message_id,))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"DB Update Error log_delivery: {e}")
            conn.rollback()
            return None
        finally:
            release_connection(conn)

    def log_reply(self, message_id: str, reply_text: str) -> dict:
        conn = get_connection()
        if not conn: return None
        sentiment = self._analyze_reply_sentiment(reply_text)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                UPDATE message_tracking
                SET replied = TRUE, reply_text = %s, replied_at = NOW(), sentiment = %s
                WHERE message_id = %s
                RETURNING *
                """
                cur.execute(query, (reply_text, sentiment, message_id))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"DB Update Error log_reply: {e}")
            conn.rollback()
            return None
        finally:
            release_connection(conn)

    def log_conversion(self, message_id: str, conversion_type: str) -> dict:
        conn = get_connection()
        if not conn: return None
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                UPDATE message_tracking
                SET converted = TRUE, conversion_type = %s, converted_at = NOW()
                WHERE message_id = %s
                RETURNING *
                """
                cur.execute(query, (conversion_type, message_id))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"DB Update Error log_conversion: {e}")
            conn.rollback()
            return None
        finally:
            release_connection(conn)

    def _analyze_reply_sentiment(self, reply_text: str) -> str:
        """Keyword based sentiment matching engine."""
        text = str(reply_text).lower()
        positive_keywords = ["yes", "interested", "okay", "sure", "when", "how", "confirm", "ready", "ok", "haan", "ha"]
        negative_keywords = ["no", "not", "busy", "later", "nahi", "mat", "stop", "unsubscribe", "don't", "dont"]
        
        # Tokenize conservatively by spacing and punctuation
        tokens = [word.strip(".,!?\"'") for word in text.split()]
        pos_count = sum(1 for w in tokens if w in positive_keywords)
        neg_count = sum(1 for w in tokens if w in negative_keywords)
        
        if pos_count > neg_count: return "positive"
        elif neg_count > pos_count: return "negative"
        return "neutral"

    def get_message_outcomes(self, lead_id: str) -> list:
        conn = get_connection()
        if not conn: return []
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM message_tracking WHERE lead_id = %s ORDER BY sent_at DESC", (lead_id,))
                rows = cur.fetchall()
                res = []
                for r in rows:
                    d = dict(r)
                    for k,v in d.items(): 
                        if hasattr(v, 'isoformat'): d[k] = v.isoformat()
                    res.append(d)
                return res
        except Exception as e:
            logger.error(f"DB Select Error: {e}")
            return []
        finally:
            release_connection(conn)

    def get_performance_stats(self) -> dict:
        conn = get_connection()
        default_res = {
            "total_sent": 0, "delivery_rate": 0.0, "read_rate": 0.0, 
            "reply_rate": 0.0, "conversion_rate": 0.0, "positive_sentiment_rate": 0.0,
            "by_trigger_type": {}, "by_urgency_level": {}, "best_performing_trigger": "N/A", "worst_performing_trigger": "N/A"
        }
        if not conn: return default_res
        
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query_overall = """
                SELECT 
                    COUNT(*) as total_sent,
                    COUNT(NULLIF(delivered, FALSE)) as delivered_cnt,
                    COUNT(NULLIF(read, FALSE)) as read_cnt,
                    COUNT(NULLIF(replied, FALSE)) as replied_cnt,
                    COUNT(NULLIF(converted, FALSE)) as converted_cnt,
                    COUNT(NULLIF(sentiment = 'positive', FALSE)) as pos_sent_cnt
                FROM message_tracking
                """
                cur.execute(query_overall)
                overall = cur.fetchone()
                
                tot = overall['total_sent'] or 0
                deliv = overall['delivered_cnt'] or 0
                rd = overall['read_cnt'] or 0
                repl = overall['replied_cnt'] or 0
                conv = overall['converted_cnt'] or 0
                pos_s = overall['pos_sent_cnt'] or 0
                
                cur.execute("""
                SELECT trigger_type, COUNT(*) as sent, COUNT(NULLIF(converted, FALSE)) as converted
                FROM message_tracking GROUP BY trigger_type
                """)
                triggers = cur.fetchall()
                by_trigger = {}
                best_t, worst_t = None, None
                b_r, w_r = -1.0, 2.0
                
                for t in triggers:
                    tsent = t['sent']
                    tconv = t['converted']
                    tr = round(tconv / tsent, 4) if tsent > 0 else 0.0
                    trig_name = t['trigger_type']
                    by_trigger[trig_name] = {"sent": tsent, "converted": tconv, "conversion_rate": tr}
                    if tsent >= 1:
                        if tr > b_r: b_r, best_t = tr, trig_name
                        if tr < w_r: w_r, worst_t = tr, trig_name
                
                cur.execute("""
                SELECT urgency_level, COUNT(*) as sent, COUNT(NULLIF(converted, FALSE)) as converted
                FROM message_tracking GROUP BY urgency_level
                """)
                urgencies = cur.fetchall()
                by_urgency = {}
                for u in urgencies:
                    usent = u['sent']
                    uconv = u['converted']
                    ur = round(uconv / usent, 4) if usent > 0 else 0.0
                    by_urgency[u['urgency_level']] = {"sent": usent, "converted": uconv, "conversion_rate": ur}

            return {
                "total_sent": tot,
                "delivery_rate": round(deliv / tot, 4) if tot > 0 else 0.0,
                "read_rate": round(rd / deliv, 4) if deliv > 0 else 0.0,
                "reply_rate": round(repl / deliv, 4) if deliv > 0 else 0.0,
                "conversion_rate": round(conv / tot, 4) if tot > 0 else 0.0,
                "positive_sentiment_rate": round(pos_s / repl, 4) if repl > 0 else 0.0,
                "by_trigger_type": by_trigger,
                "by_urgency_level": by_urgency,
                "best_performing_trigger": best_t or "N/A",
                "worst_performing_trigger": worst_t or "N/A"
            }
        except Exception as e:
            logger.error(f"DB Stats Error: {e}")
            return default_res
        finally:
            release_connection(conn)

# ---------------------------------------------------------
# 2. FEEDBACK OPTIMIZATION LOOP
# ---------------------------------------------------------
class FeedbackLoop:
    """
    Consumes the structural feedback metrics and generates AI-driven
    adjustments to automatically feed into the internal decision parameters.
    """
    def calculate_score_adjustments(self) -> dict:
        expected = {
            "NEW_LEAD_ASSIGNED": 0.15,
            "PIPELINE_STAGE_CHANGE": 0.20,
            "COLD_REENGAGEMENT": 0.10,
            "OFFER_LETTER_SENT": 0.60,
            "FEES_DISCUSSION": 0.50,
            "SEAT_BLOCK_PENDING": 0.55
        }
        
        tracker = MessageTracker()
        stats = tracker.get_performance_stats()
        by_trigger = stats.get("by_trigger_type", {})
        
        adjustments = []
        for trig, tr_stats in by_trigger.items():
            exp_rate = expected.get(trig)
            if exp_rate is not None:
                actual = tr_stats["conversion_rate"]
                gap = round(actual - exp_rate, 4)
                
                sugg, pts = "no_change", 0
                if actual > exp_rate + 0.1:
                    sugg, pts = "increase_weight", 5 # Overperforming -> boost
                elif actual < exp_rate - 0.1:
                    sugg, pts = "decrease_weight", -5 # Underperforming -> dock
                    
                adjustments.append({
                    "trigger_type": trig,
                    "expected_rate": exp_rate,
                    "actual_rate": actual,
                    "gap": gap,
                    "suggestion": sugg,
                    "adjustment_points": pts
                })
                
        summary = f"Analyzed {len(adjustments)} message triggers for optimization."
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_messages_analyzed": stats.get("total_sent", 0),
            "adjustments": adjustments,
            "summary": summary
        }

    def generate_feedback_report(self) -> str:
        """Compiles technical stats into a human readable CRM report."""
        tracker = MessageTracker()
        stats = tracker.get_performance_stats()
        adj_data = self.calculate_score_adjustments()
        now_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
        
        lines = [
            "================================",
            "LEAD101 FEEDBACK REPORT",
            f"Generated: {now_str}",
            "================================",
            "OVERALL PERFORMANCE:",
            f"- Total Messages Sent: {stats['total_sent']}",
            f"- Delivery Rate: {stats['delivery_rate'] * 100:.1f}%",
            f"- Conversion Rate: {stats['conversion_rate'] * 100:.1f}%",
            ""
        ]
        
        best = stats['best_performing_trigger']
        worst = stats['worst_performing_trigger']
        if best != "N/A" and best in stats['by_trigger_type']:
            lines.append(f"TOP PERFORMING: {best} ({(stats['by_trigger_type'][best]['conversion_rate'] * 100):.0f}% conversion)")
        if worst != "N/A" and worst in stats['by_trigger_type']:
            lines.append(f"NEEDS IMPROVEMENT: {worst} ({(stats['by_trigger_type'][worst]['conversion_rate'] * 100):.0f}% conversion)")
            
        lines.extend(["", "SCORING ADJUSTMENTS RECOMMENDED:"])
        for adj in adj_data["adjustments"]:
            a_rate, e_rate = adj["actual_rate"] * 100, adj["expected_rate"] * 100
            sugg, pts = adj["suggestion"].replace("_", " "), adj["adjustment_points"]
            sugg_str = f"no change needed" if pts == 0 else f"{sugg} {'+5' if pts > 0 else '-5'}"
            lines.append(f"- {adj['trigger_type']}: actual {a_rate:.0f}% vs expected {e_rate:.0f}% -> {sugg_str}")
            
        lines.append("================================")
        report = "\n".join(lines)
        
        conn = get_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO feedback_reports (report_text, adjustments, generated_at)
                        VALUES (%s, %s, NOW())
                    """, (report, Json(adj_data["adjustments"])))
                conn.commit()
            except Exception as e:
                logger.error(f"DB Insert Report Error: {e}")
                conn.rollback()
            finally:
                release_connection(conn)
            
        return report

    def get_lead_engagement_score(self, lead_id: str) -> dict:
        """Personalizes the score injection based on this specific user's behavioral history."""
        tracker = MessageTracker()
        msgs = tracker.get_message_outcomes(lead_id)
        
        msgs_received = len(msgs)
        replied = any(m.get("replied") for m in msgs)
        converted = any(m.get("converted") for m in msgs)
        
        # Scrape latest known sentiment
        latest_sentiment = "neutral"
        for m in msgs: 
            if m.get("sentiment"):
                latest_sentiment = m.get("sentiment")
                break
                
        # Engagement Grading Logic 
        base = 0
        if replied: base += 30
        if latest_sentiment == "positive": base += 20
        elif latest_sentiment == "negative": base -= 20
        if converted: base += 40
        if msgs_received > 5 and not replied: base -= 10
            
        rec = "maintain"
        if base >= 30: rec = "increase_priority"
        elif base < 0: rec = "deprioritize"
            
        return {
            "lead_id": lead_id,
            "messages_received": msgs_received,
            "replied": replied,
            "sentiment": latest_sentiment,
            "converted": converted,
            "engagement_score": base,
            "recommendation": rec
        }

# ---------------------------------------------------------
# 3. GLOBAL ENRICHED COMPUTATION HOOK
# ---------------------------------------------------------
def get_enriched_lead_score(lead_data: dict) -> dict:
    """
    Main Entry Hook: Takes standard CRM data, compiles behavioral scoring, 
    then injects the Feedback loop modifier resulting in a true 1-to-100 metric.
    """
    base_score = calculate_lead_score(lead_data)
    lead_id = str(lead_data.get("id", "unknown"))
    
    # Calculate Engagement Bonus Profile
    fl = FeedbackLoop()
    eng_data = fl.get_lead_engagement_score(lead_id)
    eng_bonus = eng_data.get("engagement_score", 0)
    
    # Absolute ceiling bounds ensuring predictable ranges (0-100 rules applied)
    final = max(0, min(base_score + eng_bonus, 100))
    
    return {
        "lead_id": lead_id,
        "base_score": base_score,
        "engagement_bonus": eng_bonus,
        "final_score": final,
        "recommendation": eng_data.get("recommendation", "maintain"),
        "should_prioritize": final >= 70
    }
