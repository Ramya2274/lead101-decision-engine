import os
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.warning("DATABASE_URL not found in environment variables. Database operations will fail.")

# Connection pool instance
db_pool = None

def init_db():
    global db_pool
    if not DATABASE_URL:
        logger.error("Cannot initialize DB: DATABASE_URL is missing")
        return
        
    try:
        # Initialize connection pool
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        if db_pool:
            logger.info("PostgreSQL connection pool created successfully")
    except Exception as e:
        logger.error(f"Error creating PostgreSQL connection pool: {e}")
        return

    # Database Schema Definitions
    create_approval_queue_table = """
    CREATE TABLE IF NOT EXISTS approval_queue (
        approval_id VARCHAR PRIMARY KEY,
        lead_id VARCHAR,
        lead_name VARCHAR,
        lead_score INTEGER,
        trigger_type VARCHAR,
        stage VARCHAR,
        urgency_level VARCHAR,
        reasoning TEXT,
        generated_message TEXT,
        final_message TEXT,
        status VARCHAR DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        approved_at TIMESTAMP,
        rejected_at TIMESTAMP,
        rejection_reason TEXT,
        full_pipeline_result JSONB
    );
    """

    create_message_tracking_table = """
    CREATE TABLE IF NOT EXISTS message_tracking (
        message_id VARCHAR PRIMARY KEY,
        lead_id VARCHAR,
        lead_name VARCHAR,
        lead_score INTEGER,
        stage VARCHAR,
        trigger_type VARCHAR,
        channel VARCHAR,
        urgency_level VARCHAR,
        generated_message TEXT,
        counselor_name VARCHAR,
        reasoning TEXT,
        sent_at TIMESTAMP DEFAULT NOW(),
        delivered BOOLEAN DEFAULT FALSE,
        delivered_at TIMESTAMP,
        read BOOLEAN DEFAULT FALSE,
        read_at TIMESTAMP,
        replied BOOLEAN DEFAULT FALSE,
        reply_text TEXT,
        replied_at TIMESTAMP,
        sentiment VARCHAR,
        converted BOOLEAN DEFAULT FALSE,
        conversion_type VARCHAR,
        converted_at TIMESTAMP
    );
    """

    create_feedback_reports_table = """
    CREATE TABLE IF NOT EXISTS feedback_reports (
        id SERIAL PRIMARY KEY,
        report_text TEXT,
        adjustments JSONB,
        generated_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(create_approval_queue_table)
                cur.execute(create_message_tracking_table)
                cur.execute(create_feedback_reports_table)
            conn.commit()
            logger.info("PostgreSQL tables tracked computationally")
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL tables: {e}")
            conn.rollback()
        finally:
            release_connection(conn)

def get_connection():
    """Fetches a connection to execute queries."""
    global db_pool
    if db_pool is None:
        try:
            db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        except Exception as e:
            logger.error(f"Failed to initialize pool on demand: {e}")
            return None
    try:
        if db_pool:
            return db_pool.getconn()
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
    return None

def release_connection(conn):
    """Safely relinquishes connections back into the connection pool."""
    global db_pool
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error releasing connection to pool: {e}")
