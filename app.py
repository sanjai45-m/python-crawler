import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import pool, extras
from urllib.parse import urlparse
import requests
import time
from flask_cors import CORS
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://neondb_owner:npg_vH7FTAlr5hLd@ep-autumn-cell-a1giggcu-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require')
API_KEY = "AIzaSyCr1sfsW7LhMeodPJv7I_4ddAfaZMvqyiU"
ENGINE_ID = "63dd1934e204e4745"

# Create a connection pool
try:
    postgres_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        dsn=DATABASE_URL,
        sslmode='require'
    )
    print("Connection pool created successfully")
except Exception as e:
    print("Error creating connection pool:", e)
    raise

# Initialize database tables and indexes
def initialize_database():
    conn = None
    try:
        conn = postgres_pool.getconn()
        with conn.cursor() as cursor:
            # Start transaction
            conn.autocommit = False
            
            # Create jobs table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    company TEXT NOT NULL,
                    experience TEXT,
                    location TEXT,
                    skills TEXT[],
                    salary TEXT,
                    link TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,
                    posted_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Verify UNIQUE constraint exists
            cursor.execute("""
                SELECT conname FROM pg_constraint 
                WHERE conrelid = 'jobs'::regclass AND contype = 'u'
            """)
            constraints = cursor.fetchall()
            
            if not constraints:
                cursor.execute("ALTER TABLE jobs ADD CONSTRAINT jobs_link_key UNIQUE (link)")
            
            # Create indexes if not exists
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(title)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(location)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source)
            """)
            
            conn.commit()
            print("Database initialized and verified")
            return True
            
    except Exception as e:
        if conn:
            conn.rollback()
        print("Database initialization failed:", e)
        raise
    finally:
        if conn:
            postgres_pool.putconn(conn)

# Cleanup old jobs
def cleanup_old_jobs():
    conn = None
    try:
        conn = postgres_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM jobs WHERE posted_date < NOW() - INTERVAL '30 DAYS'"
            )
            conn.commit()
            print(f"Cleaned up {cursor.rowcount} old jobs")
    except Exception as e:
        print("Cleanup failed:", e)
    finally:
        if conn:
            postgres_pool.putconn(conn)

# Store jobs in database
def store_jobs(jobs_data, source="google"):
    conn = None
    try:
        conn = postgres_pool.getconn()
        with conn.cursor() as cursor:
            # Start transaction
            conn.autocommit = False
            
            inserted_count = 0
            duplicates_count = 0
            
            for job in jobs_data:
                try:
                    cursor.execute("""
                        INSERT INTO jobs (title, company, location, link, source)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (link) DO NOTHING
                        RETURNING id
                    """, (
                        job.get('job_title_from_search'),
                        job.get('company_name'),
                        job.get('location', ''),
                        job.get('career_url'),
                        source
                    ))
                    
                    if cursor.fetchone():
                        inserted_count += 1
                    else:
                        duplicates_count += 1
                        
                except Exception as e:
                    print(f"Error inserting job {job.get('career_url')}: {e}")
                    continue
            
            conn.commit()
            return {
                "new_jobs": inserted_count,
                "duplicates": duplicates_count
            }
            
    except Exception as e:
        if conn:
            conn.rollback()
        print("Error storing jobs:", e)
        raise
    finally:
        if conn:
            postgres_pool.putconn(conn)

# Fetch jobs from database
def fetch_jobs(role=None, location=None, source=None, limit=100):
    conn = None
    try:
        conn = postgres_pool.getconn()
        with conn.cursor(cursor_factory=extras.DictCursor) as cursor:
            query = "SELECT * FROM jobs WHERE 1=1"
            params = []
            
            if role:
                query += " AND title ILIKE %s"
                params.append(f"%{role}%")
            
            if location:
                query += " AND location ILIKE %s"
                params.append(f"%{location}%")
            
            if source:
                query += " AND source = %s"
                params.append(source)
            
            query += " ORDER BY posted_date DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            jobs = cursor.fetchall()
            
            # Convert to list of dictionaries
            return [dict(job) for job in jobs]
            
    except Exception as e:
        print("Error fetching jobs:", e)
        raise
    finally:
        if conn:
            postgres_pool.putconn(conn)

# API Endpoints
@app.route('/search-careers', methods=['GET'])
def search_careers():
    role = request.args.get('role')
    city = request.args.get('city')

    if not role or not city:
        return jsonify({"error": "Please provide both 'role' and 'city' query parameters."}), 400

    query = f"{role} jobs in {city}"
    results = get_google_api_results(query)
    
    # Store the results in database
    storage_result = store_jobs(results, source="google")
    
    return jsonify({
        "query": query,
        "results_count": len(results),
        "storage_result": storage_result,
        "data": results
    })

@app.route('/jobs', methods=['GET'])
def get_jobs():
    role = request.args.get('role')
    location = request.args.get('location')
    source = request.args.get('source')
    limit = request.args.get('limit', default=100, type=int)
    
    try:
        jobs = fetch_jobs(role, location, source, limit)
        return jsonify({
            "count": len(jobs),
            "jobs": jobs
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Your existing Google API functions
# Your existing Google API functions
def get_company_name_from_url(url):
    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname or ''
        hostname = hostname.replace('www.', '')
        parts = hostname.split('.')
        for part in parts:
            if part not in ['com', 'in', 'org', 'net']:
                return part.capitalize()
        return hostname.split('.')[0].capitalize()
    except Exception as e:
        return "Unknown"

def get_google_api_results(query, pages=10):
    base_url = "https://www.googleapis.com/customsearch/v1"
    all_results = []
    seen_urls = set()

    for i in range(pages):
        start_index = i * 10 + 1
        params = {
            'key': API_KEY,
            'cx': ENGINE_ID,
            'q': query,
            'start': start_index,
            'num': 10
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'items' not in data:
                break

            for item in data['items']:
                link = item.get('link')
                title = item.get('title', '').strip()

                if not link or link in seen_urls:
                    continue

                if any(x in link for x in ['linkedin.com', 'naukri.com', 'indeed.com', 'glassdoor.com']):
                    continue

                if any(x in link.lower() for x in ['careers', 'jobs', 'join-us', 'hiring']):
                    company_name = get_company_name_from_url(link)
                    all_results.append({
                        "company_name": company_name,
                        "job_title_from_search": title,
                        "career_url": link
                    })
                    seen_urls.add(link)

            time.sleep(1.5)

        except Exception as e:
            print(f"Error: {e}")
            break

    return all_results
if __name__ == '__main__':
    # Initialize database on startup
    initialize_database()
    # Cleanup old jobs
    cleanup_old_jobs()
    
    app.run(host='0.0.0.0', port=8082, debug=True)