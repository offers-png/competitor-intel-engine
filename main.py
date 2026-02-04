# main.py
# Competitor Intelligence Engine - Strong Single-File MVP (Replit-ready)

import asyncio
import csv
import io
import os
import re
import uuid
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
import httpx

# PDF generation
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

# =========================
# APP SETUP
# =========================

app = FastAPI(title="Competitor Intelligence Engine", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# SQLITE DATABASE SETUP
# =========================

DB_PATH = "jobs.db"

def init_database():
    """Initialize SQLite database with jobs and reports tables"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_id TEXT PRIMARY KEY,
        status TEXT,
        created_at TEXT,
        completed_at TEXT,
        progress INTEGER DEFAULT 0,
        order_data TEXT,
        result TEXT,
        error TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reports (
        job_id TEXT PRIMARY KEY,
        pdf_data BLOB,
        csv_data TEXT,
        FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
    """)
    
    conn.commit()
    conn.close()
    print(f"Database initialized: {DB_PATH}")

init_database()

def get_db_connection():
    """Get a database connection"""
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# =========================
# DATABASE OPERATIONS
# =========================

def db_create_job(job_id: str, order: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new job in the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    created_at = datetime.utcnow().isoformat()
    
    cursor.execute("""
    INSERT INTO jobs (job_id, status, created_at, progress, order_data)
    VALUES (?, ?, ?, ?, ?)
    """, (job_id, "queued", created_at, 0, json.dumps(order)))
    
    conn.commit()
    conn.close()
    
    return {
        "job_id": job_id,
        "status": "queued",
        "created_at": created_at,
        "completed_at": None,
        "progress_percent": 0,
        "order": order,
        "result": None,
        "error": None
    }

def db_get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job from the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT job_id, status, created_at, completed_at, progress, order_data, result, error
    FROM jobs WHERE job_id = ?
    """, (job_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return None
    
    return {
        "job_id": row[0],
        "status": row[1],
        "created_at": row[2],
        "completed_at": row[3],
        "progress_percent": row[4] or 0,
        "order": json.loads(row[5]) if row[5] else None,
        "result": json.loads(row[6]) if row[6] else None,
        "error": row[7]
    }

def db_update_job(job_id: str, **updates):
    """Update a job in the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    field_map = {
        "status": "status",
        "completed_at": "completed_at",
        "progress_percent": "progress",
        "result": "result",
        "error": "error"
    }
    
    for key, value in updates.items():
        if key in field_map:
            db_field = field_map[key]
            if key == "result" and value is not None:
                value = json.dumps(value)
            cursor.execute(f"UPDATE jobs SET {db_field} = ? WHERE job_id = ?", (value, job_id))
    
    conn.commit()
    conn.close()

def db_get_all_jobs() -> List[Dict[str, Any]]:
    """Get all jobs from the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT job_id, status, created_at, completed_at, progress, order_data, result, error
    FROM jobs ORDER BY created_at DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    jobs = []
    for row in rows:
        jobs.append({
            "job_id": row[0],
            "status": row[1],
            "created_at": row[2],
            "completed_at": row[3],
            "progress_percent": row[4] or 0,
            "order": json.loads(row[5]) if row[5] else None,
            "result": json.loads(row[6]) if row[6] else None,
            "error": row[7]
        })
    
    return jobs

def db_save_report(job_id: str, pdf_data: bytes, csv_data: str):
    """Save report data to database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    INSERT OR REPLACE INTO reports (job_id, pdf_data, csv_data)
    VALUES (?, ?, ?)
    """, (job_id, pdf_data, csv_data))
    
    conn.commit()
    conn.close()

def db_get_report(job_id: str) -> Optional[Dict[str, Any]]:
    """Get report data from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT pdf_data, csv_data FROM reports WHERE job_id = ?", (job_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return None
    
    return {"pdf": row[0], "csv": row[1]}

# In-memory cache for active jobs (faster access during processing)
JOBS_CACHE: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = asyncio.Lock()

# Concurrency limits (protects from rate-limits & resource spikes)
MAX_CONCURRENT_JOBS = 2
MAX_CONCURRENT_BUSINESS_ANALYSIS = 8

JOB_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
BIZ_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_BUSINESS_ANALYSIS)

# Shared HTTP client (faster + safer than creating per-request)
HTTP_CLIENT: Optional[httpx.AsyncClient] = None

DEFAULT_HEADERS = {
    "User-Agent": "CompetitorIntelBot/1.2 (contact: support@yoursite.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# =========================
# MODELS
# =========================

class OrderRequest(BaseModel):
    email: EmailStr
    business_name: str = Field(min_length=1, max_length=120)
    industry: str = Field(min_length=2, max_length=50)
    location: str = Field(min_length=2, max_length=120)
    radius_miles: int = Field(default=10, ge=1, le=50)

class JobStatus(BaseModel):
    job_id: str
    status: str  # queued | running | completed | failed
    created_at: str
    completed_at: Optional[str] = None
    progress_percent: int = 0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# =========================
# HELPERS
# =========================

def utcnow_iso() -> str:
    return datetime.utcnow().isoformat()

async def set_job(job_id: str, **updates):
    """Update job in both cache and database"""
    async with JOBS_LOCK:
        if job_id in JOBS_CACHE:
            JOBS_CACHE[job_id].update(updates)
    db_update_job(job_id, **updates)

async def get_job(job_id: str) -> Dict[str, Any]:
    """Get job from cache or database"""
    async with JOBS_LOCK:
        if job_id in JOBS_CACHE:
            return JOBS_CACHE[job_id].copy()
    
    job = db_get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

async def safe_progress(job_id: str, pct: int, status: Optional[str] = None):
    pct = max(0, min(100, int(pct)))
    conn = get_db_connection()
    cursor = conn.cursor()
    if status:
        cursor.execute(
            "UPDATE jobs SET progress=?, status=? WHERE job_id=?",
            (pct, status, job_id)
        )
    else:
        cursor.execute(
            "UPDATE jobs SET progress=? WHERE job_id=?",
            (pct, job_id)
        )
    conn.commit()
    conn.close()

async def retry_async(fn, *args, tries=3, base_delay=0.6, **kwargs):
    """Simple retry with exponential backoff."""
    last_exc: Optional[Exception] = None
    for i in range(tries):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            await asyncio.sleep(base_delay * (2 ** i))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Retry failed with no exception captured")

# =========================
# API ENDPOINTS
# =========================

@app.post("/start-analysis")
async def start_analysis(order: OrderRequest):
    job_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO jobs (job_id, status, created_at, completed_at, progress, order_data, result, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (job_id, "queued", created_at, None, 0, json.dumps(order.dict()), None, None)
    )
    conn.commit()
    conn.close()

    asyncio.create_task(run_job_safe(job_id))

    return {
        "job_id": job_id,
        "status": "queued",
        "created_at": created_at,
        "completed_at": None,
        "progress_percent": 0,
        "result": None,
        "error": None
    }


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    return JobStatus(**(await get_job(job_id)))


@app.get("/jobs/{job_id}/pdf")
async def download_pdf(job_id: str):
    report = db_get_report(job_id)
    if not report or not report.get("pdf"):
        raise HTTPException(status_code=404, detail="PDF report not found")
    
    return Response(
        content=report["pdf"],
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=competitor_report_{job_id[:8]}.pdf"}
    )


@app.get("/jobs/{job_id}/csv")
async def download_csv(job_id: str):
    report = db_get_report(job_id)
    if not report or not report.get("csv"):
        raise HTTPException(status_code=404, detail="CSV report not found")
    
    return Response(
        content=report["csv"],
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=competitor_report_{job_id[:8]}.csv"}
    )


@app.get("/health")
async def health_check():
    all_jobs = db_get_all_jobs()
    running = len([j for j in all_jobs if j["status"] == "running"])
    queued = len([j for j in all_jobs if j["status"] == "queued"])
    completed = len([j for j in all_jobs if j["status"] == "completed"])
    return {
        "status": "healthy",
        "timestamp": utcnow_iso(),
        "storage": "sqlite",
        "total_jobs": len(all_jobs),
        "running_jobs": running,
        "queued_jobs": queued,
        "completed_jobs": completed
    }


@app.get("/jobs")
async def list_jobs():
    """List all jobs"""
    all_jobs = db_get_all_jobs()
    return {
        "total": len(all_jobs),
        "jobs": all_jobs
    }


@app.get("/")
async def root():
    all_jobs = db_get_all_jobs()
    return {
        "name": "Competitor Intelligence Engine",
        "version": "1.2.0",
        "storage": "sqlite (persistent)",
        "endpoints": {
            "start_analysis": "POST /start-analysis",
            "job_status": "GET /jobs/{job_id}",
            "list_jobs": "GET /jobs",
            "download_pdf": "GET /jobs/{job_id}/pdf",
            "download_csv": "GET /jobs/{job_id}/csv",
            "health": "GET /health",
            "docs": "GET /docs"
        },
        "stats": {
            "total_jobs": len(all_jobs),
            "completed": len([j for j in all_jobs if j["status"] == "completed"]),
            "running": len([j for j in all_jobs if j["status"] == "running"])
        }
    }

# =========================
# JOB RUNNER WITH ERROR HANDLING
# =========================

async def run_job_safe(job_id: str):
    async with JOB_SEMAPHORE:
        try:
            await run_job(job_id)
        except Exception as e:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE jobs SET status=?, error=? WHERE job_id=?",
                ("failed", str(e), job_id)
            )
            conn.commit()
            conn.close()
            print(f"Job {job_id} failed: {e}")


async def run_job(job_id: str):
    job = await get_job(job_id)
    order = job["order"]

    await safe_progress(job_id, 1, status="running")

    # STEP 1: Scrape Google Maps (10%)
    await safe_progress(job_id, 10)
    businesses = await scrape_google_maps(order["industry"], order["location"], order["radius_miles"])

    if not businesses:
        raise ValueError("No competitors found in this location")

    # STEP 2: Analyze competitors in parallel (20% -> 65%)
    await safe_progress(job_id, 20)

    analyzed: List[Dict[str, Any]] = []

    async def analyze_business_guarded(b: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        async with BIZ_SEMAPHORE:
            try:
                return await analyze_business(b)
            except Exception as e:
                print(f"Business analysis failed for {b.get('name')}: {e}")
                return None

    tasks = [asyncio.create_task(analyze_business_guarded(b)) for b in businesses]
    total = len(tasks)

    for idx, t in enumerate(asyncio.as_completed(tasks), start=1):
        result = await t
        if result:
            analyzed.append(result)
        pct = 20 + int((idx / total) * 45)
        await safe_progress(job_id, pct)

    if not analyzed:
        raise ValueError("All competitor analyses failed (network/rate limits). Try again later.")

    # STEP 3: Score & sort (65% -> 75%)
    await safe_progress(job_id, 65)
    for b in analyzed:
        b["score"] = calculate_score(b)
    analyzed.sort(key=lambda x: x.get("score", 0), reverse=True)
    await safe_progress(job_id, 75)

    # STEP 4: Market summary (75% -> 85%)
    summary = generate_market_summary(analyzed)
    await safe_progress(job_id, 85)

    # STEP 5: Generate outputs (85% -> 95%)
    pdf_url, csv_url = await generate_reports(job_id, analyzed, summary, order)
    await safe_progress(job_id, 95)

    # STEP 6: Save result (95% -> 98%)
    result_payload = {
        "summary": summary,
        "pdf": pdf_url,
        "csv": csv_url,
        "top_10": analyzed[:10],
        "total_analyzed": len(analyzed),
    }

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE jobs SET result=?, progress=? WHERE job_id=?",
        (json.dumps(result_payload), 98, job_id)
    )
    conn.commit()
    conn.close()

    # STEP 7: Notify user (98% -> 100%)
    await send_email(order["email"], job_id, pdf_url, csv_url, summary)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE jobs
        SET status=?, completed_at=?, progress=?
        WHERE job_id=?
        """,
        ("completed", datetime.utcnow().isoformat(), 100, job_id)
    )
    conn.commit()
    conn.close()
    
    print(f"Job {job_id} completed successfully")

# =========================
# SCRAPING (STUBBED ASYNC)
# =========================

async def scrape_google_maps(industry: str, location: str, radius: int) -> List[Dict[str, Any]]:
    """
    Replace this with SerpAPI / other provider integration.
    For now, stub + sleep with mock data.
    """
    print(f"Scraping Google Maps: {industry} in {location} (radius {radius}mi)")
    await asyncio.sleep(0.6)

    # Mock data - returns different businesses based on industry
    base_businesses = [
        {
            "name": f"ABC {industry.title()} Services",
            "website": f"https://abc{industry.lower().replace(' ', '')}.com",
            "place_id": "ChIJabc123",
            "rating": 4.8,
            "reviews": 247,
            "address": f"123 Main St, {location}",
            "phone": "(315) 555-0100",
        },
        {
            "name": f"Quick Response {industry.title()}",
            "website": f"https://quickresponse{industry.lower().replace(' ', '')}.com",
            "place_id": "ChIJqrs456",
            "rating": 4.6,
            "reviews": 189,
            "address": f"456 Oak Ave, {location}",
            "phone": "(315) 555-0200",
        },
        {
            "name": f"Elite {industry.title()} Co",
            "website": f"https://elite{industry.lower().replace(' ', '')}.com",
            "place_id": "ChIJxyz789",
            "rating": 4.5,
            "reviews": 156,
            "address": f"789 Elm St, {location}",
            "phone": "(315) 555-0300",
        },
        {
            "name": f"Budget {industry.title()} Inc",
            "website": None,
            "place_id": "ChIJdef012",
            "rating": 3.8,
            "reviews": 42,
            "address": f"321 Pine Rd, {location}",
            "phone": "(315) 555-0400",
        },
        {
            "name": f"FastFix {industry.title()}",
            "website": f"https://fastfix{industry.lower().replace(' ', '')}.com",
            "place_id": "ChIJghi345",
            "rating": 3.6,
            "reviews": 22,
            "address": f"654 Maple Dr, {location}",
            "phone": "(315) 555-0500",
        },
    ]
    
    return base_businesses

# =========================
# BUSINESS ANALYSIS
# =========================

async def analyze_business(business: Dict[str, Any]) -> Dict[str, Any]:
    website = business.get("website")

    website_task = analyze_website(website) if website else async_return({})
    review_task = analyze_reviews(business)
    marketing_task = analyze_marketing(website) if website else async_return({})
    email_task = extract_email(website) if website else async_return(None)

    website_analysis, review_intel, marketing, email = await asyncio.gather(
        website_task, review_task, marketing_task, email_task,
        return_exceptions=False
    )

    return {
        **business,
        "website_analysis": website_analysis or {},
        "review_intelligence": review_intel or {},
        "marketing_visibility": marketing or {},
        "email": email,
    }

async def async_return(value):
    return value

async def get_http_client() -> httpx.AsyncClient:
    """Get or create HTTP client - ensures client is available even outside ASGI context."""
    global HTTP_CLIENT
    if HTTP_CLIENT is None:
        HTTP_CLIENT = httpx.AsyncClient(timeout=httpx.Timeout(12.0), headers=DEFAULT_HEADERS)
    return HTTP_CLIENT

async def analyze_website(url: str) -> Dict[str, Any]:
    if not url:
        return {}

    async def fetch_html(u: str) -> Tuple[int, str]:
        client = await get_http_client()
        resp = await client.get(u, follow_redirects=True)
        return resp.status_code, resp.text

    try:
        status_code, html = await retry_async(fetch_html, url, tries=2, base_delay=0.5)
        html_l = (html or "").lower()

        return {
            "exists": True,
            "status_code": status_code,
            "ssl": url.startswith("https://"),
            "mobile_friendly": "viewport" in html_l,
            "has_contact_form": ("<form" in html_l) or ("contact" in html_l),
            "has_booking_cta": ("book" in html_l) or ("schedule" in html_l) or ("appointment" in html_l),
            "has_emergency_cta": ("emergency" in html_l) or ("24/7" in html_l) or ("same day" in html_l),
            "has_phone": "tel:" in html_l,
            "cms_detected": detect_cms(html_l),
        }
    except Exception as e:
        return {"exists": False, "error": str(e)}

def detect_cms(html_l: str) -> str:
    if "wp-content" in html_l or "wordpress" in html_l:
        return "wordpress"
    if "wix.com" in html_l:
        return "wix"
    if "squarespace" in html_l:
        return "squarespace"
    if "shopify" in html_l:
        return "shopify"
    return "custom"

async def analyze_reviews(business: Dict[str, Any]) -> Dict[str, Any]:
    reviews = int(business.get("reviews", 0) or 0)
    rating = float(business.get("rating", 0) or 0)

    velocity = "high" if reviews > 100 else "moderate" if reviews > 30 else "low"

    reputation_strength = (
        "strong" if rating >= 4.5 and reviews > 50
        else "moderate" if rating >= 4.0
        else "weak"
    )

    return {
        "rating": rating,
        "review_count": reviews,
        "review_velocity": velocity,
        "estimated_monthly_reviews": round(reviews / 12, 2) if reviews else 0,
        "reputation_strength": reputation_strength,
    }

async def analyze_marketing(url: str) -> Dict[str, Any]:
    """
    Marketing visibility detection via tracking pixels and public signals.
    """
    if not url:
        return {}

    try:
        status_code, html = await analyze_website_fetch_html(url)
        html_l = html.lower()

        pixels = []
        if "gtag(" in html_l or "google-analytics.com" in html_l or "googletagmanager.com" in html_l:
            pixels.append("google_analytics")
        if "fbq(" in html_l or "connect.facebook.net" in html_l:
            pixels.append("meta_pixel")
        if "ttq.track" in html_l or ("tiktok" in html_l and "pixel" in html_l):
            pixels.append("tiktok_pixel")
        if "snaptr(" in html_l or "sc-static.net" in html_l:
            pixels.append("snap_pixel")
        if "googleadservices.com" in html_l or "doubleclick.net" in html_l:
            pixels.append("google_ads_tag")

        return {
            "pixels_detected": pixels,
            "google_ads_likely": "google_ads_tag" in pixels,
            "meta_ads_likely": "meta_pixel" in pixels,
            "tiktok_ads_likely": "tiktok_pixel" in pixels,
            "snap_ads_likely": "snap_pixel" in pixels,
        }
    except Exception as e:
        return {"error": str(e), "pixels_detected": []}

async def analyze_website_fetch_html(url: str) -> Tuple[int, str]:
    async def fetch(u: str) -> Tuple[int, str]:
        client = await get_http_client()
        resp = await client.get(u, follow_redirects=True)
        return resp.status_code, resp.text

    return await retry_async(fetch, url, tries=2, base_delay=0.5)

async def extract_email(url: str) -> Optional[str]:
    """
    MVP approach: derive email from domain.
    """
    if not url:
        return None
    domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
    if not domain:
        return None
    return f"info@{domain}"

# =========================
# SCORING ALGORITHM
# =========================

def calculate_score(business: Dict[str, Any]) -> int:
    score = 0

    # Reputation (40 points max)
    rating = float(business.get("rating", 0) or 0)
    if rating >= 4.7:
        score += 30
    elif rating >= 4.5:
        score += 25
    elif rating >= 4.0:
        score += 15
    elif rating >= 3.5:
        score += 5

    reviews = int(business.get("reviews", 0) or 0)
    if reviews >= 200:
        score += 10
    elif reviews >= 100:
        score += 8
    elif reviews >= 50:
        score += 5
    elif reviews >= 20:
        score += 2

    # Website (25 points max)
    website = business.get("website_analysis", {}) or {}
    if website.get("exists"):
        score += 10
        if website.get("mobile_friendly"):
            score += 5
        if website.get("has_booking_cta"):
            score += 5
        if website.get("ssl"):
            score += 3
        if website.get("has_emergency_cta"):
            score += 2

    # Marketing (20 points max)
    marketing = business.get("marketing_visibility", {}) or {}
    if marketing.get("google_ads_likely"):
        score += 10
    if marketing.get("meta_ads_likely"):
        score += 5
    if marketing.get("tiktok_ads_likely") or marketing.get("snap_ads_likely"):
        score += 3
    if len(marketing.get("pixels_detected", [])) > 0:
        score += 2

    # Review engagement (15 points max)
    review_intel = business.get("review_intelligence", {}) or {}
    velocity = review_intel.get("review_velocity", "low")
    if velocity == "high":
        score += 10
    elif velocity == "moderate":
        score += 5

    strength = review_intel.get("reputation_strength", "weak")
    if strength == "strong":
        score += 5
    elif strength == "moderate":
        score += 3

    return min(score, 100)

# =========================
# MARKET SUMMARY (RULE-BASED)
# =========================

def generate_market_summary(businesses: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not businesses:
        return {}

    total = len(businesses)
    avg_rating = sum(float(b.get("rating", 0) or 0) for b in businesses) / total
    avg_reviews = sum(int(b.get("reviews", 0) or 0) for b in businesses) / total
    avg_score = sum(int(b.get("score", 0) or 0) for b in businesses) / total

    weak = [b for b in businesses if b.get("score", 0) < 50]
    moderate = [b for b in businesses if 50 <= b.get("score", 0) < 70]
    strong = [b for b in businesses if b.get("score", 0) >= 70]

    opportunities = []

    low_reviews = [b for b in businesses if int(b.get("reviews", 0) or 0) < 30]
    if total and (len(low_reviews) / total) > 0.3:
        opportunities.append({
            "type": "review_gap",
            "severity": "high",
            "insight": f"{len(low_reviews)} of {total} competitors have under 30 reviews",
            "action": "Run a review campaign to pass multiple competitors quickly"
        })

    no_booking = [b for b in businesses if not (b.get("website_analysis", {}) or {}).get("has_booking_cta")]
    if total and (len(no_booking) / total) > 0.4:
        opportunities.append({
            "type": "feature_gap",
            "severity": "medium",
            "insight": f"{len(no_booking)} competitors lack online booking",
            "action": "Add online booking to win more calls and conversions"
        })

    no_website = [b for b in businesses if not b.get("website")]
    if total and (len(no_website) / total) > 0.2:
        opportunities.append({
            "type": "digital_presence_gap",
            "severity": "medium",
            "insight": f"{len(no_website)} competitors have no website",
            "action": "A professional website gives you an edge over these competitors"
        })

    return {
        "total_competitors": total,
        "average_rating": round(avg_rating, 2),
        "average_reviews": round(avg_reviews, 0),
        "average_score": round(avg_score, 1),
        "distribution": {"weak": len(weak), "moderate": len(moderate), "strong": len(strong)},
        "top_3": [{"name": b.get("name"), "score": b.get("score"), "rating": b.get("rating"), "reviews": b.get("reviews")} for b in businesses[:3]],
        "opportunities": opportunities,
        "market_saturation": "high" if total > 20 else "moderate" if total > 10 else "low"
    }

# =========================
# REPORT GENERATION (PDF + CSV)
# =========================

async def generate_reports(job_id: str, businesses: List[Dict[str, Any]], summary: Dict[str, Any], order: Dict[str, Any]) -> Tuple[str, str]:
    # Generate PDF
    pdf_bytes = generate_pdf_report(businesses, summary, order)
    
    # Generate CSV
    csv_content = generate_csv_report(businesses)
    
    # Store in database for persistence
    db_save_report(job_id, pdf_bytes, csv_content)
    
    # Return download URLs
    pdf_url = f"/jobs/{job_id}/pdf"
    csv_url = f"/jobs/{job_id}/csv"
    
    print(f"Generated reports for job {job_id}")
    return pdf_url, csv_url


def generate_pdf_report(businesses: List[Dict[str, Any]], summary: Dict[str, Any], order: Dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, spaceAfter=20)
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=14, spaceAfter=10, spaceBefore=15)
    normal_style = styles['Normal']
    
    elements = []
    
    # Title
    elements.append(Paragraph("Competitor Intelligence Report", title_style))
    elements.append(Paragraph(f"<b>Business:</b> {order.get('business_name', 'N/A')}", normal_style))
    elements.append(Paragraph(f"<b>Industry:</b> {order.get('industry', 'N/A')}", normal_style))
    elements.append(Paragraph(f"<b>Location:</b> {order.get('location', 'N/A')}", normal_style))
    elements.append(Paragraph(f"<b>Generated:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}", normal_style))
    elements.append(Spacer(1, 20))
    
    # Market Summary
    elements.append(Paragraph("Market Summary", heading_style))
    elements.append(Paragraph(f"Total Competitors Analyzed: {summary.get('total_competitors', 0)}", normal_style))
    elements.append(Paragraph(f"Average Rating: {summary.get('average_rating', 0):.1f} stars", normal_style))
    elements.append(Paragraph(f"Average Reviews: {summary.get('average_reviews', 0):.0f}", normal_style))
    elements.append(Paragraph(f"Average Score: {summary.get('average_score', 0):.1f}/100", normal_style))
    elements.append(Paragraph(f"Market Saturation: {summary.get('market_saturation', 'N/A').title()}", normal_style))
    
    dist = summary.get('distribution', {})
    elements.append(Paragraph(f"Competitor Distribution: {dist.get('strong', 0)} strong, {dist.get('moderate', 0)} moderate, {dist.get('weak', 0)} weak", normal_style))
    elements.append(Spacer(1, 15))
    
    # Opportunities
    opportunities = summary.get('opportunities', [])
    if opportunities:
        elements.append(Paragraph("Key Opportunities", heading_style))
        for opp in opportunities:
            elements.append(Paragraph(f"<b>{opp.get('type', '').replace('_', ' ').title()}</b> ({opp.get('severity', 'medium')} priority)", normal_style))
            elements.append(Paragraph(f"  - {opp.get('insight', '')}", normal_style))
            elements.append(Paragraph(f"  - Action: {opp.get('action', '')}", normal_style))
            elements.append(Spacer(1, 5))
        elements.append(Spacer(1, 10))
    
    # Top Competitors Table
    elements.append(Paragraph("Top Competitors", heading_style))
    
    table_data = [["Rank", "Name", "Score", "Rating", "Reviews"]]
    for idx, b in enumerate(businesses[:10], start=1):
        table_data.append([
            str(idx),
            str(b.get("name", "N/A"))[:30],
            str(b.get("score", 0)),
            f"{b.get('rating', 0):.1f}",
            str(b.get("reviews", 0))
        ])
    
    table = Table(table_data, colWidths=[0.5*inch, 3*inch, 0.8*inch, 0.8*inch, 0.8*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ALIGN', (1, 1), (1, -1), 'LEFT'),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(table)
    
    doc.build(elements)
    return buffer.getvalue()


def generate_csv_report(businesses: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    
    fieldnames = [
        "rank", "name", "score", "rating", "reviews", "phone", "address",
        "website", "email", "has_ssl", "mobile_friendly", "has_booking",
        "has_emergency_cta", "cms", "review_velocity", "reputation_strength",
        "google_ads", "meta_ads", "pixels_detected"
    ]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for idx, b in enumerate(businesses, start=1):
        website_analysis = b.get("website_analysis", {}) or {}
        review_intel = b.get("review_intelligence", {}) or {}
        marketing = b.get("marketing_visibility", {}) or {}
        
        writer.writerow({
            "rank": idx,
            "name": b.get("name", ""),
            "score": b.get("score", 0),
            "rating": b.get("rating", 0),
            "reviews": b.get("reviews", 0),
            "phone": b.get("phone", ""),
            "address": b.get("address", ""),
            "website": b.get("website", ""),
            "email": b.get("email", ""),
            "has_ssl": website_analysis.get("ssl", False),
            "mobile_friendly": website_analysis.get("mobile_friendly", False),
            "has_booking": website_analysis.get("has_booking_cta", False),
            "has_emergency_cta": website_analysis.get("has_emergency_cta", False),
            "cms": website_analysis.get("cms_detected", ""),
            "review_velocity": review_intel.get("review_velocity", ""),
            "reputation_strength": review_intel.get("reputation_strength", ""),
            "google_ads": marketing.get("google_ads_likely", False),
            "meta_ads": marketing.get("meta_ads_likely", False),
            "pixels_detected": ", ".join(marketing.get("pixels_detected", []))
        })
    
    return output.getvalue()

# =========================
# EMAIL NOTIFICATION (STUB)
# =========================

async def send_email(to: str, job_id: str, pdf: str, csv: str, summary: Dict[str, Any]):
    """
    Stub for email notification. Replace with actual email service integration.
    """
    await asyncio.sleep(0.1)
    print(f"Email sent to {to} | job {job_id} | PDF {pdf} | CSV {csv} | competitors {summary.get('total_competitors')}")

# =========================
# STARTUP / SHUTDOWN
# =========================

@app.on_event("startup")
async def startup_event():
    global HTTP_CLIENT
    HTTP_CLIENT = httpx.AsyncClient(timeout=httpx.Timeout(12.0), headers=DEFAULT_HEADERS)
    print("Competitor Intelligence Engine started")
    print("API docs: /docs")

@app.on_event("shutdown")
async def shutdown_event():
    global HTTP_CLIENT
    if HTTP_CLIENT:
        await HTTP_CLIENT.aclose()
        HTTP_CLIENT = None


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
