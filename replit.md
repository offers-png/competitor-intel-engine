# Competitor Intelligence Engine

## Overview

A FastAPI-based competitor intelligence engine that scrapes, analyzes, and generates comprehensive reports on local service businesses. This MVP helps businesses understand their competitive landscape by analyzing competitors' online presence, reviews, and marketing visibility.

## Current State

The application is fully functional with:
- Async job queue with progress tracking
- In-memory job storage (upgrade to PostgreSQL for production)
- PDF and CSV report generation
- Rule-based market summary and scoring

## Project Structure

```
/
├── main.py              # Complete FastAPI application (single-file MVP)
├── pyproject.toml       # Python dependencies
├── uv.lock             # Dependency lock file
└── replit.md           # This documentation
```

## Key Features

1. **Async Job Execution**: Jobs run in background with semaphore-based concurrency control
2. **Progress Tracking**: Real-time progress updates (0-100%)
3. **Google Maps Scraping**: Stubbed with mock data (replace with SerpAPI for production)
4. **Website Analysis**: SSL, mobile-friendliness, CMS detection, booking/emergency CTAs
5. **Review Intelligence**: Rating, count, velocity, and reputation strength scoring
6. **Marketing Visibility**: Pixel detection (Google Analytics, Meta, TikTok, Snap)
7. **Competitive Scoring**: Deterministic 0-100 score based on multiple factors
8. **PDF/CSV Reports**: Professional reports with ReportLab
9. **Email Notifications**: Stubbed (integrate SendGrid/Resend for production)

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API info and available endpoints |
| GET | `/docs` | Interactive Swagger documentation |
| GET | `/health` | Health check with job queue status |
| POST | `/start-analysis` | Start a new competitor analysis job |
| GET | `/jobs/{job_id}` | Get job status and results |
| GET | `/jobs/{job_id}/pdf` | Download PDF report |
| GET | `/jobs/{job_id}/csv` | Download CSV report |

## Usage Example

### Start an Analysis

```bash
curl -X POST "http://localhost:5000/start-analysis" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "business_name": "My Plumbing Co",
    "industry": "plumber",
    "location": "Syracuse, NY",
    "radius_miles": 10
  }'
```

### Check Job Status

```bash
curl "http://localhost:5000/jobs/{job_id}"
```

## Scoring Algorithm

Businesses are scored 0-100 based on:

- **Reputation (40 pts)**: Rating (0-30) + Review count (0-10)
- **Website (25 pts)**: Exists (10) + Mobile-friendly (5) + Booking CTA (5) + SSL (3) + Emergency CTA (2)
- **Marketing (20 pts)**: Google Ads (10) + Meta Ads (5) + TikTok/Snap (3) + Any pixels (2)
- **Review Engagement (15 pts)**: Velocity (0-10) + Reputation strength (0-5)

## Configuration

The following environment variables/constants can be adjusted in `main.py`:

- `MAX_CONCURRENT_JOBS`: Maximum parallel jobs (default: 2)
- `MAX_CONCURRENT_BUSINESS_ANALYSIS`: Max parallel business analyses (default: 8)
- `DEFAULT_HEADERS`: HTTP client headers for web scraping

## Dependencies

- **FastAPI**: Async web framework
- **Pydantic**: Request/response validation with email support
- **httpx**: Async HTTP client
- **ReportLab**: PDF generation
- **uvicorn**: ASGI server

## Future Enhancements

1. Integrate SerpAPI/Outscraper for real Google Maps data
2. Add PostgreSQL database for persistent job storage
3. Implement email service (SendGrid/Resend) for notifications
4. Create web UI dashboard for job submission
5. Add webhook support for real-time status updates
6. Enhanced sentiment analysis on reviews
