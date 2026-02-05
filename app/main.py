"""
FastAPI app to serve German meal plans (Speisenplan) with scheduled updates.
Features:
- API endpoint to get current meal plan
- APScheduler job to download & parse PDF periodically
"""
from datetime import datetime
import time
from typing import Generic, Optional, TypeVar
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from models import DayDict, HealthCheckResponse, MealDict, Mealplan
from database import db_stats, fetch_day, fetch_meal, fetch_mealplan, init_db
from scheduler import download_and_parse_pdf

startup_time = time.time()
# =========================
# RESPONSE MODELS
# =========================
T = TypeVar('T')

class ApiResponse(BaseModel, Generic[T]):
    """Generic API response wrapper"""
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None

# =========================
# CONFIGURATION
# =========================
# FastAPI app
app = FastAPI(
    title="mensa API", 
    description="API to retrieve meal plans (Speisenplan)", 
    version="1.0.0"
)

# Start scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(download_and_parse_pdf, "interval", hours=24)
scheduler.start()
init_db()

# Initial fetch on startup
download_and_parse_pdf()

# =========================
# ENDPOINTS
# =========================
@app.get("/mealplan",
    summary="Retrieve meal plan for a specific week and year",
    response_model=ApiResponse[Mealplan],
    responses={
        200: {"description": "Meal plan retrieved successfully"},
        404: {"description": "Meal plan not found"},
    }
)
def get_mealplan(week: int, year: int):
    """
    Retrieve the meal plan for a specific week and year.
    """
    data = fetch_mealplan(year=year, week=week)
    if not data:
        raise HTTPException(status_code=404, detail="Meal plan not available")
    return {"success": True, "data": data}

@app.get("/day",
    summary="Retrieve mealplan for a specific day",
    response_model=ApiResponse[DayDict],
    responses={
        200: {"description": "Day retrieved successfully"},
        404: {"description": "Day not found"},
    }
)
def get_day(date: str):
    """
    Retrieve mealplan for a specific day
    """
    data = fetch_day(datestring=date)
    if not data:
        raise HTTPException(status_code=404, detail="Day not found")
    return {"success": True, "data": data}

@app.get("/meal", 
    summary="Retrieve meal for a specific day",
    response_model=ApiResponse[MealDict],
    responses={
        200: {"description": "Meal retrieved successfully"},
        404: {"description": "Meal not found"},
    })
def get_meal(meal_id: int):
    """
    Retrieve meal by ID
    """
    data = fetch_meal(meal_id=meal_id)
    if not data:
        raise HTTPException(status_code=404, detail="Meal not found")
    return {"success": True, "data": data}

@app.get("/health",
    summary="Health check endpoint",
    response_model=ApiResponse[HealthCheckResponse],
    responses={
        200: {"description": "Service is healthy"},
        503: {"description": "Service is unhealthy"},
    }
)
def health_check():
    """
    Comprehensive health check including:
    - API status and version
    - Database connectivity and statistics
    - Scheduler status
    - Uptime information
    """
    stats = db_stats()
    
    # Calculate uptime
    uptime = round(time.time() - startup_time, 2)
    
    # Check scheduler status
    scheduler_running = scheduler.running
    scheduler_jobs = len(scheduler.get_jobs())
    next_run = None
    if scheduler_jobs > 0:
        jobs = scheduler.get_jobs()
        next_run_time = jobs[0].next_run_time
        if next_run_time:
            next_run = next_run_time.isoformat()
    
    # Determine overall health
    is_healthy = (
        stats["healthy"] and 
        scheduler_running and 
        scheduler_jobs > 0
    )
    
    response = {
        "status": "healthy" if is_healthy else "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "version": app.version,
        "uptime_seconds": uptime,
        "database": {
            "healthy": stats["healthy"],
            "total_meals": stats["total_meals"],
            "total_days": stats["total_days"],
            "total_mealplans": stats["total_mealplans"],
            "last_update": stats["last_update"],
            "last_mealplan": stats["last_mealplan"],
            "oldest_mealplan": stats["oldest_mealplan"],
            "database_size_mb": stats["database_size_mb"]
        },
        "scheduler": {
            "running": scheduler_running,
            "jobs_count": scheduler_jobs,
            "next_run": next_run
        }
    }
    
    # Add error info if unhealthy
    if "error" in stats:
        response["database"]["error"] = stats["error"]
    
    # Return 503 if unhealthy
    status_code = 200 if is_healthy else 503
    
    return {"success": is_healthy, "data": response}


@app.get("/health/simple",
    summary="Simple health check (returns 200 OK if healthy)",
    responses={
        200: {"description": "Service is healthy"},
        503: {"description": "Service is unhealthy"},
    }
)
def simple_health_check():
    """
    Lightweight health check for load balancers and monitoring tools.
    Returns minimal data with appropriate HTTP status code.
    """
    stats = db_stats()
    is_healthy = stats["healthy"] and scheduler.running
    
    if is_healthy:
        return {"status": "ok"}
    else:
        raise HTTPException(status_code=503, detail="Service unhealthy")