"""
FastAPI app to serve German meal plans (Speisenplan) with scheduled updates.
Features:
- API endpoint to get current meal plan
- APScheduler job to download & parse PDF periodically
"""
import logging
from datetime import datetime
import time
from typing import Generic, List, Optional, TypeVar
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler

from parse import import_historical_data
from models import *
from database import *
from scheduler import download_and_parse_pdf

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
logger = logging.getLogger("mensa-api")
logger.setLevel(logging.INFO)

# If no handlers are configured (e.g., when not run under uvicorn), add one
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

startup_time = time.time()
scheduler: BackgroundScheduler | None = None
intel: MealIntelligence | None = None

T = TypeVar('T')


class ApiResponse(BaseModel, Generic[T]):
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    global intel

    logger.info("Application startup initiated")

    try:
        logger.info("Initializing database schema")
        init_db()
        logger.info("Database schema initialized")

        logger.info("Initializing MealIntelligence")
        intel = MealIntelligence()
        intel.build_embeddings_index()
        logger.info("MealIntelligence initialized")

        logger.info("Configuring APScheduler (Europe/Berlin, daily at 07:00)")
        scheduler = BackgroundScheduler(timezone="Europe/Berlin")
        scheduler.add_job(
            download_and_parse_pdf,
            "cron", hour=7, minute=0,
            kwargs={"intel": intel}
        )
        scheduler.start()
        logger.info("APScheduler started with %d job(s)", len(scheduler.get_jobs()))
        
        # logger.info("Triggering initial meal plan download")
        # ok = download_and_parse_pdf()
        # if not ok:
        #     logger.warning("Initial meal plan download reported failure")
        # else:
        #     logger.info("Initial meal plan download completed successfully")

        # --- DB stats check after startup ---
        stats = db_stats()
        logger.info(
            "Database stats after startup | healthy=%s, mealplans=%s, days=%s, meals=%s, "
            "oldest=%s, latest=%s, size_mb=%s",
            stats.get("healthy"),
            stats.get("total_mealplans"),
            stats.get("total_days"),
            stats.get("total_meals"),
            stats.get("oldest_mealplan"),
            stats.get("last_mealplan"),
            stats.get("database_size_mb"),
        )
        if not stats.get("healthy"):
            logger.warning("Database reported unhealthy status at startup: %s", stats.get("error"))

        if stats.get("total_mealplans", 0) == 0:
            logger.warning("No meal plans found in database after startup")
            logger.info("Importing historical meal plan data from archive")
            import_historical_data()
            logger.info("Historical data import completed")

    except Exception as exc:
        logger.exception("Startup sequence failed: %s", exc)
        # Yield anyway so health endpoints can expose the failure
        yield
        return

    logger.info("Application startup completed successfully")
    yield

    logger.info("Application shutdown initiated")
    if scheduler and scheduler.running:
        logger.info("Shutting down APScheduler")
        scheduler.shutdown()
    logger.info("Application shutdown completed")


app = FastAPI(
    title="mensa API",
    description="API to retrieve meal plans (Speisenplan)",
    version="1.1.1",
    lifespan=lifespan,
)
    
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
def get_mealplan(year: int, week: int):
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
    summary="Retrieve meal by ID",
    response_model=ApiResponse[MealAPIResponse],
    responses={
        200: {"description": "Meal retrieved successfully"},
        404: {"description": "Meal not found"},
    })
def get_meal(meal_id: int):
    """
    Retrieve meal by ID
    """
    if not intel or not intel.meal_embeddings:
        raise HTTPException(status_code=503, detail="Meal index not ready")
    data = fetch_meal(meal_id=meal_id, intel=intel)
    if not data:
        raise HTTPException(status_code=404, detail="Meal not found")
    return {"success": True, "data": data}

@app.get("/search", 
    summary="Search for meals by name",
    response_model=ApiResponse[List[SearchMealAPIResponseDict]],
    responses={
        200: {"description": "Meals retrieved successfully"},
        404: {"description": "Meals not found"},
    })
def search_meals(name: str):
    """
    Search for meals by name
    """
    if not intel or not intel.meal_embeddings:
        raise HTTPException(status_code=503, detail="Search index not ready")
    data = search_meals_db(query_term=name, intel=intel)
    if not data:
        raise HTTPException(status_code=404, detail="Meals not found")
    return {"success": True, "data": data}

@app.get("/health", response_model=ApiResponse[HealthCheckResponse])
def health_check():
    stats = db_stats()
    uptime = round(time.time() - startup_time, 2)

    global scheduler
    scheduler_running = bool(scheduler and scheduler.running)
    scheduler_jobs = len(scheduler.get_jobs()) if scheduler else 0
    next_run = None
    if scheduler and scheduler_jobs > 0:
        jobs = scheduler.get_jobs()
        if jobs and jobs[0].next_run_time:
            next_run = jobs[0].next_run_time.isoformat()

    is_healthy = (
        stats["healthy"] and
        scheduler_running and
        scheduler_jobs > 0
    )

    if is_healthy:
        logger.debug(
            "Health check OK | uptime=%ss, db_mealplans=%s, scheduler_jobs=%s",
            uptime, stats["total_mealplans"], scheduler_jobs,
        )
    else:
        logger.warning(
            "Health check UNHEALTHY | uptime=%ss, db_healthy=%s, "
            "scheduler_running=%s, scheduler_jobs=%s, db_error=%s",
            uptime,
            stats.get("healthy"),
            scheduler_running,
            scheduler_jobs,
            stats.get("error"),
        )
    embedding_stats = {
        "indexed_meals": len(intel.meal_embeddings),
        "cache_exists": os.path.exists(intel.cache_file),
        "model_loaded": intel is not None,
    }
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
            "database_size_mb": stats["database_size_mb"],
        },
        "scheduler": {
            "running": scheduler_running,
            "jobs_count": scheduler_jobs,
            "next_run": next_run,
        },
        "embeddings": embedding_stats
    }
    if "error" in stats:
        response["database"]["error"] = stats["error"]

    status_code = 200 if is_healthy else 503
    return {"success": is_healthy, "data": response}


@app.get("/health/simple")
def simple_health_check():
    stats = db_stats()
    global scheduler
    scheduler_running = bool(scheduler and scheduler.running)
    is_healthy = stats["healthy"] and scheduler_running

    if is_healthy:
        logger.debug("Simple health check OK")
        return {"status": "ok"}
    else:
        logger.warning(
            "Simple health check UNHEALTHY | db_healthy=%s, scheduler_running=%s",
            stats.get("healthy"),
            scheduler_running,
        )
        raise HTTPException(status_code=503, detail="Service unhealthy")
