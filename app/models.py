from dataclasses import dataclass
from typing import Dict, Optional, TypedDict
from datetime import datetime

class DatabaseStats(TypedDict):
    """Database statistics for health check"""
    healthy: bool
    total_meals: int
    total_days: int
    total_mealplans: int
    last_update: Optional[str]  # ISO timestamp
    last_mealplan: Optional[str]  # e.g., "2025-W05"
    oldest_mealplan: Optional[str]
    database_size_mb: Optional[float]

class HealthCheckResponse(TypedDict):
    """Health check response"""
    status: str  # "healthy" or "unhealthy"
    timestamp: str  # ISO timestamp
    version: str
    uptime_seconds: Optional[float]
    database: DatabaseStats
    scheduler: dict
class MealDict(TypedDict):
    """Meal information with database ID and name"""
    id: int
    name: str

class DayDict(TypedDict):
    """Day information with weekday and meals"""
    weekday: str
    meals: Dict[str, Optional[MealDict]]  # category -> meal object with id and name

@dataclass
class Mealplan:
    year: int
    week: int
    days: Dict[str, DayDict]  # ISO date -> day data