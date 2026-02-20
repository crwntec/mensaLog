from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from typing_extensions import TypedDict

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
    embeddings: Dict[str, Any]
class MealDict(TypedDict):
    """Meal information with database ID and name"""
    id: int
    name: str

class SearchMealAPIResponseDict(TypedDict):
    id: int
    name: str
    similarity: float

class MealAPIResponse(TypedDict):
    id: int
    name: str
    num_servings: int
    dates_served: Dict[str, str] # Serving date -> weekday
    avg_distance: int
    similar_meals: List[SearchMealAPIResponseDict]

class DayDict(TypedDict):
    """Day information with weekday and meals"""
    weekday: str
    meals: Dict[str, Optional[MealDict]]  # category -> meal object with id and name

@dataclass
class Mealplan:
    year: int
    week: int
    days: Dict[str, DayDict]  # ISO date -> day data