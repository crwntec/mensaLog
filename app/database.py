from datetime import datetime
import os
import sqlite3
from typing import List
from services.meal_intelligence import MealIntelligence
from models import MealAPIResponse, MealDict, Mealplan, SearchMealAPIResponseDict

init_db_query = """
CREATE TABLE IF NOT EXISTS mealplan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    week INTEGER NOT NULL,
    UNIQUE(year, week)
);

CREATE TABLE IF NOT EXISTS meal(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS day(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mealplan_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    weekday TEXT NOT NULL,
    tagesgericht_id INTEGER,
    vegetarisch_id INTEGER,
    pizza_pasta_id INTEGER,
    wok_id INTEGER,
    FOREIGN KEY (mealplan_id) REFERENCES mealplan(id),
    FOREIGN KEY (tagesgericht_id) REFERENCES meal(id),
    FOREIGN KEY (vegetarisch_id) REFERENCES meal(id),
    FOREIGN KEY (pizza_pasta_id) REFERENCES meal(id),
    FOREIGN KEY (wok_id) REFERENCES meal(id)
);
"""

# Category name mapping - maps various historical names to canonical names
CATEGORY_MAPPING = {
    "Gericht 1": "Tagesgericht",
    "Gericht 2": "Vegetarisch",
    "Tagesgericht": "Tagesgericht",
    "Vegetarisch": "Vegetarisch",
    "Pizza & Pasta": "Pizza & Pasta",
    "Aus dem Wok": "Wok"
}

def connect_db():
    conn = sqlite3.connect('mealplan.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with connect_db() as conn:
        print(f"{datetime.now()} Creating database")
        cursor = conn.cursor()
        cursor.executescript(init_db_query)
        conn.commit()

def db_stats() -> dict:
    """
    Get comprehensive database statistics for health monitoring.
    
    Returns:
        Dictionary with database health and statistics
    """
    with connect_db() as conn:
        cursor = conn.cursor()
        try:
            # Check if tables exist
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('meal', 'day', 'mealplan')
            """)
            tables = {row['name'] for row in cursor.fetchall()}
            required_tables = {'meal', 'day', 'mealplan'}
            
            if not required_tables.issubset(tables):
                return {
                    "healthy": False,
                    "total_meals": 0,
                    "total_days": 0,
                    "total_mealplans": 0,
                    "last_update": None,
                    "last_mealplan": None,
                    "oldest_mealplan": None,
                    "database_size_mb": None,
                    "error": f"Missing tables: {required_tables - tables}"
                }
            
            # Count meals
            cursor.execute("SELECT COUNT(*) as count FROM meal")
            total_meals = cursor.fetchone()["count"]
            
            # Count days
            cursor.execute("SELECT COUNT(*) as count FROM day")
            total_days = cursor.fetchone()["count"]
            
            # Count mealplans
            cursor.execute("SELECT COUNT(*) as count FROM mealplan")
            total_mealplans = cursor.fetchone()["count"]
            
            # Get last mealplan info
            cursor.execute("""
                SELECT year, week, MAX(year * 100 + week) as latest
                FROM mealplan
                GROUP BY year, week
                ORDER BY latest DESC
                LIMIT 1
            """)
            last_mealplan_row = cursor.fetchone()
            last_mealplan = None
            if last_mealplan_row:
                last_mealplan = f"{last_mealplan_row['year']}-W{last_mealplan_row['week']:02d}"
            
            # Get oldest mealplan info
            cursor.execute("""
                SELECT year, week, MIN(year * 100 + week) as oldest
                FROM mealplan
                GROUP BY year, week
                ORDER BY oldest ASC
                LIMIT 1
            """)
            oldest_mealplan_row = cursor.fetchone()
            oldest_mealplan = None
            if oldest_mealplan_row:
                oldest_mealplan = f"{oldest_mealplan_row['year']}-W{oldest_mealplan_row['week']:02d}"
            
            # Get last update (most recent day entry)
            cursor.execute("""
                SELECT MAX(date) as last_date
                FROM day
            """)
            last_date_row = cursor.fetchone()
            last_update = last_date_row["last_date"] if last_date_row else None
            
            # Get database file size
            db_size_mb = None
            if os.path.exists('mealplan.db'):
                db_size_bytes = os.path.getsize('mealplan.db')
                db_size_mb = round(db_size_bytes / (1024 * 1024), 2)
            
            # Determine if healthy
            healthy = (
                total_meals > 0 and 
                total_days > 0 and 
                total_mealplans > 0 and
                last_update is not None
            )
            
            return {
                "healthy": healthy,
                "total_meals": total_meals,
                "total_days": total_days,
                "total_mealplans": total_mealplans,
                "last_update": last_update,
                "last_mealplan": last_mealplan,
                "oldest_mealplan": oldest_mealplan,
                "database_size_mb": db_size_mb
            }
            
        except Exception as e:
            print(f"Database health check failed: {e}")
            return {
                "healthy": False,
                "total_meals": 0,
                "total_days": 0,
                "total_mealplans": 0,
                "last_update": None,
                "last_mealplan": None,
                "oldest_mealplan": None,
                "database_size_mb": None,
                "error": str(e)
            }

def normalize_category(category_name):
    """
    Normalize category names using the mapping.
    Returns the canonical name or the original if not in mapping.
    """
    return CATEGORY_MAPPING.get(category_name, category_name)

def create_mealplan(data: Mealplan, intel: MealIntelligence):
    with connect_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO mealplan (year, week) VALUES (?, ?)", 
                (data.year, data.week)
            )
            mealplan_id = cursor.lastrowid
            
            for date_iso, day_data in data.days.items():
                # Initialize meal IDs as None
                meal_ids = {
                    "Tagesgericht": None,
                    "Vegetarisch": None,
                    "Pizza & Pasta": None,
                    "Wok": None
                }
                
                # Get or create meal IDs for each category
                for category, meal_text in day_data["meals"].items():
                    meal_id = None
                    if intel:
                        existing_id, similarity = intel.find_similar_meal(meal_text)
                        if existing_id:
                            print(f"    Found similar meal(sim={similarity:.3f}): {meal_text[:40]}")
                            meal_id = existing_id
                    
                    # Normalize the category name
                    normalized_category = normalize_category(category)
                    if meal_id is None:
                        cursor.execute(
                            "INSERT OR IGNORE INTO meal (name) VALUES (?)", 
                            (meal_text,)
                        )
                        cursor.execute(
                            "SELECT id FROM meal WHERE name = ?", 
                            (meal_text,)
                        )
                        meal_id = cursor.fetchone()[0]
                        if intel:
                            new_embedding = intel.encode_meal(meal_text)
                            intel.meal_embeddings[meal_id] = new_embedding
                    meal_ids[normalized_category] = meal_id
                
                # Insert day with all meal IDs
                cursor.execute("""
                    INSERT INTO day (mealplan_id, date, weekday, 
                                     tagesgericht_id, vegetarisch_id, pizza_pasta_id, wok_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    mealplan_id, 
                    date_iso, 
                    day_data["weekday"],
                    meal_ids.get("Tagesgericht"),
                    meal_ids.get("Vegetarisch"),
                    meal_ids.get("Pizza & Pasta"),
                    meal_ids.get("Wok")
                ))
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to create mealplan: {e}")

def fetch_mealplan(year, week):
    with connect_db() as conn:
        cursor = conn.cursor()
        
        try:
            # Get mealplan
            cursor.execute("SELECT * FROM mealplan WHERE year = ? AND week = ?", (year, week))
            mealplan_row = cursor.fetchone()
            
            if not mealplan_row:
                return None
            
            # Get days with meals using JOINs
            cursor.execute("""
                SELECT 
                    day.date, 
                    day.weekday,
                    day.tagesgericht_id,
                    m1.name as tagesgericht,
                    day.vegetarisch_id,
                    m2.name as vegetarisch,
                    day.pizza_pasta_id,
                    m3.name as pizza_pasta,
                    day.wok_id,
                    m4.name as wok
                FROM day
                LEFT JOIN meal m1 ON day.tagesgericht_id = m1.id
                LEFT JOIN meal m2 ON day.vegetarisch_id = m2.id
                LEFT JOIN meal m3 ON day.pizza_pasta_id = m3.id
                LEFT JOIN meal m4 ON day.wok_id = m4.id
                WHERE day.mealplan_id = ?
                ORDER BY day.date
            """, (mealplan_row["id"],))
            
            days_rows = cursor.fetchall()
            
            # Build the Mealplan object
            days = {}
            for row in days_rows:
                days[row["date"]] = {
                    "weekday": row["weekday"],
                    "meals": {
                        "Tagesgericht": {
                            "id": row["tagesgericht_id"],
                            "name": row["tagesgericht"]
                        } if row["tagesgericht"] else None,
                        "Vegetarisch": {
                            "id": row["vegetarisch_id"],
                            "name": row["vegetarisch"]
                        } if row["vegetarisch"] else None,
                        "Pizza & Pasta": {
                            "id": row["pizza_pasta_id"],
                            "name": row["pizza_pasta"]
                        } if row["pizza_pasta"] else None,
                        "Wok": {
                            "id": row["wok_id"],
                            "name": row["wok"]
                        } if row["wok"] else None
                    }
                }
            
            return Mealplan(
                year=mealplan_row["year"],
                week=mealplan_row["week"],
                days=days
            )
        except Exception as e:
            conn.rollback()
            print(f"Fetching mealplan failed: {e}")

    
def fetch_day(datestring):
    with connect_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    day.weekday,
                    day.tagesgericht_id,
                    m1.name AS tagesgericht,
                    day.vegetarisch_id,
                    m2.name AS vegetarisch,
                    day.pizza_pasta_id,
                    m3.name AS pizza_pasta,
                    day.wok_id,
                    m4.name AS wok
                FROM day
                LEFT JOIN meal m1 ON day.tagesgericht_id = m1.id
                LEFT JOIN meal m2 ON day.vegetarisch_id = m2.id
                LEFT JOIN meal m3 ON day.pizza_pasta_id = m3.id
                LEFT JOIN meal m4 ON day.wok_id = m4.id
                WHERE day.date = ?
            """, (datestring,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return {
                "weekday": row["weekday"],
                "meals": {
                    "Tagesgericht": {
                        "id": row["tagesgericht_id"],
                        "name": row["tagesgericht"]
                    } if row["tagesgericht"] else None,
                    "Vegetarisch": {
                        "id": row["vegetarisch_id"],
                        "name": row["vegetarisch"]
                    } if row["vegetarisch"] else None,
                    "Pizza & Pasta": {
                        "id": row["pizza_pasta_id"],
                        "name": row["pizza_pasta"]
                    } if row["pizza_pasta"] else None,
                    "Wok": {
                        "id": row["wok_id"],
                        "name": row["wok"]
                    } if row["wok"] else None
                }
            }
        except Exception as e:
            conn.rollback()
            print(f"Fetching day failed: {e}")
            return None

def search_meals_db(query_term: str, intel: MealIntelligence) -> List[SearchMealAPIResponseDict]:
    with connect_db() as conn:
        cursor = conn.cursor()
        
        results = intel.find_top_similar_meals(query_term, top_k=10)
        
        if results:
            meal_ids = [meal_id for meal_id, score in results]
            score_map = {meal_id: round(score, 3) for meal_id, score in results}
            placeholders = ','.join('?' * len(meal_ids))
            cursor.execute(f"SELECT id, name FROM meal WHERE id IN ({placeholders})", tuple(meal_ids))
            rows = cursor.fetchall()
            id_to_row = {row['id']: row for row in rows}
            
            result_list = [{
                "id": id_to_row[mid]['id'],
                "name": id_to_row[mid]['name'],
                "similarity": score_map[mid]
            } for mid in meal_ids if mid in id_to_row]
            return result_list
        
        return []

def fetch_meal(meal_id: int, intel: MealIntelligence):
    with connect_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM meal WHERE id = ?", (meal_id,))
            meal_row = cursor.fetchone()
            
            if not meal_row:
                return None

            cursor.execute("""
                SELECT date, weekday 
                FROM day 
                WHERE tagesgericht_id = ? 
                   OR vegetarisch_id = ? 
                   OR pizza_pasta_id = ? 
                   OR wok_id = ?
                ORDER BY date ASC
            """, (meal_id, meal_id, meal_id, meal_id))
            
            days_rows = cursor.fetchall()
            num_servings = len(days_rows)
            
            avg_distance = 0
            if num_servings > 1:
                sum_distance = 0
                for i in range(num_servings - 1):
                    d1 = datetime.strptime(days_rows[i]["date"], "%Y-%m-%d")
                    d2 = datetime.strptime(days_rows[i+1]["date"], "%Y-%m-%d")
                    sum_distance += (d2 - d1).days
                avg_distance = round(sum_distance / (num_servings - 1))
            
            # Use meal name directly as semantic query, exclude self
            similar_meals = search_meals_db(query_term=meal_row["name"], intel=intel)
            similar_meals = [m for m in similar_meals if m["id"] != meal_id]

            return MealAPIResponse(
                id=meal_row["id"],
                name=meal_row["name"],
                num_servings=num_servings,
                dates_served={row["date"]: row["weekday"] for row in days_rows},
                avg_distance=avg_distance,
                similar_meals=similar_meals
            )
        except Exception as e:
            print(f"Fetching meal failed: {e}")
            return None