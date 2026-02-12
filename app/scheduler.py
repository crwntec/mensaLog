from datetime import datetime
import os
import re
import requests
from models import Mealplan
from database import create_mealplan, fetch_mealplan
from services.pdf_parser import extract_meals
from bs4 import BeautifulSoup
UPDATE_INTERVAL_HOURS = 24

def get_current_week_range():
    """
    Get the current two-week range based on the pattern:
    Weeks 6,7 → published in week 6
    Weeks 8,9 → published in week 8
    
    Returns:
        tuple: (first_week, second_week) for the current range
    """
    current_week = datetime.now().isocalendar()[1]
    
    # Determine which two-week block we're in
    # Even weeks start a new block: 6-7, 8-9, 10-11, etc.
    if current_week % 2 == 0:
        first_week = current_week
        second_week = current_week + 1
    else:
        first_week = current_week - 1
        second_week = current_week
    
    return first_week, second_week

def scrape_pdf_url():
    """
    Scrape the PDF URL from the website.
    """
    BASE_URL = "https://www.malteser-st-bernhard-gymnasium.de/"
    page = requests.get(BASE_URL)
    soup = BeautifulSoup(page.content, "html.parser")
    mensa_link = soup.find('h3', string='Mensa Angebot der nächsten 2 Wochen').find_parent('a')
    
    if mensa_link:
        return BASE_URL + mensa_link['href']
    else:
        print(f"Scraping failed: No link found")
        return None

def download_and_parse_pdf():
    try:
        # 1. Determine the week range (e.g., 06 and 07)
        first_week, second_week = get_current_week_range()
        year = datetime.now().year
        
        # Check database to avoid redundant work
        if fetch_mealplan(year, first_week) and fetch_mealplan(year, second_week):
            print(f"[{datetime.now()}] Weeks {first_week}/{second_week} already exist. Skipping.")
            return True

        # 2. Scrape the URL
        pdf_url = scrape_pdf_url()
        if not pdf_url:
            return False

        # 3. Download and Save with custom naming: KW{week1}_KW{week2}.pdf
        # We use zfill(2) to ensure week 6 becomes "06"
        filename = f"KW{str(first_week).zfill(2)}_KW{str(second_week).zfill(2)}.pdf"
        save_dir = f"./archive/{year}"
        os.makedirs(save_dir, exist_ok=True)
        pdf_path = os.path.join(save_dir, filename)

        print(f"[{datetime.now()}] Downloading to {pdf_path}...")
        response = requests.get(pdf_url, timeout=10)
        response.raise_for_status()
        
        with open(pdf_path, "wb") as f:
            f.write(response.content)

        # 4. Parse the PDF
        # Note: extract_meals returns a Mealplan object containing ALL days found
        parsed_data = extract_meals(pdf_path)
        if not parsed_data:
            return False

        # 5. Split the data by week
        week1_days = {}
        week2_days = {}

        for date_iso, day_data in parsed_data.days.items():
            date_obj = datetime.fromisoformat(date_iso)
            day_week = date_obj.isocalendar()[1]
            
            if day_week == first_week:
                week1_days[date_iso] = day_data
            elif day_week == second_week:
                week2_days[date_iso] = day_data

        # 6. Save separate objects to Database
        if week1_days:
            create_mealplan(Mealplan(year=year, week=first_week, days=week1_days))
            print(f"Stored Week {first_week}")
        
        if week2_days:
            create_mealplan(Mealplan(year=year, week=second_week, days=week2_days))
            print(f"Stored Week {second_week}")

        return True

    except Exception as e:
        print(f"Error: {e}")
        return False
if __name__ == "__main__":
    # Test the script
    download_and_parse_pdf()