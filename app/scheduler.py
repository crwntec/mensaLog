from datetime import datetime
import os
import re
import pdfplumber
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

       # 3. Download the full PDF temporarily
        temp_filename = f"temp_KW{str(first_week).zfill(2)}_KW{str(second_week).zfill(2)}.pdf"
        save_dir = f"./archive/{year}"
        os.makedirs(save_dir, exist_ok=True)
        
        print(f"[{datetime.now()}] Downloading to temp {temp_filename}...")
        response = requests.get(pdf_url, timeout=10)
        response.raise_for_status()
        
        with open(temp_filename, "wb") as f:
            f.write(response.content)

        with pdfplumber.open(temp_filename) as pdf:
            if len(pdf.pages) < 2:
                print("PDF doesn't have 2 pages!")
                os.remove(temp_filename)
                return False
            
            # Process week 1 (page 0)
            week1_filename = f"KW{str(first_week).zfill(2)}.pdf"
            week1_path = os.path.join(save_dir, week1_filename)
            with open(week1_path, "wb") as f:
                f.write(pdf.pages[0].to_image().original)  # Save page 0 as PDF
            
            week1_data = extract_meals(pdf.pages[0])  # Pass Page object directly
            if week1_data:
                create_mealplan(Mealplan(year=year, week=first_week, days=week1_data.days))
                print(f"Stored Week {first_week} from {week1_filename}")

            # Process week 2 (page 1)  
            week2_filename = f"KW{str(second_week).zfill(2)}.pdf"
            week2_path = os.path.join(save_dir, week2_filename)
            with open(week2_path, "wb") as f:
                f.write(pdf.pages[1].to_image().original)  # Save page 1 as PDF
            
            week2_data = extract_meals(pdf.pages[1])
            if week2_data:
                create_mealplan(Mealplan(year=year, week=second_week, days=week2_data.days))
                print(f"Stored Week {second_week} from {week2_filename}")

        # 5. Clean up temp file
        os.remove(temp_filename)
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False
if __name__ == "__main__":
    # Test the script
    download_and_parse_pdf()