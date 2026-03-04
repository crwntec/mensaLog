from datetime import datetime
import logging
import os
import re
import pdfplumber
import requests
from services.meal_intelligence import MealIntelligence
from models import Mealplan
from database import create_mealplan, fetch_mealplan
from services.pdf_parser import extract_meals
from bs4 import BeautifulSoup

UPDATE_INTERVAL_HOURS = 24
logger = logging.getLogger("mensa-api")


def get_current_week_range():
    """
    Get the current two-week range based on the pattern:
    Weeks 6,7 → published in week 6
    Weeks 8,9 → published in week 8

    Returns:
        tuple: (first_week, second_week) for the current range
    """
    current_week = datetime.now().isocalendar()[1]

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
    logger.info("Scraping PDF URL from %s", BASE_URL)
    page = requests.get(BASE_URL)
    soup = BeautifulSoup(page.content, "html.parser")
    mensa_link = soup.find('h3', string='Mensa Angebot der nächsten 2 Wochen').find_parent('a')

    if mensa_link:
        url = BASE_URL + mensa_link['href']
        logger.info("Found PDF URL: %s", url)
        return url
    else:
        logger.error("Scraping failed: no 'Mensa Angebot' link found on page")
        return None


def download_and_parse_pdf(intel: MealIntelligence):
    try:
        first_week, second_week = get_current_week_range()
        year = datetime.now().year

        if fetch_mealplan(year, first_week) and fetch_mealplan(year, second_week):
            logger.info("Weeks %s/%s already exist in database, skipping fetch", first_week, second_week)
            return True

        pdf_url = scrape_pdf_url()
        if not pdf_url:
            return False

        temp_filename = f"temp_KW{str(first_week).zfill(2)}_KW{str(second_week).zfill(2)}.pdf"
        save_dir = f"./archive/{year}"
        os.makedirs(save_dir, exist_ok=True)

        logger.info("Downloading PDF to %s", temp_filename)
        response = requests.get(pdf_url, timeout=10)
        response.raise_for_status()

        with open(temp_filename, "wb") as f:
            f.write(response.content)

        with pdfplumber.open(temp_filename) as pdf:
            if len(pdf.pages) < 2:
                logger.error("PDF only has %d page(s), expected at least 2", len(pdf.pages))
                os.remove(temp_filename)
                return False

            # Week 1 — only store if not already in DB
            if not fetch_mealplan(year, first_week):
                week1_data = extract_meals(pdf.pages[0])
                if week1_data:
                    create_mealplan(Mealplan(year=year, week=first_week, days=week1_data.days), intel=intel)
                    logger.info("Stored week %s from %s (%d days)", first_week, temp_filename, len(week1_data.days))
                else:
                    logger.warning("No meal data extracted for week %s (page 0)", first_week)
            else:
                logger.info("Week %s already exists in database, skipping", first_week)

            # Week 2 — only store if not already in DB
            if not fetch_mealplan(year, second_week):
                week2_data = extract_meals(pdf.pages[1])
                if week2_data:
                    create_mealplan(Mealplan(year=year, week=second_week, days=week2_data.days), intel=intel)
                    logger.info("Stored week %s from %s (%d days)", second_week, temp_filename, len(week2_data.days))
                else:
                    logger.warning("No meal data extracted for week %s (page 1)", second_week)
            else:
                logger.info("Week %s already exists in database, skipping", second_week)

        return True

    except requests.HTTPError as e:
        logger.error("HTTP error downloading PDF: %s", e)
        return False
    except requests.RequestException as e:
        logger.error("Network error during PDF fetch: %s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error in download_and_parse_pdf: %s", e)
        return False


if __name__ == "__main__":
    download_and_parse_pdf()