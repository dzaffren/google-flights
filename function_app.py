import azure.functions as func
import datetime
import json
import logging
import os

from serpapi import GoogleSearch
from core import extract_best_price_flight, upload_to_blob

app = func.FunctionApp()

@app.timer_trigger(schedule="* * 8 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def google_flights(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    api_key = os.getenv("SERPAPI_KEY")
    blob_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "flightdata"
    
    if not api_key or not blob_conn_str:
        logging.error("Missing environment variables.")
        return
    
    params = {
        "engine": "google_flights",
        "departure_id": "KUL",
        "arrival_id": "LHR",
        "outbound_date": "2026-12-25",
        "currency": "USD",
        "hl": "en",
        "api_key": api_key
    }

    try:
        search = GoogleSearch(params)
        results = search.get_dict()
        flight_data = extract_best_price_flight(results)

        if not flight_data:
            logging.warning("No flights found for today.")
            return

        # Use scrape_date as blob filename
        blob_filename = f"{flight_data['scrape_date']}.json"
        upload_to_blob(blob_conn_str, container_name, blob_filename, flight_data)

    except Exception as e:
        logging.exception(f"Error during scraping: {e}")

    logging.info('Python timer trigger function executed.')