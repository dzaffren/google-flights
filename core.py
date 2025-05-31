import datetime
import json
import os
import logging
from serpapi import GoogleSearch
from azure.storage.blob import BlobServiceClient

def extract_best_price_flight(data):
    flights = data.get("best_flights") or data.get("other_flights")
    if not flights or not isinstance(flights, list):
        return None

    best = flights[0]
    summary = best["flights"][0] if best.get("flights") else {}

    return {
        "scrape_date": datetime.date.today().isoformat(),
        "origin": summary.get("departure_airport", {}).get("id", "KUL"),
        "destination": summary.get("arrival_airport", {}).get("id", "LHR"),
        "outbound_date": "2025-12-25",
        "price": best.get("price"),
        "airline": summary.get("airline"),
        "duration_mins": summary.get("duration"),
        "stops": len(best.get("layovers", [])),
        "overnight": summary.get("overnight"),
        "often_delayed": summary.get("often_delayed_by_over_30_min"),
        "carbon_emissions_g": best.get("carbon_emissions", {}).get("this_flight")
    }

def upload_to_blob(blob_conn_str, container_name, file_name, content_json):
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Ensure the container exists
    try:
        container_client.create_container()
    except Exception:
        pass  # Already exists

    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(json.dumps(content_json), overwrite=True)
    logging.info(f"Uploaded blob: {file_name}")
        
@app.timer_trigger(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
        
    logging.info("Flight price scraper function started...")

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