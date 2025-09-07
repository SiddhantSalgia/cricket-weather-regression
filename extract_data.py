import requests
import json
import os
import pandas as pd
from datetime import datetime

API_KEY = "API_KEY"
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"

def extract_innings_data(path: str):
    innings_data = []
    for file_name in os.listdir(path):
        if file_name.endswith(".json"):
            filepath = os.path.join(path,file_name)
            with open(filepath, "r", encoding="utf-8") as f:
                match = json.load(f)
            if datetime.strptime(match["info"]["dates"][0],"%Y-%m-%d") < datetime.strptime("2022-01-01","%Y-%m-%d"):
                continue
            else:
                date = match["info"]["dates"][0]
                venue = match["info"]["venue"]
                city = match["info"]["city"]

                innings = match["innings"]
                for inning in innings:
                    team = inning["team"]
                    total_runs = 0
                    for over in inning["overs"]:
                        for delivery in over["deliveries"]:
                            total_runs +=delivery["runs"]["total"]
                    
                    run_rate = total_runs/50

                    row = {
                        "Date": date,
                        "City": city,
                        "Venue": venue,
                        "Team": team,
                        "Runs": total_runs,
                        "Run Rate": run_rate
                    }
                    innings_data.append(row)
    limit = len(innings_data)
    data_rows = []
    count = 0
    for inning in innings_data:
        date = inning["Date"]
        city = inning["City"]
        venue = inning["Venue"]
        team = inning["Team"]
        runs = inning["Runs"]
        run_rate = inning["Run Rate"]

        url = f"{BASE_URL}{city}/{date}?unitGroup=metric&key={API_KEY}&include=days"
        response = requests.get(url)
        if response.status_code == 200:
            weather = response.json()["days"][0]
            count+=1
            data_rows.append({
                    "Date": date,
                    "City": city,
                    "Venue": venue,
                    "Team": team,
                    "Runs": runs,
                    "Run Rate": run_rate,
                    "Temp": weather["temp"],
                    "Humidity": weather["humidity"],
                    "Windspeed": weather["windspeed"],
                    "Precipitation": weather["precip"]
                })
            print(f"Row {count} added")
            if count >= limit:
                print("Limit reached. Breaking loop.")
                break
        else:
            print(f"Failed for {city}, {date}")

    return data_rows

def generate_csv(innings_data):
    cricket_weather_dataset = pd.DataFrame(innings_data)
    upload_path = "UPLOAD_PATH"
    file_name = "Cricket_weather_dataset.csv"
    cricket_weather_dataset.to_csv(upload_path + file_name)

DATA_PATH = "/odis_male_json/"
innings_data = extract_innings_data(DATA_PATH)
generate_csv(innings_data)
