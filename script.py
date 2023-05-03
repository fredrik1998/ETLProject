import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

def extract_data(API_KEY, city):
    api_url = "http://api.openweathermap.org/data/2.5/forecast?"
    complete_url = f"{api_url}q={city}&appid={API_KEY}"
    response = requests.get(complete_url, params={'units' : 'metric'})
    return json.loads(response.text)

def transform_data(forecast_data):
    city_name = forecast_data['city']['name']
    transformed_data = []
    for data in forecast_data['list']:
        date = data['dt_txt'].split()[0]
        time = data['dt_txt'].split()[1] 
        transformed_data.append({
            'city': city_name,
            'date': date,
            'time': time,
            'datetime': data['dt_txt'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'], 
        })

    return transformed_data


def load_data(transformed_data):
    df = pd.DataFrame(transformed_data)
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.date
    df.set_index('datetime', inplace=True)
    return df

def show_data(df):
    fig, axis = plt.subplots(3, 1, figsize=(12, 8))
    axis[0].plot(df['temperature'], label='Temperature')
    axis[0].set_xlabel('Datetime')
    axis[0].set_ylabel('Temperature')
    axis[0].set_title('Temperature Forecast')
    axis[0].legend()
    
    axis[1].plot(df['humidity'], label='Humidity')
    axis[1].set_xlabel('Datetime')
    axis[1].set_ylabel('Humidity')
    axis[1].set_title('Humidity Forecast')
    axis[1].legend()
    
    axis[2].plot(df['pressure'], label='Pressure')
    axis[2].set_xlabel('Datetime')
    axis[2].set_ylabel('Pressure')
    axis[2].set_title('Pressure Forecast')
    axis[2].legend()
    
    plt.tight_layout()
    plt.show()


def main():
    API_KEY = 'c4396d65189bf06a392f5e57db17ae82'
    cities = ['Stockholm', 'Gothenburg', 'Malmo',]
    file_name_csv = 'forecast.csv'
    file_name_json = 'forecast.json'

    database_uri = f"postgresql://postgres:noob123@localhost:5432/postgres"
    engine = create_engine(database_uri)

    for city in cities:
        forecast_data = extract_data(API_KEY, city)
        transformed_data = transform_data(forecast_data)
        df = load_data(transformed_data)
        df.to_csv(f"{city}_{file_name_csv}", encoding='utf-8', index=False)
        df.to_json(f"{city}_{file_name_json}", date_format='iso', orient='records', lines=True)
        table_name = f"{city}_forecast".lower().replace(" ", "_")
        df.to_sql(name=table_name,
                  con=engine,
                  index=False,
                  if_exists='replace')
        show_data(df)

if __name__ == "__main__":
    main()