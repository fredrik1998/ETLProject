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
        date = data['dt_txt']
        transformed_data.append({
            'city': city_name,
            'date': date,
            'datetime': data['dt_txt'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'], 
        })

    return transformed_data

def load_data(transformed_data):
    df = pd.DataFrame(transformed_data)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    return df

def show_data(df):
    plt.figure(figsize=(12, 6))
    plt.plot(df['temperature'], label='Temperature')
    plt.plot(df['humidity'], label='Humidity')
    plt.plot(df['pressure'], label='Pressure')
    plt.xlabel('Datetime')
    plt.ylabel('Values')
    plt.title('Forecast')
    plt.legend()
    plt.show()

def main():
    API_KEY = 'c4396d65189bf06a392f5e57db17ae82'
    city = input('Enter city')
    
    database_uri = f"postgresql://postgres:noob123@localhost:5432/postgres"
    engine = create_engine(database_uri)
    forecast_data = extract_data(API_KEY, city)
    transformed_data = transform_data(forecast_data)
    df = load_data(transformed_data)
    df.to_sql(name='forecast',
                con=engine,
                index=False,
                if_exists='append')
    show_data(df)

if __name__ == "__main__":
    main()
