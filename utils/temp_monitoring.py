import requests
import aiohttp
import asyncio
import datetime
from utils.data_generation import month_to_season

API_KEY = '52f287091947af2c98c4f4f748163e24'


# Синхронный запрос к OpenWeatherMap (текущая температура)
def get_current_temperature_sync(city_name, api_key=API_KEY):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city_name,
        'appid': api_key,
        'units': 'metric'
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    # температура в градусах Цельсия
    return data['main']['temp']


# Асинхронный запрос к OpenWeatherMap (текущая температура)
async def get_current_temperature_async(session, city, api_key):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }

    try:
        async with session.get(base_url, params=params, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            temperature = data['main']['temp']
            return temperature
    except aiohttp.ClientResponseError as http_err:
        print(f"HTTP ошибка для города {city}: {http_err}")
    except aiohttp.ClientError as client_err:
        print(f"Ошибка клиента для города {city}: {client_err}")
    except asyncio.TimeoutError:
        print(f"Таймаут для города {city}.")
    except KeyError:
        print(f"Невозможно извлечь температуру для города {city}.")
    return None


# Асинхронный запрос для списка городов
async def get_multiple_cities_temperature_async(cities, api_key=API_KEY):
    """
    Асинхронное получение температуры для списка городов.
    Возвращает словарь {city_name: temperature}
    """
    async with aiohttp.ClientSession() as session:
        tasks = [get_current_temperature_async(session, city, api_key) for city in cities]
        results = await asyncio.gather(*tasks)
        return dict(zip(cities, results))


# Функция для проверки аномальности температуры города
def is_temp_anomaly(city_name, current_temp, stats, date=None):
    """
    Проверить, является ли текущая температура аномальной для данного города.
    Используем статистику по сезонам (season_mean и season_std) для определения.
    За сезон берем текущий сезон по дате или по текущей дате, если дата не указана.
    """
    if date is None:
        date = datetime.datetime.now()
    current_season = month_to_season[date.month]

    # Получаем статистику для города и сезона
    city_season_stats = stats[(stats['city'] == city_name) & (stats['season'] == current_season)]
    if city_season_stats.empty:
        # Если нет данных - считаем, что не можем определить
        return False, None, None

    mean_temp = city_season_stats['season_mean'].values[0]
    std_temp = city_season_stats['season_std'].values[0]

    # Проверка на аномалию
    lower_bound = mean_temp - 2 * std_temp
    upper_bound = mean_temp + 2 * std_temp

    is_anomalous = current_temp < lower_bound or current_temp > upper_bound
    return is_anomalous, mean_temp, std_temp
