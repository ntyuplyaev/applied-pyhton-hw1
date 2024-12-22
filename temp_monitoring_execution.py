import time
import asyncio
import pandas as pd
from utils.temp_monitoring import (
    get_current_temperature_sync,
    get_multiple_cities_temperature_async,
    is_temp_anomaly
)

# Загрузка файла со статистиками по городам,
# который мы получили на этапе анализа данных
stats = pd.read_csv('stats.csv')

test_cities = ["Berlin", "Cairo", "Dubai", "Beijing", "Moscow"]


# Синхронный анализ
def synchronous_analysis(cities, stats):
    start_time = time.time()
    for city in cities:
        try:
            current_temp = get_current_temperature_sync(city)
            anomalous, mean_temp, std_temp = is_temp_anomaly(city, current_temp, stats)
            if anomalous:
                print(f"Город: {city}, Текущая температура: {current_temp:.2f}°C -> АНОМАЛИЯ! "
                      f"(Средняя сезонная: {mean_temp:.2f}°C, Ст. откл.: {std_temp:.2f}°C)")
            else:
                print(f"Город: {city}, Текущая температура: {current_temp:.2f}°C в норме "
                      f"(Средняя сезонная: {mean_temp:.2f}°C, Ст. откл.: {std_temp:.2f}°C)")
        except Exception as e:
            print(f"Не удалось получить данные для {city}: {e}")
    end_time = time.time()
    print(f"Время синхронного анализа: {end_time - start_time:.2f} секунд")


# Асинхронный анализ
async def asynchronous_analysis(cities, stats):
    start_time = time.time()
    city_temps = await get_multiple_cities_temperature_async(cities)
    for city, current_temp in city_temps.items():
        if current_temp is None:
            print(f"[ASYNC] Не удалось получить температуру для города {city}.")
            continue
        anomalous, mean_temp, std_temp = is_temp_anomaly(city, current_temp, stats)
        if anomalous:
            print(f"[ASYNC] Город: {city}, Текущая температура: {current_temp:.2f}°C -> АНОМАЛИЯ! "
                  f"(Средняя сезонная: {mean_temp:.2f}°C, Ст. откл.: {std_temp:.2f}°C)")
        else:
            print(f"[ASYNC] Город: {city}, Текущая температура: {current_temp:.2f}°C в норме "
                  f"(Средняя сезонная: {mean_temp:.2f}°C, Ст. откл.: {std_temp:.2f}°C)")
    end_time = time.time()
    print(f"Время асинхронного анализа: {end_time - start_time:.2f} секунд")


def main():
    print("Синхронный анализ:")
    synchronous_analysis(test_cities, stats)

    print("\nАсинхронный анализ:")
    asyncio.run(asynchronous_analysis(test_cities, stats))


if __name__ == "__main__":
    main()


"""
Все функции работают успешно.
Температуры и аномалии совпадают для синхронных и асинхронных запросов.
Асинхронный запрос в данном скрипте отрабатывает быстрее синхронного: 0.19 секунд против 0.93 секунд

Логи после выполнения скрипта:

Синхронный анализ:
Город: Berlin, Текущая температура: 5.76°C в норме (Средняя сезонная: 0.11°C, Ст. откл.: 4.93°C)
Город: Cairo, Текущая температура: 21.42°C в норме (Средняя сезонная: 14.94°C, Ст. откл.: 4.99°C)
Город: Dubai, Текущая температура: 22.96°C в норме (Средняя сезонная: 20.05°C, Ст. откл.: 4.76°C)
Город: Beijing, Текущая температура: -4.06°C в норме (Средняя сезонная: -1.95°C, Ст. откл.: 4.83°C)
Город: Moscow, Текущая температура: -1.82°C в норме (Средняя сезонная: -9.87°C, Ст. откл.: 4.75°C)
Время синхронного анализа: 0.97 секунд

Асинхронный анализ:
[ASYNC] Город: Berlin, Текущая температура: 5.76°C в норме (Средняя сезонная: 0.11°C, Ст. откл.: 4.93°C)
[ASYNC] Город: Cairo, Текущая температура: 21.42°C в норме (Средняя сезонная: 14.94°C, Ст. откл.: 4.99°C)
[ASYNC] Город: Dubai, Текущая температура: 22.96°C в норме (Средняя сезонная: 20.05°C, Ст. откл.: 4.76°C)
[ASYNC] Город: Beijing, Текущая температура: -4.06°C в норме (Средняя сезонная: -1.95°C, Ст. откл.: 4.83°C)
[ASYNC] Город: Moscow, Текущая температура: -1.82°C в норме (Средняя сезонная: -9.87°C, Ст. откл.: 4.75°C)
Время асинхронного анализа: 0.19 секунд
"""

