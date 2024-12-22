import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import matplotlib.pyplot as plt
import seaborn as sns
from utils.data_analysis import compute_season_stats
from utils.temp_monitoring import (
    get_multiple_cities_temperature_async,
    is_temp_anomaly
)


def main():
    st.title("Мониторинг Температур")

    # Загрузка исторических данных
    st.header("Загрузка исторических данных о температуре")
    uploaded_file = st.file_uploader("Загрузите CSV файл с историческими данными", type=["csv"])

    if uploaded_file is not None:
        try:
            # Чтение загруженного файла
            data = pd.read_csv(uploaded_file, parse_dates=['timestamp'])

            # Отображение первых нескольких строк
            st.subheader("Просмотр данных")
            st.dataframe(data.head())

            # Проверка необходимых столбцов
            required_columns = {'city', 'timestamp', 'temperature', 'season'}
            if not required_columns.issubset(data.columns):
                st.error(f"Файл должен содержать столбцы: {', '.join(required_columns)}")
            else:
                st.success("Файл успешно загружен и проверен!")
                st.session_state['data'] = data

                # Выбор города из выпадающего списка
                st.header("Выбор города")
                cities = sorted(data['city'].unique())
                selected_city = st.selectbox("Выберите город для анализа", cities)

                # Инициализация фильтра даты в session_state
                if 'reset_filter' not in st.session_state:
                    st.session_state['reset_filter'] = False

                # Добавление фильтра по диапазону дат
                st.header("Фильтр по диапазону дат")
                min_date = data['timestamp'].min().date()
                max_date = data['timestamp'].max().date()

                start_date, end_date = st.date_input(
                    "Выберите диапазон дат для анализа",
                    value=[min_date, max_date],
                    min_value=min_date,
                    max_value=max_date
                )

                # Проверка корректности выбранных дат
                if start_date > end_date:
                    st.error("Начальная дата должна быть раньше конечной даты.")
                    return

                # Фильтрация данных на основе выбранного диапазона дат
                mask = (data['timestamp'].dt.date >= start_date) & (data['timestamp'].dt.date <= end_date)
                filtered_data = data.loc[mask]

                if filtered_data.empty:
                    st.warning("Нет данных в выбранном диапазоне дат.")
                    return

                # Вычисление сезонной статистики на отфильтрованных данных
                st.header("Вычисление сезонной статистики")
                stats = compute_season_stats(filtered_data)
                st.session_state['stats'] = stats
                st.write("Статистика по сезонам:")
                st.dataframe(stats[stats['city'] == selected_city].head())

                # Форма для ввода API-ключа OpenWeatherMap
                st.header("Ввод API-ключа OpenWeatherMap")
                with st.form(key='api_key_form'):
                    api_key = st.text_input("Введите ваш API-ключ OpenWeatherMap", type="password")
                    submit_button = st.form_submit_button(label='Проверить и Получить Текущую Температуру')

                if submit_button:
                    if not api_key:
                        st.warning("Введите ваш API-ключ OpenWeatherMap.")
                    else:
                        with st.spinner('Получение текущей температуры...'):
                            asyncio.run(fetch_and_display_temperature(selected_city, api_key, filtered_data, stats))

                # Отображение Описательной Статистики
                st.header("Описательная статистика по температуре")
                city_data = filtered_data[filtered_data['city'] == selected_city]['temperature']

                st.subheader(f"Статистика для {selected_city}")
                desc = city_data.describe()
                st.write(desc)

                st.subheader("Визуализации распределения температур")

                # Гистограмма
                fig1, ax1 = plt.subplots()
                sns.histplot(city_data, bins=20, kde=True, ax=ax1)
                ax1.set_title(f"Гистограмма температур для {selected_city}")
                ax1.set_xlabel("Температура (°C)")
                ax1.set_ylabel("Частота")
                st.pyplot(fig1)

                # Боксплот
                fig2, ax2 = plt.subplots()
                sns.boxplot(x=city_data, ax=ax2)
                ax2.set_title(f"Боксплот температур для {selected_city}")
                ax2.set_xlabel("Температура (°C)")
                st.pyplot(fig2)

                # Временной Ряд Температур с Выделением Аномалий
                st.header("Временной ряд температур с аномалиями")
                city_data_sorted = filtered_data[filtered_data['city'] == selected_city].sort_values('timestamp')
                # Объединение данных города с сезонной статистикой
                city_data_sorted = city_data_sorted.merge(stats, on=['city', 'season'], how='left')
                # Определение аномалий
                city_data_sorted['is_anomaly'] = city_data_sorted.apply(
                    lambda row: row['temperature'] < (row['season_mean'] - 2 * row['season_std']) or
                                row['temperature'] > (row['season_mean'] + 2 * row['season_std']),
                    axis=1
                )

                # Скользящая Средняя
                window_size = st.slider("Выберите размер окна для скользящей средней", min_value=1, max_value=30, value=7)
                city_data_sorted['moving_avg'] = city_data_sorted['temperature'].rolling(window=window_size).mean()

                fig3, ax3 = plt.subplots(figsize=(10, 5))
                ax3.plot(city_data_sorted['timestamp'], city_data_sorted['temperature'], label='Температура')
                ax3.plot(city_data_sorted['timestamp'], city_data_sorted['moving_avg'], label=f'Скользящая Средняя ({window_size} дн.)', color='orange')
                # Выделение аномалий
                anomalies = city_data_sorted[city_data_sorted['is_anomaly']]
                ax3.scatter(anomalies['timestamp'], anomalies['temperature'], color='red', label='Аномалии')
                ax3.set_title(f"Временной ряд температур для {selected_city}")
                ax3.set_xlabel("Дата")
                ax3.set_ylabel("Температура (°C)")
                ax3.legend()
                st.pyplot(fig3)

                # Сезонные Профили, mean + std
                st.header("Сезонные профили температур")
                season_stats = stats[stats['city'] == selected_city]

                if not season_stats.empty:
                    fig6, ax6 = plt.subplots(figsize=(8, 5))
                    ax6.bar(
                        season_stats['season'],
                        season_stats['season_mean'],
                        yerr=season_stats['season_std'],
                        capsize=5,
                        color='skyblue',
                        label='Среднее значение'
                    )
                    ax6.set_title(f"Сезонные профили температур для {selected_city}")
                    ax6.set_xlabel("Сезон")
                    ax6.set_ylabel("Средняя температура (°C)")
                    ax6.errorbar(
                        season_stats['season'],
                        season_stats['season_mean'],
                        yerr=season_stats['season_std'],
                        fmt='none',
                        ecolor='black',
                        capsize=5,
                        label='Стандартное отклонение'
                    )
                    ax6.legend()
                    st.pyplot(fig6)

                # Количество Аномалий по Сезонам
                st.header("Количество аномалий по сезонам")
                anomaly_counts = city_data_sorted.groupby('season')['is_anomaly'].sum().reset_index()
                fig4, ax4 = plt.subplots()
                sns.barplot(data=anomaly_counts, x='season', y='is_anomaly', palette='viridis', ax=ax4)
                ax4.set_title(f"Количество аномалий по сезонам для {selected_city}")
                ax4.set_xlabel("Сезон")
                ax4.set_ylabel("Количество аномалий")
                st.pyplot(fig4)

                # Тепловая Карта Температур по Дням и Сезонам
                st.header("Тепловая карта температур по дням и сезонам")
                # Создание дополнительных столбцов для дня и месяца
                city_data_sorted['day'] = city_data_sorted['timestamp'].dt.day
                city_data_sorted['month'] = city_data_sorted['timestamp'].dt.month
                pivot_table = city_data_sorted.pivot_table(values='temperature', index='season', columns='day', aggfunc='mean')

                fig5, ax5 = plt.subplots(figsize=(15, 6))
                sns.heatmap(pivot_table, cmap='coolwarm', ax=ax5)
                ax5.set_title(f"Тепловая карта средних температур по дням и сезонам для {selected_city}")
                ax5.set_xlabel("День месяца")
                ax5.set_ylabel("Сезон")
                st.pyplot(fig5)
        except Exception as e:
            st.error(f"Произошла ошибка при обработке файла: {e}")

    else:
        st.info("Загрузите CSV файл для продолжения.")


async def fetch_and_display_temperature(city, api_key, data, stats):
    try:
        async with aiohttp.ClientSession() as session:
            city_temps = await get_multiple_cities_temperature_async([city], api_key)

        if city not in city_temps or city_temps[city] is None:
            st.error("Не удалось получить текущую температуру. Проверьте ваш API-ключ.")
            return

        temp = city_temps[city]
        # Проверка аномалии
        anomalous, mean_temp, std_temp = is_temp_anomaly(city, temp, stats)

        # Отображение результата
        if anomalous:
            st.error(
                f"Город: {city}, Текущая температура: {temp:.2f}°C -> АНОМАЛИЯ! "
                f"(Средняя сезонная: {mean_temp:.2f}°C, Ст. откл.: {std_temp:.2f}°C)"
            )
        else:
            st.success(
                f"Город: {city}, Текущая температура: {temp:.2f}°C в норме "
                f"(Средняя сезонная: {mean_temp:.2f}°C, Ст. откл.: {std_temp:.2f}°C)"
            )
    except aiohttp.ClientResponseError as http_err:
        if http_err.status == 401:
            st.error("Некорректный API-ключ. Пожалуйста, проверьте его и попробуйте снова.")
        else:
            st.error(f"HTTP ошибка: {http_err}")
    except Exception as e:
        st.error(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    main()
