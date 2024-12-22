import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial


# Вычисление скользящего среднего по каждому городу
def compute_moving_average(df, window=30):
    df = df.sort_values(['city', 'timestamp'])
    df['moving_avg'] = df.groupby('city')['temperature'].rolling(window=window, min_periods=1).mean().reset_index(0, drop=True)
    return df


# Расчет среднего и стандартного отклонения по сезонам и городам
def compute_season_stats(df):
    stats = df.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    stats = stats.rename(columns={'mean': 'season_mean', 'std': 'season_std'})
    return stats


# Выявление аномалий
def detect_anomalies(df):
    df['anomaly'] = ((df['temperature'] > df['season_mean'] + 2 * df['season_std']) |
                     (df['temperature'] < df['season_mean'] - 2 * df['season_std']))
    return df


# Функция для обработки одного города
def process_city(city_df, stats):
    # Объединяем статистику с данными города
    city_df = city_df.merge(stats, on=['city', 'season'], how='left')
    city_df = detect_anomalies(city_df)
    return city_df


# Последовательный анализ
def sequential_analysis(df):
    df = compute_moving_average(df)
    stats = compute_season_stats(df)
    df = process_city(df, stats)
    return df, stats


# Параллельный анализ с использованием multiprocessing
def parallel_analysis_multiprocessing(df, stats, n_jobs=None):
    if n_jobs is None:
        n_jobs = cpu_count()

    cities = df['city'].unique()
    city_dfs = [df[df['city'] == city].copy() for city in cities]

    # Создаем partial функцию с фиксированными stats
    worker = partial(process_city, stats=stats)

    with Pool(processes=n_jobs) as pool:
        result_dfs = pool.map(worker, city_dfs)

    # Объединение результатов
    df = pd.concat(result_dfs, ignore_index=True)
    return df


# Параллельный анализ
def parallel_analysis_main(df):
    df = compute_moving_average(df)
    stats = compute_season_stats(df)
    df_par = parallel_analysis_multiprocessing(df, stats)
    return df_par
