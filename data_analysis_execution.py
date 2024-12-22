import pandas as pd
import time
from utils.data_analysis import sequential_analysis, parallel_analysis_main


def main():
    df = pd.read_csv('utils/temperature_data.csv')

    # Последовательный анализ
    start_time = time.time()
    df_seq, stats_seq = sequential_analysis(df.copy())
    seq_time = time.time() - start_time
    print(f"Время последовательного анализа: {seq_time:.2f} секунд")

    # Параллельный анализ
    start_time = time.time()
    df_par = parallel_analysis_main(df.copy())
    par_time = time.time() - start_time
    print(f"Время параллельного анализа: {par_time:.2f} секунд")

    # Проверка, что результаты совпадают
    df_seq_sorted = df_seq.sort_values(['city', 'timestamp']).reset_index(drop=True)
    df_par_sorted = df_par.sort_values(['city', 'timestamp']).reset_index(drop=True)

    # Выбираем только столбцы, которые сравниваются
    compare_columns = ['moving_avg', 'season_mean', 'season_std', 'anomaly']
    comparison = df_seq_sorted[compare_columns].compare(df_par_sorted[compare_columns])

    stats_seq.to_csv('stats.csv')

    if comparison.empty:
        print("Результаты последовательного и параллельного анализа совпадают")
    else:
        print("Есть расхождения между результатами анализа")

    # Выявили 2441 аномалий
    anomalies = df_seq[df_seq['anomaly']]
    num_anomalies = anomalies.shape[0]
    print(f'Выявлено {num_anomalies} аномалий')


if __name__ == "__main__":
    main()


"""
Параллелизация неэффективна в данном случаеn и работает медленнее синхронного варианта кода - 0.72 секунд против 0.07 секунд,
так как у нас слишком маленький набор данных, который очень быстро обрабаывается последовательно. 
Поэтому в данном случае нет особого смысла в параллелизации (такой подход занимает большее количество времени)
"""