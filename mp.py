import csv
import time
import multiprocessing
from collections import defaultdict
from lib import exibir_maiores_intervalos, calcular_diferenca_datas, cria_estrutura_devices, process_device_data, get_top_50_intervals

if __name__ == '__main__':
    initial_time = time.time()
    
    devices_data = cria_estrutura_devices("devices.csv")
    
    n_processes = 8
    # 2. Processamento em paralelo
    with multiprocessing.Pool(processes=n_processes) as pool:
        results = pool.starmap(process_device_data, devices_data.items())
    
    # 3. Combinar resultados
    combined_temperature_intervals = []
    combined_humidity_intervals = []
    combined_luminosity_intervals = []
    for t_ints, h_ints, l_ints in results:
        combined_temperature_intervals.extend(t_ints)
        combined_humidity_intervals.extend(h_ints)
        combined_luminosity_intervals.extend(l_ints)
    
    # 4. Encontrar os 50 maiores intervalos para cada variável
    top_50_temperature = get_top_50_intervals(combined_temperature_intervals)
    top_50_humidity = get_top_50_intervals(combined_humidity_intervals)
    top_50_luminosity = get_top_50_intervals(combined_luminosity_intervals)
    
    final_time = time.time()
    
    # 5. Exibir os resultados
    exibir_maiores_intervalos(top_50_temperature, "temperatura")
    exibir_maiores_intervalos(top_50_humidity, "umidade")
    exibir_maiores_intervalos(top_50_luminosity, "luminosidade")
    
    total_time = final_time - initial_time
    print(f"Tempo total de processamento: {total_time:.2f} segundos.")
