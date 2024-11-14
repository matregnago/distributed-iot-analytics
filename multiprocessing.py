import csv
import time
import multiprocessing
from collections import defaultdict
from lib import exibir_maiores_intervalos, calcular_diferenca_datas

def process_device_data(device_name, measurements):
    temperature_intervals = []
    humidity_intervals = []
    luminosity_intervals = []
      
    if measurements:
        prev_temp = measurements[0]["temperatura"]
        prev_hum = measurements[0]["umidade"]
        prev_lum = measurements[0]["luminosidade"]
        
        temp_start_data = measurements[0]["data"]
        hum_start_data = measurements[0]["data"]
        lum_start_data = measurements[0]["data"]
        
        for i in range(1, len(measurements)):
            current_meas = measurements[i]
            # temperatura
            if current_meas["temperatura"] != prev_temp:
                interval = calcular_diferenca_datas(temp_start_data, measurements[i-1]["data"])
                temperature_intervals.append({
                    "device": device_name,
                    "value": prev_temp,
                    "interval_start_date": temp_start_data,
                    "interval_end_date": measurements[i-1]["data"],
                    "interval_time": interval
                })
                prev_temp = current_meas["temperatura"]
                temp_start_data = current_meas["data"]
                
            # umidade
            if current_meas["umidade"] != prev_hum:
                interval = calcular_diferenca_datas(hum_start_data, measurements[i-1]["data"])
                humidity_intervals.append({
                    "device": device_name,
                    "value": prev_hum,
                    "interval_start_date": hum_start_data,
                    "interval_end_date": measurements[i-1]["data"],
                    "interval_time": interval
                })
                prev_hum = current_meas["umidade"]
                hum_start_data = current_meas["data"]
                
            # luminosidade
            if current_meas["luminosidade"] != prev_lum:
                interval = calcular_diferenca_datas(lum_start_data, measurements[i-1]["data"])
                luminosity_intervals.append({
                    "device": device_name,
                    "value": prev_lum,
                    "interval_start_date": lum_start_data,
                    "interval_end_date": measurements[i-1]["data"],
                    "interval_time": interval
                })
                prev_lum = current_meas["luminosidade"]
                lum_start_data = current_meas["data"]
        
        # Fechar intervalos após a última medição
        last_meas = measurements[-1]
        if prev_temp is not None:
            interval = calcular_diferenca_datas(temp_start_data, last_meas["data"])
            temperature_intervals.append({
                "device": device_name,
                "value": prev_temp,
                "interval_start_date": temp_start_data,
                "interval_end_date": last_meas["data"],
                "interval_time": interval
            })
        if prev_hum is not None:
            interval = calcular_diferenca_datas(hum_start_data, last_meas["data"])
            humidity_intervals.append({
                "device": device_name,
                "value": prev_hum,
                "interval_start_date": hum_start_data,
                "interval_end_date": last_meas["data"],
                "interval_time": interval
            })
        if prev_lum is not None:
            interval = calcular_diferenca_datas(lum_start_data, last_meas["data"])
            luminosity_intervals.append({
                "device": device_name,
                "value": prev_lum,
                "interval_start_date": lum_start_data,
                "interval_end_date": last_meas["data"],
                "interval_time": interval
            })
    
    return (temperature_intervals, humidity_intervals, luminosity_intervals)

def get_top_50_intervals(intervals):
    # Ordena os intervalos pelo tempo do intervalo (interval_time) em ordem decrescente
    sorted_intervals = sorted(intervals, key=lambda x: x["interval_time"], reverse=True)
    return sorted_intervals[:50]

if __name__ == '__main__':
    initial_time = time.time()
    
    # 1. Leitura e agrupamento do CSV por device
    devices_data = defaultdict(list)
    with open("devices.csv", mode='r', newline='', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        next(csv_reader)  # pular a linha de cabeçalho
        for row in csv_reader:
            device_name = row[1]
            data_medicao = row[3]
            if not(row[4]) or not(row[5]) or not(row[6]):
                 continue
            temperatura = float(row[4])
            umidade = float(row[5])
            luminosidade = float(row[6])
            
            devices_data[device_name].append({
                "data": data_medicao,
                "temperatura": temperatura,
                "umidade": umidade,
                "luminosidade": luminosidade
            })
    
    # 2. Processamento em paralelo
    with multiprocessing.Pool(processes=8) as pool:
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
