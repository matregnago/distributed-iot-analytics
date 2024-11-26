import csv
import time
import multiprocessing
from collections import defaultdict
import datetime

def exibir_maiores_intervalos(intervalos, sensor_tipo):
    intervalos_ordenados = sorted(intervalos, key=lambda x: x['interval_time'], reverse=True)
    resultado = f"Top 50 maiores intervalos para {sensor_tipo}:\n"
    i = 1
    for intervalo in intervalos_ordenados[:50]:
        device = intervalo['device']
        value = intervalo['value']
        interval_start_date = intervalo['interval_start_date']
        interval_end_date = intervalo['interval_end_date']
        interval_time = intervalo['interval_time']
        
        resultado += f"{i} Travamento:\n"
        resultado += f"  Dispositivo: {device}\n"
        resultado += f"  Valor: {value}\n"
        resultado += f"  Data Inicial: {interval_start_date}\n"
        resultado += f"  Data Final: {interval_end_date}\n"
        resultado += f"  Duração: {interval_time}\n\n"
        i += 1
    return resultado

def gerar_string_resultados(intervalos_temperatura, intervalos_umidade, intervalos_luminosidade, tempo_total):
    # Exibir os intervalos e combinar com o tempo total
    resultado_temperatura = exibir_maiores_intervalos(intervalos_temperatura, "temperatura")
    resultado_umidade = exibir_maiores_intervalos(intervalos_umidade, "umidade")
    resultado_luminosidade = exibir_maiores_intervalos(intervalos_luminosidade, "luminosidade")

    # Montando a string final com todas as informações
    resultado_completo = f"{resultado_temperatura}\n{resultado_umidade}\n{resultado_luminosidade}"
    resultado_completo += f"\nTempo total de processamento: {tempo_total:.2f} segundos.\n"
    
    return resultado_completo

def calcular_diferenca_datas(datainicial, datafinal):
    formatos_data = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]  # Formatos com e sem microssegundos
    
    for formato in formatos_data:
        try:
            dt_inicial = datetime.datetime.strptime(datainicial, formato)
            dt_final = datetime.datetime.strptime(datafinal, formato)
            return dt_final - dt_inicial  # Retorna a diferença em timedelta
        except ValueError:
            continue
    
    try:
        if "." not in datainicial:
            datainicial += ".000000"
        if "." not in datafinal:
            datafinal += ".000000"
        
        dt_inicial = datetime.datetime.strptime(datainicial, "%Y-%m-%d %H:%M:%S.%f")
        dt_final = datetime.datetime.strptime(datafinal, "%Y-%m-%d %H:%M:%S.%f")
        return dt_final - dt_inicial  # Retorna a diferença em timedelta
    except ValueError:
        raise ValueError(f"Formato de data inválido para as datas: {datainicial} ou {datafinal}")


def cria_estrutura_devices(filename):
    devices_data = defaultdict(list)
    with open(filename, mode='r', newline='', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='|')
        next(csv_reader)
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
    return devices_data


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

def multiproc(n_processes):
    initial_time = time.time()
    nthreads = int(n_processes)
    devices_data = cria_estrutura_devices("dados_recebidos.csv")
    
    with multiprocessing.Pool(processes=nthreads) as pool:
        results = pool.starmap(process_device_data, devices_data.items())
    
    combined_temperature_intervals = []
    combined_humidity_intervals = []
    combined_luminosity_intervals = []
    for t_ints, h_ints, l_ints in results:
        combined_temperature_intervals.extend(t_ints)
        combined_humidity_intervals.extend(h_ints)
        combined_luminosity_intervals.extend(l_ints)
    
    top_50_temperature = get_top_50_intervals(combined_temperature_intervals)
    top_50_humidity = get_top_50_intervals(combined_humidity_intervals)
    top_50_luminosity = get_top_50_intervals(combined_luminosity_intervals)
    
    final_time = time.time()
    total_time = final_time - initial_time

    resultado = gerar_string_resultados(top_50_temperature, top_50_humidity, top_50_luminosity, total_time)

    return resultado
