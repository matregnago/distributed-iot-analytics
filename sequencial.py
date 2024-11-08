import csv
import time
from lib import exibir_maiores_intervalos, calcular_diferenca_datas
from device import Device, Sensor

devices = {}

temperature_intervals = []
humidity_intervals = []
luminosity_intervals = []

def register_row(row):
    if '' in row:
        return
    device_name = row[1]
    current_date = row[3]
    temperature = float(row[4])
    humidity = float(row[5])
    luminosity = float(row[6])
    
    if device_name not in devices:
        temperature_sensor = Sensor(temperature, current_date, current_date)
        humidity_sensor = Sensor(humidity, current_date, current_date)
        luminosity_sensor = Sensor(luminosity, current_date, current_date)
        devices[device_name] = Device(temperature_sensor, humidity_sensor, luminosity_sensor)
    
    else:
        device = devices[device_name]
        
        # Atualização para sensor de temperatura
        if device.temperature.value == temperature:
            device.temperature.interval_end_date = current_date
        else:
            interval = calcular_diferenca_datas(device.temperature.interval_start_date, device.temperature.interval_end_date)
            temperature_intervals.append({
                "device": device_name,
                "value": device.temperature.value,
                "interval_start_date": device.temperature.interval_start_date,
                "interval_end_date": device.temperature.interval_end_date,
                "interval_time": interval
            })
            device.temperature.value = temperature
            device.temperature.interval_start_date = current_date
            device.temperature.interval_end_date = current_date
        
        # Atualização para sensor de umidade
        if device.humidity.value == humidity:
            device.humidity.interval_end_date = current_date
        else:
            interval = calcular_diferenca_datas(device.humidity.interval_start_date, device.humidity.interval_end_date)
            humidity_intervals.append({
                "device": device_name,
                "value": device.humidity.value,
                "interval_start_date": device.humidity.interval_start_date,
                "interval_end_date": device.humidity.interval_end_date,
                "interval_time": interval
            })
            device.humidity.value = humidity
            device.humidity.interval_start_date = current_date
            device.humidity.interval_end_date = current_date
        
        # Atualização para sensor de luminosidade
        if device.luminosity.value == luminosity:
            device.luminosity.interval_end_date = current_date
        else:
            interval = calcular_diferenca_datas(device.luminosity.interval_start_date, device.luminosity.interval_end_date)
            luminosity_intervals.append({
                "device": device_name,
                "value": device.luminosity.value,
                "interval_start_date": device.luminosity.interval_start_date,
                "interval_end_date": device.luminosity.interval_end_date,
                "interval_time": interval
            })
            device.luminosity.value = luminosity
            device.luminosity.interval_start_date = current_date
            device.luminosity.interval_end_date = current_date

initial_time = time.time()

# Leitura do arquivo CSV
with open("devices.csv", mode='r', newline='', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    for line in csv_reader:
        row = line[0].split('|')
        register_row(row)

final_time = time.time()

exibir_maiores_intervalos(temperature_intervals, "temperatura")
exibir_maiores_intervalos(humidity_intervals, "umidade")
exibir_maiores_intervalos(luminosity_intervals, "luminosidade")

total_time = final_time - initial_time
print(f"{final_time:.2f}")