import csv
import datetime

devices = {}

temperature_intervals = []
humidity_intervals = []
luminosity_intervals = []

class Sensor:
    def __init__(self, value: float, interval_start_date: str, interval_end_date: str):
        self.value = value
        self.interval_start_date = interval_start_date
        self.interval_end_date = interval_end_date

class Device:
    def __init__(self, temperature: Sensor, humidity: Sensor, luminosity: Sensor):
        self.temperature = temperature
        self.humidity = humidity
        self.luminosity = luminosity

# Função para calcular a diferença entre datainicial e datafinal
def calcular_diferenca(datainicial, datafinal):
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
            interval = calcular_diferenca(device.temperature.interval_start_date, device.temperature.interval_end_date)
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
            interval = calcular_diferenca(device.humidity.interval_start_date, device.humidity.interval_end_date)
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
            interval = calcular_diferenca(device.luminosity.interval_start_date, device.luminosity.interval_end_date)
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

# Leitura do arquivo CSV
with open("devices.csv", mode='r', newline='', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    for line in csv_reader:
        row = line[0].split('|')
        register_row(row)

# Função para exibir os 5 maiores intervalos
def exibir_maiores_intervalos(intervalos, sensor_tipo):
    intervalos_ordenados = sorted(intervalos, key=lambda x: x['interval_time'], reverse=True)
    print(f"Top 5 maiores intervalos para {sensor_tipo}:")
    i=1
    for intervalo in intervalos_ordenados[:5]:
        device = intervalo['device']
        value = intervalo['value']
        interval_start_date = intervalo['interval_start_date']
        interval_end_date = intervalo['interval_end_date']
        interval_time = intervalo['interval_time']
        
        print(f"{i} Travamento:")
        print(f"  Dispositivo: {device}")
        print(f"  Valor: {value}")
        print(f"  Data Inicial: {interval_start_date}")
        print(f"  Data Final: {interval_end_date}")
        print(f"  Duração: {interval_time}\n")
        i+=1
    print('\n\n')
exibir_maiores_intervalos(temperature_intervals, "temperatura")
exibir_maiores_intervalos(humidity_intervals, "umidade")
exibir_maiores_intervalos(luminosity_intervals, "luminosidade")
