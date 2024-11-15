import time
from lib import cria_estrutura_devices, exibir_maiores_intervalos, process_device_data, get_top_50_intervals, gerar_chunks

if __name__ == '__main__':
    initial_time = time.time()
    intervalos_temperatura = []
    intervalos_umidade = []
    intervalos_luminosidade = []
    devices_data = cria_estrutura_devices("devices.csv")

    # Exemplo em um algoritmo paralelo
    # n_processes = 28
    # chunks = gerar_chunks(devices_data, n_processes)

    # first_chunk = chunks[0]                       # Primeiro chunk
    # first_device = next(iter(first_chunk))        # Primeiro device
    # first_reading = first_chunk[first_device][0]  # Primeira leitura do primeiro device
    
    # for chunk in chunks:
    #     for device in iter(chunk):
    #         temperature_intervals, humidity_intervals, luminosity_intervals = process_device_data(device, chunk[device])
    #         intervalos_temperatura.extend(temperature_intervals)
    #         intervalos_umidade.extend(humidity_intervals)
    #         intervalos_luminosidade.extend(luminosity_intervals)
    
    for device in iter(devices_data):
        temperature_intervals, humidity_intervals, luminosity_intervals = process_device_data(device, devices_data[device])
        intervalos_temperatura.extend(temperature_intervals)
        intervalos_umidade.extend(humidity_intervals)
        intervalos_luminosidade.extend(luminosity_intervals)


    top_50_temperature = get_top_50_intervals(intervalos_temperatura)
    top_50_humidity = get_top_50_intervals(intervalos_umidade)
    top_50_luminosity = get_top_50_intervals(intervalos_luminosidade)
    final_time = time.time()
    exibir_maiores_intervalos(top_50_temperature, "temperatura")
    exibir_maiores_intervalos(top_50_humidity, "umidade")
    exibir_maiores_intervalos(top_50_luminosity, "luminosidade")

    total_time = final_time - initial_time
    print(f"Tempo total de processamento: {total_time:.2f} segundos.")


    