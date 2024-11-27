import dask
from dask import delayed
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import time




def exibir_maiores_intervalos(intervalos, sensor_tipo):
    resultado = f"Top 50 maiores intervalos para {sensor_tipo}:\n"
    for i, intervalo in enumerate(intervalos.head(50).itertuples(), start=1):
        device = intervalo.dispositivo
        value = intervalo.valor
        interval_start_date = intervalo.inicio
        interval_end_date = intervalo.fim
        interval_time = intervalo.duracao
        
        resultado += f"{i} Travamento:\n"
        resultado += f"  Dispositivo: {device}\n"
        resultado += f"  Valor: {value}\n"
        resultado += f"  Data Inicial: {interval_start_date}\n"
        resultado += f"  Data Final: {interval_end_date}\n"
        resultado += f"  Duração: {interval_time}\n\n"
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



# Função para processar intervalos
def processar_intervalos_dispositivo(dispositivo_df, sensor_coluna):
    dispositivo_df = dispositivo_df.sort_values('data')
    dispositivo_df['mudou'] = dispositivo_df[sensor_coluna] != dispositivo_df[sensor_coluna].shift()
    dispositivo_df['grupo'] = dispositivo_df['mudou'].cumsum()
    
    # Calcula intervalos
    intervalos = (
        dispositivo_df.groupby('grupo')
        .agg(
            valor=(sensor_coluna, 'first'),
            inicio=('data', 'first'),
            fim=('data', 'last')
        )
        .reset_index(drop=True)
    )
    intervalos['duracao'] = pd.to_datetime(intervalos['fim']) - pd.to_datetime(intervalos['inicio'])
    intervalos['dispositivo'] = dispositivo_df['device'].iloc[0]
    return intervalos

# Função para encontrar os 50 maiores intervalos
def encontrar_top_50(df, sensor_coluna):
    # Processa cada dispositivo separadamente
    intervalos = df.groupby('device').apply(
        processar_intervalos_dispositivo,
        sensor_coluna=sensor_coluna,
        meta={'valor': 'float64', 'inicio': 'datetime64[ns]', 'fim': 'datetime64[ns]', 'duracao': 'timedelta64[ns]', 'dispositivo': 'object'}
    )
    
    # Ordena pelos maiores intervalos
    intervalos = intervalos.compute()
    intervalos = intervalos.sort_values('duracao', ascending=False).head(50)
    return intervalos

def dask_parallel(nprocess):
    num_process = int(nprocess)
    dask.config.set({'distributed.worker.threads': 4})  # Número de threads por worker
    dask.config.set({'distributed.worker.nprocs': num_process})  # Número de processos

    # Leitura do arquivo CSV sem parse_dates
    df = dd.read_csv('dados_recebidos.csv', sep='|', na_values=[''], dtype={
        'device': 'object',
        'temperatura': 'float64',
        'umidade': 'float64',
        'luminosidade': 'float64',
        'data': 'object'  # Leia como string inicialmente
    })

    # Conversão manual da coluna 'data' para datetime
    df['data'] = dd.to_datetime(df['data'], errors='coerce')

    df = df.dropna(subset=['device', 'data', 'temperatura', 'umidade', 'luminosidade'],) 


    initial_time = time.time()

    # Encontrar os 50 maiores intervalos para cada sensor
    top_50_temperatura = encontrar_top_50(df, 'temperatura')
    top_50_umidade = encontrar_top_50(df, 'umidade')
    top_50_luminosidade = encontrar_top_50(df, 'luminosidade')

    final_time = time.time()

    total_time = final_time - initial_time
    # Exibir os resultados
    exibir_maiores_intervalos(top_50_temperatura, "temperatura")
    exibir_maiores_intervalos(top_50_umidade, "umidade")
    exibir_maiores_intervalos(top_50_luminosidade, "luminosidade")
    resultado = gerar_string_resultados(top_50_temperatura, top_50_umidade, top_50_luminosidade, total_time) 
    return resultado
