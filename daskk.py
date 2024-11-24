import dask
from dask import delayed
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

dask.config.set({'distributed.worker.threads': 4})  # Número de threads por worker
dask.config.set({'distributed.worker.nprocs': 2})  # Número de processos



def exibir_maiores_intervalos(intervalos, sensor_tipo):
    print(f"Top 5 maiores intervalos para {sensor_tipo}:\n")
    for i, intervalo in enumerate(intervalos.head(5).itertuples(), start=1):
        device = intervalo.dispositivo
        value = intervalo.valor
        interval_start_date = intervalo.inicio
        interval_end_date = intervalo.fim
        interval_time = intervalo.duracao
        
        print(f"{i} Travamento:")
        print(f"  Dispositivo: {device}")
        print(f"  Valor: {value}")
        print(f"  Data Inicial: {interval_start_date}")
        print(f"  Data Final: {interval_end_date}")
        print(f"  Duração: {interval_time}\n")
    print('\n')



# Leitura do arquivo CSV sem parse_dates
df = dd.read_csv('devices.csv', sep='|', na_values=[''], dtype={
    'device': 'object',
    'temperatura': 'float64',
    'umidade': 'float64',
    'luminosidade': 'float64',
    'data': 'object'  # Leia como string inicialmente
})

# Conversão manual da coluna 'data' para datetime
df['data'] = dd.to_datetime(df['data'], errors='coerce')

df = df.dropna(subset=['device', 'data', 'temperatura', 'umidade', 'luminosidade'],) 


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

# Encontrar os 50 maiores intervalos para cada sensor
top_50_temperatura = encontrar_top_50(df, 'temperatura')
top_50_umidade = encontrar_top_50(df, 'umidade')
top_50_luminosidade = encontrar_top_50(df, 'luminosidade')

# Exibir os resultados
exibir_maiores_intervalos(top_50_temperatura, "temperatura")
exibir_maiores_intervalos(top_50_umidade, "umidade")
exibir_maiores_intervalos(top_50_luminosidade, "luminosidade")
