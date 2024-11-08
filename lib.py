import datetime

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
