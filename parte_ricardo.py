import csv
from datetime import datetime

# Função para calcular a diferença entre datainicial e datafinal
def calcular_diferenca(datainicial, datafinal):
    formatos_data = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]  # Formatos com e sem microssegundos
    
    for formato in formatos_data:
        try:
            dt_inicial = datetime.strptime(datainicial, formato)
            dt_final = datetime.strptime(datafinal, formato)
            return dt_final - dt_inicial  # Retorna a diferença em timedelta
        except ValueError:
            continue
    
    try:
        if "." not in datainicial:
            datainicial += ".000000"
        if "." not in datafinal:
            datafinal += ".000000"
        
        dt_inicial = datetime.strptime(datainicial, "%Y-%m-%d %H:%M:%S.%f")
        dt_final = datetime.strptime(datafinal, "%Y-%m-%d %H:%M:%S.%f")
        return dt_final - dt_inicial
    except ValueError:
        raise ValueError(f"Formato de data inválido para as datas: {datainicial} ou {datafinal}")

# Função para registrar e calcular intervalos para cada variável (temperatura, umidade, luminosidade)
def registrar_leitura(device, data, temperatura, umidade, luminosidade):
    if device not in hash_map:
        # Primeira leitura do dispositivo
        hash_map[device] = {
            "temperatura": {"valor": temperatura, "datainicial": data, "datafinal": data},
            "umidade": {"valor": umidade, "datainicial": data, "datafinal": data},
            "luminosidade": {"valor": luminosidade, "datainicial": data, "datafinal": data},
        }
    else:
        # Verifica a temperatura
        if hash_map[device]['temperatura']['valor'] == temperatura:
            hash_map[device]['temperatura']['datafinal'] = data
        else:
            diferenca = calcular_diferenca(hash_map[device]['temperatura']['datainicial'], hash_map[device]['temperatura']['datafinal'])
            intervalos_temperatura.append({
                "device": device,
                "valor": hash_map[device]['temperatura']['valor'],
                "datainicial": hash_map[device]['temperatura']['datainicial'],
                "datafinal": hash_map[device]['temperatura']['datafinal'],
                "diferenca": diferenca
            })
            hash_map[device]['temperatura'] = {"valor": temperatura, "datainicial": data, "datafinal": data}
        
        # Verifica a umidade
        if hash_map[device]['umidade']['valor'] == umidade:
            hash_map[device]['umidade']['datafinal'] = data
        else:
            diferenca = calcular_diferenca(hash_map[device]['umidade']['datainicial'], hash_map[device]['umidade']['datafinal'])
            intervalos_umidade.append({
                "device": device,
                "valor": hash_map[device]['umidade']['valor'],
                "datainicial": hash_map[device]['umidade']['datainicial'],
                "datafinal": hash_map[device]['umidade']['datafinal'],
                "diferenca": diferenca
            })
            hash_map[device]['umidade'] = {"valor": umidade, "datainicial": data, "datafinal": data}
        
        # Verifica a luminosidade
        if hash_map[device]['luminosidade']['valor'] == luminosidade:
            hash_map[device]['luminosidade']['datafinal'] = data
        else:
            diferenca = calcular_diferenca(hash_map[device]['luminosidade']['datainicial'], hash_map[device]['luminosidade']['datafinal'])
            intervalos_luminosidade.append({
                "device": device,
                "valor": hash_map[device]['luminosidade']['valor'],
                "datainicial": hash_map[device]['luminosidade']['datainicial'],
                "datafinal": hash_map[device]['luminosidade']['datafinal'],
                "diferenca": diferenca
            })
            hash_map[device]['luminosidade'] = {"valor": luminosidade, "datainicial": data, "datafinal": data}

# Inicialização dos hash_map e listas de intervalos
hash_map = {}
intervalos_temperatura = []
intervalos_umidade = []
intervalos_luminosidade = []

# Caminho do arquivo CSV
caminho = './devices.csv'

# Leitura do arquivo CSV
with open(caminho, mode='r', newline='', encoding='utf-8') as arquivo_csv:
    leitor_csv = csv.reader(arquivo_csv)
    next(leitor_csv)  # Pula o cabeçalho
    for linha in leitor_csv:
        partes = linha[0].split('|')
        if not partes[1] or not partes[3] or not partes[4] or not partes[5] or not partes[6]:
            continue

        device = partes[1]
        data = partes[3]
        temperatura = partes[4]
        umidade = partes[5]
        luminosidade = partes[6]
        
        # Registrar a leitura de temperatura, umidade e luminosidade
        registrar_leitura(device, data, temperatura, umidade, luminosidade)

# Ordenar os intervalos pelas maiores diferenças de tempo
intervalos_temperatura_ordenados = sorted(intervalos_temperatura, key=lambda x: x['diferenca'], reverse=True)
intervalos_umidade_ordenados = sorted(intervalos_umidade, key=lambda x: x['diferenca'], reverse=True)
intervalos_luminosidade_ordenados = sorted(intervalos_luminosidade, key=lambda x: x['diferenca'], reverse=True)

# Exibir os 50 maiores intervalos
def exibir_resultados(intervalos_ordenados, tipo):
    print()
    print(f"50 maiores intervalos em que a {tipo} não mudou:")
    i = 1
    for intervalo in intervalos_ordenados[:50]:
        device = intervalo['device']
        valor = intervalo['valor']
        datainicial = intervalo['datainicial']
        datafinal = intervalo['datafinal']
        diferenca = intervalo['diferenca']
        
        print(f"Dispositivo {i}: {device}")
        print(f"  {tipo.capitalize()}: {valor}")
        #print(f"  Data Inicial: {datainicial}")
        #print(f"  Data Final: {datafinal}")
        print(f"  Duração: {diferenca}")
        i += 1

# Exibindo os resultados
exibir_resultados(intervalos_temperatura_ordenados, "temperatura")
exibir_resultados(intervalos_umidade_ordenados, "umidade")
exibir_resultados(intervalos_luminosidade_ordenados, "luminosidade")