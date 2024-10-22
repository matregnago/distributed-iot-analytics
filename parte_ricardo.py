import csv
from datetime import datetime

# Função para calcular a diferença entre datainicial e datafinal
def calcular_diferenca(datainicial, datafinal):
    formatos_data = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]  # Formatos com e sem microssegundos
    
    # Tentar primeiro com microssegundos, depois sem
    for formato in formatos_data:
        try:
            dt_inicial = datetime.strptime(datainicial, formato)
            dt_final = datetime.strptime(datafinal, formato)
            return dt_final - dt_inicial  # Retorna a diferença em timedelta
        except ValueError:
            continue
    
    # Tratamento para quando as strings não coincidirem em formato (com/sem microssegundos)
    try:
        # Converter datainicial e datafinal para o mesmo formato (adicionando .000000 caso não tenha microssegundos)
        if "." not in datainicial:
            datainicial += ".000000"
        if "." not in datafinal:
            datafinal += ".000000"
        
        dt_inicial = datetime.strptime(datainicial, "%Y-%m-%d %H:%M:%S.%f")
        dt_final = datetime.strptime(datafinal, "%Y-%m-%d %H:%M:%S.%f")
        return dt_final - dt_inicial  # Retorna a diferença em timedelta
    except ValueError:
        raise ValueError(f"Formato de data inválido para as datas: {datainicial} ou {datafinal}")

# Função para registrar e calcular intervalos
def registrar_leitura(device, data, temperatura):
    if device not in hash_map:
        # Primeira leitura do dispositivo
        hash_map[device] = {
            "temperatura": temperatura,
            "datainicial": data,
            "datafinal": data,
        }
    else:
        # O dispositivo já existe, verificar a temperatura
        if hash_map[device]['temperatura'] == temperatura:
            # A temperatura não mudou, atualiza a data final
            hash_map[device]['datafinal'] = data
        else:
            # A temperatura mudou, finalize o intervalo anterior
            diferenca = calcular_diferenca(hash_map[device]['datainicial'], hash_map[device]['datafinal'])
            # Armazena o intervalo finalizado
            intervalos.append({
                "device": device,
                "temperatura": hash_map[device]['temperatura'],
                "datainicial": hash_map[device]['datainicial'],
                "datafinal": hash_map[device]['datafinal'],
                "diferenca": diferenca
            })
            
            # Reinicia o intervalo com a nova temperatura
            hash_map[device] = {
                "temperatura": temperatura,
                "datainicial": data,
                "datafinal": data,
            }

# Inicialização do hash_map e lista de intervalos
hash_map = {}
intervalos = []

# Caminho do arquivo CSV
caminho = './devices.csv'

# Leitura do arquivo CSV
with open(caminho, mode='r', newline='', encoding='utf-8') as arquivo_csv:
    leitor_csv = csv.reader(arquivo_csv)
    next(leitor_csv)  # Pula o cabeçalho
    for linha in leitor_csv:
        # Separar as partes da linha
        partes = linha[0].split('|')
        # Verificar se a linha está completa
        if not partes[1] or not partes[3] or not partes[4]:
            continue
            
        device = partes[1]
        data = partes[3]
        temperatura = partes[4]
        
        # Registrar a leitura de temperatura no dispositivo
        registrar_leitura(device, data, temperatura)

# Ordenar os intervalos pela maior diferença de tempo
intervalos_ordenados = sorted(intervalos, key=lambda x: x['diferenca'], reverse=True)

# Exibir os 50 maiores intervalos
print("50 maiores intervalos em que a temperatura não mudou:")


i=1
for intervalo in intervalos_ordenados[:50]:
    device = intervalo['device']
    temperatura = intervalo['temperatura']
    datainicial = intervalo['datainicial']
    datafinal = intervalo['datafinal']
    diferenca = intervalo['diferenca']
    
    print(f"Dispositivo {i}: {device}")
    print(f"  Temperatura: {temperatura}")
    print(f"  Data Inicial: {datainicial}")
    print(f"  Data Final: {datafinal}")
    print(f"  Duração: {diferenca}")
    i += 1