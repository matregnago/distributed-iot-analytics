# Cliente-Servidor para Processamento de Dados IoT

## Objetivo

O objetivo deste projeto é desenvolver uma aplicação distribuída que permita o envio e processamento paralelo de dados coletados por sensores em um sistema IoT para cidades inteligentes. O sistema envolve um cliente que envia arquivos CSV com leituras de sensores para um servidor remoto, que os processa e retorna as informações solicitadas. O processamento inclui a identificação de "travamentos" nas leituras dos sensores, isto é, períodos em que os valores de temperatura, umidade ou luminosidade não variam durante um certo intervalo de tempo.

### Descrição do fluxo do algoritmo

1. **Conexão Cliente-Servidor:**
   - O cliente se conecta ao servidor.

2. **Envio do Arquivo CSV:**
   - O cliente seleciona e envia um arquivo CSV contendo as leituras de sensores ao servidor.

3. **Recebimento do CSV**
   - O servidor realiza o recebimetno do CSV encaminhado pelo cliente e gera um arquivo para depois conseguir realizar o processamento das informações

3. **Seleção do Método de Processamento:**
   - O cliente escolhe qual tipo de metodo ele deseja para visualizar o processamento do CSV encaminhado.

3. **Processamento no Servidor:**
   - O servidor rprocessa os dados paralelamente, buscando identificar os maiores intervalos de "travamento" nas leituras de temperatura, umidade e luminosidade e retorna essa inforação ao cliente.

## Tecnologias Utilizadas

- **Linguagens:** Python (para o cliente, servidor, mpi, multiprocessing, dask, sequencial) e C (OpenMP e sequencial)
- **Protocolos de Comunicação:** Sockets TCP


## Instalação das bibliotecas

```bash
pip install "dask[complete]" mpi4py
```

## Como Rodar o Projeto

### 1. Inicie o servidor:

```bash
python3 server.py
```

### 2. Inicie o cliente:

```bash
python3 client.py
```
### 3. Siga os passos demonstrados no cliente:
Deverá informar o CSV e após poderá selecionar o metodo que deseja visualizar as informações juntamente com o numero de threads (caso tenha no metodo selecionado).

### 4. Arquivo CSV
Os arquivos CSV devem seguir o formato padrão, contendo dados separados por "|", como:

```bash
id|device|contagem|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc|latitude|longitude
```

As datas devem seguir conforme o seguinte exemplo:
```bash
"2022-12-20 12:53:04.238644"
```
Para o desenvolvimento do trabalho, foi utilizada a base de dados da [City Living Lab](https://www.citylivinglab.com/iot-inova-rs).


## Exemplo de Resultado

```bash
Top 50 maiores intervalos para luminosidade:
1 Travamento
 Dispositivo: sirrosteste_UCS_AMV-04
 Valor: 0.00
 Início: 2024-04-27 20:10:07.191
 Fim: 2024-06-27 18:02:23.056
 Duração: 5262735.87 segundos
2 Travamento
 Dispositivo: sirrosteste_UCS_AMV-10
 Valor: 0.00
 Início: 2024-05-12 17:29:40.027
 Fim: 2024-06-27 07:04:49.230
 Duração: 3936909.20 segundos
3 Travamento
 Dispositivo: sirrosteste_UCS_AMV-22
 Valor: -1.00
 Início: 2024-07-29 23:03:46.970
 Fim: 2024-08-30 13:24:44.267
 Duração: 2730057.30 segundos
4 Travamento
 Dispositivo: sirrosteste_UCS_AMV-14
 Valor: 0.80
 Início: 2023-03-06 07:00:18.794
 Fim: 2023-04-04 16:41:08.917
 Duração: 2540450.12 segundos
5 Travamento
 Dispositivo: sirrosteste_UCS_AMV-17
 Valor: -1.00
 Início: 2024-03-22 08:28:55.881
 Fim: 2024-04-17 08:31:03.479
 Duração: 2246527.60 segundos
 ...
```

## Contribuindo

Se você quiser contribuir para o projeto, por favor, faça um fork e envie pull requests.
