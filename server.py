import socket
import pickle
from sequencial import sequencial
from multiproc import multiproc
from daskk import dask_parallel
from openmp import openmp_paralel
from sequencial_c import sequencial_c
import subprocess
import os

host = "localhost"
port = 7270
data_payload = 4096


def exec_mpi(n_threads):
    run_command = ["mpiexec", "-n", str(n_threads), "python3", "mpi.py"]
    try:
        result = subprocess.run(run_command, check=True, text=True, capture_output=True)
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Erro na execução: {e}")
        print("Saída de erro:")
        print(e.stderr)

def enviar_string(string, client):       
    # Envia primeiro o tamanho da string
    string_length = len(string)
    client.send(pickle.dumps(string_length))
    
    total_sent = 0  # Variável para controlar o envio
    while total_sent < string_length:
        sent = client.send(string[total_sent:total_sent + data_payload].encode())
        total_sent += sent  # Atualiza a quantidade de dados enviados

    client.send(b"EOF")  # Sinal de fim de transmissão
    print("[Server]: Dados enviados com sucesso!")


def start_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(1)
    print(f'[Server]: Servidor iniciado em {host}:{port}')
    return sock

def handle_client(client, address):
        with open("dados_recebidos.csv", 'wb') as csv_output_file:
            print("[Server]: Iniciando recepção do arquivo...")
            while True:
                data = client.recv(data_payload)
                if not data:  # Acabou o arquivo
                    break
                if b"EOF" in data:  # Detecta o marcador de fim no bloco
                    csv_output_file.write(data.replace(b"EOF", b""))
                    print("[Server]: Final do arquivo recebido.")
                    break
                csv_output_file.write(data)  # Escreve os dados recebidos no arquivo
            
            print("[Server]: Arquivo recebido com sucesso!")

        # Enviar confirmação ao cliente
        success_message = "[Server]: Arquivo recebido com sucesso!"
        client.send(success_message.encode())

        while True:
            data = client.recv(data_payload)
            if not data:
                print("[Server]: Conexão encerrada pelo cliente.")
                break 

            try:
                option, n_threads = pickle.loads(data)
                print(f"[Server]: Opção recebida: {option}, Threads: {n_threads}")
            except Exception as e:
                print(f"[Server]: Erro ao desserializar dados: {e}")
                break

            match option:
                case 1:
                    res_sequencial = sequencial()
                    enviar_string(res_sequencial, client)
                case 2:
                    res_sequencial_c = sequencial_c()
                    enviar_string(res_sequencial_c, client)
                case 3:
                    res_openmp = openmp_paralel(n_threads)
                    enviar_string(res_openmp, client)
                case 4:
                    res_multiproc = multiproc(n_threads)
                    enviar_string(res_multiproc, client)
                case 5:
                    res_mpi = exec_mpi(n_threads)
                    enviar_string(res_mpi, client)
                case 6:
                    res_dask = dask_parallel(n_threads)
                    enviar_string(res_dask, client)
                case _:
                    message = "[Server]: Encerrando conexão com o cliente."
                    client.send(message.encode())
                    client.close()
                    break  

if __name__ == '__main__':
    sock = start_server()

    try:
        while True:
            client, address = sock.accept()
            print(f"[Server]: Conexão aceita de {address}")
            handle_client(client, address)
    except KeyboardInterrupt:
        print("Interrupção do servidor pelo teclado!")
    finally:
        print("[Server]: Encerrando o servidor.")
        sock.close()
