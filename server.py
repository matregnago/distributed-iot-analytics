import socket
import pickle
from sequencial import sequencial
from multiproc import multiproc
from daskk import dask_parallel
from openmp import openmp_paralel
from sequencial_c import sequencial_c
host = "localhost"
port = 7270
data_payload = 4096


def enviar_string(string, client):       
    for i in range(0, len(string), data_payload):
        client.send(string[i:i+data_payload].encode())

def start_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(1)
    print(f'[Server]: Servidor iniciado em {host}:{port}')
    return sock

def handle_client(client, address):
    
        # Receber arquivo CSV
        with open("dados_recebidos.csv", 'wb') as csv_output_file:
            print("[Server]: Iniciando recepção do arquivo...")
            while True:
                data = client.recv(data_payload)
                if not data:  # Conexão encerrada
                    break
                if b"EOF" in data:  # Detecta o marcador de fim no bloco
                    # Escreve tudo antes do EOF
                    csv_output_file.write(data.replace(b"EOF", b""))
                    print("[Server]: Final do arquivo recebido.")
                    break
                csv_output_file.write(data)  # Escreve os dados recebidos no arquivo
            
            print("[Server]: Arquivo recebido com sucesso!")

        # Enviar confirmação ao cliente
        success_message = "[Server]: Arquivo recebido com sucesso!"
        client.send(success_message.encode())

        # Continuar com a lógica adicional do servidor
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
                    msg = "Resultado MPI Teste do srv"
                    client.send(msg.encode())
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
