import socket
import pickle
from sequencial import sequencial
from multiproc import multiproc

host = "localhost"
port = 7270
data_payload = 4096

def enviar_string(string, client):
    client.sendall(str(len(string)).encode('utf-8') + b'\n')            
    for i in range(0, len(string), data_payload):
        client.sendall(string[i:i+data_payload].encode('utf-8'))

def start_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(1)
    print(f'[Server]: Servidor iniciado em {host}:{port}')
    return sock

def handle_client(client, address):
    with open("dados_recebidos.csv", 'w', newline='') as csv_output_file:
        while True:
            data = client.recv(data_payload).decode()
            if data == "EOF":  # Verifica se recebeu o marcador de fim
                break
            if not data:
                break
            csv_output_file.write(data)
        
    success_message = "[Server]: Arquivo recebido com sucesso!"
    print(success_message)
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
                msg = "Resultado Openmp Teste do srv"
                client.send(msg.encode())
            case 3:
                res_multiproc = multiproc(n_threads)
                enviar_string(res_multiproc, client)
            case 4:
                msg = "Resultado MPI Teste do srv"
                client.send(msg.encode())
            case 5:
                msg = "Resultado Dask Teste do srv"
                client.send(msg.encode())
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
