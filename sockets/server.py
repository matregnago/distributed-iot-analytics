import socket
import pickle

host = "localhost"
port = 7270
data_payload = 2048

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((host, port))
sock.listen(1)

print(f'[Server]: Servidor iniciado em {host}:{port}')

try:
    while True:
        client, address = sock.accept()
        print(f"[Server]: Conexão aceita de {address}")

        with open("output.csv", 'w', newline='') as csv_output_file:
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
                    msg = "Resultado Sequencial Teste do srv"
                    client.send(msg.encode())
                case 2:
                    msg = "Resultado Openmp Teste do srv"
                    client.send(msg.encode())
                case 3:
                    msg = "Resultado MultiProc Teste do srv"
                    client.send(msg.encode())
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

except KeyboardInterrupt:
    print("Interrupção do servidor pelo teclado!")
finally:
    print("[Server]: Encerrando o servidor.")
    sock.close()
