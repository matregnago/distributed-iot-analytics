import socket

host = "localhost"
port = 727
data_payload= 1024 

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((host, port))
sock.listen(1)

print(f'Servidor iniciado em {host}:{port}')

try:
    while True:
        client, address = sock.accept()
        print(f"Conexão aceita de {address}, recebendo arquivo...")
        
        with open("output.csv", 'w', newline='') as csv_output_file:
            while True:
                data = client.recv(data_payload).decode()
                if not data:
                    break
                print(f"Escrevendo: {data}")
                csv_output_file.write(data)
        print("Arquivo recebido com sucesso! Encerrando conexão.")
        client.close()
        break;
except KeyboardInterrupt:
    print("Interrupção do server pelo teclado!")
finally:
    sock.close()

