import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_address = ("localhost", 727)

print("Conectando-se ao servidor...")
sock.connect(server_address)

print("Conectado ao servidor. Enviando arquivo...")
try:
    with open("devices.csv") as csv_input_file:
        for line in csv_input_file:
            data = line
            # print(f"Enviando: {data}")
            sock.send(data.encode())  
    print("Arquivo enviado com sucesso! Encerrando conexão.")
except KeyboardInterrupt:
    print("Interrupção do cliente pelo teclado!")
finally:
    print("Encerrando a conexão com o servidor")
    sock.close()
