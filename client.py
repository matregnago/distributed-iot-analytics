import socket
import pickle

data_payload = 4096
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
address = "localhost"
port = 7270
server_address = (address, port)

print("Conectando-se ao servidor...")
sock.connect(server_address)

print(f"Conectado ao servidor {address}:{port}!")

while True:
    file_name = input("Digite o caminho para o arquivo CSV: ")
    try:
        with open(file_name, 'rb') as csv_input_file:  # Abrir em modo binário
            print(f"Iniciando envio do arquivo {file_name} ao servidor")

            while chunk := csv_input_file.read(data_payload):  # Ler o arquivo em blocos de 4 KB
                sock.sendall(chunk)  # Enviar cada bloco para o servidor
            
            sock.sendall(b"EOF")  # Enviar marcador indicando o fim do arquivo

            # Espera pela mensagem de sucesso do servidor
            msg_server_success = sock.recv(data_payload).decode()
            print(msg_server_success)  # Certifique-se de que a mensagem é recebida corretamente

            break

    except FileNotFoundError:
        print(f"Arquivo {file_name} não encontrado. Verifique o caminho e tente novamente.")
        continue


# Menu de interação após envio do arquivo
try:
    while True:
        print("\nMenu de paralelização")
        print("0. Encerrar conexão")
        print("1. Sequencial")
        print("2. OpenMP (C)")
        print("3. Multiprocessing")
        print("4. MPI")
        print("5. Dask")
        option = input("Digite a sua opção: ")

        try:
            option = int(option)
        except ValueError:
            print("Por favor, digite um número válido.")
            continue

        if option == 0:
            print("Encerrando a conexão com o servidor...")
            sock.close()
            print("Finalizando programa.")
            break
        elif option in [1, 2, 3, 4, 5]:
            n_threads = 0
            if option > 1:
                try:
                    n_threads = int(input("Digite o número de threads: "))
                except ValueError:
                    print("Número de threads inválido. Tente novamente.")
                    continue
            tupla_para_enviar = (option, n_threads)
            data = pickle.dumps(tupla_para_enviar)
            sock.sendall(data)

            # Receber e processar resposta do servidor
            tamanho = 28029
            received_string = b''
            while len(received_string) < tamanho:
                chunk = sock.recv(data_payload)
                received_string += chunk

            print(received_string.decode())
        else:
            print("Digite uma opção válida.")
            continue

except KeyboardInterrupt:
    print("Interrupção do cliente pelo teclado!")
finally:
    print("Encerrando a conexão com o servidor...")
    sock.close()
