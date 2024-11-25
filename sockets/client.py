import socket
import pickle

data_payload = 2048
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
        with open(file_name, 'r') as csv_input_file:
                print(f"Iniciando envio do arquivo {file_name} ao servidor")
                for line in csv_input_file:
                    data = line
                    sock.send(data.encode())
                sock.send("EOF".encode())  # Envia o marcador indicando o fim do arquivo
                break;

    except FileNotFoundError:
        print(f"Arquivo {file_name} não encontrado. Verifique o caminho e tente novamente.")
        continue

msg_server_success = sock.recv(data_payload).decode()
print(msg_server_success)

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
            response = sock.recv(data_payload).decode()
            print(f"Resposta do servidor: {response}")
        else:
            print("Digite uma opção válida.")
            continue


except KeyboardInterrupt:
    print("Interrupção do cliente pelo teclado!")
finally:
    print("Encerrando a conexão com o servidor...")
    sock.close()
