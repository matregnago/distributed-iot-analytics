import subprocess
import os

def compile_and_run_c_program(source_file, output_file, input_argument, compiler="gcc", numero_threads=4):
    """
    Compila e executa um programa C usando Python.

    :param source_file: Arquivo-fonte C (ex: "openmp.c").
    :param output_file: Nome do executável gerado (ex: "openmp_exec").
    :param input_argument: Argumento necessário na execução (ex: "devices.csv").
    :param compiler: Compilador a ser usado (default: gcc).
    :param numero_threads: Número de threads a serem usadas.
    """
    # Verifica se o arquivo-fonte existe
    if not os.path.exists(source_file):
        print(f"Erro: O arquivo-fonte '{source_file}' não foi encontrado.")
        return

    # Verifica se o arquivo de argumento existe
    if not os.path.exists(input_argument):
        print(f"Erro: O arquivo de argumento '{input_argument}' não foi encontrado.")
        return

    # Comando de compilação
    compile_command = [compiler, "-fopenmp", "-o", output_file, source_file]

    try:
        # Compila o programa
        subprocess.run(compile_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro na compilação: {e}")
        return

    # Configura a variável de ambiente para o número de threads
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(numero_threads)

    # Comando de execução
    run_command = [f"./{output_file}", input_argument]

    try:
        # Executa o programa com o número de threads definido
        result = subprocess.run(run_command, check=True, text=True, capture_output=True, env=env)
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Erro na execução: {e}")
        print("Saída de erro:")
        print(e.stderr)


def openmp_paralel(n_threads):
    # Defina os nomes dos arquivos e argumento
    source_file = "openmp.c"       # Nome do arquivo-fonte em C
    output_file = "openmp"         # Nome do executável gerado
    input_argument = "devices.csv" # Nome do arquivo CSV passado como argumento
    numero_threads = int(n_threads)             # Número de threads a serem usadas

    # Chama a função para compilar e executar
    resposta = compile_and_run_c_program(source_file, output_file, input_argument, numero_threads=numero_threads)
    return resposta