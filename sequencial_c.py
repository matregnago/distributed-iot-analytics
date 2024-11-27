import subprocess
import os

def compile_and_run_c_program(source_file, output_file, input_argument, compiler="gcc"):
    """
    Compila e executa um programa C usando Python.

    :param source_file: Arquivo-fonte C (ex: "openmp.c").
    :param output_file: Nome do executável gerado (ex: "openmp_exec").
    :param input_argument: Argumento necessário na execução (ex: "devices.csv").
    :param compiler: Compilador a ser usado (default: gcc).
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

    print(f"Compilando o programa '{source_file}'...")
    try:
        # Compila o programa
        subprocess.run(compile_command, check=True)
        print("Compilação bem-sucedida!")
    except subprocess.CalledProcessError as e:
        print(f"Erro na compilação: {e}")
        return

    # Configura a variável de ambiente para o número de threads
    env = os.environ.copy()
    # Comando de execução
    run_command = [f"./{output_file}", input_argument]

    print(f"Executando o programa '{output_file}' com o argumento '{input_argument}'...")
    try:
        # Executa o programa com o número de threads definido
        result = subprocess.run(run_command, check=True, text=True, capture_output=True, env=env)
        print("Saída do programa:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Erro na execução: {e}")
        print("Saída de erro:")
        print(e.stderr)


if __name__ == "__main__":
    # Defina os nomes dos arquivos e argumento
    source_file = "sequencial.c"       # Nome do arquivo-fonte em C
    output_file = "sequencial"         # Nome do executável gerado
    input_argument = "devices.csv"     # Nome do arquivo CSV passado como argumento

    # Chama a função para compilar e executar
    compile_and_run_c_program(source_file, output_file, input_argument)