import subprocess
import os

def compile_and_run_c_program(source_file, output_file, input_argument, compiler="gcc"):
    if not os.path.exists(source_file):
        print(f"Erro: O arquivo-fonte '{source_file}' não foi encontrado.")
        return

    if not os.path.exists(input_argument):
        print(f"Erro: O arquivo de argumento '{input_argument}' não foi encontrado.")
        return

    compile_command = [compiler, "-fopenmp", "-o", output_file, source_file]

    try:
        subprocess.run(compile_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro na compilação: {e}")
        return

    env = os.environ.copy()
    run_command = [f"./{output_file}", input_argument]

    try:
        result = subprocess.run(run_command, check=True, text=True, capture_output=True, env=env)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Erro na execução: {e}")
        print("Saída de erro:")
        print(e.stderr)


def sequencial_c():
    source_file = "sequencial.c"      
    output_file = "sequencial"       
    input_argument = "dados_recebidos.csv"
    
    resposta = compile_and_run_c_program(source_file, output_file, input_argument)
    return resposta
