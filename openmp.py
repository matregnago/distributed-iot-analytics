import subprocess
import os

def compile_and_run_c_program(source_file, output_file, input_argument, compiler="gcc", numero_threads=1):
    if not os.path.exists(source_file):
        print(f"Erro: O arquivo-fonte '{source_file}' não foi encontrado.")
        return

    if not os.path.exists(input_argument):
        print(f"Erro: O arquivo de argumento '{input_argument}' não foi encontrado.")
        return
    
    subprocess.run(f"export NUM_THREADS={numero_threads}", shell=True)

    compile_command = [compiler, "-fopenmp", "-o", output_file, source_file]

    print(f"Compilando o programa '{source_file}'...")
    try:
        subprocess.run(compile_command, check=True)
        print("Compilação bem-sucedida!")
    except subprocess.CalledProcessError as e:
        print(f"Erro na compilação: {e}")
        return

    run_command = [f"./{output_file}", input_argument]

    print(f"Executando o programa '{output_file}' com o argumento '{input_argument}'...")
    try:
        result = subprocess.run(run_command, check=True, text=True, capture_output=True)
        print("Saída do programa:")
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Erro na execução: {e}")
        print("Saída de erro:")
        print(e.stderr)


def openmp_paralel(n_threads):
    
    source_file = "openmp.c"       
    output_file = "openmp"         
    input_argument = "devices.csv" 
    numero_threads = int(n_threads)             

    resposta = compile_and_run_c_program(source_file, output_file, input_argument, numero_threads=numero_threads)
    return resposta