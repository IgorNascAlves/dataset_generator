import json
import random
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from groq import Groq

# Carrega as variáveis do arquivo .env
load_dotenv()

# Inicialize o cliente da API com a chave carregada do .env
client = Groq(
    api_key=os.getenv("GROQ_API_KEY"),  # Obtém a chave da variável de ambiente
)
# Set random seed for reproducibility
random.seed(0)

# Define a function to generate random dates within a given range
def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))

# função para gerar dados longos com o modelo LLM quebrando em diferentes requests
def llm_generator_long(col_range, num_rows, data_depente=None, rerun=None):
    context = col_range[1]
    qtd_rows = 0
    final_result = []
    num_rows_final = num_rows

    while qtd_rows < num_rows_final:
        #pedir de 10 em 10
        num_rows = 10
        prompt = f'''
        Gere {num_rows} linhas de dados de teste para a coluna com o seguinte contexto: {context}.
        Retorne a resposta **exatamente** no formato de lista JSON de strings, sem explicações ou textos adicionais. Exemplo de retorno: ["string1", "string2", "string3"].
        '''
        if data_depente is not None:
            prompt = f'''
            Gere {num_rows} linhas de dados de teste para a coluna com o seguinte contexto: {context}.
            A coluna dependente é: {data_depente[qtd_rows:]}.
            Retorne a resposta **exatamente** no formato de lista JSON de strings, sem explicações ou textos adicionais. Exemplo de retorno: ["string1", "string2", "string3"].
            '''
        if rerun is not None:
            prompt += rerun[0] + rerun[1]
        # Faz uma solicitação para gerar o texto com base no prompt
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,  # Envia o prompt para a API
                }
            ],
            # model="llama3-8b-8192",  # Ou outro modelo disponível
            model="llama3-70b-8192"
        )
        generated_data = chat_completion.choices[0].message.content.strip()
        # Tenta transformar o resultado em uma lista JSON. Se falhar, processa a string.
        try:
            # Converte a resposta em uma lista se estiver no formato correto
            result = json.loads(generated_data)
        except json.JSONDecodeError:
            # Se o resultado não estiver em JSON, tentamos extrair as strings manualmente
            result = generated_data.split("\n")  # Separar por linhas

        qtd_rows = len(result)
        print("prompt: ", prompt)

        final_result += result

    print(len(final_result))

    if len(final_result) > num_rows_final:
        return final_result[:num_rows_final]

    return final_result


def llm_generator(col_range, num_rows, data_depente=None, rerun=None):
    context = col_range[1]
    prompt = f'''
    Gere {num_rows} linhas de dados de teste para a coluna com o seguinte contexto: {context}.
    Retorne a resposta **exatamente** no formato de lista JSON de strings, sem explicações ou textos adicionais. Exemplo de retorno: ["string1", "string2", "string3"].
    '''
    if data_depente is not None:
        prompt = f'''
        Gere {num_rows} linhas de dados de teste para a coluna com o seguinte contexto: {context}.
        A coluna dependente é: {data_depente}.
        Retorne a resposta **exatamente** no formato de lista JSON de strings, sem explicações ou textos adicionais. Exemplo de retorno: ["string1", "string2", "string3"].
        '''
    if rerun is not None:
        prompt += rerun[0] + rerun[1]
    # Faz uma solicitação para gerar o texto com base no prompt
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,  # Envia o prompt para a API
            }
        ],
        # model="llama3-8b-8192",  # Ou outro modelo disponível
        model="llama3-70b-8192"
    )

    # Retorna o conteúdo gerado
    generated_data = chat_completion.choices[0].message.content.strip()

    # Tenta transformar o resultado em uma lista JSON. Se falhar, processa a string.
    try:
        # Converte a resposta em uma lista se estiver no formato correto
        result = json.loads(generated_data)
    except json.JSONDecodeError:
        # Se o resultado não estiver em JSON, tentamos extrair as strings manualmente
        result = generated_data.split("\n")  # Separar por linhas

    # check if the number of rows is correct
    if len(result) != num_rows:
        context_problem = [f"Tenho {len(result)} inves de {num_rows}.", 
                           f"arruma isso aqui {result}"]
        print(context_problem[0])
        print(context_problem[1])

        # se maior só pega o número de linhas correto
        if len(result) > num_rows:
            return result[:num_rows]

        return llm_generator_long(col_range, num_rows, data_depente=data_depente, rerun=context_problem)

    print(f"Generated data for {context}: {result}")
    print("prompt: ", prompt)

    return result

# Helper function to create random data based on column type and range
def generate_column_data(col_type, col_range, num_rows, data_depente=None):
    if col_type == "unique":
        return np.arange(1, num_rows + 1)  # Unique values for primary keys
    if col_type == "integer":
        if col_range == "None":
            return np.random.randint(100, 10000, size=num_rows)  # Random integer range for milhas/valores
        else:
            return np.random.randint(col_range[0], col_range[1] + 1, size=num_rows)
    
    elif col_type == "float":
        if col_range == "None":
            return np.round(np.random.uniform(100, 2000, size=num_rows), 2)  # Random float for valor_passagem/valor_venda
        else:
            return np.round(np.random.uniform(col_range[0], col_range[1], size=num_rows), 2)
        
    elif col_type == "string":
        list_of_strings = [string for string in col_range]
        if col_range[0] == "LLM":
            return llm_generator(col_range, num_rows, data_depente)
        return [random.choice(list_of_strings) for _ in range(num_rows)]
    
    elif col_type == "date":
        start_date = datetime.strptime(col_range[0], "%d/%m/%Y")
        end_date = datetime.strptime(col_range[1], "%d/%m/%Y")
        return [random_date(start_date, end_date) for _ in range(num_rows)]
    
# Function to create DataFrames from JSON
def create_table_from_json(json_data):
    tables = {}
    for table in json_data["data"]:
        table_name = table["table_name"]
        num_rows = table["number_of_rows"]
        data = {}
        list_col_empety = []
        
        for column in table["columns"]:
            col_name = column["column_name"]
            col_type = column["type"]
            col_range = column["range"] if column["range"] != "None" else "None"
            if column.get("unique", False):
                col_type = "unique"
            
            if column.get("depente", False):
                list_col_empety.append(column)
                #pular para o próximo loop
                continue

            data[col_name] = generate_column_data(col_type, col_range, num_rows)
        
        for column in list_col_empety:
            col_name = column["column_name"]
            col_type = column["type"]
            col_range = column["range"] if column["range"] != "None" else "None"
            col_name_depente = column["depente"]

            if column.get("unique", False):
                col_type = "unique"
            
            data[col_name] = generate_column_data(col_type, col_range, num_rows, data_depente=data[col_name_depente])

        tables[table_name] = pd.DataFrame(data)
        
    return tables

# Read the JSON file
# project_name = 'curso_3817'
project_name = 'curso_4167'
json_file_path = f'schemas/{project_name}.json'
with open(json_file_path, 'r') as file:
    json_data = json.load(file)

# Create the tables
tables = create_table_from_json(json_data)

# Directory to save CSV files
output_dir = f'data/{project_name}'
os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist

# Save each table as a CSV file using its table name
for table_name, df in tables.items():
    csv_file_path = os.path.join(output_dir, f"{table_name}.csv")
    df.to_csv(csv_file_path, index=False)
    print(f"Saved {table_name} to {csv_file_path}")
