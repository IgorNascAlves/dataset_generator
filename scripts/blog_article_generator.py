#extract nome,categoria from file data\curso_4167\livros_mais_vendidos.csv

import pandas as pd

# Read the CSV file
csv_file_path = 'data/curso_4167/livros_mais_vendidos.csv'
df = pd.read_csv(csv_file_path)

# Extract the columns
df = df[['nome', 'categoria']]
# print(df.head())

from dotenv import load_dotenv
from groq import Groq
import os

# Carrega as variáveis do arquivo .env
load_dotenv()

# Inicialize o cliente da API com a chave carregada do .env
client = Groq(
    api_key=os.getenv("GROQ_API_KEY"),  # Obtém a chave da variável de ambiente
)

import random

# passe por cada categoria e gere um artigo para cada uma
for categoria in df['categoria'].unique():
    # categoria = random.choice(df['categoria'].unique())
    # print(f'Categoria escolhida: {categoria}')
    # filter livros by categoria
    livros = df[df['categoria'] == categoria]['nome'].unique()
    # print(f"quantidade de livros: {len(livros)}")
    prompt = f'''
            Você é um redator de um blog e precisa escrever um artigo sobre os livros.
            Comentando sobre os livros, sobre a categoria e quem iria gostar de ler.
            Gere 4 paragrafos de texto falando sobre o tema e os livros.
            Categoria dos livros: {categoria}.
            Livros disponiveis na busca: {livros}.
            '''
            
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
    # print(chat_completion.choices[0].message.content.strip())

    #criar pasta se não existir
    if not os.path.exists('data/curso_4167/artigos'):
        os.makedirs('data/curso_4167/artigos')

    #salvar o texto gerado em um arquivo txt
    file_name = f'data/curso_4167/artigos/artigo_{categoria}.txt'
    with open(file_name,  "w", encoding="utf-8") as f:
        f.write(chat_completion.choices[0].message.content.strip())
    print(f'Arquivo salvo: {file_name}')
