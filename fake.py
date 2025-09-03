import pandas as pd
import numpy as np
from faker import Faker
import random

# Initialize Faker for Brazilian Portuguese data and set seeds for reproducibility
fake = Faker('pt_BR')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Brazilian states and some real cities
estados_cidades = {
    'SP': ['São Paulo', 'Campinas', 'Santos', 'Sorocaba', 'Ribeirão Preto'],
    'RJ': ['Rio de Janeiro', 'Niterói', 'Petrópolis', 'Angra dos Reis', 'Volta Redonda'],
    'MG': ['Belo Horizonte', 'Uberlândia', 'Ouro Preto', 'Juiz de Fora', 'Contagem'],
    'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas', 'Santa Maria', 'Novo Hamburgo'],
    'BA': ['Salvador', 'Feira de Santana', 'Vitória da Conquista', 'Itabuna', 'Ilhéus'],
    'PR': ['Curitiba', 'Londrina', 'Maringá', 'Foz do Iguaçu', 'Ponta Grossa']
}

# Product categories and product names per category
categorias_produtos = ['Eletrônicos', 'Roupas', 'Livros', 'Brinquedos', 'Beleza', 'Alimentos']
produtos_por_categoria = {
    'Eletrônicos': ['Smartphone', 'Notebook', 'Fone de ouvido', 'Smartwatch', 'Câmera'],
    'Roupas': ['Camisa', 'Calça', 'Vestido', 'Tênis', 'Jaqueta'],
    'Livros': ['Romance', 'Ficção', 'Biografia', 'Autoajuda', 'Tecnologia'],
    'Brinquedos': ['Boneca', 'Carrinho', 'Quebra-cabeça', 'Lego', 'Pelúcia'],
    'Beleza': ['Perfume', 'Creme', 'Shampoo', 'Batom', 'Loção'],
    'Alimentos': ['Chocolate', 'Café', 'Azeite', 'Queijo', 'Suco']
}

# Number of rows to generate
# NOTE: 3000_000 equals 300.000 rows (underscore is just a visual separator).
# Generating this many rows with Faker can be very slow and memory-intensive.
num_linhas = 3000_000

# Generate data
data = []
for i in range(1, num_linhas + 1):
    # Randomly pick state and city
    estado = random.choice(list(estados_cidades.keys()))
    cidade = random.choice(estados_cidades[estado])

    # Randomly pick category and product within that category
    categoria = random.choice(categorias_produtos)
    produto = random.choice(produtos_por_categoria[categoria])

    # Quantity between 1 and 10
    quantidade = random.randint(1, 10)

    # 10% of transactions are returns (negative quantity)
    if random.random() < 0.1:
        quantidade *= -1

    # Unit price between 10 and 2000, rounded to 2 decimals
    preco_unitario = round(random.uniform(10, 2000), 2)

    # Discount between 0% and 30% of unit price, rounded to 2 decimals
    desconto = round(random.uniform(0, 0.3) * preco_unitario, 2)

    # Append a new record
    # NOTE: Faker calls (name, address, datetime) are relatively expensive operations.
    # Doing this in a tight loop for millions of rows will be slow.
    data.append([
        i,
        fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        fake.name(),      # Customer
        fake.name(),      # Seller
        produto,
        categoria,
        quantidade,
        preco_unitario,
        desconto,
        estado,
        cidade,
        fake.street_address()
    ])

# Create DataFrame from the generated list of rows
# NOTE: Holding millions of rows in memory as Python objects will consume a lot of RAM.
df = pd.DataFrame(data, columns=[
    'id_venda', 'data_hora', 'cliente', 'vendedor', 'produto', 'categoria',
    'quantidade', 'preco_unitario', 'desconto', 'estado', 'cidade', 'endereco'
])

# Save to CSV using ';' as separator (common in pt-BR locales)
# TIP: Consider specifying encoding='utf-8-sig' if opening in Excel on Windows.
df.to_csv('ecommerce_fake.csv', sep=';', index=False)
print("CSV successfully generated: ecommerce_fake.csv")