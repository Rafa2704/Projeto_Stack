# Explicação do Código: Ingestão de Dados da Camada Silver para Gold

Este código Python representa um pipeline de ingestão de dados, que extrai informações da camada Silver (possivelmente armazenada em um servidor MinIO) e carrega esses dados transformados na camada Gold. O processo envolve a manipulação de DataFrames usando a biblioteca Pandas e a interação com sistemas de armazenamento em nuvem, como MinIO, além de transferência para um banco de dados MySQL.

## Estrutura do Código

1. **Definição do DAG (Directed Acyclic Graph):**
   - Um DAG no Apache Airflow que define o fluxo de execução do pipeline.
   - Agendado para ser executado uma vez (`schedule_interval="@once"`).

2. **Extração de Dados da Camada Silver para DataFrames Pandas:**
   - O código utiliza o MinIO para acessar objetos na camada Silver e carregar esses dados em DataFrames Pandas.
   - Dados extraídos incluem informações sobre produtos, pedidos, itens de pedido, pagamentos e clientes.

3. **Transformação de Dados (Data Cleaning e Renomeação de Colunas):**
   - Manipulação dos DataFrames Pandas para preparar os dados para carregamento na camada Gold.
   - Preenchimento de valores nulos, formatação de datas, tratamento de maiúsculas e renomeação de colunas para um formato padronizado.

4. **Carregamento dos Dados Transformados na Camada Gold:**
   - Os DataFrames Pandas transformados são escritos em arquivos Parquet temporários.
   - Esses arquivos Parquet são então carregados na camada Gold do MinIO, especificamente no diretório `gold/olist/vendas/`.

5. **Carga Adicional para o MySQL:**
   - Os dados transformados também são carregados em uma tabela MySQL chamada `TB_VENDAS` no banco de dados `BD_STACK`.

6. **Limpeza de Arquivos Temporários:**
   - Ao final do processo, há uma tarefa para limpar os arquivos temporários gerados durante a execução.

## Variáveis Configuráveis:

- As variáveis de conexão (como `data_lake_server`, `mysql_server`, etc.) são obtidas a partir do Airflow Variables.
- O acesso ao MinIO é configurado usando a biblioteca MinIO, e a conexão ao MySQL é estabelecida usando SQLAlchemy.

## Execução do Pipeline:

- O pipeline é definido para ser executado manualmente ou conforme agendamento.
- O DAG inclui três tarefas: extração para ouro (`extract_silver_to_gold`), carga no MySQL (`venda_to_mysql`), e limpeza de arquivos temporários (`clean_files_on_staging`).

Este código representa uma implementação de um processo de ingestão de dados utilizando as funcionalidades do Apache Airflow em conjunto com Pandas para manipulação e MinIO para armazenamento.
