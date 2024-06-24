
# Validação DES-IF

## Descrição

Este projeto é um pacote que executa validações da DES-IF 3.1 conforme o anexo de erros e alertas disponível no site da ABRASF. O objetivo é automatizar a validação dos arquivos DES-IF, assegurando que estejam em conformidade com as regras estabelecidas. Para mais informações sobre as mensagens de erros e alertas, acesse o [anexo oficial](https://abrasf.org.br/biblioteca/arquivos-publicos/anexo-mensagens-de-erros-e-alertas-versao-3-1/viewdocument/17).

## Estrutura do Projeto

- `app/`
  - `config.py`: Arquivo de configuração do banco de dados.
  - `db.py`: Inicializa o banco de dados utilizando SQLAlchemy e PyMySQL.
  - `main.py`: Arquivo principal do projeto onde a validação é iniciada.
  - `teste.py`: Teste de leitura e extração de conteúdo do arquivo txt.
  - `models/`: Contém todos os modelos de tabelas do banco de dados.
  - `helpers/`: Contém ajudadores que auxiliam no código.
  - `classes/`: Contém todas as classes de validações, sendo a principal `validador_desif.py`.

## Instalação

1. Clone o repositório para sua máquina local:
   ```bash
   git clone https://github.com/wandersonbarradas/validacao_desif
   ```

2. Navegue até o diretório do projeto:
   ```bash
   cd validacao-desif
   ```

3. Instale o pacote em modo de desenvolvimento:
   ```bash
   pip install -e .
   ```

4. Crie um arquivo `.env` na raiz do projeto com as informações do banco de dados, exemplo:
   ```env
   DB_USERNAME=root
   DB_PASSWORD=
   DB_HOST=127.0.0.1
   DB_PORT=3306
   DB_DATABASE=api_desif
   ```

## Comandos Disponíveis

### Validar

Este comando executa a função de validação para realizar as validações necessárias.

```bash
validar
```

### Teste

Este comando verifica se o sistema está lendo o arquivo txt corretamente.

```bash
teste
```

## Estrutura do Projeto

- **app/**: Diretório principal contendo todo o projeto.
  - **config.py**: Configurações do banco de dados.
  - **db.py**: Inicializa o banco de dados utilizando SQLAlchemy e PyMySQL.
  - **main.py**: Arquivo principal que inicia a validação.
  - **teste.py**: Teste de leitura e extração de conteúdo do arquivo txt.
  - **models/**: Contém todos os modelos de tabelas do banco de dados.
  - **helpers/**: Contém ajudadores que auxiliam no código.
  - **classes/**: Contém todas as classes de validações, sendo a principal `validador_desif.py`.


Feito por Wanderson Barradas
