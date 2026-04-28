# cdr-analise

Importador local Python para carregar arquivos CDR zipados no MySQL.

---

## Pré-requisitos

- Python 3.11+
- MySQL 8.0+

---

## Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/steve108/cdr-analise.git
cd cdr-analise

# 2. Crie e ative um ambiente virtual
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Crie o arquivo de configuração
copy .env.example .env   # Windows
cp .env.example .env     # Linux / macOS
# Edite .env com suas credenciais MySQL e caminhos de diretório
```

---

## Banco de dados

Execute o script SQL no seu servidor MySQL para criar as tabelas e views:

```bash
mysql -h localhost -u seu_usuario -p nome_do_banco < schema.sql
```

---

## Configuração (.env)

| Variável | Descrição |
|---|---|
| `MYSQL_HOST` | Endereço do servidor MySQL |
| `MYSQL_PORT` | Porta (padrão: 3306) |
| `MYSQL_DATABASE` | Nome do banco de dados |
| `MYSQL_USER` | Usuário MySQL |
| `MYSQL_PASSWORD` | Senha MySQL |
| `CDR_INPUT_DIR` | Pasta onde ficam os ZIPs a importar |
| `CDR_PROCESSED_DIR` | ZIPs processados com sucesso são movidos aqui |
| `CDR_ERROR_DIR` | ZIPs com erro são movidos aqui |
| `CDR_TEMP_DIR` | Pasta temporária de extração (apagada automaticamente) |

---

## Execução

Coloque os arquivos `.zip` em `CDR_INPUT_DIR` e execute:

```bash
python import_cdr.py
```

O script processa todos os ZIPs encontrados na pasta em ordem alfabética.

---

## Comportamento em caso de interrupção

Se o processo for interrompido, basta executar novamente:

- ZIPs já importados com sucesso (`status = done`) são ignorados pelo hash SHA-256.
- CSVs já concluídos dentro de um lote em andamento são pulados.
- Linhas já inseridas são retomadas via `last_line_imported`; duplicatas são bloqueadas pelo índice único `raw_hash`.
- ZIPs em `CDR_ERROR_DIR` podem ser movidos de volta para `CDR_INPUT_DIR` para nova tentativa.

---

## Estrutura de arquivos

```
cdr-analise/
├── import_cdr.py        # Script principal de importação
├── schema.sql           # Criação das tabelas e views MySQL
├── requirements.txt     # Dependências Python
├── .env.example         # Modelo de configuração
└── data/                # Criado automaticamente na primeira execução
    ├── input/           # Coloque os ZIPs aqui
    ├── processed/       # ZIPs importados com sucesso
    ├── error/           # ZIPs com falha
    └── temp/            # Extração temporária
```

---

## Views disponíveis

| View | Descrição |
|---|---|
| `vw_cdr_monthly_by_line` | Consumo mensal por MSISDN (linha) |
| `vw_cdr_monthly_summary` | Resumo mensal consolidado de toda a base |
