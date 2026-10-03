# Ingestão no Limite — Pipeline de Ingestão de Alta Performance

Solução para o desafio de Engenharia de Dados **[Ingestão no Limite](https://github.com/mpraes/ingestao_no_limite)**.

## 🚀 Destaques da Arquitetura & Performance

- **Streaming Puro de I/O**: Descompressão e leitura direta dos arquivos `.zip` em memória (RFC 4180 / Latin-1 → UTF-8) sem escrita prévia no disco.
- **Tabela UNLOGGED & COPY Stream**: Uso do protocolo `COPY FROM STDIN` via `psycopg2.cursor.copy_expert` para throughput máximo com impacto mínimo em WAL.
- **Padrão ELT Estrito**: Carga de 100% dos dados brutos com derivação e enriquecimento em memória das 6 colunas de negócio.
- **Pico de Memória RAM**: **15.44 MiB** (limite: 1.024 MiB — apenas 1,5% do teto).
- **Throughput Sustentado**: **~200.000 linhas/segundo** (~10.5x mais rápido que o mínimo exigido).
- **Data Quality**: **0 erros** nos 13 Gates DQ da competição.

## 📁 Estrutura do Repositório

```
.
├── Dockerfile              # Container de execução oficial
├── requirements.txt        # Dependências mínimas de runtime
├── participante.json       # Metadados de identificação
└── src/
    ├── __init__.py
    ├── loader.py           # DDL otimizado e serializador TSV de alta velocidade
    ├── reader.py           # Leitor streaming de arquivos .zip (RFC 4180)
    ├── transformer.py      # Transformação e derivação das 13 colunas de negócio
    └── main.py             # Entrypoint da orquestração
```

## 🛠️ Execução Local

```bash
docker run --rm \
  --cpus="2.0" --memory="1g" --memory-swap="1g" \
  -v /caminho/para/zips:/data:ro \
  -e PARTICIPANTE=jpdagostin \
  -e PG_TABLE=jpdagostin_empresas \
  -e PG_HOST=postgres_db \
  -e PG_PORT=5432 \
  -e PG_USER=homelab_postgres \
  -e PG_PASSWORD=sua_senha \
  -e PG_DB=db_empresas \
  submissao_jpdagostin
```
