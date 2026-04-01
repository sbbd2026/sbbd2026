# Integração Multidimensional de Dados do SUS: Uma Abordagem ETLT com Modelagem Snowflake

> SBBD 2026

Este repositório disponibiliza os artefatos científicos do paper, organizados para garantir reprodutibilidade, rastreabilidade e navegação clara dos resultados.

---


| O que você procura                        | Onde encontrar                                                                         |
|-------------------------------------------|----------------------------------------------------------------------------------------|
| Documentação dos modelos, testes e lineage| [Documentação dbt](https://sbbd2026.github.io/sbbd2026/dbt_docs/)                      |
| Correções pós-publicação                  | [Errata](./ERRATA.md)                                                                  |
| Diagrama do modelo OLAP                   | [Modelagem](./docs/modelagem/snowflake_schema.png)                                     |
| Dicionário de todas as tabelas            | [Dicionário de Dados](./docs/dicionario_dados.pdf)                                     |
| Log bruto do pipeline                     | [log_bruto.log](./pipeline/log_bruto.log)                                              |
| Script de extração de métricas            | [extrair_metricas.py](./pipeline/extrair_metricas.py)                                  |
| Resumo geral do pipeline                  | [resumo_pipeline.csv](./resultados/resumo_pipeline.csv)                                |
| Carga por UF                              | [carga_por_uf.csv](./resultados/carga_por_uf.csv)                                      |
| Dimensões pré T2                          | [dimensoes_pre_t2.csv](./resultados/dimensoes_pre_t2.csv)                              |
| Dimensões pós T2                          | [dimensoes_pos_t2.csv](./resultados/dimensoes_pos_t2.csv)                              |
| Testes de qualidade — Aud1                | [testes_aud1.csv](./resultados/testes_aud1.csv)                                        |
| Testes de qualidade — Aud2                | [testes_aud2.csv](./resultados/testes_aud2.csv)                                        |
| Relatório de qualidade completo           | [relatorio_qualidade.txt](./resultados/relatorio_qualidade.txt)                        |

## Modelagem OLAP

O modelo adota o esquema **Snowflake**, implementado no DuckDB, composto por 20 tabelas: 2 tabelas fato, 1 bridge table, 16 tabelas de dimensão e 1 dimensão derivada.

As 16 tabelas de dimensão refletem a natureza dos microdados do SIH/RD, cujas variáveis são majoritariamente categóricas e codificadas, cada domínio é normalizado em uma tabela
própria com chave primária, conforme exigido pelo vocabulário analítico do DATASUS.

O modelo é enriquecido pela tabela fato `socioeconomico`, construída a partir de cinco fontes integradas: CNES (leitos e médicos), IBGE (população e PIB per capita), SIM
(óbitos infantis) e SINASC (nascidos vivos), consolidadas em granularidade município-ano.
Ao todo, o banco antes de **T2** totaliza **398.940.744 linhas** e pós **T2** **398.940.771**.

![Diagrama Snowflake](./docs/modelagem/snowflake_schema.png)
