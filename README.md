# Integração Multidimensional de Dados do SUS: Uma Abordagem ETLT com Modelagem Snowflake

> SBBD 2026
Este repositório disponibiliza os artefatos científicos do paper, organizados para garantir reprodutibilidade, rastreabilidade e navegação clara dos resultados. Correções pós-publicação estão disponíveis na [Errata](./ERRATA.md).
---

<div align="center">

| O que você procura                        | Onde encontrar                                                                         |
|-------------------------------------------|----------------------------------------------------------------------------------------|
| Documentação dos modelos, testes e lineage| [Documentação dbt](https://sbbd2026.github.io/sbbd2026/dbt_docs/)                      |
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
</div>

## Documentação dbt

A documentação interativa do projeto está disponível em:
[https://sbbd2026.github.io/sbbd2026/dbt_docs/](https://sbbd2026.github.io/sbbd2026/dbt_docs/)

A seguir estão as áreas da interface utilizadas nesta pesquisa:

![Página inicial da documentação dbt](https://raw.githubusercontent.com/sbbd2026/sbbd2026/main/docs/imagens/pagina_inicial_doc.png)

- **Dados Brutos (Sources):** contém as 20 tabelas do schema `main` — as fontes originais carregadas no DuckDB. As descrições das colunas e os testes declarativos (nulidade, unicidade,
relacionamento e domínio) são definidos nos arquivos `.yml`. Cada tabela documenta suas colunas com descrição, tipo e testes associados, conforme ilustrado abaixo para a tabela `main.internacoes`:

![Documentação da tabela main.internacoes](./docs/imagens/main_internacoes.png)

- **Projects — Regras de negócio `.sql` + Transformações T2:** contém os modelos `.sql` com as transformações do estágio T2, os testes customizados de regras de negócio em SQL e os testes declarativos `.yml` aplicados aos modelos `stg_*`.

- **Linhagem:** geração automática de um DAG com o fluxo completo dos dados, à esquerda as fontes brutas, ao centro as transformações T2 e à direita os testes realizados `.sql` em Aud2, conforme ilustrado abaixo:

![Lineage graph da stg_internacoes](./docs/imagens/linhagem_internacoes.png)



## Modelagem OLAP

O modelo adota o esquema **Snowflake**, implementado no DuckDB, composto por 20 tabelas: 2 tabelas fato, 1 bridge table, 16 tabelas de dimensão e 1 dimensão derivada.

As 16 tabelas de dimensão refletem a natureza dos microdados do SIH/RD, cujas variáveis são majoritariamente categóricas e codificadas, cada domínio é normalizado em uma tabela
própria com chave primária.

O modelo é enriquecido pela tabela fato `socioeconomico`, construída a partir de cinco fontes integradas: CNES (leitos e médicos), IBGE (população e PIB per capita), SIM
(óbitos infantis) e SINASC (nascidos vivos), consolidadas em granularidade município-ano.

Ao todo, o banco antes de **T2** totaliza **398.940.744 linhas** e pós **T2** **398.940.771**. Esse incremento de 27 registros decorre da inserção estratégica de metadados via `dbt seeds` (12 registros em cid_manuais, 7 em procedimentos_manuais) e inserção via `SQL` de 8 registros sentinela nas tabelas de domínio, garantindo a integridade referencial completa do modelo.

![Diagrama Snowflake](docs/modelagem/snowflake_schema.png)

## Qualidade dos Dados

A camada de qualidade é implementada com testes declarativos no dbt, organizados em duas auditorias: Aud1, executada sobre os dados brutos após a carga inicial, e Aud2, executada após as transformações do estágio T2. Os resultados completos de ambas as auditorias estão disponíveis em [testes_aud1.csv](./resultados/testes_aud1.csv) e [testes_aud2.csv](./resultados/testes_aud2.csv), contendo para cada teste: tipo, status (aprovado/reprovado), quantidade de registros com falha, universo de análise e percentual de erro sobre o universo.

O universo de análise representa o denominador utilizado para calcular o percentual de erro de cada teste. Ele varia conforme o contexto semântico do teste:

- **Testes sobre tabelas de dimensão:** utilizam o total de registros da própria dimensão. Por exemplo, o teste de unicidade sobre `DESCRICAO` da tabela `sexo` tem universo 3, pois a tabela possui apenas 3 registros.
- **Testes sobre a tabela fato:** utilizam o total de internações (197.312.203) como denominador padrão.
- **Testes sobre subconjuntos semânticos:** quando a regra de negócio se aplica apenas a um subconjunto da fato, o universo é restrito a esse grupo. Por exemplo, o teste `UTI_INT_TO / MARCA_UTI` considera apenas as internações onde houve uso de UTI, e não o total de internações.

A fórmula utilizada em todos os resultados e sua aplicação para o teste da tabela `sexo` são apresentadas a seguir:

<div align="center">
  <img src="docs/imagens/formula.png" alt="Fórmula de cálculo do percentual de erro" width="600">
</div>


Os resultados dos testes agrupados em Aud1 são apresentados a seguir:
<div align="center">

| Tipo de Teste    | Pass | Fail | Total | % Sucesso |
|:-----------------|-----:|-----:|------:|----------:|
| Nulidade         |   75 |    0 |    75 |      100% |
| Unicidade        |   28 |    6 |    34 |     82,4% |
| Relacionamento   |    6 |   26 |    32 |     18,8% |
| Domínio          |   41 |    9 |    50 |       82% |
| Regra de negócio |    6 |    6 |    12 |       50% |
| **Total**        | **156** | **47** | **203** | **76,85%** |
</div>

A baixa taxa de aprovação nos testes de relacionamento (18,8%) é diretamente explicada por registros cujo valor `0` não possui correspondência no dicionário do DATASUS: colunas como `RACA_COR`, `INSTRU`, `VINCPREV` e `CBOR`, preenchidas com `0` na fonte, não encontram correspondência nas tabelas de dimensão, gerando falhas de integridade referencial em massa. Os resultados individuais de cada teste, com o detalhamento por variável, universo de análise e percentual de erro, estão disponíveis em [testes_aud1.csv](resultados/testes_aud1.csv).

### Data Profiling

Após a identificação das falhas na Aud1, foi realizado data profiling via SQL diretamente no SGBD DBeaver para compreender a natureza de cada inconsistência antes da implementação das correções no estágio T2.

**Tabela `sexo`:** o profiling revelou que o dicionário do DATASUS registra dois códigos para o sexo feminino (`2` e `3`), enquanto os microdados do SIH/RD utilizam exclusivamente os códigos `1` e `3`. O código `2` nunca aparece nos registros de internação. A correção aplicada no T2 foi a remoção da linha correspondente ao código `2`.
```sql
SELECT 
    SEXO,
    DESCRICAO,
    COUNT(*) AS total
FROM sexo
GROUP BY SEXO, DESCRICAO
ORDER BY SEXO;
```

![Data Profiling - Tabela Sexo](docs/imagens/data_profiling_sexo.png)

**Tabela `procedimentos`:** o profiling identificou 153 casos de `NOME_PROC` duplicado para códigos `PROC_REA` distintos, limitação herdada da própria tabela SIGTAP do DATASUS. Neste caso, nenhuma correção foi aplicada, pois a duplicidade reflete a realidade da fonte oficial.
```sql
SELECT 
    NOME_PROC,
    COUNT(*) AS total_codigos,
    LIST(PROC_REA) AS codigos
FROM procedimentos
GROUP BY NOME_PROC
HAVING COUNT(*) > 1
ORDER BY total_codigos DESC;
```
![Data Profiling - Tabela Procedimentos](docs/imagens/data_profiling_procedimentos.png)

**Regra de negócio — `IDADE`:** as regras de negócio foram definidas a partir de uma análise exploratória dos dados. Uma amostra de 100.000 registros foi extraída do banco e submetida ao ydata profiling, que permitiu identificar variáveis semanticamente correlacionadas. A correlação negativa entre `NASC` e `IDADE`, quanto maior a idade, mais antiga a data de nascimento, é semanticamente esperada e confirmada pelo gráfico de interações abaixo:

![Correlação NASC x IDADE](docs/imagens/corr_idade_nasc.png)

A partir dessas correlações, foram criadas regras que verificam a coerência entre campos relacionados. No caso da `IDADE`, a regra verifica se o valor armazenado é consistente com a idade calculada a partir das datas `NASC` e `DT_INTER`. Na Aud1, 1.875.400 registros apresentaram divergência, evidenciando erros sistemáticos de preenchimento nos sistemas de origem. A correção aplicada no T2 recalculou o campo `IDADE` diretamente a partir das datas, eliminando a dependência do valor original. Na Aud2, o teste foi aprovado com zero falhas.
```sql
SELECT
    N_AIH,
    NASC,
    DT_INTER,
    IDADE AS idade_armazenada,
    DATE_DIFF('year', NASC, DT_INTER)
    - CASE
        WHEN MONTH(DT_INTER) < MONTH(NASC)
          OR (MONTH(DT_INTER) = MONTH(NASC) AND DAY(DT_INTER) < DAY(NASC))
        THEN 1 ELSE 0
      END AS idade_calculada
FROM {{ source('main', 'internacoes') }}
WHERE NASC IS NOT NULL
  AND IDADE != (
        DATE_DIFF('year', NASC, DT_INTER)
        - CASE
            WHEN MONTH(DT_INTER) < MONTH(NASC)
              OR (MONTH(DT_INTER) = MONTH(NASC) AND DAY(DT_INTER) < DAY(NASC))
            THEN 1 ELSE 0
          END
  )
```


 

### Estágio T2 — Transformações e Correções

Com base no data profiling, as correções semânticas foram implementadas no estágio T2 com SQL no dbt. Apenas os casos em que a correção era possível e rastreável foram tratados, limitações estruturais da fonte, como as duplicatas em `procedimentos`, foram documentadas mas não alteradas.

**Tabela `sexo`:** a linha correspondente ao código `2` foi removida com SQL no dbt, mantendo apenas os códigos `1` (Masculino) e `3` (Feminino), que são os únicos presentes nos microdados do SIH/RD. A tabela passou de 3 para 2 registros após o T2.

**Campo `IDADE`:** o campo foi recalculado diretamente a partir das datas `NASC` e `DT_INTER` com SQL no dbt, eliminando a dependência do valor original da fonte. Os 1.875.400 registros com divergência foram corrigidos e o teste foi aprovado com zero falhas na Aud2.

### Auditoria 2 (Aud2)

Após o T2, a Aud2 revalidou um subconjunto de 47 testes críticos sobre os dados transformados:

| Tipo de Teste    | Pass  | Fail  | Total | % Sucesso  |
|:-----------------|------:|------:|------:|-----------:|
| Unicidade        |     1 |     5 |     6 |     16,7%  |
| Relacionamento   |    26 |     0 |    26 |      100%  |
| Domínio          |     9 |     0 |     9 |      100%  |
| Regra de negócio |     3 |     3 |     6 |       50%  |
| **Total**        | **39** | **8** | **47** | **82,97%** |

Os testes de relacionamento e domínio atingiram 100% de aprovação após o T2, confirmando a efetividade das correções aplicadas. As falhas remanescentes concentram-se em testes de unicidade, que refletem limitações estruturais herdadas da fonte DATASUS, e em regras de negócio que permanecem como limitações conhecidas da fonte. Os resultados individuais de cada teste estão disponíveis em [testes_aud2.csv](./resultados/testes_aud2.csv).
