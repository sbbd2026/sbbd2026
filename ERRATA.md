# Errata

**Artigo:** Integração Multidimensional de Dados do SUS: Uma Abordagem ETLT com Modelagem Snowflake  
**Evento:** SBBD 2026  
**Data da errata:** março de 2026  
**Tipo:** Erro de digitação

---

## Tabela 3 — Resultados dos testes de qualidade de dados (SIH/SUS)
```
+------------------+-------+------+-------+-----------+
| Tipo de Teste    | Pass  | Fail | Total | % Sucesso |
+------------------+-------+------+-------+-----------+
| nulidade         |    75 |    0 |    75 |      100% |
| unicidade        |    28 |    6 |    34 |     82,4% |
| relacionamento   |     6 |   26 |    32 |     18,8% |
| domínio          |    41 |    9 |    50 |       82% |
| regra de negócio |     6 |    6 |    12 |       50% |
+------------------+-------+------+-------+-----------+
| TOTAL            |   156 |   47 |   203 |   76,85%  |
+------------------+-------+------+-------+-----------+
```

## Tabela 4 — Amostra de testes de qualidade aplicados antes e após o tratamento (T2)
```
+-------------------------+-------------------------+-----------+-----------+
| Variável                | Tipo                    | Aud. 1 (%)| Aud. 2 (%)|
+-------------------------+-------------------------+-----------+-----------+
| MORTE / CID_MORTE       | Coerência semântica     |    0,0038 |         0 |
| IDADE / DT_INTER e NASC | Coerência semântica     |      0,94 |         0 |
| INSC_PN / SEXO          | Coerência semântica     |       100 |    0,0018 |
| UTI_INT_TO / DIAS_PERM  | Coerência semântica     |      1,04 |         0 |
+-------------------------+-------------------------+-----------+-----------+
```

> Os valores corretos estão refletidos nos dados disponibilizados neste repositório.
