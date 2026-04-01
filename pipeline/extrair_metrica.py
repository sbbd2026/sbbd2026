"""
extrair_metricas.py
Lê o log bruto do pipeline ETLT, imprime um relatório estruturado
no terminal e exporta CSVs com os resultados para a pasta resultados/.

Entrada:
    log_bruto.log — arquivo de log gerado pela execução do pipeline 
    
Saída (pasta resultados/):
    resumo_pipeline.csv         — métricas gerais do pipeline
    carga_por_uf.csv            — internações e tempo por UF
    dimensoes_pre_t2.csv        — registros por tabela antes do T2
    dimensoes_pos_t2.csv        — registros por tabela após o T2
    testes_aud1.csv             — resultados dos 203 testes da Auditoria 1
    testes_aud2.csv             — resultados dos testes da Auditoria 2
    relatorio_qualidade.txt     — relatório completo em texto
"""

import re
import csv
import sys
import argparse
from pathlib import Path


# ======================= CONFIGURAÇÃO =======================

LOG_PADRAO = Path(__file__).parent / "log_bruto.log"
SAIDA      = Path(__file__).parent / "resultados"

RE_TIMESTAMP    = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
RE_INTERNACOES  = re.compile(r'Fatos:\s+([\d,]+)\s+internações\s+\|\s+([\d,]+)\s+atendimentos')
RE_DIMENSAO_PRE = re.compile(r'pipeline_load\s+-\s+(\w+)\s{2,}([\d,]+)\s*$')
RE_TOTAL_T1     = re.compile(r'\[dbt (?:T1|Aud1)\].*Done\. PASS=(\d+) WARN=\d+ ERROR=(\d+) SKIP=\d+ NO-OP=\d+ TOTAL=(\d+)')
RE_TOTAL_T2     = re.compile(r'\[dbt (?:T2 test|Aud2 test)\].*Done\. PASS=(\d+) WARN=\d+ ERROR=(\d+) SKIP=\d+ NO-OP=\d+ TOTAL=(\d+)')
RE_FASE         = re.compile(r'\[dbt\] ((?:T1|Aud1|T2|Aud2)) [—–-]')
RE_PASS         = re.compile(r'\[dbt ((?:T\d|Aud\d))(?:\s+\w+)?\].*?(\d+) of (\d+) PASS\s+(\S+)')
RE_FAIL         = re.compile(r'\[dbt ((?:T\d|Aud\d))(?:\s+\w+)?\].*?(\d+) of (\d+) FAIL\s+(\d+)\s+(\S+)')
RE_CARGA_INI    = re.compile(r'=== PIPELINE')
RE_UF           = re.compile(r'pipeline_load\s+-\s+([A-Z]{2})\s+(\d+)\s+([\d,]+)\s+([\d,]+)\s+([\d.]+)s\s*$')
RE_DISCO        = re.compile(r'Disco:\s+([\d.]+)\s+MB')
RE_TEMPO_TOT    = re.compile(r'Tempo:\s+([\d.]+)s')
RE_TOTAL_LINHAS = re.compile(r'Total:\s+([\d,]+)\s+linhas')
RE_CONTAGEM_POS = re.compile(r'CONTAGEM FINAL DAS TABELAS')
RE_TABELA_POS   = re.compile(r'pipeline_load\s+-\s+([\w_]+)\s+([\d,]+)\s*$')

UNIVERSOS = {
    'internacoes':    197_312_203,
    'obitos':           8_501_763,
    'homens':          81_763_104,
    'uti_int_to':       1_096_836,
    'val_uti':         13_569_682,
    'cbor':                 2_813,
    'especialidade':           70,
    'procedimentos':        5_394,
    'cid':                 14_253,
    'nacionalidade':          332,
    'sexo':                     3,
}

TIPOS_ORDEM = ["nulidade", "unicidade", "relacionamento", "domínio", "regra de negócio"]


# ======================= HELPERS =======================

def limpar_numero(s):
    return s.replace(',', '').replace('s', '').strip()

def formatar_numero(n):
    return f"{n:,}".replace(',', '.') if n is not None else "0"

def get_universo(nome_teste, fase='T1'):
    n = nome_teste.lower()
    if 'insc_pn' in n:
        return UNIVERSOS['homens'] if fase == 'T1' else UNIVERSOS['internacoes']
    if 'morte_false_cid' in n or 'morte_cid_morte' in n:  return UNIVERSOS['obitos']
    if 'uti_int_to_dias' in n or 'uti_int_to_marca' in n: return UNIVERSOS['uti_int_to']
    if 'val_uti' in n:                                    return UNIVERSOS['val_uti']
    if 'source_unique' in n or 'unique_stg' in n:
        for chave in ('cbor', 'especialidade', 'procedimentos', 'cid', 'nacionalidade', 'sexo'):
            if chave in n:
                return UNIVERSOS[chave]
    return UNIVERSOS['internacoes']

def calcular_pct(nome_teste, n_erros, fase='T1'):
    universo = get_universo(nome_teste, fase)
    if universo == 0: return 'N/A', 0
    return f"{(n_erros / universo) * 100:.4f}%", universo

def classificar_teste(nome):
    if 'not_null_' in nome:      return 'nulidade'
    if 'unique_' in nome:        return 'unicidade'
    if 'relationships_' in nome: return 'relacionamento'
    if 'accepted_' in nome:      return 'domínio'
    return 'regra de negócio'


# ======================= PARSER =======================

def encontrar_ultima_execucao(linhas):
    ultimo_idx = 0
    for i, linha in enumerate(linhas):
        if RE_CARGA_INI.search(linha):
            ultimo_idx = i
    return linhas[ultimo_idx:]

def parsear_log(caminho):
    try:
        texto = caminho.read_text(encoding='utf-8', errors='replace')
    except Exception as e:
        print(f"Erro ao ler arquivo: {e}")
        return None

    linhas = texto.splitlines()
    linhas = encontrar_ultima_execucao(linhas)

    r = {
        'data_execucao':      None,
        'total_internacoes':  0,
        'total_atendimentos': 0,
        'disco_mb':           0.0,
        'tempo_segundos':     0.0,
        'total_linhas':       0,
        'dimensoes_pre':      {},
        'dimensoes_pos':      {},
        'carga_uf':           [],
        'T1': {'testes': [], 'pass': 0, 'error': 0, 'total': 0},
        'T2': {'testes': [], 'pass': 0, 'error': 0, 'total': 0},
    }

    fase_atual      = None
    em_bloco_uf     = False
    em_bloco_pre    = False
    em_bloco_pos    = False
    ufs_vistas      = set()

    # tabelas conhecidas para bloco pré T2
    TABELAS_CONHECIDAS = {
        'municipios','cid','procedimentos','especialidade','nacionalidade',
        'instrucao','raca_cor','sexo','vincprev','contraceptivos','car_int',
        'cbor','marca_uti','etnia','complexidade','tempo','hospital','socioeconomico'
    }

    for linha in linhas:

        if r['data_execucao'] is None:
            m = RE_TIMESTAMP.match(linha)
            if m: r['data_execucao'] = m.group(1)

        if m := RE_INTERNACOES.search(linha):
            r['total_internacoes']  = int(limpar_numero(m.group(1)))
            r['total_atendimentos'] = int(limpar_numero(m.group(2)))

        if m := RE_DISCO.search(linha):        r['disco_mb']       = float(m.group(1))
        if m := RE_TEMPO_TOT.search(linha):    r['tempo_segundos'] = float(m.group(1))
        if m := RE_TOTAL_LINHAS.search(linha): r['total_linhas']   = int(limpar_numero(m.group(1)))

        # ======================= bloco pré T2 (dimensões iniciais) =======================
        if 'DIMENSÃO' in linha and 'REGISTROS' in linha:
            em_bloco_pre = True
            continue
        if em_bloco_pre and '-----' in linha:
            continue
        if em_bloco_pre:
            if 'UF    PARQUETS' in linha or 'Pós-processamento' in linha or 'CARGA' in linha:
                em_bloco_pre = False
            elif m := RE_DIMENSAO_PRE.search(linha):
                tabela = m.group(1)
                if tabela in TABELAS_CONHECIDAS:
                    r['dimensoes_pre'][tabela] = int(limpar_numero(m.group(2)))

        # ======================= bloco carga por UF =======================
        if 'UF    PARQUETS' in linha:
            em_bloco_uf = True
            continue
        if em_bloco_uf and 'Pós-processamento' in linha:
            em_bloco_uf = False
        if em_bloco_uf:
            if m := RE_UF.search(linha):
                uf = m.group(1)
                if uf not in ufs_vistas:
                    ufs_vistas.add(uf)
                    r['carga_uf'].append({
                        'uf':             uf,
                        'parquets':       int(limpar_numero(m.group(2))),
                        'internacoes':    int(limpar_numero(m.group(3))),
                        'atendimentos':   int(limpar_numero(m.group(4))),
                        'tempo_segundos': float(limpar_numero(m.group(5))),
                    })

        # ======================= bloco pós T2 (contagem final) =======================
        if RE_CONTAGEM_POS.search(linha):
            em_bloco_pos = True
            continue
        if em_bloco_pos:
            if 'TABELA' in linha and 'REGISTROS' in linha:
                continue
            if '-----' in linha:
                continue
            if m := RE_TABELA_POS.search(linha):
                tabela = m.group(1)
                if tabela not in ('TABELA',):
                    r['dimensoes_pos'][tabela] = int(limpar_numero(m.group(2)))
            elif linha.strip() and 'pipeline_load' not in linha:
                em_bloco_pos = False

        # ======================= fase dbt =======================
        if m := RE_FASE.search(linha):
            fase_atual = 'T1' if m.group(1) in ('T1', 'Aud1') else 'T2'

        if fase_atual:
            if m := RE_TOTAL_T1.search(linha):
                r['T1'].update({'pass': int(m.group(1)), 'error': int(m.group(2)), 'total': int(m.group(3))})
            elif m := RE_TOTAL_T2.search(linha):
                r['T2'].update({'pass': int(m.group(1)), 'error': int(m.group(2)), 'total': int(m.group(3))})
            elif m := RE_FAIL.search(linha):
                f_raw, _, _, n_err, nome = m.groups()
                f = 'T1' if f_raw in ('T1', 'Aud1') else 'T2'
                r[f]['testes'].append({'nome': nome, 'status': 'fail',
                                       'falhas': int(n_err), 'tipo': classificar_teste(nome)})
            elif m := RE_PASS.search(linha):
                f_raw, _, _, nome = m.groups()
                f = 'T1' if f_raw in ('T1', 'Aud1') else 'T2'
                r[f]['testes'].append({'nome': nome, 'status': 'pass',
                                       'falhas': 0, 'tipo': classificar_teste(nome)})

    # pós-processamento: adiciona internacoes e internacao_procedimento ao pré
    if r['total_internacoes']:
        r['dimensoes_pre']['internacoes']            = r['total_internacoes']
        r['dimensoes_pre']['internacao_procedimento'] = r['total_atendimentos']

    return r


# =======================RELATÓRIO =======================

def gerar_relatorio(r, out=sys.stdout):
    def w(texto=''): out.write(texto + '\n')
    sep = '=' * 95

    w(sep)
    w('  RELATÓRIO DE QUALIDADE — SIH/SUS')
    w(f"  Execução : {r.get('data_execucao', 'N/A')}")
    w(sep)

    # ======================= Tabela 1: dimensões pré T2 =======================
    if r['dimensoes_pre']:
        w(f"\n  DIMENSÕES — PRÉ T2 (carga inicial)")
        w(f"  {'TABELA':<30} {'REGISTROS':>15}")
        w(f"  {'-' * 47}")
        for nome, qtd in sorted(r['dimensoes_pre'].items()):
            w(f"  {nome:<30} {formatar_numero(qtd):>15}")
        w(f"  {'-' * 47}")

    # ======================= Tabela 2: estado final do banco pós T2 =======================
    if r['dimensoes_pos']:
        w(f"\n  ESTADO FINAL DO BANCO — PÓS T2 (com sentinelas e seeds)")
        w(f"  {'TABELA':<30} {'REGISTROS':>15}")
        w(f"  {'-' * 47}")
        for nome, qtd in sorted(r['dimensoes_pos'].items()):
            w(f"  {nome:<30} {formatar_numero(qtd):>15}")
        w(f"  {'-' * 47}")

    # ======================= carga por UF =======================
    if r['carga_uf']:
        w(f"\n  CARGA POR UF")
        w(f"  {'UF':<6} {'PARQUETS':>10} {'INTERNAÇÕES':>15} {'ATENDIMENTOS':>15} {'TEMPO (s)':>12}")
        w(f"  {'-' * 62}")
        for row in r['carga_uf']:
            w(f"  {row['uf']:<6} {row['parquets']:>10} "
              f"{formatar_numero(row['internacoes']):>15} "
              f"{formatar_numero(row['atendimentos']):>15} "
              f"{row['tempo_segundos']:>12.1f}")
        w(f"  {'-' * 62}")

    # ======================= resumo geral =======================
    w(f"\n  RESUMO GERAL")
    w(f"  Internações  : {formatar_numero(r['total_internacoes'])}")
    w(f"  Atendimentos : {formatar_numero(r['total_atendimentos'])}")
    if r['total_linhas']:
        w(f"  Total linhas : {formatar_numero(r['total_linhas'])}")
    if r['disco_mb']:
        w(f"  Disco        : {r['disco_mb']} MB")
    if r['tempo_segundos']:
        w(f"  Tempo total  : {r['tempo_segundos']}s ({r['tempo_segundos']/60:.1f} min)")

    # ======================= testes por fase =======================
    for fase in ('T1', 'T2'):
        d      = r[fase]
        falhas = [t for t in d['testes'] if t['status'] == 'fail']
        w(f"\n\n  [ {fase} ]")
        w(f"  PASS={d['pass']}  FAIL={d['error']}  TOTAL={d['total']}")
        if d['total'] > 0:
            w(f"  Taxa de sucesso : {d['pass'] / d['total'] * 100:.1f}%"
              f"   Taxa de falha : {d['error'] / d['total'] * 100:.1f}%")

        tipos = {t: {'pass': 0, 'fail': 0} for t in TIPOS_ORDEM}
        for t in d['testes']:
            tipo = t['tipo']
            if tipo not in tipos: tipos[tipo] = {'pass': 0, 'fail': 0}
            tipos[tipo]['pass' if t['status'] == 'pass' else 'fail'] += 1

        w(f"\n  {'TIPO DE TESTE':<30} {'PASS':>6}  {'FAIL':>6}  {'TOTAL':>6}  {'% SUCESSO':>10}")
        w(f"  {'-' * 65}")
        for tipo in TIPOS_ORDEM:
            c = tipos[tipo]
            total = c['pass'] + c['fail']
            if total == 0: continue
            pct = f"{c['pass'] / total * 100:.1f}%"
            w(f"  {tipo:<30} {c['pass']:>6}  {c['fail']:>6}  {total:>6}  {pct:>10}")

        if not falhas:
            w(f"\n  ✓ Nenhuma falha em {fase}.")
            continue

        w(f"\n  {'TESTE':<55} {'ERROS':>15}  {'UNIVERSO':>15}  {'% UNIVERSO':>12}")
        w(f"  {'-' * 100}")
        for t in sorted(falhas, key=lambda x: x['falhas'], reverse=True):
            pct, universo = calcular_pct(t['nome'], t['falhas'], fase)
            nome_exibido  = t['nome'][:52] + '...' if len(t['nome']) > 55 else t['nome']
            w(f"  {nome_exibido:<55} {formatar_numero(t['falhas']):>15}  "
              f"{formatar_numero(universo):>15}  {pct:>12}")
        w(f"  {'-' * 100}")

    w(f"\n{sep}\n")


# ======================= EXPORTAÇÃO CSV =======================

def salvar_csv(dados, campos, caminho):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        if isinstance(dados, list): w.writerows(dados)
        else:                       w.writerow(dados)
    n = len(dados) if isinstance(dados, list) else 1
    print(f"  ✔ {caminho.name:<40} ({n} registros)")

def exportar_csvs(r, saida):
    print(f"\nExportando CSVs para {saida}/")

    # resumo geral
    salvar_csv(
        {'data_execucao': r['data_execucao'], 'ufs': len(r['carga_uf']),
         'internacoes': r['total_internacoes'], 'atendimentos': r['total_atendimentos'],
         'total_linhas': r['total_linhas'], 'disco_mb': r['disco_mb'],
         'tempo_segundos': r['tempo_segundos']},
        ['data_execucao','ufs','internacoes','atendimentos',
         'total_linhas','disco_mb','tempo_segundos'],
        saida / 'resumo_pipeline.csv'
    )

    # carga por UF
    salvar_csv(
        r['carga_uf'],
        ['uf','parquets','internacoes','atendimentos','tempo_segundos'],
        saida / 'carga_por_uf.csv'
    )

    # dimensões pré T2
    pre = [{'tabela': k, 'registros': v} for k, v in sorted(r['dimensoes_pre'].items())]
    salvar_csv(pre, ['tabela','registros'], saida / 'dimensoes_pre_t2.csv')

    # dimensões pós T2
    pos = [{'tabela': k, 'registros': v} for k, v in sorted(r['dimensoes_pos'].items())]
    salvar_csv(pos, ['tabela','registros'], saida / 'dimensoes_pos_t2.csv')

    # testes Aud1 e Aud2
    for fase, nome_csv in [('T1','testes_aud1.csv'), ('T2','testes_aud2.csv')]:
        rows = []
        for t in r[fase]['testes']:
            pct, universo = calcular_pct(t['nome'], t['falhas'], fase)
            rows.append({'teste': t['nome'], 'tipo': t['tipo'], 'status': t['status'],
                         'falhas': t['falhas'], 'universo': universo, 'pct_universo': pct})
        salvar_csv(rows, ['teste','tipo','status','falhas','universo','pct_universo'],
                   saida / nome_csv)

    print(f"\nPronto! Todos os CSVs em: {saida.resolve()}/\n")


# ======================= MAIN =======================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analisa log do pipeline SIH/SUS')
    parser.add_argument('--log',          type=Path, default=LOG_PADRAO)
    parser.add_argument('--saida',        type=Path, default=SAIDA)
    parser.add_argument('--so-relatorio', action='store_true')
    parser.add_argument('--so-csv',       action='store_true')
    args = parser.parse_args()

    print("--- INICIANDO PROCESSAMENTO ---")

    if not args.log.exists():
        print(f"ERRO: Log não encontrado em {args.log.resolve()}")
        sys.exit(1)

    r = parsear_log(args.log)
    if r is None:
        sys.exit(1)

    if not args.so_csv:
        gerar_relatorio(r)

    if not args.so_relatorio:
        exportar_csvs(r, args.saida)

    # txt sempre gerado
    print("--- SALVANDO RELATÓRIO TXT ---")
    try:
        args.saida.mkdir(parents=True, exist_ok=True)
        caminho_txt = args.saida / 'relatorio_qualidade.txt'
        with open(caminho_txt, 'w', encoding='utf-8') as f:
            gerar_relatorio(r, out=f)
            f.flush()
        print(f"  ✔ relatorio_qualidade.txt — {caminho_txt.stat().st_size} bytes")
    except Exception as e:
        print(f"ERRO AO GRAVAR TXT: {e}")