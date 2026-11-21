"""
Série 3: throttling.

Dispara rajadas de chamadas simultâneas de tamanho crescente e conta, em cada rajada, quantas
chamadas a Lambda aceitou e quantas rejeitou por limite de concorrência (HTTP 429).

Diferenças em relação ao `invoke()` de tests/invoke_lambda.py (que fica intacto para as outras séries):
- cliente boto3 próprio, SEM retry: cada linha do log é exatamente uma tentativa. Com retry, o
  botocore repete o 429 sozinho, escondendo o throttle e somando o tempo de espera na latência;
- o resultado de cada chamada tem um `outcome` (ok, throttled, function_error, server_error,
  timeout, network) e o `reason` do throttle;
- `LogType='Tail'` traz a linha REPORT da invocação: `duration_ms` e `init_duration_ms` (cold start);
- as chamadas de uma rajada esperam numa barreira e saem juntas (`dispatch_offset_ms` mede o
  quanto o disparo se espalhou);
- uma rajada de aquecimento, descartada, abre antes todas as conexões HTTPS; o resumo de cada
  rajada mostra o quanto a chegada na AWS se espalhou (`received_at`) e avisa se passou de 200 ms.

Uso (da raiz do repo):
    python -m tests.series_throttle
    python -m tests.series_throttle --bursts 1,5,8,10,12,15,20,25,50,100,200,500 --rounds 3
"""
import argparse
import base64
import json
import os
import re
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    ConnectionError as BotoConnectionError,
    HTTPClientError,
    ReadTimeoutError,
)
from dotenv import load_dotenv

from tests.invoke_lambda import save_log

# Carrega o .env antes de qualquer cliente: região, nome da função e THROTTLE_MAX vêm dele
load_dotenv()

DEFAULT_BURSTS = [1, 5, 10, 25, 50, 100, 200, 500]
DEFAULT_PAUSE_S = 30
LOG_PATH = 'results/raw.jsonl'

# Linha REPORT do log (texto) e o equivalente no formato JSON de log da Lambda
_REPORT_TEXT = {
    'duration_ms': re.compile(r'\tDuration: ([\d.]+) ms'),
    'billed_duration_ms': re.compile(r'Billed Duration: ([\d.]+) ms'),
    'init_duration_ms': re.compile(r'Init Duration: ([\d.]+) ms'),
}
_REPORT_JSON = {
    'duration_ms': 'durationMs',
    'billed_duration_ms': 'billedDurationMs',
    'init_duration_ms': 'initDurationMs',
}


# ---------------------------------------------------------------------------
# Invocação (uma tentativa por chamada)
# ---------------------------------------------------------------------------

def make_client(region: str, max_pool: int):
    """Cliente Lambda sem retry, com timeouts explícitos e pool do tamanho da maior rajada."""
    return boto3.client(
        'lambda',
        region_name=region,
        config=Config(
            retries={'mode': 'standard', 'total_max_attempts': 1},
            connect_timeout=5,
            read_timeout=30,
            max_pool_connections=max(10, max_pool),
        ),
    )


def parse_report(log_result_b64) -> dict:
    """
    Extrai Duration, Billed Duration e Init Duration da linha REPORT (LogType='Tail').
    `report_found` False quer dizer que a linha não veio: aí init_duration_ms None NÃO significa warm.
    """
    out = {k: None for k in _REPORT_TEXT}
    out['report_found'] = False
    if not log_result_b64:
        return out
    log = base64.b64decode(log_result_b64).decode('utf-8', 'replace')

    for line in log.splitlines():
        if '"platform.report"' not in line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and record.get('type') == 'platform.report':
            metrics = record.get('record', {}).get('metrics', {})
            for field, key in _REPORT_JSON.items():
                if key in metrics:
                    out[field] = float(metrics[key])
            out['report_found'] = out['duration_ms'] is not None
            return out

    for field, pattern in _REPORT_TEXT.items():
        match = pattern.search(log)
        if match:
            out[field] = float(match.group(1))
    out['report_found'] = out['duration_ms'] is not None
    return out


def _row(payload, elapsed_ms, **fields) -> dict:
    """Linha do log: mesmos campos do invoke() da main, mais os da série de throttle."""
    row = {
        'client_latency_ms': elapsed_ms,
        'status_code': None,
        'error': None,
        'request_id': None,
        'payload_received': None,
        'payload_sent': payload,
        'outcome': None,
        'reason': None,
        'retry_attempts': None,
        'report_found': None,
        'duration_ms': None,
        'billed_duration_ms': None,
        'init_duration_ms': None,
    }
    row.update(fields)
    return row


def invoke_once(client, function_name: str, payload: dict) -> dict:
    """
    Uma única tentativa de invocação. Classificação em `outcome`:
    - 'ok' / 'function_error' (a função executou; os dois são HTTP 200);
    - 'throttled': HTTP 429, com `reason` (ex.: ConcurrentInvocationLimitExceeded);
    - 'server_error': HTTP 5xx;
    - 'timeout' / 'network': o cliente não recebeu a resposta.
    Outros 4xx (função ou região erradas, permissão) e falta de credenciais propagam: a série para.
    """
    body_bytes = json.dumps(payload).encode('utf-8')
    start = time.perf_counter()
    try:
        response = client.invoke(FunctionName=function_name, Payload=body_bytes, LogType='Tail')
        elapsed_ms = (time.perf_counter() - start) * 1000
        raw = response['Payload'].read() if 'Payload' in response else None
    except ClientError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        meta = e.response.get('ResponseMetadata', {})
        err = e.response.get('Error', {})
        status = meta.get('HTTPStatusCode')
        if status == 429:
            outcome = 'throttled'
        elif (status or 0) >= 500:
            outcome = 'server_error'
        else:
            raise
        return _row(
            payload, elapsed_ms,
            outcome=outcome,
            status_code=status,
            error=err.get('Message') or str(e),
            reason=e.response.get('Reason') or err.get('Code'),
            request_id=meta.get('RequestId'),
            retry_attempts=meta.get('RetryAttempts'),
        )
    except (HTTPClientError, BotoConnectionError) as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        timed_out = isinstance(e, (ReadTimeoutError, ConnectTimeoutError))
        return _row(
            payload, elapsed_ms,
            outcome='timeout' if timed_out else 'network',
            error=f'{type(e).__name__}: {e}',
        )

    meta = response.get('ResponseMetadata', {})
    body = json.loads(raw) if raw else None
    if response.get('FunctionError'):
        outcome = 'function_error'
        error = body.get('errorMessage', 'erro na função') if isinstance(body, dict) else 'erro na função'
    else:
        outcome, error = 'ok', None

    return _row(
        payload, elapsed_ms,
        outcome=outcome,
        status_code=response.get('StatusCode'),
        error=error,
        request_id=meta.get('RequestId'),
        retry_attempts=meta.get('RetryAttempts'),
        payload_received=body,
        **parse_report(response.get('LogResult')),
    )


# ---------------------------------------------------------------------------
# Rajadas
# ---------------------------------------------------------------------------

def run_burst(function_name: str, size: int, rnd: int, run_id: str, invoke_fn, serie: str = 'throttle') -> list:
    """
    Dispara `size` chamadas que esperam juntas numa barreira e saem ao mesmo tempo.
    O payload é montado depois da barreira, então `sent_at` é a hora real do disparo.
    """
    released = []
    barrier = threading.Barrier(size, action=lambda: released.append(time.perf_counter()), timeout=60)

    def call(i):
        barrier.wait()
        offset_ms = (time.perf_counter() - released[0]) * 1000
        payload = {
            'serie': serie,
            'iteration': i,
            'sent_at': datetime.now(timezone.utc).isoformat(),
            'burst_size': size,
            'round': rnd,
            'run_id': run_id,
        }
        row = invoke_fn(function_name, payload)
        row['dispatch_offset_ms'] = offset_ms
        return row

    with ThreadPoolExecutor(max_workers=size) as executor:
        futures = [executor.submit(call, i) for i in range(size)]
        # f.result() repassa erros de configuração (4xx, credenciais) e interrompe a série
        return [f.result() for f in futures]


def _fmt(values) -> str:
    """min / mediana / max (e p95 com 20 amostras ou mais)."""
    if not values:
        return '-'
    text = f'min={min(values):.1f} med={statistics.median(values):.1f} max={max(values):.1f}'
    if len(values) >= 20:
        text += f' p95={statistics.quantiles(values, n=20)[18]:.1f}'
    return text + ' ms'


ARRIVAL_SPREAD_WARN_MS = 200


def arrival_spread_ms(rows: list):
    """
    Diferença entre a primeira e a última chegada na Lambda (`received_at`, relógio da AWS), só das
    chamadas que executaram. Mede se a rajada chegou junta: um valor alto indica atraso no cliente
    (ex.: conexões HTTPS novas), e aí a concorrência real fica abaixo do tamanho da rajada.
    """
    stamps = []
    for r in rows:
        received = (r.get('payload_received') or {}).get('received_at')
        if received:
            stamps.append(datetime.fromisoformat(received))
    if len(stamps) < 2:
        return None
    return (max(stamps) - min(stamps)).total_seconds() * 1000


def summarize(rows: list) -> dict:
    """Contagem por outcome, motivos do throttle e latências das chamadas aceitas (cold e warm separados)."""
    counts = {}
    for r in rows:
        counts[r['outcome']] = counts.get(r['outcome'], 0) + 1
    reasons = {}
    for r in rows:
        if r['outcome'] == 'throttled':
            reasons[r['reason']] = reasons.get(r['reason'], 0) + 1

    ok = [r for r in rows if r['outcome'] == 'ok']
    cold = [r for r in ok if r['report_found'] and r['init_duration_ms'] is not None]
    warm = [r for r in ok if r['report_found'] and r['init_duration_ms'] is None]

    return {
        'size': len(rows),
        'counts': counts,
        'reasons': reasons,
        'cold': len(cold),
        'no_report': sum(1 for r in ok if not r['report_found']),
        'lat_warm': [r['client_latency_ms'] for r in warm],
        'lat_cold': [r['client_latency_ms'] for r in cold],
        'duration_ok': [r['duration_ms'] for r in ok if r['duration_ms'] is not None],
        'max_offset_ms': max((r['dispatch_offset_ms'] for r in rows), default=0.0),
        'arrival_spread_ms': arrival_spread_ms(rows),
        'retries': sum(r.get('retry_attempts') or 0 for r in rows),
    }


def print_summary(size: int, s: dict):
    c = s['counts']
    others = {k: v for k, v in c.items() if k not in ('ok', 'throttled')}
    print(f"  ok={c.get('ok', 0)}  throttled={c.get('throttled', 0)}  outros={others or 0}")
    if s['reasons']:
        print(f"  motivos do throttle: {s['reasons']}")
    spread = s['arrival_spread_ms']
    spread_txt = f'{spread:.1f} ms' if spread is not None else '-'
    print(f"  cold (Init Duration)={s['cold']}  disparo espalhado em {s['max_offset_ms']:.1f} ms"
          f"  chegada na AWS espalhada em {spread_txt}")
    if s['max_offset_ms'] > ARRIVAL_SPREAD_WARN_MS:
        # As threads demoraram a começar a chamada depois da barreira (CPU/GIL do cliente)
        print(f"  AVISO: o disparo no cliente se espalhou por {s['max_offset_ms']:.0f} ms (threads/CPU do PC); "
              f"a concorrência real ficou abaixo do tamanho da rajada.")
    elif spread is not None and spread > ARRIVAL_SPREAD_WARN_MS:
        # O disparo saiu junto, mas as chamadas chegaram espalhadas: atraso na rede/conexão
        print(f"  AVISO: o disparo saiu junto, mas a chegada na AWS se espalhou por {spread:.0f} ms "
              f"(provável conexão HTTPS nova); a concorrência real ficou abaixo do tamanho da rajada.")
    print(f"  latência cliente warm: {_fmt(s['lat_warm'])}")
    if s['lat_cold']:
        print(f"  latência cliente cold: {_fmt(s['lat_cold'])}")
    print(f"  duração interna (ok): {_fmt(s['duration_ok'])}")
    if s['no_report']:
        print(f"  AVISO: {s['no_report']} chamada(s) ok sem linha REPORT (não classificáveis cold/warm)")
    if s['retries']:
        print(f"  AVISO: {s['retries']} retry(s) registrados; deveria ser 0")
    if sum(c.values()) != size:
        print(f"  AVISO: {sum(c.values())} resultados para uma rajada de {size}")


def run(function_name: str, bursts: list, rounds: int, pause_s: float, invoke_fn,
        run_id: str, log_path: str = LOG_PATH) -> list:
    """
    Executa as rajadas (`rounds` vezes a lista inteira), grava cada chamada no log e devolve os resumos.
    O log é gravado só pelo thread principal, depois de cada rajada.
    """
    summaries = []
    for rnd in range(1, rounds + 1):
        for idx, size in enumerate(bursts):
            print(f"\n[rodada {rnd}/{rounds}] rajada de {size} chamadas simultâneas...")
            rows = run_burst(function_name, size, rnd, run_id, invoke_fn)
            for r in rows:
                save_log(r, log_path)
            s = summarize(rows)
            print_summary(size, s)
            summaries.append({'round': rnd, 'burst_size': size, **s})

            last = rnd == rounds and idx == len(bursts) - 1
            if not last and pause_s > 0:
                print(f"  pausando {pause_s:.0f}s...")
                time.sleep(pause_s)

    print('\n' + '=' * 60)
    for rnd in range(1, rounds + 1):
        first = first_throttle_by_reason([x for x in summaries if x['round'] == rnd])
        if not first:
            print(f"Rodada {rnd}: nenhuma rajada teve throttle")
            continue
        print(f"Rodada {rnd}: primeiro throttle por motivo:")
        for reason, size in first.items():
            print(f"  {reason}: a partir da rajada de {size}")
    return summaries


def first_throttle_by_reason(summaries: list) -> dict:
    """{motivo: menor rajada (na ordem executada) em que esse motivo apareceu}."""
    first = {}
    for s in summaries:
        for reason in s['reasons']:
            first.setdefault(reason, s['burst_size'])
    return first


# ---------------------------------------------------------------------------
# Execução real
# ---------------------------------------------------------------------------

def warm_up(function_name: str, size: int, run_id: str, invoke_fn) -> dict:
    """
    Rajada de aquecimento do tamanho da maior rajada, descartada (não vai para o log).

    Abre de uma vez as conexões HTTPS que as rajadas vão usar. Sem isso, cada rajada maior que a
    anterior abre conexões novas, que atrasam a chegada das chamadas na AWS (na execução curta de
    2026-11-21, ~1,1 s para 4 conexões novas): a rajada deixa de chegar junta e o throttle pode não
    aparecer. Também deixa os ambientes da Lambda aquecidos, então todas as rajadas medidas começam
    no mesmo estado. As chamadas acima do limite de concorrência sofrem throttle e não executam.
    """
    print(f"Aquecimento: rajada de {size} chamadas para abrir as conexões (não gravada)...")
    s = summarize(run_burst(function_name, size, 0, run_id, invoke_fn, serie='warmup'))
    print(f"  {s['counts']}")
    return s


def collect_meta(client, function_name: str, run_id: str, bursts: list, rounds: int, pause_s: float) -> dict:
    """Limites da conta e configuração da função (sem ARN, que contém o ID da conta)."""
    account = client.get_account_settings().get('AccountLimit', {})
    reserved = client.get_function_concurrency(FunctionName=function_name).get('ReservedConcurrentExecutions')
    config = client.get_function_configuration(FunctionName=function_name)
    return {
        'run_id': run_id,
        'region': client.meta.region_name,
        'function_name': function_name,
        'account_concurrent_executions': account.get('ConcurrentExecutions'),
        'account_unreserved_concurrent_executions': account.get('UnreservedConcurrentExecutions'),
        'function_reserved_concurrent_executions': reserved,
        'memory_mb': config.get('MemorySize'),
        'timeout_s': config.get('Timeout'),
        'runtime': config.get('Runtime'),
        'bursts': bursts,
        'rounds': rounds,
        'pause_s': pause_s,
        'boto3_version': boto3.__version__,
    }


def _parse_bursts(text: str) -> list:
    bursts = [int(x) for x in text.split(',') if x.strip()]
    if not bursts or any(b < 1 for b in bursts):
        raise argparse.ArgumentTypeError('rajadas devem ser inteiros >= 1, separados por vírgula')
    return bursts


def main(argv=None):
    parser = argparse.ArgumentParser(description='Série 3: rajadas de concorrência crescente (throttling).')
    parser.add_argument('--bursts', type=_parse_bursts, default=DEFAULT_BURSTS,
                        help='tamanhos das rajadas, ex.: 1,5,10,25 (padrão: %(default)s)')
    parser.add_argument('--rounds', type=int, default=1, help='quantas vezes repetir a lista (padrão: 1)')
    parser.add_argument('--pause', type=float, default=DEFAULT_PAUSE_S,
                        help='segundos entre rajadas (padrão: %(default)s)')
    args = parser.parse_args(argv)

    function_name = os.getenv('FUNCTION_NAME')
    if not function_name:
        raise SystemExit('FUNCTION_NAME não definido (configure o .env).')
    throttle_max = int(os.getenv('THROTTLE_MAX', '500'))
    if max(args.bursts) > throttle_max:
        raise SystemExit(f'Rajada de {max(args.bursts)} acima de THROTTLE_MAX={throttle_max}.')
    if args.rounds < 1:
        raise SystemExit('--rounds precisa ser >= 1.')

    region = os.getenv('AWS_REGION', 'sa-east-1')
    client = make_client(region, throttle_max)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')

    meta = collect_meta(client, function_name, run_id, args.bursts, args.rounds, args.pause)
    os.makedirs('results', exist_ok=True)
    meta_path = f'results/throttle_meta_{run_id}.json'
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)
    print(f"Função {function_name} em {region}; limite de concorrência da conta: "
          f"{meta['account_concurrent_executions']} (metadados em {meta_path})")

    invoke_fn = lambda fn, payload: invoke_once(client, fn, payload)
    warm_up(function_name, max(args.bursts), run_id, invoke_fn)
    if args.pause > 0:
        # O aquecimento grande estoura o limite de taxa de invocações; sem pausa, a primeira rajada
        # medida é rejeitada por FunctionInvocationRateLimitExceeded (run 20261121T020624Z)
        print(f"  pausando {args.pause:.0f}s depois do aquecimento...")
        time.sleep(args.pause)

    print(f"Série THROTTLE {run_id}: rajadas {args.bursts}, {args.rounds} rodada(s), pausa {args.pause:.0f}s")
    run(function_name, args.bursts, args.rounds, args.pause, invoke_fn, run_id)
    print(f"\nLog: {LOG_PATH}")


if __name__ == '__main__':
    main()
