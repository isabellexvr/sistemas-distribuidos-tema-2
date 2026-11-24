"""
Enriquece results/raw.jsonl com as métricas internas que a Lambda grava no CloudWatch Logs.

Para cada chamada que executou, busca no log group /aws/lambda/<FUNCTION_NAME> a linha
"REPORT RequestId: ..." e extrai Init Duration (só em cold starts), Duration,
Billed Duration e Max Memory Used. Salva tudo em results/enriched.jsonl.

Uso (da raiz do repo, depois de rodar alguma série):
    python -m tests.cloudwatch_fetch
"""
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

INPUT_PATH = 'results/raw.jsonl'
OUTPUT_PATH = 'results/enriched.jsonl'
SLEEP_BETWEEN_CALLS_S = 0.1           # para não estourar o limite de chamadas do CloudWatch Logs
WINDOW_BEFORE = timedelta(minutes=5)
WINDOW_AFTER = timedelta(minutes=30)
MAX_PAGES = 20

METRIC_FIELDS = ('init_duration_ms', 'duration_ms', 'billed_duration_ms', 'memory_used_mb')

_REPORT_TEXT = {
    'duration_ms': re.compile(r'\tDuration: ([\d.]+) ms'),
    'billed_duration_ms': re.compile(r'Billed Duration: ([\d.]+) ms'),
    'init_duration_ms': re.compile(r'Init Duration: ([\d.]+) ms'),
    'memory_used_mb': re.compile(r'Max Memory Used: (\d+) MB'),
}
_REPORT_JSON = {
    'duration_ms': 'durationMs',
    'billed_duration_ms': 'billedDurationMs',
    'init_duration_ms': 'initDurationMs',
    'memory_used_mb': 'maxMemoryUsedMB',
}


def parse_report(message: str, request_id: str):
    """
    Lê uma mensagem de log. Devolve as métricas se for o REPORT desse request_id, senão None.
    Aceita o formato texto ("REPORT RequestId: ...") e o formato JSON (`platform.report`).
    """
    if message.lstrip().startswith('{'):
        try:
            record = json.loads(message)
        except ValueError:
            return None
        if not isinstance(record, dict) or record.get('type') != 'platform.report':
            return None
        body = record.get('record') or {}
        if body.get('requestId') != request_id:
            return None
        found = body.get('metrics') or {}
        metrics = {field: None for field in METRIC_FIELDS}
        for field, key in _REPORT_JSON.items():
            if key in found:
                metrics[field] = int(found[key]) if field == 'memory_used_mb' else float(found[key])
        return metrics

    if f'REPORT RequestId: {request_id}' not in message:
        return None
    metrics = {field: None for field in METRIC_FIELDS}
    for field, pattern in _REPORT_TEXT.items():
        match = pattern.search(message)
        if match:
            value = match.group(1)
            metrics[field] = int(value) if field == 'memory_used_mb' else float(value)
    return metrics


def _time_window_ms(row: dict):
    """(início, fim) da busca em epoch ms, em volta do sent_at da chamada; None se não houver sent_at."""
    sent_at = (row.get('payload_sent') or {}).get('sent_at')
    try:
        sent = datetime.fromisoformat(sent_at)
    except (TypeError, ValueError):
        return None
    if sent.tzinfo is None:
        # Sem fuso: assume a hora local de quem rodou a série (datetime.now().isoformat())
        sent = sent.astimezone()
    return int((sent - WINDOW_BEFORE).timestamp() * 1000), int((sent + WINDOW_AFTER).timestamp() * 1000)


def fetch_metrics_for_request(logs_client, request_id: str, function_name: str, window=None) -> dict:
    """
    Busca o REPORT de um request_id no log group da função.
    Devolve as métricas e `cloudwatch_error` (None se achou, ou o motivo de não ter achado).
    """
    empty = {field: None for field in METRIC_FIELDS}
    params = {
        'logGroupName': f'/aws/lambda/{function_name}',
        'filterPattern': f'"{request_id}"',
        'limit': 50,
    }
    if window:
        params['startTime'], params['endTime'] = window

    try:
        for _ in range(MAX_PAGES):
            response = logs_client.filter_log_events(**params)
            for event in response.get('events', []):
                metrics = parse_report(event.get('message', ''), request_id)
                if metrics is not None:
                    return {**metrics, 'cloudwatch_error': None}
            token = response.get('nextToken')
            if not token:
                break
            params['nextToken'] = token
    except logs_client.exceptions.ResourceNotFoundException:
        return {**empty, 'cloudwatch_error': 'log_group_not_found'}

    return {**empty, 'cloudwatch_error': 'no_report_line'}


def should_fetch(row: dict) -> bool:
    """Só chamadas que executaram têm REPORT: com request_id, HTTP 200 e outcome ok/function_error (se existir)."""
    if not row.get('request_id'):
        return False
    if row.get('status_code') != 200:
        return False
    return row.get('outcome') in (None, 'ok', 'function_error')


def merge_metrics(row: dict, metrics: dict) -> dict:
    """Preenche só os campos vazios da linha; valores que já vieram da invocação são mantidos."""
    merged = dict(row)
    for field in METRIC_FIELDS:
        if merged.get(field) is None:
            merged[field] = metrics.get(field)
    merged['cloudwatch_error'] = metrics.get('cloudwatch_error')
    return merged


def make_logs_client(region: str):
    """Cliente do CloudWatch Logs. Aqui retry é desejável: não é medição, e o limite de chamadas é baixo."""
    return boto3.client('logs', region_name=region, config=Config(retries={'mode': 'adaptive', 'max_attempts': 8}))


def run(input_path: str = INPUT_PATH, output_path: str = OUTPUT_PATH, logs_client=None, sleep_s: float = SLEEP_BETWEEN_CALLS_S):
    """Lê o raw.jsonl, enriquece com o CloudWatch e salva o enriched.jsonl. Devolve as linhas enriquecidas."""
    fn = os.getenv('FUNCTION_NAME')
    if not fn:
        raise SystemExit('FUNCTION_NAME não definido (configure o .env).')
    if not os.path.exists(input_path):
        print(f"{input_path} não encontrado. Rode as séries primeiro.")
        return []
    if logs_client is None:
        logs_client = make_logs_client(os.getenv('AWS_REGION', 'sa-east-1'))

    rows, bad_lines = [], 0
    with open(input_path, encoding='utf-8') as f:
        for n, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except ValueError:
                bad_lines += 1
                print(f"  AVISO: linha {n} de {input_path} não é JSON válido; pulada")
    print(f"Enriquecendo {len(rows)} registros do CloudWatch...")

    enriched, fetched, skipped = [], 0, 0
    for i, row in enumerate(rows):
        if not should_fetch(row):
            skipped += 1
            enriched.append(row)
            continue
        print(f"  [{i + 1}/{len(rows)}] buscando {row['request_id'][:8]}...")
        metrics = fetch_metrics_for_request(logs_client, row['request_id'], fn, _time_window_ms(row))
        enriched.append(merge_metrics(row, metrics))
        fetched += 1
        if sleep_s:
            time.sleep(sleep_s)

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        for r in enriched:
            f.write(json.dumps(r, default=str) + '\n')

    not_found = sum(1 for r in enriched if r.get('cloudwatch_error'))
    cold = [r for r in enriched if r.get('init_duration_ms') is not None]
    print(f"\nSalvo: {output_path} com {len(enriched)} registros "
          f"({fetched} consultados, {skipped} pulados por não terem executado, {not_found} sem REPORT no CloudWatch)")
    print(f"  cold starts confirmados (com init_duration_ms): {len(cold)}")
    print(f"  demais registros (sem init_duration_ms):        {len(enriched) - len(cold)}")
    if not_found:
        print("  AVISO: o CloudWatch pode levar alguns minutos para receber os logs; "
              "rode de novo mais tarde se faltarem registros recentes.")
    if bad_lines:
        print(f"  AVISO: {bad_lines} linha(s) inválida(s) de {input_path} ficaram fora do {output_path}.")
    return enriched


if __name__ == '__main__':
    run()
