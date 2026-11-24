import os
import re
import json
import time
import base64
import threading
import warnings
import boto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    ConnectionError as BotoConnectionError,
    HTTPClientError,
    ReadTimeoutError,
)
from datetime import datetime, timezone

# Erros 5xx do Lambda que são falha transitória do serviço (viram linha `server_error`).
# Os demais 5xx modelados (InvalidZipFileException, ResourceNotReadyException, KMS*, ENI*, Subnet*...)
# indicam função mal configurada ou quebrada e interrompem a série.
_TRANSIENT_5XX = {'ServiceException', 'EC2ThrottledException', 'EC2UnexpectedException'}


def _client_config() -> Config:
    """
    Configuração explícita do cliente:
    - sem retry: cada linha do log é exatamente uma tentativa (retries escondem throttles,
      somam backoff na latência e, em timeout de leitura, podem executar a função duas vezes);
    - timeouts explícitos (read_timeout acima do timeout da função);
    - pool de conexões do tamanho da concorrência máxima, para não pagar TCP+TLS novo
      a cada chamada quando houver mais de 10 threads.
    """
    return Config(
        retries={'mode': 'standard', 'total_max_attempts': 1},
        connect_timeout=5,
        read_timeout=30,
        max_pool_connections=max(10, int(os.getenv('THROTTLE_MAX', '10'))),
    )


# Singleton do cliente boto3 — reutiliza a conexão entre chamadas.
# Criado sob lock: a sessão padrão do boto3 não é thread-safe.
_lambda_client = None
_client_lock = threading.Lock()
_log_lock = threading.Lock()


def get_lambda_client():
    """Retorna o cliente boto3 configurado. Singleton thread-safe."""
    global _lambda_client
    if _lambda_client is None:
        with _client_lock:
            if _lambda_client is None:
                _lambda_client = boto3.client(
                    'lambda',
                    region_name=os.getenv('AWS_REGION', 'sa-east-1'),
                    config=_client_config(),
                )
    return _lambda_client


# Linha REPORT do log em texto e o equivalente no formato JSON (advanced logging)
_REPORT_PATTERNS = {
    'duration_ms': re.compile(r'\tDuration: ([\d.]+) ms'),
    'billed_duration_ms': re.compile(r'Billed Duration: ([\d.]+) ms'),
    'init_duration_ms': re.compile(r'Init Duration: ([\d.]+) ms'),
}
_JSON_METRICS = {
    'duration_ms': 'durationMs',
    'billed_duration_ms': 'billedDurationMs',
    'init_duration_ms': 'initDurationMs',
}


def parse_report(log_result_b64) -> dict:
    """
    Extrai Duration, Billed Duration e Init Duration da linha REPORT devolvida com LogType='Tail'.
    Campos ausentes (ex.: Init Duration numa invocação warm) ficam None.
    `report_found` diz se a linha REPORT veio no log: sem ela, `init_duration_ms` None NÃO quer
    dizer warm (ex.: log JSON com nível de sistema WARN suprime o platform.report).
    """
    metrics = {k: None for k in _REPORT_PATTERNS}
    metrics['report_found'] = False
    if not log_result_b64:
        return metrics
    log = base64.b64decode(log_result_b64).decode('utf-8', 'replace')

    for line in log.splitlines():
        if '"platform.report"' in line:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if not isinstance(record, dict) or record.get('type') != 'platform.report':
                continue
            found = record.get('record', {}).get('metrics', {})
            for field, key in _JSON_METRICS.items():
                if key in found:
                    metrics[field] = float(found[key])
            metrics['report_found'] = metrics['duration_ms'] is not None
            return metrics

    for field, pattern in _REPORT_PATTERNS.items():
        match = pattern.search(log)
        if match:
            metrics[field] = float(match.group(1))
    metrics['report_found'] = metrics['duration_ms'] is not None
    return metrics


def _result(elapsed_ms, function_name, payload, **fields) -> dict:
    """Monta uma linha do log com todos os campos presentes (None quando não se aplica)."""
    client = get_lambda_client()
    row = {
        'outcome': None,
        'client_latency_ms': elapsed_ms,
        'client_ts_utc': datetime.now(timezone.utc).isoformat(),
        'function_name': function_name,
        'region': client.meta.region_name,
        'status_code': None,
        'error': None,
        'error_code': None,
        'reason': None,
        'request_id': None,
        'retry_attempts': None,
        'cold_start': None,
        'log_stream': None,
        'report_found': None,
        'duration_ms': None,
        'billed_duration_ms': None,
        'init_duration_ms': None,
        'payload_received': None,
        'payload_sent': payload,
    }
    row.update(fields)
    return row


def invoke(function_name: str, payload: dict) -> dict:
    """
    Invoca a Lambda (uma única tentativa, sem retry) e devolve uma linha de resultado.

    `outcome` classifica a chamada:
    - 'ok': a função executou e respondeu (status_code 200);
    - 'function_error': a função executou mas levantou erro (status_code também é 200,
      por isso não filtre só por status_code);
    - 'throttled': HTTP 429 (TooManyRequestsException); `reason` diz qual limite estourou;
    - 'server_error': HTTP 5xx transitório do serviço Lambda (ServiceException, EC2Throttled/Unexpected
      ou 5xx sem código modelado);
    - 'timeout' / 'network': o cliente não recebeu a resposta completa (status_code None).

    Erros de configuração NÃO viram linha: a exceção é propagada para interromper a série. Isso vale
    para 4xx (função ou região erradas, permissão, parâmetro inválido), para 5xx que indicam função
    quebrada ou mal configurada (InvalidZipFileException, ResourceNotReadyException, KMS*, ENI*...) e
    para falta de credenciais.

    Outros campos: client_latency_ms (tempo percebido pelo cliente, até o retorno de client.invoke),
    request_id (para cruzar com o CloudWatch), retry_attempts (deve ser sempre 0), cold_start e
    log_stream (vindos do handler), report_found e duration_ms / billed_duration_ms /
    init_duration_ms (linha REPORT do log da invocação).
    """
    client = get_lambda_client()
    payload_bytes = json.dumps(payload).encode('utf-8')

    # Marca o tempo ANTES de chamar a Lambda
    start = time.perf_counter()

    try:
        response = client.invoke(
            FunctionName=function_name,
            Payload=payload_bytes,
            LogType='Tail',
        )
        # Tempo DEPOIS do retorno
        elapsed_ms = (time.perf_counter() - start) * 1000
        # O Payload é streaming: a leitura também pode falhar por erro de transporte
        raw_payload = response['Payload'].read() if 'Payload' in response else None

    except ClientError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        meta = e.response.get('ResponseMetadata', {})
        err = e.response.get('Error', {})
        status = meta.get('HTTPStatusCode')
        code = err.get('Code')
        if status == 429:
            outcome = 'throttled'
        elif (status or 0) >= 500 and (
            code in _TRANSIENT_5XX
            or code not in {s.name for s in client.meta.service_model.error_shapes}
        ):
            outcome = 'server_error'
        else:
            raise  # erro de configuração/permissão ou função quebrada: não é medição
        return _result(
            elapsed_ms, function_name, payload,
            outcome=outcome,
            status_code=status,
            error=err.get('Message') or str(e),
            error_code=code,
            reason=e.response.get('Reason'),
            request_id=meta.get('RequestId'),
            retry_attempts=meta.get('RetryAttempts'),
        )

    except (HTTPClientError, BotoConnectionError) as e:
        # Bases de todos os erros de transporte do botocore: timeouts, conexão recusada ou
        # derrubada, SSL, proxy, falha ao ler o corpo da resposta
        elapsed_ms = (time.perf_counter() - start) * 1000
        timed_out = isinstance(e, (ReadTimeoutError, ConnectTimeoutError))
        return _result(
            elapsed_ms, function_name, payload,
            outcome='timeout' if timed_out else 'network',
            error=str(e),
            error_code=type(e).__name__,
        )

    meta = response.get('ResponseMetadata', {})
    body = json.loads(raw_payload) if raw_payload else None

    # Erro de função: a invocação chega com 200 e FunctionError preenchido
    if response.get('FunctionError'):
        outcome = 'function_error'
        error = body.get('errorMessage', 'erro na função') if isinstance(body, dict) else 'erro na função'
    else:
        outcome = 'ok'
        error = None

    from_handler = body if outcome == 'ok' and isinstance(body, dict) else {}
    report = parse_report(response.get('LogResult'))
    if outcome == 'ok' and not report['report_found']:
        warnings.warn(
            'Invocação sem linha REPORT no LogResult: duration/init ficam None e a linha '
            'não pode ser classificada como cold/warm. Confira o LoggingConfig da função.',
            RuntimeWarning,
            stacklevel=2,
        )

    return _result(
        elapsed_ms, function_name, payload,
        outcome=outcome,
        status_code=response['StatusCode'],
        error=error,
        error_code=response.get('FunctionError'),
        request_id=meta.get('RequestId'),
        retry_attempts=meta.get('RetryAttempts'),
        cold_start=from_handler.get('cold_start'),
        log_stream=from_handler.get('log_stream'),
        payload_received=body,
        **report,
    )


def save_log(result: dict, path: str = 'results/raw.jsonl'):
    """Salva cada invocação em arquivo JSONL (uma linha por chamada). Seguro entre threads."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    line = json.dumps(result, default=str) + '\n'
    with _log_lock:
        with open(path, 'a', encoding='utf-8', newline='\n') as f:
            f.write(line)


# Teste rápido: rodar direto pra ver se funciona
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    fn = os.getenv('FUNCTION_NAME')
    if not fn:
        raise SystemExit('FUNCTION_NAME não definido (configure o .env a partir do .env.example).')

    print(f"Invocando Lambda: {fn}\n")

    test_payload = {
        'serie': 'manual',
        'iteration': 0,
        'sent_at': datetime.now(timezone.utc).isoformat(),
    }

    r = invoke(fn, test_payload)
    print(json.dumps(r, indent=2, default=str))

    save_log(r)
    print(f"\nLog salvo em results/raw.jsonl")
