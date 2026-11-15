import os
import time
import json
import boto3
from datetime import datetime, timezone


# Singleton do cliente boto3 — reutiliza a conexão entre chamadas
_lambda_client = None


def get_lambda_client():
    """Retorna cliente boto3 configurado. Singleton pra não recriar toda hora."""
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client(
            'lambda',
            region_name=os.getenv('AWS_REGION', 'sa-east-1')
        )
    return _lambda_client


def invoke(function_name: str, payload: dict) -> dict:
    """
    Invoca a Lambda e devolve um dicionário com:
    - client_latency_ms: tempo percebido pelo cliente (do invoke ao retorno)
    - status_code: 200, 429 (throttle), 500 (erro)
    - error: mensagem de erro se houver
    - request_id: ID da requisição (para cruzar com CloudWatch depois)
    - payload_received: resposta da Lambda (None se deu erro)
    - payload_sent: o que foi enviado
    """
    client = get_lambda_client()
    payload_bytes = json.dumps(payload).encode('utf-8')

    # Marca o tempo ANTES de chamar a Lambda
    start = time.perf_counter()

    try:
        response = client.invoke(
            FunctionName=function_name,
            Payload=payload_bytes,
        )
        # Tempo DEPOIS do retorno
        elapsed_ms = (time.perf_counter() - start) * 1000

        status_code = response['StatusCode']
        request_id = response['ResponseMetadata']['RequestId']

        # Lê o corpo da resposta
        if 'Payload' in response:
            body = json.loads(response['Payload'].read())
        else:
            body = None

        # Detecta se a Lambda reportou erro de função (FunctionError)
        if response.get('FunctionError'):
            error = body.get('errorMessage', 'erro na função') if body else 'erro na função'
        else:
            error = None

        return {
            'client_latency_ms': elapsed_ms,
            'status_code': status_code,
            'error': error,
            'request_id': request_id,
            'payload_received': body,
            'payload_sent': payload,
        }

    except Exception as e:
        # Erro de rede, throttle do lado do cliente, etc.
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            'client_latency_ms': elapsed_ms,
            'status_code': 0,
            'error': str(e),
            'request_id': None,
            'payload_received': None,
            'payload_sent': payload,
        }


def save_log(result: dict, path: str = 'results/raw.jsonl'):
    """Salva cada invocação em arquivo JSONL (uma linha por chamada)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a') as f:
        f.write(json.dumps(result, default=str) + '\n')


# Teste rápido: rodar direto pra ver se funciona
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    fn = os.getenv('FUNCTION_NAME')
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
