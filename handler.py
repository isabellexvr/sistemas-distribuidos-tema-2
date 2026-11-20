import json
from datetime import datetime, timezone

# Verdadeiro só na primeira invocação de cada ambiente de execução (o módulo é carregado
# uma vez por ambiente). Ambientes inicializados de forma proativa pela AWS também começam
# com True, mas não têm "Init Duration" no REPORT da invocação.
_COLD = True


def lambda_handler(event, context):
    """
    Função Lambda simples.
    Recebe um payload com timestamp de envio e devolve a hora atual.
    Usada para medir latência cliente-side vs duração interna.
    """
    global _COLD
    cold_start, _COLD = _COLD, False

    # Hora em que a Lambda começou a executar (UTC, relógio da AWS: não comparar
    # diretamente com sent_at, que vem do relógio do cliente)
    received_at = datetime.now(timezone.utc).isoformat()

    # Lê info do ambiente de execução
    memory_mb = int(context.memory_limit_in_mb)  # o runtime entrega como string
    remaining_ms = context.get_remaining_time_in_millis()

    return {
        "status": "ok",
        "received_at": received_at,
        "sent_at": event.get("sent_at"),       # eco do que veio
        "serie": event.get("serie"),           # qual série de teste
        "iteration": event.get("iteration"),  # qual iteração
        "cold_start": cold_start,
        "aws_request_id": context.aws_request_id,
        "log_stream": context.log_stream_name,  # identifica o ambiente de execução
        "memory_mb": memory_mb,
        "remaining_time_ms": remaining_ms,
    }
