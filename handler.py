import json
from datetime import datetime, timezone


def lambda_handler(event, context):
    """
    Função Lambda simples.
    Recebe um payload com timestamp de envio e devolve a hora atual.
    Usada para medir latência cliente-side vs duração interna.
    """
    # Hora em que a Lambda começou a executar (UTC)
    received_at = datetime.now(timezone.utc).isoformat()

    # Lê info do ambiente de execução
    memory_mb = context.memory_limit_in_mb
    remaining_ms = context.get_remaining_time_in_millis()

    return {
        "status": "ok",
        "received_at": received_at,
        "sent_at": event.get("sent_at"),       # eco do que veio
        "serie": event.get("serie"),           # qual série de teste
        "iteration": event.get("iteration"),  # qual iteração
        "memory_mb": memory_mb,
        "remaining_time_ms": remaining_ms,
    }