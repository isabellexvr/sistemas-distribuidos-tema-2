import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from tests.invoke_lambda import invoke, save_log

load_dotenv()


def run():
    """
    Série 1: cold start.
    Faz uma chamada, espera muito tempo, faz outra.
    Cada chamada é isolada — sem concorrência.
    Não assumir que TODA chamada após pausa é fria — apenas suspeita.
    Vamos confirmar com init_duration_ms do CloudWatch depois.
    """
    fn = os.getenv('FUNCTION_NAME')
    iterations = int(os.getenv('COLD_ITERATIONS', '20'))
    pause_seconds = 600  # 10 minutos

    print(f"Série COLD: {iterations} chamadas com pausa de {pause_seconds}s entre cada")
    print(f"Atenção: esta série vai demorar ~{iterations * pause_seconds / 60:.0f} minutos")

    results = []

    for i in range(iterations):
        sent_at = datetime.now(timezone.utc).isoformat()
        payload = {'serie': 'cold', 'iteration': i, 'sent_at': sent_at}

        print(f"  [{i+1}/{iterations}] chamando... (esperando {pause_seconds}s antes)")
        r = invoke(fn, payload)

        # TODO: marcar suspected_cold=True (suspeita, vamos confirmar no CloudWatch)
        r['suspected_cold'] = True

        # TODO: salvar log
        save_log(r)
        results.append(r)

        # Imprimir latência da chamada
        if r['status_code'] == 200:
            print(f"    latência: {r['client_latency_ms']:.1f} ms")
        else:
            print(f"    ERRO: {r.get('error', 'desconhecido')}")

        # TODO: pausar antes da próxima (se não for a última)
        if i < iterations - 1:
            # Para não bloquear 10 min seguidos sem feedback, imprime a cada minuto
            for remaining in range(pause_seconds, 0, -60):
                print(f"    aguardando... {remaining}s restantes")
                time.sleep(60)

    # TODO: estatísticas finais
    latencies = [r['client_latency_ms'] for r in results if r['status_code'] == 200]
    latencies.sort()

    print(f"\nResumo COLD:")
    print(f"  amostras: {len(latencies)}")
    print(f"  min:      {latencies[0]:.1f} ms")
    print(f"  mediana:  {latencies[len(latencies)//2]:.1f} ms")
    print(f"  p95:      {latencies[int(len(latencies)*0.95)]:.1f} ms")
    print(f"  p99:      {latencies[int(len(latencies)*0.99) if len(latencies)>100 else -1]:.1f} ms")
    print(f"  max:      {latencies[-1]:.1f} ms")


if __name__ == '__main__':
    run()