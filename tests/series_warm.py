import math
import os
from datetime import datetime, timezone
from statistics import median

from dotenv import load_dotenv

from tests.invoke_lambda import invoke, save_log


def percentile(sorted_values, percent):
    """Retorna o percentil pelo método do posto mais próximo."""
    return sorted_values[math.ceil(len(sorted_values) * percent / 100) - 1]


def run():
    """Mede a latência de invocações sequenciais, sem pausa entre elas."""
    load_dotenv()
    function_name = os.getenv('FUNCTION_NAME')
    if not function_name:
        raise SystemExit('Configure FUNCTION_NAME no ambiente ou no arquivo .env.')

    try:
        iterations = int(os.getenv('WARM_ITERATIONS', '200'))
        if iterations <= 0:
            raise ValueError
    except ValueError:
        raise SystemExit('WARM_ITERATIONS deve ser um número inteiro positivo.')

    print(f'Série WARM: {iterations} chamadas seguidas, sem pausa')
    results = []

    for i in range(iterations):
        payload = {
            'serie': 'warm',
            'iteration': i,
            'sent_at': datetime.now(timezone.utc).isoformat(),
        }
        result = invoke(function_name, payload)
        result['suspected_cold'] = False
        save_log(result)
        results.append(result)

        if (i + 1) % 50 == 0:
            latencies = [r['client_latency_ms'] for r in results
                         if r['status_code'] == 200 and not r.get('error')]
            if latencies:
                average = sum(latencies) / len(latencies)
                print(f'  [{i + 1}/{iterations}] latência média: {average:.1f} ms')
            else:
                print(f'  [{i + 1}/{iterations}] nenhuma chamada bem-sucedida')

    latencies = sorted(r['client_latency_ms'] for r in results
                       if r['status_code'] == 200 and not r.get('error'))
    print('\nResumo WARM:')
    print(f'  amostras: {len(latencies)}')
    print(f'  falhas: {len(results) - len(latencies)}')
    if not latencies:
        raise SystemExit('Nenhuma chamada bem-sucedida; verifique results/raw.jsonl.')

    mediana = median(latencies)
    print(f'  min: {latencies[0]:.1f} ms')
    print(f'  mediana / p50: {mediana:.1f} ms')
    print(f'  p95: {percentile(latencies, 95):.1f} ms')
    print(f'  p99: {percentile(latencies, 99):.1f} ms')
    print(f'  max: {latencies[-1]:.1f} ms')


if __name__ == '__main__':
    run()
