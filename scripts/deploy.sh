#!/bin/bash
# Empacota o handler e atualiza o código da função Lambda (que já precisa existir).
# Lê FUNCTION_NAME e AWS_REGION do .env, se existir; variáveis já exportadas têm precedência.
set -euo pipefail

cd "$(dirname "$0")/.."

# Remove espaços no início e no fim
trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  printf '%s' "${s%"${s##*[![:space:]]}"}"
}

# Carrega o .env sem sobrescrever o que já veio do ambiente, aceitando o mesmo que o python-dotenv
# do cliente costuma aceitar: CRLF, última linha sem quebra, `export`, aspas e comentário no fim da linha.
if [ -f .env ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    line="$(trim "${line%$'\r'}")"
    case "$line" in ''|\#*) continue ;; esac
    line="${line#export }"
    case "$line" in *=*) ;; *) continue ;; esac
    key="$(trim "${line%%=*}")"
    value="$(trim "${line#*=}")"
    case "$value" in
      \"*\"|\'*\') value="${value:1:${#value}-2}" ;;
      *) value="$(trim "${value%%[[:space:]]#*}")" ;;
    esac
    if ! [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      echo "Linha inválida no .env: $line" >&2
      exit 1
    fi
    if [ -z "${!key:-}" ]; then export "$key=$value"; fi
  done < .env
fi

FUNCTION_NAME="${FUNCTION_NAME:-tema2-light}"
REGION="${AWS_REGION:-sa-east-1}"

# Acha um Python que execute de verdade (no Windows, `python` pode ser o atalho da Store)
runs() { command -v "$1" >/dev/null 2>&1 && "$1" -c 'import sys' >/dev/null 2>&1; }
PY=""
if [ -n "${PYTHON:-}" ]; then
  runs "$PYTHON" || { echo "PYTHON=$PYTHON não executa." >&2; exit 1; }
  PY="$PYTHON"
else
  for candidate in python3 python py; do
    if runs "$candidate"; then PY="$candidate"; break; fi
  done
fi
[ -n "$PY" ] || { echo "Python não encontrado (defina PYTHON=...)." >&2; exit 1; }

# Zip novo a cada deploy, só com handler.py na raiz (dispensa o binário zip).
# `python -m zipfile -c` gera um zip vazio sem erro se o arquivo faltar, por isso as duas checagens.
[ -f handler.py ] || { echo "handler.py não encontrado na raiz do projeto." >&2; exit 1; }
mkdir -p build
rm -f build/handler.zip
"$PY" -m zipfile -c build/handler.zip handler.py
"$PY" -c 'import sys, zipfile; sys.exit(zipfile.ZipFile("build/handler.zip").namelist() != ["handler.py"])' \
  || { echo "build/handler.zip não contém só handler.py." >&2; exit 1; }

if [ "${PACKAGE_ONLY:-0}" = "1" ]; then
  echo "Pacote gerado em build/handler.zip (PACKAGE_ONLY=1, deploy não executado)."
  exit 0
fi

aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file fileb://build/handler.zip \
  --region "$REGION" \
  --output text --query 'LastUpdateStatus'

# Só termina quando a atualização foi aplicada, para a próxima invocação já usar o código novo
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"

echo "Lambda $FUNCTION_NAME atualizada com sucesso em $REGION."
