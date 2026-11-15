#!/bin/bash
# Empacota o handler e atualiza o código da função Lambda
set -e

cd "$(dirname "$0")/.."

# Zipa o handler
cd lambda
mkdir -p ../build
zip ../build/handler.zip handler.py
cd ..

# Atualiza o código da função (assumindo que ela já foi criada)
FUNCTION_NAME="${FUNCTION_NAME:-tema2-light}"
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file fileb://build/handler.zip \
  --region sa-east-1

echo "Lambda $FUNCTION_NAME atualizada com sucesso."
