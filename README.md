
## Pré-requisitos

- Python 3.12+
- AWS CLI configurada
- Conta AWS ativa (free tier é suficiente — Lambda tem 1M de invocações grátis/mês)

## Setup — o que cada integrante precisa fazer (uma vez)

### Passo 1 — Criar conta AWS

Cada integrante cria sua própria conta em https://aws.amazon.com/ com e-mail pessoal. O cartão é só pra validação (cobrança temporária de US$1 devolvida em 3 dias); no volume do trabalho não vai custar nada.

**Não usar contas institucionais / AWS Academy / Cloud Builder Labs** — elas bloqueiam `lambda:CreateFunction` via SCP (Service Control Policy), mesmo pra usuários Admin.

### Passo 2 — Criar usuário IAM com access keys

No console AWS (https://console.aws.amazon.com):

1. Busca "IAM" na barra de cima → entra em IAM
2. Menu lateral → Users → Create user
3. User name: `admin-cli`
4. NÃO marcar "Provide user access to the AWS Management Console" (vamos só usar CLI)
5. Next
6. "Attach policies directly" → busca e marca `AdministratorAccess`
7. Next → Create user
8. Clica no user criado → aba "Security credentials"
9. "Access keys" → "Create access key"
10. Caso de uso: "Command Line Interface (CLI)"
11. Marca o checkbox de acknowledgment → Next → Create access key
12. **Copia Access key ID e Secret access key** (a Secret não aparece de novo)

### Passo 3 — Configurar a CLI

```bash
aws configure
# AWS Access Key ID: <cola o Access key ID>
# AWS Secret Access Key: <cola a Secret access key>
# Default region name: sa-east-1
# Default output format: json

# Testa (deve mostrar Account, UserId, Arn sem "organizations"):
aws sts get-caller-identity