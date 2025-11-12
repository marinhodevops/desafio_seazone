# Workflow Full AI - Local Supabase (Postgres)

## Pré-requisitos
- Postgres local (Supabase local) rodando em localhost:5432
- n8n instalado e rodando (use node 22 via nvm ou Docker)
- Python 3.10+ com venv
- OpenAI API key
- Discord webhook (ou bot token para bot)

## Variáveis de ambiente
- DATABASE_URL (ex: postgres://postgres:postgres@localhost:5432/postgres)
- OPENAI_API_KEY
- DISCORD_WEBHOOK_URL (para n8n notify node)
- (opcional) DISCORD_TOKEN para bot

## Importar workflow
1. Abra n8n (http://localhost:5678).
2. Workflows → Import → selecione `workflow_full_ai.json`.
3. No painel:
   - Crie credencial OpenAI com `OPENAI_API_KEY`.
   - Crie credencial Postgres apontando para `localhost:5432`.
   - Em Settings → Environment Variables adicione `DISCORD_WEBHOOK_URL`.
4. Ajuste o node `Run Script` para o caminho do seu `process_data.py` (se necessário).

## Testar manual
1. Rode `process_data.py` localmente:
