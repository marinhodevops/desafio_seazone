import os
import asyncio
import re
import psycopg2
import psycopg2.extras
import discord
from discord.ext import commands
from openai import OpenAI

DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

if not DISCORD_TOKEN or not OPENAI_API_KEY or not DATABASE_URL:
    print('[ERROR] Missing one of DISCORD_TOKEN, OPENAI_API_KEY, DATABASE_URL')
    raise SystemExit(1)

openai = OpenAI(api_key=OPENAI_API_KEY)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

ALLOWED_TABLES = {'monthly_consolidated'}
ALLOWED_COLUMNS = {
    'property_id','owner_name','city','state','region','month',
    'num_reservations','occupied_days','gross_revenue','platform_fee_pct',
    'extra_cost','net_revenue','margin_pct','avg_rating','complaint_categories','summary_ai'
}

def sanitize_sql(sql_text):
    sql = sql_text.strip().replace(';', '')
    sql_upper = sql.upper()
    forbidden = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
    if not sql_upper.startswith('SELECT'):
        raise ValueError('Only SELECT queries are allowed.')
    for token in forbidden:
        if token in sql_upper:
            raise ValueError('Forbidden SQL operation detected.')
    if '--' in sql or '/*' in sql:
        raise ValueError('Comments in SQL are not allowed.')
    return sql

def extract_tables_columns(sql_text):
    tables = set(re.findall(r'FROM\s+([\w_\.]+)', sql_text, flags=re.IGNORECASE))
    tables |= set(re.findall(r'JOIN\s+([\w_\.]+)', sql_text, flags=re.IGNORECASE))
    cols = set(re.findall(r'SELECT\s+(.*?)\s+FROM', sql_text, flags=re.IGNORECASE|re.S))
    columns = set()
    if cols:
        for c in cols.pop().split(','):
            c = c.strip()
            c = re.sub(r'\s+AS\s+\w+', '', c, flags=re.IGNORECASE)
            c = c.split('.')[-1].strip()
            columns.add(c)
    return tables, columns

@bot.event
async def on_ready():
    print(f'Bot ready. Logged in as {bot.user}')

@bot.command(name='query', help='Ex: !query imóveis com margem negativa no último mês no Nordeste')
async def natural_query(ctx, *, query_text: str):
    await ctx.trigger_typing()
    prompt = f"""You are an assistant that receives a user instruction in Portuguese to produce a single READ-ONLY SQL SELECT statement against the table 'monthly_consolidated'. Return ONLY the SQL statement, nothing else. User instruction: {query_text} Constraints: Only reference the table monthly_consolidated. Allowed columns: {', '.join(sorted(ALLOWED_COLUMNS))}. Use WHERE, ORDER BY, LIMIT as needed. Return a valid single-line SQL."""
    try:
        resp = openai.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{'role':'user','content':prompt}],
            temperature=0
        )
        sql = resp.choices[0].message.content.strip()
        sql = sanitize_sql(sql)
        tables, columns = extract_tables_columns(sql)
        for t in tables:
            if t.lower().split('.')[-1] not in ALLOWED_TABLES:
                await ctx.send('Consulta não permitida: tabela não autorizada.')
                return
        for c in columns:
            if c and c not in ALLOWED_COLUMNS:
                await ctx.send('Consulta não permitida: coluna não autorizada.')
                return
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchmany(20)
        cur.close(); conn.close()
        if not rows:
            await ctx.send('Nenhum resultado encontrado.')
            return
        lines = []
        for r in rows:
            parts = [f"{k}: {v}" for k,v in r.items()]
            lines.append('; '.join(parts))
        msg = '\n'.join(lines[:5])
        if len(msg) > 1900:
            msg = msg[:1900] + '\n...'
        await ctx.send(f'```{msg}```')
    except Exception as e:
        await ctx.send(f'Erro ao processar a consulta: {e}')

if __name__ == '__main__':
    bot.run(DISCORD_TOKEN)
