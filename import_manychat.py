import requests
import json
import psycopg2
import logging
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações da API ManyChat
api_token = os.getenv('API_TOKEN')
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}

# Configurações do PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

# Configurações de logging
logging.basicConfig(
    filename='manychat_leads.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Valores de campo "Fase" para realizar as requisições
fase_values = ['1', '2', 'rm1']

def fetch_and_save_leads(fase_value):
    url = f'https://api.manychat.com/fb/subscriber/findByCustomField?field_id=11386122&field_value={fase_value}'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        leads = response.json().get('data', [])
        for lead in leads:
            custom_fields = {field['name']: field['value'] for field in lead['custom_fields']}
            lead_data = (
                lead['id'],
                lead['first_name'],
                lead['last_name'],
                lead['name'],
                lead['status'],
                lead['live_chat_url'],
                lead['subscribed'],
                lead['last_input_text'],
                fase_value,
                custom_fields.get('Lead'),
                custom_fields.get('Fase')
            )
            save_to_db(lead_data)
    else:
        logging.error(f"Erro ao buscar leads para fase {fase_value}: {response.status_code} - {response.text}")

def save_to_db(lead_data):
    insert_query = """
    INSERT INTO leads (subscriber_id, first_name, last_name, name, status, live_chat_url, subscribed, last_input_text, phase, custom_field_lead, custom_field_fase)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (subscriber_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        name = EXCLUDED.name,
        status = EXCLUDED.status,
        live_chat_url = EXCLUDED.live_chat_url,
        subscribed = EXCLUDED.subscribed,
        last_input_text = EXCLUDED.last_input_text,
        phase = EXCLUDED.phase,
        custom_field_lead = EXCLUDED.custom_field_lead,
        custom_field_fase = EXCLUDED.custom_field_fase;
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(insert_query, lead_data)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Lead {lead_data[0]} salvo/atualizado com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar lead {lead_data[0]}: {e}")

# Iterar sobre os valores de campo "Fase" e buscar dados
for value in fase_values:
    fetch_and_save_leads(value)

logging.info("Processo concluído.")
