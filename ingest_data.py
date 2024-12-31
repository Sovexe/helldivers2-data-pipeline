import requests
import pandas as pd
import psycopg2
import logging
from datetime import datetime
import os
import json

# Configure logging
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Define API endpoints
API_BASE_URL = 'https://helldiverstrainingmanual.com/api/v1/war'
PLANETS_URL = 'https://helldiverstrainingmanual.com/api/v1/planets'

ENDPOINTS = {
    'status': f'{API_BASE_URL}/status',
    'info': f'{API_BASE_URL}/info',
    'news': f'{API_BASE_URL}/news',
    'campaign': f'{API_BASE_URL}/campaign',
    'major_orders': f'{API_BASE_URL}/major-orders'
}

HISTORY_URL_TEMPLATE = f'{API_BASE_URL}/history/{{planetIndex}}'

def fetch_json(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f'Successfully fetched data from {url}')
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching data from {url}: {e}')
        return None

def fetch_all_data():
    data = {}
    
    # Fetch static endpoints
    for key, url in ENDPOINTS.items():
        data[key] = fetch_json(url)
    
    # Fetch planets data
    planets_data = fetch_json(PLANETS_URL)
    data['planets'] = planets_data
    
    # Fetch history for each planet (optional, can be time-consuming)
    # Uncomment the following lines if you want to include history data
    """
    history_data = {}
    if planets_data:
        for planet_index in planets_data.keys():
            history_url = HISTORY_URL_TEMPLATE.format(planetIndex=planet_index)
            history = fetch_json(history_url)
            if history:
                history_data[planet_index] = history
    data['history'] = history_data
    """
    
    return data

def store_data(data):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = conn.cursor()
        
        # Start transaction
        conn.autocommit = False
        
        # Create tables with PRIMARY KEY constraints
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS war_status (
            planetIndex INTEGER PRIMARY KEY,
            owner INTEGER,
            health INTEGER,
            regenPerSecond FLOAT,
            players INTEGER,
            warId INTEGER,
            time INTEGER,
            impactMultiplier FLOAT,
            storyBeatId32 INTEGER
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS war_info (
            planetIndex INTEGER PRIMARY KEY,
            settingsHash INTEGER,
            position_x FLOAT,
            position_y FLOAT,
            waypoints INTEGER[],
            sector INTEGER,
            maxHealth INTEGER,
            disabled BOOLEAN,
            initialOwner INTEGER,
            warId INTEGER,
            startDate INTEGER,
            endDate INTEGER,
            minimumClientVersion VARCHAR
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS war_news (
            id INTEGER PRIMARY KEY,
            published INTEGER,
            type INTEGER,
            tagIds INTEGER[],
            message TEXT
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS war_campaign (
            planetIndex INTEGER PRIMARY KEY,
            name VARCHAR,
            faction VARCHAR,
            players INTEGER,
            health INTEGER,
            maxHealth INTEGER,
            percentage FLOAT,
            defense BOOLEAN,
            majorOrder BOOLEAN,
            biome_slug VARCHAR,
            biome_description TEXT,
            expireDateTime TIMESTAMP
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS war_major_orders (
            id32 INTEGER PRIMARY KEY,
            progress INTEGER[],
            expiresIn INTEGER,
            setting_type INTEGER,
            setting_overrideTitle VARCHAR,
            setting_overrideBrief TEXT,
            setting_taskDescription TEXT,
            setting_tasks JSONB,
            setting_reward_type INTEGER,
            setting_reward_id32 INTEGER,
            setting_reward_amount INTEGER,
            setting_flags INTEGER
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS planets (
            planetIndex INTEGER PRIMARY KEY,
            name VARCHAR,
            sector VARCHAR,
            biome_slug VARCHAR,
            biome_description TEXT,
            environmentals JSONB
        );
        """)
        
        # Insert or replace data into tables
        if 'status' in data and data['status']:
            planet_status = data['status'].get('planetStatus', [])
            warId = data['status'].get('warId')
            time = data['status'].get('time')
            impactMultiplier = data['status'].get('impactMultiplier')
            storyBeatId32 = data['status'].get('storyBeatId32')
            
            for status in planet_status:
                cursor.execute("""
                INSERT INTO war_status (planetIndex, owner, health, regenPerSecond, players, warId, time, impactMultiplier, storyBeatId32)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (planetIndex) DO UPDATE
                SET owner = EXCLUDED.owner,
                    health = EXCLUDED.health,
                    regenPerSecond = EXCLUDED.regenPerSecond,
                    players = EXCLUDED.players,
                    warId = EXCLUDED.warId,
                    time = EXCLUDED.time,
                    impactMultiplier = EXCLUDED.impactMultiplier,
                    storyBeatId32 = EXCLUDED.storyBeatId32;
                """, (
                    status.get('index'),
                    status.get('owner'),
                    status.get('health'),
                    status.get('regenPerSecond'),
                    status.get('players'),
                    warId,
                    time,
                    impactMultiplier,
                    storyBeatId32
                ))
            
            logging.info('Stored war_status data successfully.')
        
        if 'info' in data and data['info']:
            planet_infos = data['info'].get('planetInfos', [])
            warId = data['info'].get('warId')
            startDate = data['info'].get('startDate')
            endDate = data['info'].get('endDate')
            minimumClientVersion = data['info'].get('minimumClientVersion')
            
            for info in planet_infos:
                position = info.get('position', {})
                waypoints = info.get('waypoints', [])
                cursor.execute("""
                INSERT INTO war_info (planetIndex, settingsHash, position_x, position_y, waypoints, sector, maxHealth, disabled, initialOwner, warId, startDate, endDate, minimumClientVersion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (planetIndex) DO UPDATE
                SET settingsHash = EXCLUDED.settingsHash,
                    position_x = EXCLUDED.position_x,
                    position_y = EXCLUDED.position_y,
                    waypoints = EXCLUDED.waypoints,
                    sector = EXCLUDED.sector,
                    maxHealth = EXCLUDED.maxHealth,
                    disabled = EXCLUDED.disabled,
                    initialOwner = EXCLUDED.initialOwner,
                    warId = EXCLUDED.warId,
                    startDate = EXCLUDED.startDate,
                    endDate = EXCLUDED.endDate,
                    minimumClientVersion = EXCLUDED.minimumClientVersion;
                """, (
                    info.get('index'),
                    info.get('settingsHash'),
                    position.get('x'),
                    position.get('y'),
                    waypoints,
                    info.get('sector'),
                    info.get('maxHealth'),
                    info.get('disabled'),
                    info.get('initialOwner'),
                    warId,
                    startDate,
                    endDate,
                    minimumClientVersion
                ))
            
            logging.info('Stored war_info data successfully.')
        
        if 'news' in data and data['news']:
            news_items = data['news']
            for news in news_items:
                cursor.execute("""
                INSERT INTO war_news (id, published, type, tagIds, message)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """, (
                    news.get('id'),
                    news.get('published'),
                    news.get('type'),
                    news.get('tagIds'),
                    news.get('message')
                ))
            logging.info('Stored war_news data successfully.')
        
        if 'campaign' in data and data['campaign']:
            campaigns = data['campaign']
            for campaign in campaigns:
                biome = campaign.get('biome', {})
                cursor.execute("""
                INSERT INTO war_campaign (planetIndex, name, faction, players, health, maxHealth, percentage, defense, majorOrder, biome_slug, biome_description, expireDateTime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (planetIndex) DO UPDATE
                SET name = EXCLUDED.name,
                    faction = EXCLUDED.faction,
                    players = EXCLUDED.players,
                    health = EXCLUDED.health,
                    maxHealth = EXCLUDED.maxHealth,
                    percentage = EXCLUDED.percentage,
                    defense = EXCLUDED.defense,
                    majorOrder = EXCLUDED.majorOrder,
                    biome_slug = EXCLUDED.biome_slug,
                    biome_description = EXCLUDED.biome_description,
                    expireDateTime = EXCLUDED.expireDateTime;
                """, (
                    campaign.get('planetIndex'),
                    campaign.get('name'),
                    campaign.get('faction'),
                    campaign.get('players'),
                    campaign.get('health'),
                    campaign.get('maxHealth'),
                    campaign.get('percentage'),
                    campaign.get('defense'),
                    campaign.get('majorOrder'),
                    biome.get('slug'),
                    biome.get('description'),
                    campaign.get('expireDateTime')
                ))
            logging.info('Stored war_campaign data successfully.')
        
        if 'major_orders' in data and data['major_orders']:
            major_orders = data['major_orders']
            for order in major_orders:
                setting = order.get('setting', {})
                cursor.execute("""
                INSERT INTO war_major_orders (
                    id32, progress, expiresIn, setting_type, setting_overrideTitle,
                    setting_overrideBrief, setting_taskDescription, setting_tasks,
                    setting_reward_type, setting_reward_id32, setting_reward_amount,
                    setting_flags
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id32) DO UPDATE
                SET progress = EXCLUDED.progress,
                    expiresIn = EXCLUDED.expiresIn,
                    setting_type = EXCLUDED.setting_type,
                    setting_overrideTitle = EXCLUDED.setting_overrideTitle,
                    setting_overrideBrief = EXCLUDED.setting_overrideBrief,
                    setting_taskDescription = EXCLUDED.setting_taskDescription,
                    setting_tasks = EXCLUDED.setting_tasks,
                    setting_reward_type = EXCLUDED.setting_reward_type,
                    setting_reward_id32 = EXCLUDED.setting_reward_id32,
                    setting_reward_amount = EXCLUDED.setting_reward_amount,
                    setting_flags = EXCLUDED.setting_flags;
                """, (
                    order.get('id32'),
                    order.get('progress'),
                    order.get('expiresIn'),
                    setting.get('type'),
                    setting.get('overrideTitle'),
                    setting.get('overrideBrief'),
                    setting.get('taskDescription'),
                    json.dumps(setting.get('tasks')),
                    setting.get('reward', {}).get('type'),
                    setting.get('reward', {}).get('id32'),
                    setting.get('reward', {}).get('amount'),
                    setting.get('flags')
                ))
            logging.info('Stored war_major_orders data successfully.')
        
        if 'planets' in data and data['planets']:
            planets = data['planets']
            for planet_index, planet in planets.items():
                biome = planet.get('biome', {})
                environmentals = planet.get('environmentals', [])
                cursor.execute("""
                INSERT INTO planets (planetIndex, name, sector, biome_slug, biome_description, environmentals)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (planetIndex) DO UPDATE
                SET name = EXCLUDED.name,
                    sector = EXCLUDED.sector,
                    biome_slug = EXCLUDED.biome_slug,
                    biome_description = EXCLUDED.biome_description,
                    environmentals = EXCLUDED.environmentals;
                """, (
                    int(planet_index),
                    planet.get('name'),
                    planet.get('sector'),
                    biome.get('slug'),
                    biome.get('description'),
                    json.dumps(environmentals)
                ))
            logging.info('Stored planets data successfully.')
        
        # If you included history data, handle it here
        """
        if 'history' in data and data['history']:
            for planet_index, history in data['history'].items():
                for record in history:
                    cursor.execute(f\"""
                    INSERT INTO history_{planet_index} (created_at, planet_index, current_health, max_health, player_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (created_at) DO UPDATE
                    SET current_health = EXCLUDED.current_health,
                        max_health = EXCLUDED.max_health,
                        player_count = EXCLUDED.player_count;
                    \""", (
                        record.get('created_at'),
                        record.get('planet_index'),
                        record.get('current_health'),
                        record.get('max_health'),
                        record.get('player_count')
                    ))
                logging.info(f'Stored history data for planet {planet_index} successfully.')
        """
        
        # Commit transaction
        conn.commit()
        logging.info('All data stored successfully.')
        
        # Close connection
        cursor.close()
        conn.close()
    except Exception as e:
        conn.rollback()
        logging.error(f'Error storing data: {e}')

def main():
    logging.info('Pipeline started.')
    data = fetch_all_data()
    store_data(data)
    logging.info('Pipeline finished successfully.')

if __name__ == '__main__':
    main()
