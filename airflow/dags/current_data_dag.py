"""
Current Data DAG - Updates every 1 hour
Generates: current-status, hero-image
Uses KMA ASOS API to fetch real-time weather data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import requests
from urllib.parse import quote
import logging

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

from html_generators import (
    generate_current_status,
    generate_hero_image,
    write_component
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# KMA ASOS API Configuration
API_URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"
STN_ID = "112"  # 인천 지점
SERVICE_KEY_RAW = "c51ff7c6232a4c5da9142dda7cfaa19a393b32eb90734a86baa46057dec1db7b"

# Weather phenomena penalties
WEATHER_PHENOMENA_PENALTIES = {
    '01': -30, '02': -30, '03': -40, '05': -40, '06': -40,  # 비, 눈
    '16': -25, '17': -20, '19': -15,  # 안개, 박무
    '40': -35, '42': -50, '43': -15,  # 연무, 황사
    '22': -45, '85': -50  # 눈보라, 폭풍
}


def calculate_beauty_score(item):
    """Calculate beauty score based on weather data"""
    score = 50.0
    
    try:
        visibility = float(item.get('vs', 0) or 0) * 10  # 시정 (10m 단위)
        cloud_amount = float(item.get('dc10Tca', 10) or 10)  # 전운량
        temperature = float(item.get('ta', 18.0) or 18.0)  # 기온
        phenomena = item.get('dmstMtphNo', 'N/A')  # 현상번호
    except ValueError:
        return 0
    
    # Visibility scoring
    if visibility > 25000:
        score += 20
    elif visibility > 10000:
        score += 10
    elif visibility < 5000:
        score -= 25
    
    # Cloud amount scoring
    if cloud_amount == 0:
        score += 15
    elif 2 <= cloud_amount <= 5:
        score += 10
    elif cloud_amount >= 8:
        score -= 15
    
    # Temperature scoring
    OPTIMAL_TEMP = 21.0
    temp_diff = abs(temperature - OPTIMAL_TEMP)
    if temp_diff <= 3:
        score += 10
    elif temp_diff > 10:
        score -= 10
    
    # Weather phenomena penalty
    if phenomena != 'N/A' and phenomena in WEATHER_PHENOMENA_PENALTIES:
        score += WEATHER_PHENOMENA_PENALTIES[phenomena]
    
    return int(max(0, min(100, score)))


def fetch_current_weather():
    """Fetch latest weather data from KMA ASOS API"""
    yesterday = datetime.now() - timedelta(days=1)
    start_dt = yesterday.strftime("%Y%m%d")
    end_dt = yesterday.strftime("%Y%m%d")
    
    service_key = quote(SERVICE_KEY_RAW, safe='')
    
    params = {
        'serviceKey': service_key,
        'numOfRows': '1',
        'pageNo': '1',
        'dataType': 'JSON',
        'dataCd': 'ASOS',
        'dateCd': 'HR',
        'startDt': start_dt,
        'endDt': end_dt,
        'startHh': '23',
        'endHh': '23',
        'stnIds': STN_ID
    }
    
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        header = data.get('response', {}).get('header', {})
        if header.get('resultCode') == '00':
            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            if items:
                return items[0] if isinstance(items, list) else items
        
        logging.error(f"API Error: {header.get('resultMsg')}")
        return None
        
    except Exception as e:
        logging.error(f"Failed to fetch weather data: {e}")
        return None


def get_weather_condition(cloud_amount, phenomena):
    """Determine weather condition and emoji"""
    if phenomena in ['01', '02', '03', '05', '06']:
        return '비/눈', '🌧️'
    elif phenomena in ['40', '42']:
        return '황사/연무', '🌫️'
    elif phenomena in ['16', '17', '19']:
        return '안개', '🌁'
    
    cloud_amount = float(cloud_amount or 10)
    if cloud_amount <= 2:
        return '맑음', '☀️'
    elif cloud_amount <= 5:
        return '구름 조금', '⛅'
    elif cloud_amount <= 7:
        return '흐림', '☁️'
    else:
        return '매우 흐림', '🌫️'


def update_current_components(**context):
    """
    Main task: Update current-status and hero-image components
    """
    # Step 1: Fetch current weather data from KMA API
    weather_item = fetch_current_weather()
    
    if not weather_item:
        logging.warning("Failed to fetch weather data, skipping update")
        return
    
    # Step 2: Calculate beauty score
    score = calculate_beauty_score(weather_item)
    
    # Step 3: Determine weather condition
    cloud_amount = weather_item.get('dc10Tca', 10)
    phenomena = weather_item.get('dmstMtphNo', 'N/A')
    condition, emoji = get_weather_condition(cloud_amount, phenomena)
    
    print(f"✓ Beauty score: {score}")
    print(f"✓ Weather: {condition} {emoji}")
    print(f"✓ Data: 기온 {weather_item.get('ta')}°C, 시정 {weather_item.get('vs')}x10m, 운량 {cloud_amount}/10")
    
    # Step 4: Generate and write current-status
    weather_data = {
        'condition': condition,
        'emoji': emoji
    }
    status_html = generate_current_status(weather_data)
    write_component('current-status', status_html)
    
    # Step 5: Generate and write hero-image
    hero_html = generate_hero_image('/images/latest.jpg')
    write_component('hero-image', hero_html)
    
    print(f"✓ Updated current components at {datetime.now().isoformat()}")
    
    # Push data to XCom
    context['task_instance'].xcom_push(key='beauty_score', value=score)
    context['task_instance'].xcom_push(key='weather_condition', value=condition)


# Define DAG
with DAG(
    'current_data_dag',
    default_args=default_args,
    description='Update current weather status and hero image every hour using KMA ASOS API',
    schedule='0 * * * *',  # Every hour
    catchup=False,
    tags=['current', 'weather', 'kma', 'html'],
) as dag:
    
    update_task = PythonOperator(
        task_id='update_current_components',
        python_callable=update_current_components,
    )
