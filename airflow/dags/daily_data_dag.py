"""
Daily Data DAG - Updates once per day at dawn
Generates: best-time, hourly-chart
Uses KMA ASOS API to fetch yesterday's 24-hour weather data
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
    generate_best_time,
    generate_hourly_chart,
    write_component
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# KMA ASOS API Configuration
API_URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"
STN_ID = "112"  # 인천 지점
SERVICE_KEY_RAW = "c51ff7c6232a4c5da9142dda7cfaa19a393b32eb90734a86baa46057dec1db7b"

# Weather phenomena penalties
WEATHER_PHENOMENA_PENALTIES = {
    '01': -30, '02': -30, '03': -40, '05': -40, '06': -40,
    '16': -25, '17': -20, '19': -15,
    '40': -35, '42': -50, '43': -15,
    '22': -45, '85': -50
}


def calculate_beauty_score(item):
    """Calculate beauty score based on weather data"""
    score = 50.0
    
    try:
        visibility = float(item.get('vs', 0) or 0) * 10
        cloud_amount = float(item.get('dc10Tca', 10) or 10)
        temperature = float(item.get('ta', 18.0) or 18.0)
        phenomena = item.get('dmstMtphNo', 'N/A')
    except ValueError:
        return 0
    
    if visibility > 25000:
        score += 20
    elif visibility > 10000:
        score += 10
    elif visibility < 5000:
        score -= 25
    
    if cloud_amount == 0:
        score += 15
    elif 2 <= cloud_amount <= 5:
        score += 10
    elif cloud_amount >= 8:
        score -= 15
    
    OPTIMAL_TEMP = 21.0
    temp_diff = abs(temperature - OPTIMAL_TEMP)
    if temp_diff <= 3:
        score += 10
    elif temp_diff > 10:
        score -= 10
    
    if phenomena != 'N/A' and phenomena in WEATHER_PHENOMENA_PENALTIES:
        score += WEATHER_PHENOMENA_PENALTIES[phenomena]
    
    return int(max(0, min(100, score)))


def fetch_yesterday_hourly_data():
    """Fetch yesterday's 24-hour weather data from KMA ASOS API"""
    yesterday = datetime.now() - timedelta(days=1)
    start_dt = yesterday.strftime("%Y%m%d")
    end_dt = yesterday.strftime("%Y%m%d")
    
    service_key = quote(SERVICE_KEY_RAW, safe='')
    
    params = {
        'serviceKey': service_key,
        'numOfRows': '100',
        'pageNo': '1',
        'dataType': 'JSON',
        'dataCd': 'ASOS',
        'dateCd': 'HR',
        'startDt': start_dt,
        'endDt': end_dt,
        'startHh': '00',
        'endHh': '23',
        'stnIds': STN_ID
    }
    
    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        header = data.get('response', {}).get('header', {})
        if header.get('resultCode') == '00':
            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            return items if isinstance(items, list) else [items] if items else []
        
        logging.error(f"API Error: {header.get('resultMsg')}")
        return []
        
    except Exception as e:
        logging.error(f"Failed to fetch weather data: {e}")
        return []


def predict_hourly_beauty_scores():
    """
    Calculate beauty scores for yesterday's 24 hours using KMA data
    """
    hourly_data = fetch_yesterday_hourly_data()
    
    if not hourly_data:
        logging.warning("No hourly data available, using fallback")
        return []
    
    hourly_predictions = []
    
    for item in hourly_data:
        try:
            # Extract hour from timestamp (format: YYYY-MM-DD HH:MM)
            tm = item.get('tm', '')
            hour = int(tm.split()[1].split(':')[0]) if ' ' in tm else 0
            
            # Filter: only include hours between 7 AM and 8 PM
            if not (7 <= hour <= 20):
                continue
            
            # Calculate beauty score
            score = calculate_beauty_score(item)
            
            # Determine emoji based on score
            if score >= 90:
                emoji = '🤩'
            elif score >= 80:
                emoji = '😆'
            elif score >= 70:
                emoji = '😊'
            elif score >= 60:
                emoji = '🙂'
            elif score >= 50:
                emoji = '😌'
            elif 7 <= hour <= 8:
                emoji = '🌅'
            elif 19 <= hour <= 20:
                emoji = '🌆'
            else:
                emoji = '😴'
            
            hourly_predictions.append({
                'hour': hour,
                'score': score,
                'emoji': emoji
            })
        except Exception as e:
            logging.error(f"Error processing hourly item: {e}")
            continue
    
    # Sort by hour
    hourly_predictions.sort(key=lambda x: x['hour'])
    
    return hourly_predictions


def update_daily_components(**context):
    """
    Main task: Update best-time and hourly-chart components
    """
    # Step 1: Predict hourly beauty scores for today
    hourly_data = predict_hourly_beauty_scores()
    
    print(f"✓ Predicted beauty scores for {len(hourly_data)} hours (7am-8pm)")
    
    # Step 2: Find best time
    best_entry = max(hourly_data, key=lambda x: x['score'])
    best_hour = best_entry['hour']
    best_score = best_entry['score']
    
    print(f"✓ Best time today: {best_hour:02d}:00 (score: {best_score})")
    
    # Step 3: Generate and write best-time
    best_time_html = generate_best_time(best_hour, 0)
    write_component('best-time', best_time_html)
    
    # Step 4: Generate and write hourly-chart
    chart_html = generate_hourly_chart(hourly_data)
    write_component('hourly-chart', chart_html)
    
    print(f"✓ Updated daily components at {datetime.now().isoformat()}")
    
    # Push data to XCom
    context['task_instance'].xcom_push(key='best_hour', value=best_hour)
    context['task_instance'].xcom_push(key='best_score', value=best_score)
    context['task_instance'].xcom_push(key='hourly_data', value=hourly_data)


# Define DAG
with DAG(
    'daily_data_dag',
    default_args=default_args,
    description='Update daily best time and hourly chart using KMA ASOS API',
    schedule='0 1 * * *',  # Daily at 1 AM (after KMA data update)
    catchup=False,
    tags=['daily', 'hourly', 'kma', 'html'],
) as dag:
    
    update_task = PythonOperator(
        task_id='update_daily_components',
        python_callable=update_daily_components,
    )
