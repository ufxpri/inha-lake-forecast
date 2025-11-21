"""
Monthly Data DAG - Updates once per month on the 1st
Generates: monthly-calendar, special-dates
Uses KMA ASOS API to analyze last year's same month data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import requests
from urllib.parse import quote
from calendar import monthrange
import logging

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

from html_generators import (
    generate_monthly_calendar,
    generate_special_dates,
    write_component
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
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


def fetch_last_year_month_data(year, month):
    """Fetch last year's same month data from KMA ASOS API"""
    _, num_days = monthrange(year, month)
    
    start_dt = f"{year}{month:02d}01"
    end_dt = f"{year}{month:02d}{num_days}"
    
    service_key = quote(SERVICE_KEY_RAW, safe='')
    
    params = {
        'serviceKey': service_key,
        'numOfRows': '1000',  # Request enough for full month
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
        response = requests.get(API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        header = data.get('response', {}).get('header', {})
        if header.get('resultCode') == '00':
            items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            return items if isinstance(items, list) else [items] if items else []
        
        logging.error(f"API Error: {header.get('resultMsg')}")
        return []
        
    except Exception as e:
        logging.error(f"Failed to fetch monthly data: {e}")
        return []


def predict_monthly_beauty_scores(year, month):
    """
    Analyze last year's same month data to predict beauty scores
    
    Returns:
        dict: {
            'daily_scores': {day: score, ...},
            'special_dates': [{'day': int, 'emoji': str, 'score': int}, ...],
            'best_day': int
        }
    """
    # Fetch last year's data
    last_year = year - 1
    monthly_data = fetch_last_year_month_data(last_year, month)
    
    if not monthly_data:
        logging.warning("No monthly data available, using fallback")
        return generate_fallback_monthly_scores(year, month)
    
    # Group by day and calculate average scores
    daily_scores = {}
    daily_items = {}
    
    for item in monthly_data:
        try:
            tm = item.get('tm', '')
            if not tm:
                continue
            
            # Extract day from timestamp
            day = int(tm.split()[0].split('-')[2])
            
            if day not in daily_items:
                daily_items[day] = []
            daily_items[day].append(item)
        except Exception as e:
            logging.error(f"Error processing item: {e}")
            continue
    
    # Calculate average score for each day
    for day, items in daily_items.items():
        scores = [calculate_beauty_score(item) for item in items]
        daily_scores[day] = int(sum(scores) / len(scores)) if scores else 50
    
    # Find top 3 special dates
    sorted_days = sorted(daily_scores.items(), key=lambda x: x[1], reverse=True)
    top_days = sorted_days[:3] if len(sorted_days) >= 3 else sorted_days
    
    special_dates = []
    emoji_options = ['✨', '🥰', '💫']
    descriptions = [
        '가장 맑은 날',
        '가장 따뜻한 날',
        '가장 구름 없는 날'
    ]
    
    for i, (day, score) in enumerate(top_days):
        special_dates.append({
            'day': day,
            'emoji': emoji_options[i],
            'score': score,
            'description': descriptions[i]
        })
    
    best_day = top_days[0][0] if top_days else 15
    
    return {
        'daily_scores': daily_scores,
        'special_dates': special_dates,
        'best_day': best_day
    }


def generate_fallback_monthly_scores(year, month):
    """Generate fallback scores when API data is unavailable"""
    import random
    from calendar import monthrange
    
    _, num_days = monthrange(year, month)
    daily_scores = {}
    
    for day in range(1, num_days + 1):
        daily_scores[day] = random.randint(40, 90)
    
    sorted_days = sorted(daily_scores.items(), key=lambda x: x[1], reverse=True)
    top_days = sorted_days[:3]
    
    special_dates = [
        {'day': top_days[0][0], 'emoji': '✨', 'score': top_days[0][1], 'description': '가장 맑은 날'},
        {'day': top_days[1][0], 'emoji': '🥰', 'score': top_days[1][1], 'description': '가장 따뜻한 날'},
        {'day': top_days[2][0], 'emoji': '💫', 'score': top_days[2][1], 'description': '가장 구름 없는 날'}
    ]
    
    return {
        'daily_scores': daily_scores,
        'special_dates': special_dates,
        'best_day': top_days[0][0]
    }


def update_monthly_components(**context):
    """
    Main task: Update monthly-calendar and special-dates components
    """
    now = datetime.now()
    year = now.year
    month = now.month
    
    print(f"✓ Predicting beauty scores for {year}-{month:02d}")
    
    # Step 1: Predict monthly beauty scores
    predictions = predict_monthly_beauty_scores(year, month)
    
    daily_scores = predictions['daily_scores']
    special_dates = predictions['special_dates']
    best_day = predictions['best_day']
    
    print(f"✓ Predicted {len(daily_scores)} days")
    print(f"✓ Best day: {month}월 {best_day}일 (score: {daily_scores[best_day]})")
    print(f"✓ Special dates: {len(special_dates)}")
    
    # Step 2: Prepare special dates for calendar (emoji only)
    calendar_special_dates = [
        {'day': sd['day'], 'emoji': sd['emoji']}
        for sd in special_dates
    ]
    
    # Step 3: Generate and write monthly-calendar
    calendar_html = generate_monthly_calendar(year, month, calendar_special_dates)
    write_component('monthly-calendar', calendar_html)
    
    # Step 4: Prepare special dates for list (with descriptions)
    special_dates_data = [
        {
            'month': month,
            'day': sd['day'],
            'description': f"{sd['description']} {sd['emoji']}",
            'color': 'yellow' if i % 2 == 0 else 'pink'
        }
        for i, sd in enumerate(special_dates)
    ]
    
    # Step 5: Generate and write special-dates
    special_dates_html = generate_special_dates(special_dates_data)
    write_component('special-dates', special_dates_html)
    
    print(f"✓ Updated monthly components at {now.isoformat()}")
    
    # Push data to XCom
    context['task_instance'].xcom_push(key='best_day', value=best_day)
    context['task_instance'].xcom_push(key='special_dates', value=special_dates)
    context['task_instance'].xcom_push(key='daily_scores', value=daily_scores)


# Define DAG
with DAG(
    'monthly_data_dag',
    default_args=default_args,
    description='Update monthly calendar and special dates using KMA ASOS API',
    schedule='0 2 1 * *',  # Monthly on the 1st at 2 AM
    catchup=False,
    tags=['monthly', 'calendar', 'kma', 'html'],
) as dag:
    
    update_task = PythonOperator(
        task_id='update_monthly_components',
        python_callable=update_monthly_components,
    )
