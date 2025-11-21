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
    generate_best_date,
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
    """Fetch last year's same month data from KMA ASOS API (noon data only to stay under 1000 limit)"""
    _, num_days = monthrange(year, month)
    
    start_dt = f"{year}{month:02d}01"
    end_dt = f"{year}{month:02d}{num_days}"
    
    service_key = quote(SERVICE_KEY_RAW, safe='')
    
    # Request only noon (12:00) data for each day to stay under 1000 record limit
    # This gives us 30-31 records instead of 720-744
    params = {
        'serviceKey': service_key,
        'numOfRows': '100',
        'pageNo': '1',
        'dataType': 'JSON',
        'dataCd': 'ASOS',
        'dateCd': 'HR',
        'startDt': start_dt,
        'endDt': end_dt,
        'startHh': '12',  # Only fetch noon data
        'endHh': '12',
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


def fetch_monthly_data(**context):
    """Step 1: Fetch last year's monthly weather data"""
    now = datetime.now()
    year = now.year
    month = now.month
    
    print(f"✓ Fetching data for {year}-{month:02d} analysis")
    
    # Fetch last year's data
    last_year = year - 1
    monthly_data = fetch_last_year_month_data(last_year, month)
    
    if not monthly_data:
        logging.warning("No monthly data available from API")
        # Push empty data to trigger fallback in next task
        context['task_instance'].xcom_push(key='raw_monthly_data', value=[])
        context['task_instance'].xcom_push(key='year', value=year)
        context['task_instance'].xcom_push(key='month', value=month)
        return []
    
    print(f"✓ Fetched {len(monthly_data)} monthly records")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='raw_monthly_data', value=monthly_data)
    context['task_instance'].xcom_push(key='year', value=year)
    context['task_instance'].xcom_push(key='month', value=month)
    
    return monthly_data


def calculate_monthly_scores(**context):
    """Step 2: Calculate daily beauty scores for the month"""
    raw_data = context['task_instance'].xcom_pull(key='raw_monthly_data', task_ids='fetch_monthly_data')
    year = context['task_instance'].xcom_pull(key='year', task_ids='fetch_monthly_data')
    month = context['task_instance'].xcom_pull(key='month', task_ids='fetch_monthly_data')
    
    if not raw_data:
        # Use fallback data
        print(f"✓ Using fallback data for {year}-{month:02d}")
        predictions = generate_fallback_monthly_scores(year, month)
    else:
        # Process real data
        daily_scores = {}
        daily_items = {}
        
        for item in raw_data:
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
        
        predictions = {'daily_scores': daily_scores}
    
    print(f"✓ Calculated scores for {len(predictions['daily_scores'])} days")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='daily_scores', value=predictions['daily_scores'])
    
    return predictions['daily_scores']


def find_special_dates(**context):
    """Step 3: Identify special dates (top 3 best days)"""
    daily_scores = context['task_instance'].xcom_pull(key='daily_scores', task_ids='calculate_monthly_scores')
    
    if not daily_scores:
        raise ValueError("No daily scores from previous task")
    
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
    
    print(f"✓ Found {len(special_dates)} special dates")
    print(f"✓ Best day: {best_day}일 (score: {daily_scores[best_day]})")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='special_dates', value=special_dates)
    context['task_instance'].xcom_push(key='best_day', value=best_day)
    
    return special_dates


def generate_calendar_component(**context):
    """Step 4: Generate and write monthly-calendar component"""
    year = context['task_instance'].xcom_pull(key='year', task_ids='fetch_monthly_data')
    month = context['task_instance'].xcom_pull(key='month', task_ids='fetch_monthly_data')
    special_dates = context['task_instance'].xcom_pull(key='special_dates', task_ids='find_special_dates')
    
    if not special_dates:
        raise ValueError("No special dates from previous task")
    
    # Prepare special dates for calendar (emoji only)
    calendar_special_dates = [
        {'day': sd['day'], 'emoji': sd['emoji']}
        for sd in special_dates
    ]
    
    calendar_html = generate_monthly_calendar(year, month, calendar_special_dates)
    write_component('monthly-calendar', calendar_html)
    
    print(f"✓ Generated monthly-calendar component")


def generate_special_dates_component(**context):
    """Step 5: Generate and write special-dates component"""
    month = context['task_instance'].xcom_pull(key='month', task_ids='fetch_monthly_data')
    special_dates = context['task_instance'].xcom_pull(key='special_dates', task_ids='find_special_dates')
    
    if not special_dates:
        raise ValueError("No special dates from previous task")
    
    # Prepare special dates for list (with descriptions)
    special_dates_data = [
        {
            'month': month,
            'day': sd['day'],
            'description': f"{sd['description']} {sd['emoji']}",
            'color': 'yellow' if i % 2 == 0 else 'pink'
        }
        for i, sd in enumerate(special_dates)
    ]
    
    special_dates_html = generate_special_dates(special_dates_data)
    write_component('special-dates', special_dates_html)
    
    print(f"✓ Generated special-dates component")


def generate_best_date_component(**context):
    """Step 6: Generate and write best-date component"""
    month = context['task_instance'].xcom_pull(key='month', task_ids='fetch_monthly_data')
    best_day = context['task_instance'].xcom_pull(key='best_day', task_ids='find_special_dates')
    
    if best_day is None:
        raise ValueError("No best day from previous task")
    
    best_date_html = generate_best_date(month, best_day)
    write_component('best-date', best_date_html)
    
    print(f"✓ Generated best-date component")


# Define DAG
with DAG(
    'monthly_data_dag',
    default_args=default_args,
    description='Update monthly calendar and special dates using KMA ASOS API',
    schedule='0 2 1 * *',  # Monthly on the 1st at 2 AM
    catchup=False,
    tags=['monthly', 'calendar', 'kma', 'html'],
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_monthly_data',
        python_callable=fetch_monthly_data,
    )
    
    calculate_task = PythonOperator(
        task_id='calculate_monthly_scores',
        python_callable=calculate_monthly_scores,
    )
    
    special_dates_task = PythonOperator(
        task_id='find_special_dates',
        python_callable=find_special_dates,
    )
    
    calendar_component_task = PythonOperator(
        task_id='generate_calendar_component',
        python_callable=generate_calendar_component,
    )
    
    special_dates_component_task = PythonOperator(
        task_id='generate_special_dates_component',
        python_callable=generate_special_dates_component,
    )
    
    best_date_component_task = PythonOperator(
        task_id='generate_best_date_component',
        python_callable=generate_best_date_component,
    )
    
    fetch_task >> calculate_task >> special_dates_task >> [calendar_component_task, special_dates_component_task, best_date_component_task]
