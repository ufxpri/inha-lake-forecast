"""
Example usage of html_generators in Airflow DAGs
"""
from html_generators import (
    generate_current_status,
    generate_hero_image,
    generate_best_time,
    generate_hourly_chart,
    generate_best_date,
    generate_monthly_calendar,
    generate_special_dates,
    write_component
)
from datetime import datetime


# ============================================
# CURRENT DAG - Updates every 10 minutes
# ============================================
def update_current_components():
    """
    Task for current_data_dag.py
    Updates: current-status, hero-image
    """
    # Example: Get current weather data from your source
    weather_data = {
        'condition': '맑음',
        'emoji': '☀️'
    }
    
    # Generate and write current-status
    status_html = generate_current_status(weather_data)
    write_component('current-status', status_html)
    
    # Generate and write hero-image
    # Assuming you have logic to save latest image to /images/latest.jpg
    hero_html = generate_hero_image('/images/latest.jpg')
    write_component('hero-image', hero_html)


# ============================================
# DAILY DAG - Updates once per day
# ============================================
def update_daily_components():
    """
    Task for daily_data_dag.py
    Updates: best-time, hourly-chart
    """
    # Example: Get today's hourly data for all 24 hours (00:00 to 23:00)
    hourly_data = [
        {'hour': 0, 'score': 30, 'emoji': '😴'},
        {'hour': 1, 'score': 25, 'emoji': '😴'},
        {'hour': 2, 'score': 20, 'emoji': '😴'},
        {'hour': 3, 'score': 22, 'emoji': '😴'},
        {'hour': 4, 'score': 28, 'emoji': '😴'},
        {'hour': 5, 'score': 35, 'emoji': '😴'},
        {'hour': 6, 'score': 45, 'emoji': '🌅'},
        {'hour': 7, 'score': 60, 'emoji': '🌅'},
        {'hour': 8, 'score': 70, 'emoji': '🌅'},
        {'hour': 9, 'score': 75, 'emoji': '😊'},
        {'hour': 10, 'score': 80, 'emoji': '😊'},
        {'hour': 11, 'score': 85, 'emoji': '😊'},
        {'hour': 12, 'score': 88, 'emoji': '🙂'},
        {'hour': 13, 'score': 90, 'emoji': '🙂'},
        {'hour': 14, 'score': 92, 'emoji': '🙂'},
        {'hour': 15, 'score': 95, 'emoji': '😆'},
        {'hour': 16, 'score': 98, 'emoji': '😆'},
        {'hour': 17, 'score': 100, 'emoji': '🤩'},  # Best time: 17:00
        {'hour': 18, 'score': 95, 'emoji': '🌆'},
        {'hour': 19, 'score': 85, 'emoji': '🌆'},
        {'hour': 20, 'score': 70, 'emoji': '🌆'},
        {'hour': 21, 'score': 60, 'emoji': '😌'},
        {'hour': 22, 'score': 50, 'emoji': '😌'},
        {'hour': 23, 'score': 40, 'emoji': '😌'},
    ]
    
    # Find best time
    best_entry = max(hourly_data, key=lambda x: x['score'])
    best_hour = best_entry['hour']
    
    # Generate and write best-time
    best_time_html = generate_best_time(best_hour, 0)  # Assuming best time is on the hour
    write_component('best-time', best_time_html)
    
    # Generate and write hourly-chart
    chart_html = generate_hourly_chart(hourly_data)
    write_component('hourly-chart', chart_html)


# ============================================
# MONTHLY DAG - Updates once per month
# ============================================
def update_monthly_components():
    """
    Task for monthly_data_dag.py
    Updates: best-date, monthly-calendar, special-dates
    """
    now = datetime.now()
    year = now.year
    month = now.month
    
    # Example: Get monthly analysis data
    special_dates = [
        {'day': 5, 'emoji': '✨'},
        {'day': 14, 'emoji': '🥰'},
        {'day': 21, 'emoji': '💫'},
    ]
    
    # Best date of the month
    best_day = 14
    
    # Generate and write best-date
    best_date_html = generate_best_date(month, best_day)
    write_component('best-date', best_date_html)
    
    # Generate and write monthly-calendar
    calendar_html = generate_monthly_calendar(year, month, special_dates)
    write_component('monthly-calendar', calendar_html)
    
    # Generate and write special-dates
    dates_data = [
        {'month': month, 'day': 5, 'description': '가장 맑은 날 ✨', 'color': 'yellow'},
        {'month': month, 'day': 14, 'description': '가장 따뜻한 날 😍', 'color': 'pink'},
        {'month': month, 'day': 21, 'description': '가장 구름 없는 날 🪁', 'color': 'yellow'},
    ]
    special_dates_html = generate_special_dates(dates_data)
    write_component('special-dates', special_dates_html)


# ============================================
# Integration with Airflow DAG
# ============================================
"""
In your actual DAG files, use like this:

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from html_generators_example import update_current_components

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'current_data_dag',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
) as dag:
    
    update_task = PythonOperator(
        task_id='update_current_components',
        python_callable=update_current_components,
    )
"""
