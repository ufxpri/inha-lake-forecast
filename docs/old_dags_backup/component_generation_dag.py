"""
Component Generation DAG for Ingyeongho Beauty Score System.

This DAG is triggered by the score calculation DAG to:
1. Generate HTML components for the mobile-first Korean design
2. Create all HTMX-compatible component files
3. Store components in the shared nginx volume
4. Handle component versioning and caching
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import redis
import psycopg2
from typing import Dict, Any, List


def log_task_failure(context):
    """Callback function for task failures."""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    error_message = f"""
    Task Failure Alert:
    - DAG ID: {dag_id}
    - Task ID: {task_id}
    - Execution Date: {execution_date}
    - Exception: {context.get('exception', 'Unknown error')}
    """
    
    print(f"TASK FAILURE: {error_message}")


def get_redis_client():
    """Get Redis client connection."""
    return redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', '6379')),
        db=int(os.getenv('REDIS_DB', '0')),
        decode_responses=True
    )


def get_db_connection():
    """Get PostgreSQL database connection."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'airflow'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow')
    )


def write_component_file(component_name: str, content: str) -> str:
    """
    Write component content to file in shared nginx volume.
    """
    output_path = f"/opt/airflow/nginx_html/components/{component_name}"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write content
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Generated {component_name} at {output_path}")
    return output_path


def generate_current_status_component(**context):
    """
    Generate current-status component for the mobile design.
    """
    try:
        # Get current beauty score from Redis
        redis_client = get_redis_client()
        score_data_str = redis_client.get('current_beauty_score')
        
        if score_data_str:
            score_data = json.loads(score_data_str)
            status_message = score_data.get('status_message', '최고예요')
            emoji = score_data.get('emoji', '😆')
        else:
            # Fallback to database
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status_message, emoji 
                FROM beauty_scores 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                status_message, emoji = result
            else:
                status_message, emoji = "최고예요", "😆"
        
        content = f"{status_message} {emoji}"
        write_component_file('current-status', content)
        
        return {"status": "success", "component": "current-status"}
        
    except Exception as e:
        print(f"❌ Error generating current status component: {e}")
        # Write fallback content
        write_component_file('current-status', "로딩중... 😐")
        raise


def generate_hero_image_component(**context):
    """
    Generate hero-image component with background image and badge.
    """
    try:
        # Get weather data for appropriate image
        redis_client = get_redis_client()
        weather_data_str = redis_client.get('current_weather_data')
        
        if weather_data_str:
            weather_data = json.loads(weather_data_str)
            condition = weather_data.get('weather_condition', 'clear')
        else:
            condition = 'clear'
        
        # Map weather conditions to appropriate images
        image_mapping = {
            'clear': "https://via.placeholder.com/400x250/87CEEB/FFFFFF?text=Clear+Sky+Ingyeongho",
            'partly_cloudy': "https://via.placeholder.com/400x250/B0C4DE/FFFFFF?text=Partly+Cloudy",
            'cloudy': "https://via.placeholder.com/400x250/708090/FFFFFF?text=Cloudy+Day",
            'overcast': "https://via.placeholder.com/400x250/696969/FFFFFF?text=Overcast",
            'rain': "https://via.placeholder.com/400x250/4682B4/FFFFFF?text=Rainy+Day",
            'snow': "https://via.placeholder.com/400x250/F0F8FF/000000?text=Snowy+Day"
        }
        
        image_url = image_mapping.get(condition, "https://via.placeholder.com/400x250/81C784/FFFFFF?text=Ingyeongho+View")
        
        content = f"""<img src="{image_url}" alt="인경호 풍경" class="hero-img">
<div class="user-badge">👤</div>"""
        
        write_component_file('hero-image', content)
        
        return {"status": "success", "component": "hero-image"}
        
    except Exception as e:
        print(f"❌ Error generating hero image component: {e}")
        # Write fallback content
        fallback_content = """<div style="background: #f0f0f0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; color: #999;">
    이미지 로딩중...
</div>"""
        write_component_file('hero-image', fallback_content)
        raise


def generate_best_time_component(**context):
    """
    Generate best-time component for today's optimal viewing time.
    """
    try:
        # Get hourly predictions from Redis
        redis_client = get_redis_client()
        predictions_str = redis_client.get('hourly_predictions')
        
        if predictions_str:
            predictions = json.loads(predictions_str)
            # Find the best time in the next 12 hours
            current_hour = datetime.now().hour
            next_12_hours = [p for p in predictions if current_hour <= p['hour'] < current_hour + 12]
            
            if next_12_hours:
                best_prediction = max(next_12_hours, key=lambda x: x['score'])
                best_time = f"{best_prediction['hour']:02d}:00"
            else:
                # If no predictions for next 12 hours, find overall best
                best_prediction = max(predictions, key=lambda x: x['score'])
                best_time = f"{best_prediction['hour']:02d}:00"
        else:
            # Fallback calculation
            current_hour = datetime.now().hour
            if 6 <= current_hour <= 8:
                best_time = "07:00"
            elif 17 <= current_hour <= 19:
                best_time = "18:00"
            else:
                best_time = "18:00"  # Default to sunset time
        
        write_component_file('best-time', best_time)
        
        return {"status": "success", "component": "best-time", "time": best_time}
        
    except Exception as e:
        print(f"❌ Error generating best time component: {e}")
        write_component_file('best-time', "--:--")
        raise


def generate_hourly_chart_component(**context):
    """
    Generate hourly-chart component with beauty scores throughout the day.
    """
    try:
        # Get hourly predictions from Redis
        redis_client = get_redis_client()
        predictions_str = redis_client.get('hourly_predictions')
        
        if predictions_str:
            predictions = json.loads(predictions_str)
            # Take first 7 predictions for the chart
            chart_data = predictions[:7]
        else:
            # Generate fallback data
            current_hour = datetime.now().hour
            chart_data = []
            for i in range(7):
                hour = (current_hour + i) % 24
                if 6 <= hour <= 8 or 17 <= hour <= 19:
                    score = 85
                    emoji = "😆"
                elif 9 <= hour <= 16:
                    score = 65
                    emoji = "😊"
                else:
                    score = 45
                    emoji = "🙂"
                
                chart_data.append({
                    'hour': hour,
                    'score': score,
                    'emoji': emoji
                })
        
        # Find the best time for highlighting
        best_index = max(range(len(chart_data)), key=lambda i: chart_data[i]['score'])
        
        # Generate chart HTML
        chart_items = []
        for i, data in enumerate(chart_data):
            active_class = "active" if i == best_index else ""
            bar_color = "#3D6AFE" if i == best_index else "#ccc"
            height = max(30, min(100, data['score']))  # Convert score to height percentage
            
            chart_items.append(f"""
                <div class="chart-item {active_class}">
                    <div class="emoji-face">{data['emoji']}</div>
                    <div class="bar" style="height: {height}%; background-color: {bar_color};"></div>
                    <div class="time-label">{data['hour']:02d}:00</div>
                </div>
            """.strip())
        
        content = "".join(chart_items)
        write_component_file('hourly-chart', content)
        
        return {"status": "success", "component": "hourly-chart"}
        
    except Exception as e:
        print(f"❌ Error generating hourly chart component: {e}")
        # Write fallback content
        fallback_content = """<div style="display: flex; align-items: center; justify-content: center; width: 100%; color: #999;">
    차트 로딩중...
</div>"""
        write_component_file('hourly-chart', fallback_content)
        raise


def generate_best_date_component(**context):
    """
    Generate best-date component for this month's optimal day.
    """
    try:
        # For now, use current date as best date
        # In production, this would analyze historical and forecast data
        current_date = datetime.now()
        best_date = f"{current_date.month}월 {current_date.day}일"
        
        write_component_file('best-date', best_date)
        
        return {"status": "success", "component": "best-date", "date": best_date}
        
    except Exception as e:
        print(f"❌ Error generating best date component: {e}")
        write_component_file('best-date', "--월 --일")
        raise


def generate_monthly_calendar_component(**context):
    """
    Generate monthly-calendar component with special dates highlighted.
    """
    try:
        current_date = datetime.now()
        month_name = f"{current_date.month}월"
        
        # Generate a simple calendar structure
        # In production, this would be more sophisticated with actual calendar logic
        calendar_html = f"""<div class="calendar-header">{month_name}</div>
<div class="calendar-grid">
    <div class="day-name sun">일</div>
    <div class="day-name">월</div>
    <div class="day-name">화</div>
    <div class="day-name">수</div>
    <div class="day-name">목</div>
    <div class="day-name">금</div>
    <div class="day-name sat">토</div>
    
    <!-- Calendar dates would be generated here based on actual month data -->
    <div class="date sun">1</div>
    <div class="date">2</div>
    <div class="date">3</div>
    <div class="date">4</div>
    <div class="date">5</div>
    <div class="date">6</div>
    <div class="date sat">7</div>
    
    <div class="date sun">8</div>
    <div class="date">9</div>
    <div class="date">10</div>
    <div class="date">11</div>
    <div class="date">12</div>
    <div class="date">13</div>
    <div class="date sat">14</div>
    
    <div class="date sun">15</div>
    <div class="date">16</div>
    <div class="date">17</div>
    <div class="date">18</div>
    <div class="date">19</div>
    <div class="date">20</div>
    <div class="date special">{current_date.day}<span>✨</span></div>
    
    <div class="date sun">22</div>
    <div class="date">23</div>
    <div class="date">24</div>
    <div class="date">25</div>
    <div class="date">26</div>
    <div class="date">27</div>
    <div class="date sat">28</div>
    
    <div class="date sun">29</div>
    <div class="date">30</div>
    <div class="date faded">1</div>
    <div class="date faded">2</div>
    <div class="date faded">3</div>
    <div class="date faded">4</div>
    <div class="date faded sat">5</div>
</div>"""
        
        write_component_file('monthly-calendar', calendar_html)
        
        return {"status": "success", "component": "monthly-calendar"}
        
    except Exception as e:
        print(f"❌ Error generating monthly calendar component: {e}")
        fallback_content = """<div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #999;">
    캘린더 로딩중...
</div>"""
        write_component_file('monthly-calendar', fallback_content)
        raise


def generate_special_dates_component(**context):
    """
    Generate special-dates component with highlighted beautiful days.
    """
    try:
        current_date = datetime.now()
        
        # Generate special dates based on current month
        # In production, this would analyze historical beauty scores
        special_dates_html = f"""
            <div class="info-card yellow">
                <span class="card-date">{current_date.month}월 {current_date.day - 2}일</span>
                <span class="card-desc">가장 맑은 날 ✨</span>
            </div>
            
            <div class="info-card pink">
                <span class="card-date">{current_date.month}월 {current_date.day + 1}일</span>
                <span class="card-desc">가장 구름 없는 날 🪁</span>
            </div>

            <div class="info-card yellow">
                <span class="card-date">{current_date.month}월 {current_date.day}일</span>
                <span class="card-desc">가장 따뜻한 날 😍</span>
            </div>
        """.strip()
        
        write_component_file('special-dates', special_dates_html)
        
        return {"status": "success", "component": "special-dates"}
        
    except Exception as e:
        print(f"❌ Error generating special dates component: {e}")
        fallback_content = """<div class="info-card yellow">
    <span class="card-date">로딩중...</span>
    <span class="card-desc">데이터 처리중 ⏳</span>
</div>"""
        write_component_file('special-dates', fallback_content)
        raise


def update_component_cache(**context):
    """
    Update component generation timestamp in Redis cache.
    """
    try:
        redis_client = get_redis_client()
        
        # Store generation timestamp
        generation_info = {
            'timestamp': datetime.now().isoformat(),
            'components_generated': [
                'current-status',
                'hero-image', 
                'best-time',
                'hourly-chart',
                'best-date',
                'monthly-calendar',
                'special-dates'
            ]
        }
        
        redis_client.setex(
            'component_generation_info',
            3600,  # 1 hour TTL
            json.dumps(generation_info)
        )
        
        print("✅ Updated component cache information")
        return {"status": "success", "timestamp": generation_info['timestamp']}
        
    except Exception as e:
        print(f"❌ Error updating component cache: {e}")
        raise


# Default arguments
default_args = {
    'owner': 'ingyeongho-ui-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 11, 21),
    'on_failure_callback': log_task_failure,
}

# Define the DAG
with DAG(
    'component_generation_dag',
    default_args=default_args,
    description='Generate HTML components for the mobile Korean UI',
    schedule_interval=None,  # Triggered by score calculation DAG
    catchup=False,
    tags=['component-generation', 'ui', 'html'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Generate current status component
    generate_current_status_task = PythonOperator(
        task_id='generate_current_status',
        python_callable=generate_current_status_component,
        provide_context=True,
    )
    
    # Task 2: Generate hero image component
    generate_hero_image_task = PythonOperator(
        task_id='generate_hero_image',
        python_callable=generate_hero_image_component,
        provide_context=True,
    )
    
    # Task 3: Generate best time component
    generate_best_time_task = PythonOperator(
        task_id='generate_best_time',
        python_callable=generate_best_time_component,
        provide_context=True,
    )
    
    # Task 4: Generate hourly chart component
    generate_hourly_chart_task = PythonOperator(
        task_id='generate_hourly_chart',
        python_callable=generate_hourly_chart_component,
        provide_context=True,
    )
    
    # Task 5: Generate best date component
    generate_best_date_task = PythonOperator(
        task_id='generate_best_date',
        python_callable=generate_best_date_component,
        provide_context=True,
    )
    
    # Task 6: Generate monthly calendar component
    generate_monthly_calendar_task = PythonOperator(
        task_id='generate_monthly_calendar',
        python_callable=generate_monthly_calendar_component,
        provide_context=True,
    )
    
    # Task 7: Generate special dates component
    generate_special_dates_task = PythonOperator(
        task_id='generate_special_dates',
        python_callable=generate_special_dates_component,
        provide_context=True,
    )
    
    # Task 8: Update component cache
    update_cache_task = PythonOperator(
        task_id='update_component_cache',
        python_callable=update_component_cache,
        provide_context=True,
    )
    
    # Set up task dependencies - most components can be generated in parallel
    [
        generate_current_status_task,
        generate_hero_image_task,
        generate_best_time_task,
        generate_hourly_chart_task,
        generate_best_date_task,
        generate_monthly_calendar_task,
        generate_special_dates_task
    ] >> update_cache_task