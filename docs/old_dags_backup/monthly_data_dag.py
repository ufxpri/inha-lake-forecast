"""
Monthly Data DAG for Ingyeongho Beauty Score System.

This DAG runs once per month (on the 1st at 3 AM) to handle monthly calculations:
1. Generate monthly calendar with special dates
2. Calculate monthly statistics and trends
3. Identify best days of the month
4. Generate monthly components (calendar, special dates, best date)
5. Archive old data and maintain database
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import psycopg2
import redis
import calendar
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


def get_db_connection():
    """Get PostgreSQL database connection."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'airflow'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow')
    )


def get_redis_client():
    """Get Redis client connection."""
    return redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', '6379')),
        db=int(os.getenv('REDIS_DB', '0')),
        decode_responses=True
    )


def write_component_file(component_name: str, content: str) -> str:
    """Write component content to file in shared nginx volume."""
    output_path = f"/opt/airflow/nginx_html/components/{component_name}"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Generated {component_name} at {output_path}")
    return output_path


def calculate_monthly_statistics(**context) -> Dict[str, Any]:
    """
    Calculate monthly statistics from historical beauty score data.
    """
    try:
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year
        
        # Get historical data from database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create monthly_stats table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_stats (
                id SERIAL PRIMARY KEY,
                year INTEGER,
                month INTEGER,
                avg_score FLOAT,
                max_score INTEGER,
                min_score INTEGER,
                best_day INTEGER,
                best_day_score INTEGER,
                total_records INTEGER,
                special_days JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Try to get historical beauty scores for this month
        # Note: In a real system, you'd have accumulated data over time
        cursor.execute("""
            SELECT 
                AVG(beauty_score) as avg_score,
                MAX(beauty_score) as max_score,
                MIN(beauty_score) as min_score,
                COUNT(*) as total_records
            FROM current_weather 
            WHERE EXTRACT(MONTH FROM timestamp) = %s 
            AND EXTRACT(YEAR FROM timestamp) = %s
        """, (current_month, current_year))
        
        result = cursor.fetchone()
        
        if result and result[3] > 0:  # If we have data
            avg_score, max_score, min_score, total_records = result
            
            # Find best day (this is simplified - in reality you'd analyze daily averages)
            cursor.execute("""
                SELECT 
                    EXTRACT(DAY FROM timestamp) as day,
                    beauty_score
                FROM current_weather 
                WHERE EXTRACT(MONTH FROM timestamp) = %s 
                AND EXTRACT(YEAR FROM timestamp) = %s
                ORDER BY beauty_score DESC
                LIMIT 1
            """, (current_month, current_year))
            
            best_day_result = cursor.fetchone()
            best_day = int(best_day_result[0]) if best_day_result else current_date.day
            best_day_score = int(best_day_result[1]) if best_day_result else 75
        else:
            # Generate mock statistics for demonstration
            avg_score = 65.5
            max_score = 95
            min_score = 25
            total_records = 30  # Assume 30 days of data
            best_day = current_date.day
            best_day_score = 85
        
        # Generate special days based on patterns
        special_days = generate_special_days(current_year, current_month, best_day)
        
        monthly_stats = {
            'year': current_year,
            'month': current_month,
            'month_name': calendar.month_name[current_month],
            'avg_score': round(avg_score, 1),
            'max_score': max_score,
            'min_score': min_score,
            'best_day': best_day,
            'best_day_score': best_day_score,
            'total_records': total_records,
            'special_days': special_days,
            'calculated_at': datetime.now().isoformat()
        }
        
        # Store in database
        cursor.execute("DELETE FROM monthly_stats WHERE year = %s AND month = %s", (current_year, current_month))
        cursor.execute("""
            INSERT INTO monthly_stats 
            (year, month, avg_score, max_score, min_score, best_day, best_day_score, total_records, special_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            current_year, current_month, avg_score, max_score, min_score,
            best_day, best_day_score, total_records, json.dumps(special_days)
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Calculated monthly statistics - Best day: {current_month}월 {best_day}일 (Score: {best_day_score})")
        return monthly_stats
        
    except Exception as e:
        print(f"❌ Error calculating monthly statistics: {e}")
        # Return fallback statistics
        current_date = datetime.now()
        return {
            'year': current_date.year,
            'month': current_date.month,
            'month_name': calendar.month_name[current_date.month],
            'avg_score': 65.0,
            'max_score': 85,
            'min_score': 45,
            'best_day': current_date.day,
            'best_day_score': 80,
            'total_records': 0,
            'special_days': [],
            'calculated_at': datetime.now().isoformat(),
            'error': str(e)
        }


def generate_special_days(year: int, month: int, best_day: int) -> List[Dict[str, Any]]:
    """
    Generate special days for the month based on weather patterns and algorithms.
    """
    special_days = []
    
    # Add the best day
    special_days.append({
        'day': best_day,
        'type': 'best',
        'description': '가장 아름다운 날',
        'emoji': '✨',
        'score': 90
    })
    
    # Add some other special days based on patterns
    days_in_month = calendar.monthrange(year, month)[1]
    
    # Clear sky days (simulate based on weather patterns)
    clear_days = [d for d in range(1, days_in_month + 1) if d % 7 == 2 or d % 7 == 5][:3]
    for day in clear_days:
        if day != best_day:
            special_days.append({
                'day': day,
                'type': 'clear',
                'description': '가장 맑은 날',
                'emoji': '☀️',
                'score': 85
            })
    
    # Warm days
    warm_days = [d for d in range(1, days_in_month + 1) if d % 5 == 0][:2]
    for day in warm_days:
        if day != best_day and not any(sd['day'] == day for sd in special_days):
            special_days.append({
                'day': day,
                'type': 'warm',
                'description': '가장 따뜻한 날',
                'emoji': '🌡️',
                'score': 80
            })
    
    # Golden hour days (weekends or special dates)
    golden_days = [d for d in range(1, days_in_month + 1) if datetime(year, month, d).weekday() in [5, 6]][:2]
    for day in golden_days:
        if day != best_day and not any(sd['day'] == day for sd in special_days):
            special_days.append({
                'day': day,
                'type': 'golden',
                'description': '황금시간이 긴 날',
                'emoji': '🌅',
                'score': 88
            })
    
    return sorted(special_days, key=lambda x: x['score'], reverse=True)[:5]  # Top 5 special days


def generate_monthly_calendar(**context) -> str:
    """
    Generate monthly calendar HTML with special dates highlighted.
    """
    try:
        # Get monthly statistics from XCom
        ti = context['task_instance']
        monthly_stats = ti.xcom_pull(key='return_value', task_ids='calculate_monthly_statistics')
        
        if not monthly_stats:
            raise ValueError("No monthly statistics available")
        
        year = monthly_stats['year']
        month = monthly_stats['month']
        month_name = monthly_stats['month_name']
        special_days = monthly_stats.get('special_days', [])
        
        # Create a mapping of special days
        special_day_map = {sd['day']: sd for sd in special_days}
        
        # Get calendar information
        cal = calendar.monthcalendar(year, month)
        today = datetime.now().day if datetime.now().month == month and datetime.now().year == year else None
        
        # Generate calendar HTML
        calendar_html = f'<div class="calendar-header">{month}월</div>\n<div class="calendar-grid">\n'
        
        # Day names
        day_names = ['일', '월', '화', '수', '목', '금', '토']
        for i, day_name in enumerate(day_names):
            css_class = 'day-name'
            if i == 0:  # Sunday
                css_class += ' sun'
            elif i == 6:  # Saturday
                css_class += ' sat'
            calendar_html += f'    <div class="{css_class}">{day_name}</div>\n'
        
        # Calendar dates
        for week in cal:
            for day in week:
                if day == 0:
                    # Empty cell for days from previous/next month
                    calendar_html += '    <div class="date faded"></div>\n'
                else:
                    css_classes = ['date']
                    
                    # Add day-specific classes
                    weekday = datetime(year, month, day).weekday()
                    if weekday == 6:  # Sunday
                        css_classes.append('sun')
                    elif weekday == 5:  # Saturday
                        css_classes.append('sat')
                    
                    # Check if it's today
                    if day == today:
                        css_classes.append('today')
                    
                    # Check if it's a special day
                    special_content = ''
                    if day in special_day_map:
                        css_classes.append('special')
                        special_day = special_day_map[day]
                        special_content = f'<span>{special_day["emoji"]}</span>'
                    
                    css_class_str = ' '.join(css_classes)
                    calendar_html += f'    <div class="{css_class_str}">{day}{special_content}</div>\n'
        
        calendar_html += '</div>'
        
        print(f"✅ Generated monthly calendar for {month_name} {year}")
        return calendar_html
        
    except Exception as e:
        print(f"❌ Error generating monthly calendar: {e}")
        # Return fallback calendar
        current_date = datetime.now()
        return f"""<div class="calendar-header">{current_date.month}월</div>
<div class="calendar-grid">
    <div class="day-name sun">일</div>
    <div class="day-name">월</div>
    <div class="day-name">화</div>
    <div class="day-name">수</div>
    <div class="day-name">목</div>
    <div class="day-name">금</div>
    <div class="day-name sat">토</div>
    
    <div class="date special">{current_date.day}<span>✨</span></div>
    <div class="date">2</div>
    <div class="date">3</div>
    <div class="date">4</div>
    <div class="date">5</div>
    <div class="date">6</div>
    <div class="date sat">7</div>
    
    <!-- Simplified fallback calendar -->
    <div style="grid-column: span 7; text-align: center; color: #999; padding: 20px;">
        캘린더 생성 중...
    </div>
</div>"""


def store_monthly_data(**context):
    """
    Store monthly data in Redis cache and clean up old data.
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        monthly_stats = ti.xcom_pull(key='return_value', task_ids='calculate_monthly_statistics')
        monthly_calendar = ti.xcom_pull(key='return_value', task_ids='generate_monthly_calendar')
        
        # Store in Redis with monthly TTL
        redis_client = get_redis_client()
        
        # Cache monthly statistics (30 day TTL)
        redis_client.setex(
            'monthly_statistics',
            2592000,  # 30 days
            json.dumps(monthly_stats)
        )
        
        # Cache monthly calendar (30 day TTL)
        redis_client.setex(
            'monthly_calendar_html',
            2592000,  # 30 days
            monthly_calendar
        )
        
        # Clean up old data (keep last 3 months)
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Clean old monthly stats
        cursor.execute("""
            DELETE FROM monthly_stats 
            WHERE created_at < NOW() - INTERVAL '3 months'
        """)
        
        # Clean old daily predictions
        cursor.execute("""
            DELETE FROM daily_predictions 
            WHERE created_at < NOW() - INTERVAL '1 month'
        """)
        
        # Archive old current weather data (keep last 7 days in main table)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_archive (
                LIKE current_weather INCLUDING ALL
            )
        """)
        
        cursor.execute("""
            INSERT INTO weather_archive 
            SELECT * FROM current_weather 
            WHERE timestamp < NOW() - INTERVAL '7 days'
        """)
        
        cursor.execute("""
            DELETE FROM current_weather 
            WHERE timestamp < NOW() - INTERVAL '7 days'
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Stored monthly data and cleaned up old records")
        return {
            'status': 'success',
            'monthly_stats': monthly_stats is not None,
            'calendar_generated': monthly_calendar is not None,
            'cleanup_completed': True,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error storing monthly data: {e}")
        raise


def generate_monthly_components(**context):
    """
    Generate monthly UI components (calendar, special dates, best date).
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        monthly_stats = ti.xcom_pull(key='return_value', task_ids='calculate_monthly_statistics')
        monthly_calendar = ti.xcom_pull(key='return_value', task_ids='generate_monthly_calendar')
        
        # 1. Generate best-date component
        if monthly_stats:
            best_date_content = f"{monthly_stats['month']}월 {monthly_stats['best_day']}일"
        else:
            current_date = datetime.now()
            best_date_content = f"{current_date.month}월 {current_date.day}일"
        write_component_file('best-date', best_date_content)
        
        # 2. Generate monthly-calendar component
        if monthly_calendar:
            calendar_content = monthly_calendar
        else:
            calendar_content = """<div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #999;">
    캘린더 로딩중...
</div>"""
        write_component_file('monthly-calendar', calendar_content)
        
        # 3. Generate special-dates component
        if monthly_stats and monthly_stats.get('special_days'):
            special_days = monthly_stats['special_days'][:3]  # Top 3 special days
            special_dates_html = ""
            
            for i, special_day in enumerate(special_days):
                card_class = "yellow" if i % 2 == 0 else "pink"
                special_dates_html += f"""
                <div class="info-card {card_class}">
                    <span class="card-date">{monthly_stats['month']}월 {special_day['day']}일</span>
                    <span class="card-desc">{special_day['description']} {special_day['emoji']}</span>
                </div>
                """.strip()
                
                if i < len(special_days) - 1:
                    special_dates_html += "\n\n"
        else:
            # Fallback special dates
            current_date = datetime.now()
            special_dates_html = f"""
            <div class="info-card yellow">
                <span class="card-date">{current_date.month}월 {current_date.day}일</span>
                <span class="card-desc">가장 아름다운 날 ✨</span>
            </div>
            
            <div class="info-card pink">
                <span class="card-date">{current_date.month}월 {current_date.day + 1}일</span>
                <span class="card-desc">가장 맑은 날 ☀️</span>
            </div>
            """.strip()
        
        write_component_file('special-dates', special_dates_html)
        
        print("✅ Generated monthly components (best-date, monthly-calendar, special-dates)")
        return {
            'status': 'success',
            'components': ['best-date', 'monthly-calendar', 'special-dates'],
            'best_date': best_date_content
        }
        
    except Exception as e:
        print(f"❌ Error generating monthly components: {e}")
        # Write fallback content
        current_date = datetime.now()
        write_component_file('best-date', f"{current_date.month}월 {current_date.day}일")
        write_component_file('monthly-calendar', """<div style="display: flex; align-items: center; justify-content: center; height: 200px; color: #999;">캘린더 로딩중...</div>""")
        write_component_file('special-dates', """<div class="info-card yellow"><span class="card-date">로딩중...</span><span class="card-desc">데이터 처리중 ⏳</span></div>""")
        raise


# Default arguments
default_args = {
    'owner': 'ingyeongho-monthly-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2024, 11, 21),
    'on_failure_callback': log_task_failure,
}

# Define the DAG
with DAG(
    'monthly_data_dag',
    default_args=default_args,
    description='Generate monthly statistics, calendar, and special dates (runs monthly on 1st at 3 AM)',
    schedule_interval='0 3 1 * *',  # Monthly on 1st at 3 AM
    catchup=False,
    tags=['monthly', 'calendar', 'statistics', 'special-dates'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Calculate monthly statistics and identify special days
    calculate_stats_task = PythonOperator(
        task_id='calculate_monthly_statistics',
        python_callable=calculate_monthly_statistics,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    # Task 2: Generate monthly calendar HTML
    generate_calendar_task = PythonOperator(
        task_id='generate_monthly_calendar',
        python_callable=generate_monthly_calendar,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    # Task 3: Store monthly data and clean up old records
    store_monthly_task = PythonOperator(
        task_id='store_monthly_data',
        python_callable=store_monthly_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    # Task 4: Generate monthly components
    generate_monthly_components_task = PythonOperator(
        task_id='generate_monthly_components',
        python_callable=generate_monthly_components,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    # Set up task dependencies
    calculate_stats_task >> generate_calendar_task >> store_monthly_task >> generate_monthly_components_task