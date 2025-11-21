"""
Daily Data DAG for Ingyeongho Beauty Score System.

This DAG runs once per day (at 6 AM) to handle daily calculations:
1. Generate hourly predictions for the next 24 hours
2. Calculate best times for today
3. Generate daily components (hourly chart, best time)
4. Analyze daily patterns and trends
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import psycopg2
import redis
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


def generate_hourly_predictions(**context) -> List[Dict[str, Any]]:
    """
    Generate detailed hourly beauty score predictions for the next 24 hours.
    """
    try:
        # Get current weather conditions as baseline
        redis_client = get_redis_client()
        current_weather_str = redis_client.get('current_weather_data')
        current_score_str = redis_client.get('current_beauty_score')
        
        if current_weather_str and current_score_str:
            current_weather = json.loads(current_weather_str)
            current_score_data = json.loads(current_score_str)
            base_score = current_score_data.get('score', 50)
            base_temp = current_weather.get('temperature', 15)
        else:
            # Fallback to database
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT beauty_score, temperature 
                FROM current_weather 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                base_score, base_temp = result
            else:
                base_score, base_temp = 50, 15
        
        hourly_predictions = []
        current_hour = datetime.now().hour
        
        # Generate predictions for next 24 hours
        for i in range(24):
            hour = (current_hour + i) % 24
            
            # Time-based adjustments
            if 6 <= hour <= 8:  # Morning golden hour
                time_adjustment = 15
                condition = "일출"
                emoji = "🌅"
            elif 17 <= hour <= 19:  # Evening golden hour
                time_adjustment = 20
                condition = "일몰"
                emoji = "🌅"
            elif 9 <= hour <= 16:  # Daytime
                time_adjustment = 10
                condition = "주간"
                emoji = "☀️"
            elif 20 <= hour <= 22:  # Evening
                time_adjustment = 5
                condition = "저녁"
                emoji = "🌆"
            else:  # Night
                time_adjustment = -10
                condition = "야간"
                emoji = "🌙"
            
            # Weather pattern simulation (in production, use weather forecast API)
            weather_variation = 0
            if i < 6:  # Next 6 hours - stable
                weather_variation = 0
            elif i < 12:  # 6-12 hours - slight change
                weather_variation = (i % 3) * 2 - 2  # -2, 0, 2
            else:  # 12-24 hours - more variation
                weather_variation = (i % 5) * 3 - 6  # -6, -3, 0, 3, 6
            
            # Temperature effect (simulate daily temperature cycle)
            temp_cycle = base_temp + (5 * (1 - abs(hour - 14) / 12))  # Peak at 2 PM
            if 15 <= temp_cycle <= 25:
                temp_adjustment = 5
            elif 10 <= temp_cycle <= 30:
                temp_adjustment = 0
            else:
                temp_adjustment = -5
            
            # Calculate predicted score
            predicted_score = base_score + time_adjustment + weather_variation + temp_adjustment
            predicted_score = max(0, min(100, predicted_score))
            
            # Adjust emoji based on final score
            if predicted_score >= 85:
                emoji = "😆"
            elif predicted_score >= 70:
                emoji = "😊"
            elif predicted_score >= 50:
                emoji = "🙂"
            elif predicted_score >= 30:
                emoji = "😐"
            else:
                emoji = "😞"
            
            hourly_predictions.append({
                'hour': hour,
                'score': predicted_score,
                'condition': condition,
                'emoji': emoji,
                'temperature': round(temp_cycle, 1),
                'timestamp': (datetime.now() + timedelta(hours=i)).isoformat(),
                'prediction_confidence': max(0.6, 1.0 - (i * 0.02))  # Confidence decreases over time
            })
        
        print(f"✅ Generated {len(hourly_predictions)} hourly predictions")
        return hourly_predictions
        
    except Exception as e:
        print(f"❌ Error generating hourly predictions: {e}")
        # Return minimal fallback predictions
        current_hour = datetime.now().hour
        return [{
            'hour': (current_hour + i) % 24,
            'score': 50 + (i % 3) * 10,  # Some variation
            'condition': "예측불가",
            'emoji': "😐",
            'temperature': 15.0,
            'timestamp': (datetime.now() + timedelta(hours=i)).isoformat(),
            'prediction_confidence': 0.5
        } for i in range(24)]


def calculate_daily_best_times(**context) -> Dict[str, Any]:
    """
    Calculate the best times for today based on hourly predictions.
    """
    try:
        # Get hourly predictions from XCom
        ti = context['task_instance']
        hourly_predictions = ti.xcom_pull(key='return_value', task_ids='generate_hourly_predictions')
        
        if not hourly_predictions:
            raise ValueError("No hourly predictions available")
        
        # Find best times
        current_hour = datetime.now().hour
        
        # Best time overall
        best_overall = max(hourly_predictions, key=lambda x: x['score'])
        
        # Best time in next 12 hours (more relevant)
        next_12_hours = [p for p in hourly_predictions[:12]]
        best_next_12h = max(next_12_hours, key=lambda x: x['score']) if next_12_hours else best_overall
        
        # Best morning time (6-12)
        morning_times = [p for p in hourly_predictions if 6 <= p['hour'] <= 12]
        best_morning = max(morning_times, key=lambda x: x['score']) if morning_times else None
        
        # Best evening time (17-21)
        evening_times = [p for p in hourly_predictions if 17 <= p['hour'] <= 21]
        best_evening = max(evening_times, key=lambda x: x['score']) if evening_times else None
        
        # Golden hour times (special calculation)
        golden_hours = [p for p in hourly_predictions if p['condition'] in ['일출', '일몰']]
        best_golden = max(golden_hours, key=lambda x: x['score']) if golden_hours else None
        
        result = {
            'best_overall': {
                'time': f"{best_overall['hour']:02d}:00",
                'score': best_overall['score'],
                'condition': best_overall['condition'],
                'emoji': best_overall['emoji']
            },
            'best_next_12h': {
                'time': f"{best_next_12h['hour']:02d}:00",
                'score': best_next_12h['score'],
                'condition': best_next_12h['condition'],
                'emoji': best_next_12h['emoji']
            },
            'best_morning': {
                'time': f"{best_morning['hour']:02d}:00",
                'score': best_morning['score'],
                'condition': best_morning['condition'],
                'emoji': best_morning['emoji']
            } if best_morning else None,
            'best_evening': {
                'time': f"{best_evening['hour']:02d}:00",
                'score': best_evening['score'],
                'condition': best_evening['condition'],
                'emoji': best_evening['emoji']
            } if best_evening else None,
            'best_golden': {
                'time': f"{best_golden['hour']:02d}:00",
                'score': best_golden['score'],
                'condition': best_golden['condition'],
                'emoji': best_golden['emoji']
            } if best_golden else None,
            'calculated_at': datetime.now().isoformat()
        }
        
        print(f"✅ Calculated daily best times - Overall: {result['best_overall']['time']}")
        return result
        
    except Exception as e:
        print(f"❌ Error calculating daily best times: {e}")
        # Fallback calculation
        current_hour = datetime.now().hour
        if 6 <= current_hour <= 12:
            best_time = "18:00"  # Evening
        else:
            best_time = "07:00"  # Morning
        
        return {
            'best_overall': {
                'time': best_time,
                'score': 75,
                'condition': '예측',
                'emoji': '😊'
            },
            'best_next_12h': {
                'time': best_time,
                'score': 75,
                'condition': '예측',
                'emoji': '😊'
            },
            'calculated_at': datetime.now().isoformat(),
            'error': str(e)
        }


def store_daily_data(**context):
    """
    Store daily predictions and best times in PostgreSQL and Redis.
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        hourly_predictions = ti.xcom_pull(key='return_value', task_ids='generate_hourly_predictions')
        best_times = ti.xcom_pull(key='return_value', task_ids='calculate_daily_best_times')
        
        # Store in PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create tables if not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_predictions (
                id SERIAL PRIMARY KEY,
                prediction_date DATE,
                hourly_data JSONB,
                best_times JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Clear old predictions and insert new ones
        today = datetime.now().date()
        cursor.execute("DELETE FROM daily_predictions WHERE prediction_date = %s", (today,))
        
        cursor.execute("""
            INSERT INTO daily_predictions (prediction_date, hourly_data, best_times)
            VALUES (%s, %s, %s)
        """, (
            today,
            json.dumps(hourly_predictions),
            json.dumps(best_times)
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Store in Redis with daily TTL
        redis_client = get_redis_client()
        
        # Cache hourly predictions (24 hour TTL)
        redis_client.setex(
            'daily_hourly_predictions',
            86400,  # 24 hours
            json.dumps(hourly_predictions)
        )
        
        # Cache best times (24 hour TTL)
        redis_client.setex(
            'daily_best_times',
            86400,  # 24 hours
            json.dumps(best_times)
        )
        
        print("✅ Stored daily predictions and best times")
        return {
            'status': 'success',
            'predictions_count': len(hourly_predictions) if hourly_predictions else 0,
            'best_times_count': len([k for k, v in best_times.items() if v and k.startswith('best_')]),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error storing daily data: {e}")
        raise


def generate_daily_components(**context):
    """
    Generate daily UI components (hourly chart, best time).
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        hourly_predictions = ti.xcom_pull(key='return_value', task_ids='generate_hourly_predictions')
        best_times = ti.xcom_pull(key='return_value', task_ids='calculate_daily_best_times')
        
        # 1. Generate best-time component
        if best_times and best_times.get('best_next_12h'):
            best_time_content = best_times['best_next_12h']['time']
        else:
            best_time_content = "--:--"
        write_component_file('best-time', best_time_content)
        
        # 2. Generate hourly-chart component
        if hourly_predictions:
            # Take first 7 predictions for the chart
            chart_data = hourly_predictions[:7]
            
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
            
            chart_content = "".join(chart_items)
        else:
            chart_content = """<div style="display: flex; align-items: center; justify-content: center; width: 100%; color: #999;">
    차트 로딩중...
</div>"""
        
        write_component_file('hourly-chart', chart_content)
        
        print("✅ Generated daily components (best-time, hourly-chart)")
        return {
            'status': 'success',
            'components': ['best-time', 'hourly-chart'],
            'best_time': best_time_content
        }
        
    except Exception as e:
        print(f"❌ Error generating daily components: {e}")
        # Write fallback content
        write_component_file('best-time', "--:--")
        write_component_file('hourly-chart', """<div style="display: flex; align-items: center; justify-content: center; width: 100%; color: #999;">차트 로딩중...</div>""")
        raise


# Default arguments
default_args = {
    'owner': 'ingyeongho-daily-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 21),
    'on_failure_callback': log_task_failure,
}

# Define the DAG
with DAG(
    'daily_data_dag',
    default_args=default_args,
    description='Generate daily predictions and best times (runs once per day at 6 AM)',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['daily', 'predictions', 'hourly-chart', 'best-times'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Generate hourly predictions for next 24 hours
    generate_predictions_task = PythonOperator(
        task_id='generate_hourly_predictions',
        python_callable=generate_hourly_predictions,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    # Task 2: Calculate best times for today
    calculate_best_times_task = PythonOperator(
        task_id='calculate_daily_best_times',
        python_callable=calculate_daily_best_times,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    # Task 3: Store daily data
    store_daily_task = PythonOperator(
        task_id='store_daily_data',
        python_callable=store_daily_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    # Task 4: Generate daily components
    generate_daily_components_task = PythonOperator(
        task_id='generate_daily_components',
        python_callable=generate_daily_components,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Set up task dependencies
    generate_predictions_task >> calculate_best_times_task >> store_daily_task >> generate_daily_components_task