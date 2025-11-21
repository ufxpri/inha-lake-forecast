"""
Current Data DAG for Ingyeongho Beauty Score System.

This DAG runs every 30 minutes to handle real-time data:
1. Fetch current weather and air quality data
2. Calculate current beauty score
3. Generate real-time components (status, hero image, current conditions)
4. Update Redis cache for immediate UI updates
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import requests
import time
import psycopg2
import redis
from typing import Dict, Any


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


def fetch_current_weather(**context) -> Dict[str, Any]:
    """
    Fetch current weather data from Korea Meteorological Administration API.
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            api_key = os.getenv('KMA_API_KEY', 'test_api_key')
            
            # Mock current weather data - replace with real API calls
            weather_data = {
                'temperature': 15.5 + (retry_count * 2),
                'weather_condition': 'clear',
                'precipitation_probability': 20,
                'humidity': 65,
                'wind_speed': 2.5,
                'visibility': 10.0,
                'pressure': 1013.25,
                'timestamp': datetime.now().isoformat(),
                'source': 'KMA',
                'location': 'Ingyeongho'
            }
            
            # Validate data
            if not -50 <= weather_data.get('temperature', 0) <= 60:
                raise ValueError(f"Temperature out of range: {weather_data.get('temperature')}")
            
            print(f"✅ Fetched current weather: {weather_data['temperature']}°C, {weather_data['weather_condition']}")
            return weather_data
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"❌ Failed to fetch weather after {max_retries} attempts: {e}")
                break
            time.sleep(2 ** retry_count)
    
    # Fallback to cached data
    try:
        redis_client = get_redis_client()
        cached_weather = redis_client.get('previous_weather_data')
        if cached_weather:
            return json.loads(cached_weather)
    except Exception:
        pass
    
    # Final fallback
    return {
        'temperature': 15.0,
        'weather_condition': 'unknown',
        'precipitation_probability': 0,
        'humidity': 50,
        'wind_speed': 0,
        'timestamp': datetime.now().isoformat(),
        'source': 'fallback'
    }


def fetch_current_air_quality(**context) -> Dict[str, Any]:
    """
    Fetch current air quality data from Korea Environment Corporation API.
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            api_key = os.getenv('KEC_API_KEY', 'test_api_key')
            
            # Mock air quality data - replace with real API calls
            air_quality_data = {
                'aqi': 45 + (retry_count * 5),
                'pm10': 35,
                'pm25': 15,
                'o3': 0.03,
                'no2': 0.02,
                'co': 0.5,
                'so2': 0.003,
                'grade': 'good',
                'timestamp': datetime.now().isoformat(),
                'source': 'KEC',
                'location': 'Ingyeongho'
            }
            
            # Validate data
            if not 0 <= air_quality_data.get('aqi', 0) <= 500:
                raise ValueError(f"AQI out of range: {air_quality_data.get('aqi')}")
            
            print(f"✅ Fetched air quality: AQI {air_quality_data['aqi']}, {air_quality_data['grade']}")
            return air_quality_data
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"❌ Failed to fetch air quality after {max_retries} attempts: {e}")
                break
            time.sleep(2 ** retry_count)
    
    # Fallback
    return {
        'aqi': 50,
        'pm10': 30,
        'pm25': 15,
        'grade': 'moderate',
        'timestamp': datetime.now().isoformat(),
        'source': 'fallback'
    }


def calculate_current_beauty_score(**context) -> Dict[str, Any]:
    """
    Calculate current beauty score based on real-time data.
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        weather_data = ti.xcom_pull(key='return_value', task_ids='fetch_current_weather')
        air_quality_data = ti.xcom_pull(key='return_value', task_ids='fetch_current_air_quality')
        
        if not weather_data or not air_quality_data:
            raise ValueError("Missing weather or air quality data")
        
        # Calculate score using the same algorithm
        base_score = 0
        
        # Weather condition (40% weight)
        weather_condition = weather_data.get('weather_condition', 'unknown')
        weather_scores = {
            'clear': 40, 'partly_cloudy': 35, 'cloudy': 25,
            'overcast': 15, 'rain': 5, 'snow': 10, 'unknown': 20
        }
        base_score += weather_scores.get(weather_condition, 20)
        
        # Temperature comfort (30% weight)
        temperature = float(weather_data.get('temperature', 15))
        if 15 <= temperature <= 25:
            base_score += 30
        elif 10 <= temperature < 15 or 25 < temperature <= 30:
            base_score += 25
        elif 5 <= temperature < 10 or 30 < temperature <= 35:
            base_score += 15
        else:
            base_score += 5
        
        # Air quality (20% weight)
        aqi = float(air_quality_data.get('aqi', 50))
        if aqi <= 50:
            base_score += 20
        elif aqi <= 100:
            base_score += 15
        elif aqi <= 150:
            base_score += 10
        elif aqi <= 200:
            base_score += 5
        else:
            base_score += 0
        
        # Time adjustment (10% weight)
        current_hour = datetime.now().hour
        if 6 <= current_hour < 9 or 17 <= current_hour < 20:
            base_score += 10
        elif 9 <= current_hour < 17:
            base_score += 8
        elif 20 <= current_hour < 22:
            base_score += 6
        else:
            base_score += 2
        
        # Golden hour bonus
        current_time = current_hour + (datetime.now().minute / 60)
        is_golden_hour = 17.5 <= current_time <= 19
        if is_golden_hour:
            base_score += 10
        
        # Penalties
        if weather_data.get('precipitation_probability', 0) > 60:
            base_score -= 50
        if aqi > 200:
            base_score -= 30
        
        # Final score
        final_score = max(0, min(100, int(base_score)))
        
        # Status message and emoji
        if final_score >= 80:
            status_message, emoji = "최고예요", "😆"
        elif final_score >= 60:
            status_message, emoji = "좋아요", "😊"
        elif final_score >= 40:
            status_message, emoji = "괜찮아요", "🙂"
        else:
            status_message, emoji = "별로예요", "😐"
        
        result = {
            'score': final_score,
            'status_message': status_message,
            'emoji': emoji,
            'weather_condition': weather_condition,
            'temperature': temperature,
            'aqi': aqi,
            'is_golden_hour': is_golden_hour,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"✅ Current beauty score: {final_score} - {status_message} {emoji}")
        return result
        
    except Exception as e:
        print(f"❌ Error calculating beauty score: {e}")
        return {
            'score': 50,
            'status_message': "데이터 처리 오류",
            'emoji': "😐",
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }


def store_current_data(**context):
    """
    Store current data in PostgreSQL and Redis cache.
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        weather_data = ti.xcom_pull(key='return_value', task_ids='fetch_current_weather')
        air_quality_data = ti.xcom_pull(key='return_value', task_ids='fetch_current_air_quality')
        score_data = ti.xcom_pull(key='return_value', task_ids='calculate_current_beauty_score')
        
        # Store in PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create tables if not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS current_weather (
                id SERIAL PRIMARY KEY,
                temperature FLOAT,
                weather_condition VARCHAR(50),
                precipitation_probability INTEGER,
                humidity INTEGER,
                wind_speed FLOAT,
                aqi INTEGER,
                beauty_score INTEGER,
                status_message VARCHAR(50),
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Insert current data (keep only latest record)
        cursor.execute("DELETE FROM current_weather")
        cursor.execute("""
            INSERT INTO current_weather 
            (temperature, weather_condition, precipitation_probability, humidity, 
             wind_speed, aqi, beauty_score, status_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            weather_data.get('temperature'),
            weather_data.get('weather_condition'),
            weather_data.get('precipitation_probability'),
            weather_data.get('humidity'),
            weather_data.get('wind_speed'),
            air_quality_data.get('aqi'),
            score_data.get('score'),
            score_data.get('status_message')
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Store in Redis with short TTL for real-time updates
        redis_client = get_redis_client()
        
        redis_client.setex('current_weather_data', 1800, json.dumps(weather_data))  # 30 min
        redis_client.setex('current_air_quality_data', 1800, json.dumps(air_quality_data))  # 30 min
        redis_client.setex('current_beauty_score', 1800, json.dumps(score_data))  # 30 min
        
        # Store as previous data for fallback (longer TTL)
        redis_client.setex('previous_weather_data', 86400, json.dumps(weather_data))  # 24 hours
        redis_client.setex('previous_air_quality_data', 86400, json.dumps(air_quality_data))  # 24 hours
        
        print("✅ Stored current data in database and cache")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
        
    except Exception as e:
        print(f"❌ Error storing current data: {e}")
        raise


def generate_current_components(**context):
    """
    Generate real-time UI components that update frequently.
    """
    try:
        # Get current data
        ti = context['task_instance']
        score_data = ti.xcom_pull(key='return_value', task_ids='calculate_current_beauty_score')
        weather_data = ti.xcom_pull(key='return_value', task_ids='fetch_current_weather')
        
        # 1. Generate current-status component
        if score_data:
            status_content = f"{score_data['status_message']} {score_data['emoji']}"
        else:
            status_content = "로딩중... 😐"
        write_component_file('current-status', status_content)
        
        # 2. Generate hero-image component
        if weather_data:
            condition = weather_data.get('weather_condition', 'clear')
            image_mapping = {
                'clear': "https://via.placeholder.com/400x250/87CEEB/FFFFFF?text=Clear+Sky+Ingyeongho",
                'partly_cloudy': "https://via.placeholder.com/400x250/B0C4DE/FFFFFF?text=Partly+Cloudy",
                'cloudy': "https://via.placeholder.com/400x250/708090/FFFFFF?text=Cloudy+Day",
                'rain': "https://via.placeholder.com/400x250/4682B4/FFFFFF?text=Rainy+Day",
                'snow': "https://via.placeholder.com/400x250/F0F8FF/000000?text=Snowy+Day"
            }
            image_url = image_mapping.get(condition, "https://via.placeholder.com/400x250/81C784/FFFFFF?text=Ingyeongho+View")
            
            hero_content = f"""<img src="{image_url}" alt="인경호 풍경" class="hero-img">
<div class="user-badge">👤</div>"""
        else:
            hero_content = """<div style="background: #f0f0f0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; color: #999;">
    이미지 로딩중...
</div>"""
        write_component_file('hero-image', hero_content)
        
        print("✅ Generated current components (status, hero-image)")
        return {"status": "success", "components": ["current-status", "hero-image"]}
        
    except Exception as e:
        print(f"❌ Error generating current components: {e}")
        # Write fallback content
        write_component_file('current-status', "로딩중... 😐")
        write_component_file('hero-image', """<div style="background: #f0f0f0; width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; color: #999;">이미지 로딩중...</div>""")
        raise


# Default arguments
default_args = {
    'owner': 'ingyeongho-current-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 11, 21),
    'on_failure_callback': log_task_failure,
}

# Define the DAG
with DAG(
    'current_data_dag',
    default_args=default_args,
    description='Process real-time weather data and current beauty scores (every 30 minutes)',
    schedule_interval=timedelta(minutes=30),  # Every 30 minutes
    catchup=False,
    tags=['current', 'real-time', 'weather', 'beauty-score'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Fetch current weather
    fetch_weather_task = PythonOperator(
        task_id='fetch_current_weather',
        python_callable=fetch_current_weather,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 2: Fetch current air quality
    fetch_air_quality_task = PythonOperator(
        task_id='fetch_current_air_quality',
        python_callable=fetch_current_air_quality,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 3: Calculate current beauty score
    calculate_score_task = PythonOperator(
        task_id='calculate_current_beauty_score',
        python_callable=calculate_current_beauty_score,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 4: Store current data
    store_data_task = PythonOperator(
        task_id='store_current_data',
        python_callable=store_current_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 5: Generate current components
    generate_components_task = PythonOperator(
        task_id='generate_current_components',
        python_callable=generate_current_components,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Set up task dependencies
    [fetch_weather_task, fetch_air_quality_task] >> calculate_score_task >> store_data_task >> generate_components_task