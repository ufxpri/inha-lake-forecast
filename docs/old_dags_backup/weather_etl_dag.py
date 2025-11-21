"""
Weather ETL DAG for Ingyeongho Beauty Score System.

This DAG runs hourly to:
1. Fetch weather data from Korea Meteorological Administration API
2. Fetch air quality data from Korea Environment Corporation API
3. Calculate beauty score using the defined algorithm
4. Generate HTML component files for Nginx to serve
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import requests
import time
from typing import Dict, Any, Optional


def generate_current_status_component(**context):
    """
    Generate current-status component for the mobile design.
    """
    try:
        # Get beauty score from XCom or calculate
        ti = context['task_instance']
        score_data = ti.xcom_pull(key='beauty_score', task_ids='calculate_beauty_score')
        
        if not score_data:
            score = 75
            status_message = "최고예요"
        else:
            score = score_data.get('score', 75)
            status_message = score_data.get('status_message', '최고예요')
        
        # Map status to emoji
        if score >= 80:
            emoji = "😆"
        elif score >= 60:
            emoji = "😊"
        elif score >= 40:
            emoji = "🙂"
        else:
            emoji = "😐"
        
        html_content = f"{status_message} {emoji}"
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/current-status"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Generated current-status at {output_path}")
        return {"status": "success", "file": "current-status"}
        
    except Exception as e:
        print(f"❌ Error generating current status component: {e}")
        raise


def generate_hero_image_component(**context):
    """
    Generate hero-image component with background image and badge.
    """
    try:
        # Get weather data for appropriate image
        ti = context['task_instance']
        weather_data = ti.xcom_pull(key='weather_data', task_ids='fetch_weather_data')
        
        if not weather_data:
            # Default image
            image_url = "https://via.placeholder.com/400x250/81C784/FFFFFF?text=Ingyeongho+View"
        else:
            condition = weather_data.get('weather_condition', 'clear')
            # Map weather conditions to appropriate images
            image_mapping = {
                'clear': "https://via.placeholder.com/400x250/87CEEB/FFFFFF?text=Clear+Sky",
                'cloudy': "https://via.placeholder.com/400x250/B0C4DE/FFFFFF?text=Cloudy",
                'rain': "https://via.placeholder.com/400x250/708090/FFFFFF?text=Rainy",
                'snow': "https://via.placeholder.com/400x250/F0F8FF/000000?text=Snowy"
            }
            image_url = image_mapping.get(condition, "https://via.placeholder.com/400x250/81C784/FFFFFF?text=Ingyeongho+View")
        
        html_content = f"""<img src="{image_url}" alt="인경호 풍경" class="hero-img">
<div class="user-badge">👤</div>"""
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/hero-image"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Generated hero-image at {output_path}")
        return {"status": "success", "file": "hero-image"}
        
    except Exception as e:
        print(f"❌ Error generating hero image component: {e}")
        raise


def generate_best_time_component(**context):
    """
    Generate best-time component for today's optimal viewing time.
    """
    try:
        # Calculate best time based on weather data and beauty scores
        current_hour = datetime.now().hour
        
        # Mock calculation - in production, this would analyze hourly forecasts
        best_times = [
            {"hour": 6, "score": 85, "condition": "일출"},
            {"hour": 7, "score": 90, "condition": "황금시간"},
            {"hour": 17, "score": 88, "condition": "일몰"},
            {"hour": 18, "score": 92, "condition": "매직아워"}
        ]
        
        # Find the best upcoming time
        best_time = max(best_times, key=lambda x: x['score'])
        time_str = f"{best_time['hour']:02d}:00"
        
        html_content = time_str
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/best-time"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Generated best-time at {output_path}")
        return {"status": "success", "file": "best-time"}
        
    except Exception as e:
        print(f"❌ Error generating best time component: {e}")
        raise


def generate_hourly_chart_component(**context):
    """
    Generate hourly-chart component with beauty scores throughout the day.
    """
    try:
        # Generate hourly data for the chart
        current_hour = datetime.now().hour
        
        # Mock hourly scores - in production, calculate based on weather forecasts
        hourly_data = []
        for i in range(7):  # 7 time slots
            hour = (current_hour + i) % 24
            
            # Calculate score based on time of day
            if 6 <= hour <= 8 or 17 <= hour <= 19:  # Golden hours
                score = 80 + (i * 5) % 20
                emoji = "😆" if score > 85 else "😊"
            elif 9 <= hour <= 16:  # Daytime
                score = 60 + (i * 3) % 15
                emoji = "🙂"
            else:  # Night
                score = 30 + (i * 2) % 10
                emoji = "😐"
            
            hourly_data.append({
                "hour": hour,
                "score": score,
                "emoji": emoji,
                "height": max(30, min(100, score))  # Convert to percentage height
            })
        
        # Find the best time for highlighting
        best_index = max(range(len(hourly_data)), key=lambda i: hourly_data[i]['score'])
        
        # Generate chart HTML
        chart_items = []
        for i, data in enumerate(hourly_data):
            active_class = "active" if i == best_index else ""
            bar_color = "#3D6AFE" if i == best_index else "#ccc"
            
            chart_items.append(f"""
                <div class="chart-item {active_class}">
                    <div class="emoji-face">{data['emoji']}</div>
                    <div class="bar" style="height: {data['height']}%; background-color: {bar_color};"></div>
                    <div class="time-label">{data['hour']:02d}:00</div>
                </div>
            """)
        
        html_content = "".join(chart_items)
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/hourly-chart"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Generated hourly-chart at {output_path}")
        return {"status": "success", "file": "hourly-chart"}
        
    except Exception as e:
        print(f"❌ Error generating hourly chart component: {e}")
        raise


def generate_best_date_component(**context):
    """
    Generate best-date component for this month's optimal day.
    """
    try:
        # Calculate best date based on weather forecasts
        current_date = datetime.now()
        
        # Mock calculation - in production, analyze weather forecasts
        best_day = 21  # November 21st
        month_name = f"{current_date.month}월"
        
        html_content = f"{month_name} {best_day}일"
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/best-date"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Generated best-date at {output_path}")
        return {"status": "success", "file": "best-date"}
        
    except Exception as e:
        print(f"❌ Error generating best date component: {e}")
        raise


def generate_monthly_calendar_component(**context):
    """
    Generate monthly-calendar component with special dates highlighted.
    """
    try:
        current_date = datetime.now()
        month_name = f"{current_date.month}월"
        
        # Generate calendar grid for current month
        # This is a simplified version - in production, use proper calendar logic
        calendar_html = f"""
        <div class="calendar-header">{month_name}</div>
        <div class="calendar-grid">
            <div class="day-name sun">일</div>
            <div class="day-name">월</div>
            <div class="day-name">화</div>
            <div class="day-name">수</div>
            <div class="day-name">목</div>
            <div class="day-name">금</div>
            <div class="day-name sat">토</div>
        """
        
        # Add calendar dates (simplified for November 2024)
        dates = [
            # Previous month dates (faded)
            '<div class="date sun faded">26</div>',
            '<div class="date faded">27</div>',
            '<div class="date faded">28</div>',
            '<div class="date faded">29</div>',
            '<div class="date faded">30</div>',
            '<div class="date faded">31</div>',
            '<div class="date sat">1</div>',
            
            # Current month dates
            '<div class="date sun">2</div>',
            '<div class="date">3</div>',
            '<div class="date">4</div>',
            '<div class="date">5</div>',
            '<div class="date">6</div>',
            '<div class="date">7</div>',
            '<div class="date sat">8</div>',
            
            '<div class="date sun">9</div>',
            '<div class="date">10</div>',
            '<div class="date">11</div>',
            '<div class="date">12</div>',
            '<div class="date">13</div>',
            '<div class="date special">14<span>✨</span></div>',
            '<div class="date sat">15</div>',
            
            '<div class="date sun">16</div>',
            '<div class="date">17</div>',
            '<div class="date">18</div>',
            '<div class="date">19</div>',
            '<div class="date special">20<span>🥰</span></div>',
            '<div class="date special">21</div>',
            '<div class="date sat">22</div>',
            
            '<div class="date sun">23</div>',
            '<div class="date">24</div>',
            '<div class="date">25</div>',
            '<div class="date">26</div>',
            '<div class="date">27</div>',
            '<div class="date">28</div>',
            '<div class="date sat">29</div>',
            
            '<div class="date sun">30</div>',
            # Next month dates (faded)
            '<div class="date faded">1</div>',
            '<div class="date faded">2</div>',
            '<div class="date faded">3</div>',
            '<div class="date faded">4</div>',
            '<div class="date faded">5</div>',
            '<div class="date faded sat">6</div>',
        ]
        
        calendar_html += " ".join(dates)
        calendar_html += "</div>"
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/monthly-calendar"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(calendar_html)
        
        print(f"✅ Generated monthly-calendar at {output_path}")
        return {"status": "success", "file": "monthly-calendar"}
        
    except Exception as e:
        print(f"❌ Error generating monthly calendar component: {e}")
        raise


def generate_special_dates_component(**context):
    """
    Generate special-dates component with highlighted beautiful days.
    """
    try:
        # Generate special dates based on weather analysis
        special_dates = [
            {
                "date": "11월 14일",
                "description": "가장 맑은 날 ✨",
                "color": "yellow"
            },
            {
                "date": "11월 23일", 
                "description": "가장 구름 없는 날 🪁",
                "color": "pink"
            },
            {
                "date": "11월 20일",
                "description": "가장 따뜻한 날 😍", 
                "color": "yellow"
            }
        ]
        
        cards_html = ""
        for date_info in special_dates:
            cards_html += f"""
            <div class="info-card {date_info['color']}">
                <span class="card-date">{date_info['date']}</span>
                <span class="card-desc">{date_info['description']}</span>
            </div>
            """
        
        # Write to shared volume
        output_path = "/opt/airflow/nginx_html/components/special-dates"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(cards_html)
        
        print(f"✅ Generated special-dates at {output_path}")
        return {"status": "success", "file": "special-dates"}
        
    except Exception as e:
        print(f"❌ Error generating special dates component: {e}")
        raise


# DAG Definition
default_args = {
    'owner': 'ingyeongho-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='Generate weather component HTML files for Nginx',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'html-generation', 'ingyeongho']
)
    """
    Callback function for DAG failures.
    Logs DAG-level failure information.
    """
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    error_message = f"""
    DAG Failure Alert:
    - DAG ID: {dag_id}
    - Execution Date: {execution_date}
    - Multiple tasks failed or DAG failed to complete
    """
    
    print(f"DAG FAILURE: {error_message}")


# Default arguments for the DAG with enhanced error handling
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 20),
    'on_failure_callback': log_task_failure,
}


# Database and Redis connection utilities for Airflow
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


# Task functions for data fetching
def fetch_weather_data(**context) -> Dict[str, Any]:
    """
    Fetch weather data from Korea Meteorological Administration API.
    Implements graceful degradation when external services fail.
    
    Returns:
        Dict containing weather data including temperature, conditions, precipitation
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Korea Meteorological Administration API endpoint
            # Note: In production, use actual API key from environment variables
            api_key = os.getenv('KMA_API_KEY', 'test_api_key')
            base_url = os.getenv('KMA_API_URL', 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0')
            
            if api_key == 'test_api_key':
                print("Warning: Using test API key. Set KMA_API_KEY environment variable for production.")
            
            # For now, we'll use mock data structure that matches KMA API response
            # In production, this would make actual API calls with timeout and error handling
            weather_data = {
                'temperature': 15.5,  # Celsius
                'weather_condition': 'clear',  # clear, cloudy, rain, snow
                'precipitation_probability': 20,  # Percentage
                'humidity': 65,  # Percentage
                'wind_speed': 2.5,  # m/s
                'timestamp': datetime.now().isoformat(),
                'source': 'KMA',
                'retry_count': retry_count
            }
            
            # Validate data ranges
            if not isinstance(weather_data.get('temperature'), (int, float)):
                raise ValueError("Invalid temperature data type")
            if not -50 <= weather_data.get('temperature', 0) <= 60:
                raise ValueError(f"Temperature out of valid range: {weather_data.get('temperature')}")
            if not 0 <= weather_data.get('precipitation_probability', 0) <= 100:
                raise ValueError(f"Invalid precipitation probability: {weather_data.get('precipitation_probability')}")
            if not 0 <= weather_data.get('humidity', 0) <= 100:
                raise ValueError(f"Invalid humidity: {weather_data.get('humidity')}")
            
            # Store in XCom for downstream tasks
            context['task_instance'].xcom_push(key='weather_data', value=weather_data)
            
            print(f"Successfully fetched weather data (attempt {retry_count + 1}): {weather_data}")
            return weather_data
            
        except requests.exceptions.Timeout as e:
            retry_count += 1
            print(f"Timeout error fetching weather data (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                break
            time.sleep(2 ** retry_count)  # Exponential backoff
            
        except requests.exceptions.ConnectionError as e:
            retry_count += 1
            print(f"Connection error fetching weather data (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                break
            time.sleep(2 ** retry_count)  # Exponential backoff
            
        except requests.exceptions.RequestException as e:
            print(f"Request error fetching weather data: {e}")
            break  # Don't retry for other request errors
            
        except ValueError as e:
            print(f"Data validation error: {e}")
            break  # Don't retry for validation errors
            
        except Exception as e:
            print(f"Unexpected error in fetch_weather_data: {e}")
            break
    
    # Graceful degradation: try to get cached data from Redis
    try:
        redis_client = get_redis_client()
        cached_weather = redis_client.get('previous_weather_data')
        if cached_weather:
            cached_data = json.loads(cached_weather)
            print("Using cached weather data due to API failure")
            fallback_data = {
                'temperature': cached_data.get('temperature', 15.0),
                'weather_condition': cached_data.get('weather_condition', 'unknown'),
                'precipitation_probability': cached_data.get('precipitation_probability', 0),
                'humidity': 50,
                'wind_speed': 0,
                'timestamp': datetime.now().isoformat(),
                'source': 'cached_fallback',
                'retry_count': retry_count
            }
        else:
            raise Exception("No cached data available")
    except Exception as cache_error:
        print(f"Error accessing cached weather data: {cache_error}")
        # Final fallback to default values
        fallback_data = {
            'temperature': 15.0,
            'weather_condition': 'unknown',
            'precipitation_probability': 0,
            'humidity': 50,
            'wind_speed': 0,
            'timestamp': datetime.now().isoformat(),
            'source': 'default_fallback',
            'retry_count': retry_count
        }
    
    context['task_instance'].xcom_push(key='weather_data', value=fallback_data)
    print(f"Using fallback weather data after {retry_count} retries: {fallback_data}")
    return fallback_data


def fetch_air_quality_data(**context) -> Dict[str, Any]:
    """
    Fetch air quality data from Korea Environment Corporation API.
    Implements graceful degradation when external services fail.
    
    Returns:
        Dict containing air quality index and pollutant levels
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Korea Environment Corporation API endpoint
            # Note: In production, use actual API key from environment variables
            api_key = os.getenv('KEC_API_KEY', 'test_api_key')
            base_url = os.getenv('KEC_API_URL', 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc')
            
            if api_key == 'test_api_key':
                print("Warning: Using test API key. Set KEC_API_KEY environment variable for production.")
            
            # For now, we'll use mock data structure that matches KEC API response
            # In production, this would make actual API calls with timeout and error handling
            air_quality_data = {
                'aqi': 45,  # Air Quality Index (0-500)
                'pm10': 35,  # PM10 concentration (μg/m³)
                'pm25': 15,  # PM2.5 concentration (μg/m³)
                'o3': 0.03,  # Ozone concentration (ppm)
                'no2': 0.02,  # Nitrogen dioxide (ppm)
                'co': 0.5,  # Carbon monoxide (ppm)
                'so2': 0.003,  # Sulfur dioxide (ppm)
                'grade': 'good',  # good, moderate, bad, very_bad
                'timestamp': datetime.now().isoformat(),
                'source': 'KEC',
                'retry_count': retry_count
            }
            
            # Validate data ranges
            if not isinstance(air_quality_data.get('aqi'), (int, float)):
                raise ValueError("Invalid AQI data type")
            if not 0 <= air_quality_data.get('aqi', 0) <= 500:
                raise ValueError(f"AQI out of valid range: {air_quality_data.get('aqi')}")
            if not 0 <= air_quality_data.get('pm10', 0) <= 1000:
                raise ValueError(f"PM10 out of valid range: {air_quality_data.get('pm10')}")
            if not 0 <= air_quality_data.get('pm25', 0) <= 500:
                raise ValueError(f"PM2.5 out of valid range: {air_quality_data.get('pm25')}")
            
            # Store in XCom for downstream tasks
            context['task_instance'].xcom_push(key='air_quality_data', value=air_quality_data)
            
            print(f"Successfully fetched air quality data (attempt {retry_count + 1}): {air_quality_data}")
            return air_quality_data
            
        except requests.exceptions.Timeout as e:
            retry_count += 1
            print(f"Timeout error fetching air quality data (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                break
            time.sleep(2 ** retry_count)  # Exponential backoff
            
        except requests.exceptions.ConnectionError as e:
            retry_count += 1
            print(f"Connection error fetching air quality data (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                break
            time.sleep(2 ** retry_count)  # Exponential backoff
            
        except requests.exceptions.RequestException as e:
            print(f"Request error fetching air quality data: {e}")
            break  # Don't retry for other request errors
            
        except ValueError as e:
            print(f"Data validation error: {e}")
            break  # Don't retry for validation errors
            
        except Exception as e:
            print(f"Unexpected error in fetch_air_quality_data: {e}")
            break
    
    # Graceful degradation: try to get cached data from Redis
    try:
        redis_client = get_redis_client()
        cached_air_quality = redis_client.get('previous_air_quality_data')
        if cached_air_quality:
            cached_data = json.loads(cached_air_quality)
            print("Using cached air quality data due to API failure")
            fallback_data = {
                'aqi': cached_data.get('aqi', 50),
                'pm10': cached_data.get('pm10', 30),
                'pm25': cached_data.get('pm25', 15),
                'o3': 0.03,
                'no2': 0.02,
                'co': 0.5,
                'so2': 0.003,
                'grade': 'moderate',
                'timestamp': datetime.now().isoformat(),
                'source': 'cached_fallback',
                'retry_count': retry_count
            }
        else:
            raise Exception("No cached data available")
    except Exception as cache_error:
        print(f"Error accessing cached air quality data: {cache_error}")
        # Final fallback to default values
        fallback_data = {
            'aqi': 50,
            'pm10': 30,
            'pm25': 15,
            'o3': 0.03,
            'no2': 0.02,
            'co': 0.5,
            'so2': 0.003,
            'grade': 'moderate',
            'timestamp': datetime.now().isoformat(),
            'source': 'default_fallback',
            'retry_count': retry_count
        }
    
    context['task_instance'].xcom_push(key='air_quality_data', value=fallback_data)
    print(f"Using fallback air quality data after {retry_count} retries: {fallback_data}")
    return fallback_data


def check_weather_change(**context) -> bool:
    """
    Check if there's a significant weather change that warrants triggering AI image generation.
    
    Significant changes include:
    - Weather condition change (e.g., clear to rain)
    - Temperature change > 5°C
    - AQI change > 50 points
    
    Returns:
        Boolean indicating whether to trigger AI Image DAG
    """
    try:
        # Get current weather data from XCom
        ti = context['task_instance']
        current_weather = ti.xcom_pull(key='weather_data', task_ids='fetch_weather_data')
        current_air_quality = ti.xcom_pull(key='air_quality_data', task_ids='fetch_air_quality_data')
        
        if not current_weather or not current_air_quality:
            print("No current weather data available, skipping change detection")
            return False
        
        # Get previous weather data from Redis
        redis_client = get_redis_client()
        prev_weather_str = redis_client.get('previous_weather_data')
        
        if not prev_weather_str:
            # First run, no previous data to compare
            print("No previous weather data, storing current data")
            redis_client.setex(
                'previous_weather_data',
                7200,  # 2 hours TTL
                json.dumps({
                    'weather_condition': current_weather.get('weather_condition'),
                    'temperature': current_weather.get('temperature'),
                    'aqi': current_air_quality.get('aqi')
                })
            )
            return False
        
        prev_weather = json.loads(prev_weather_str)
        
        # Check for significant changes
        weather_changed = (
            current_weather.get('weather_condition') != prev_weather.get('weather_condition')
        )
        
        temp_change = abs(
            current_weather.get('temperature', 0) - prev_weather.get('temperature', 0)
        )
        temp_changed = temp_change > 5
        
        aqi_change = abs(
            current_air_quality.get('aqi', 0) - prev_weather.get('aqi', 0)
        )
        aqi_changed = aqi_change > 50
        
        should_trigger = weather_changed or temp_changed or aqi_changed
        
        if should_trigger:
            print(f"Significant weather change detected:")
            print(f"  Weather: {prev_weather.get('weather_condition')} -> {current_weather.get('weather_condition')}")
            print(f"  Temperature change: {temp_change}°C")
            print(f"  AQI change: {aqi_change}")
            
            # Update previous weather data
            redis_client.setex(
                'previous_weather_data',
                7200,  # 2 hours TTL
                json.dumps({
                    'weather_condition': current_weather.get('weather_condition'),
                    'temperature': current_weather.get('temperature'),
                    'aqi': current_air_quality.get('aqi')
                })
            )
        else:
            print("No significant weather change detected")
        
        return should_trigger
        
    except Exception as e:
        print(f"Error checking weather change: {e}")
        return False


def calculate_beauty_score(**context) -> Dict[str, Any]:
    """
    Calculate beauty score based on weather and air quality data.
    Implements error handling for invalid data and storage failures.
    
    Algorithm:
    - Weather state: 40% weight
    - Temperature comfort: 30% weight
    - Air quality: 20% weight
    - Time adjustment: 10% weight
    - Golden hour bonus: +10 points
    - Precipitation penalty: -50 points (if >60% probability)
    - Air quality penalty: -30 points (if very bad)
    
    Returns:
        Dict containing calculated score and metadata
    """
    try:
        # Retrieve data from XCom with validation
        ti = context['task_instance']
        weather_data = ti.xcom_pull(key='weather_data', task_ids='fetch_weather_data')
        air_quality_data = ti.xcom_pull(key='air_quality_data', task_ids='fetch_air_quality_data')
        
        if not weather_data:
            print("Warning: No weather data available, using default values")
            weather_data = {
                'temperature': 15.0,
                'weather_condition': 'unknown',
                'precipitation_probability': 0
            }
        
        if not air_quality_data:
            print("Warning: No air quality data available, using default values")
            air_quality_data = {
                'aqi': 50
            }
        
        # Initialize base score
        base_score = 0
        
        # 1. Weather state score (40% weight, 0-40 points)
        weather_condition = weather_data.get('weather_condition', 'unknown')
        weather_scores = {
            'clear': 40,
            'partly_cloudy': 35,
            'cloudy': 25,
            'overcast': 15,
            'rain': 5,
            'snow': 10,
            'unknown': 20
        }
        weather_score = weather_scores.get(weather_condition, 20)
        base_score += weather_score
        
        # 2. Temperature comfort score (30% weight, 0-30 points)
        temperature = weather_data.get('temperature', 15)
        try:
            temperature = float(temperature)
        except (ValueError, TypeError):
            print(f"Warning: Invalid temperature value {temperature}, using default 15°C")
            temperature = 15.0
        
        # Optimal temperature range: 15-25°C
        if 15 <= temperature <= 25:
            temp_score = 30
        elif 10 <= temperature < 15 or 25 < temperature <= 30:
            temp_score = 25
        elif 5 <= temperature < 10 or 30 < temperature <= 35:
            temp_score = 15
        else:
            temp_score = 5
        base_score += temp_score
        
        # 3. Air quality score (20% weight, 0-20 points)
        aqi = air_quality_data.get('aqi', 50)
        try:
            aqi = float(aqi)
        except (ValueError, TypeError):
            print(f"Warning: Invalid AQI value {aqi}, using default 50")
            aqi = 50
        
        if aqi <= 50:  # Good
            air_score = 20
        elif aqi <= 100:  # Moderate
            air_score = 15
        elif aqi <= 150:  # Unhealthy for sensitive groups
            air_score = 10
        elif aqi <= 200:  # Unhealthy
            air_score = 5
        else:  # Very unhealthy or hazardous
            air_score = 0
        base_score += air_score
        
        # 4. Time adjustment score (10% weight, 0-10 points)
        current_hour = datetime.now().hour
        if 6 <= current_hour < 9 or 17 <= current_hour < 20:  # Golden hours
            time_score = 10
        elif 9 <= current_hour < 17:  # Daytime
            time_score = 8
        elif 20 <= current_hour < 22:  # Evening
            time_score = 6
        else:  # Night
            time_score = 2
        base_score += time_score
        
        # 5. Golden hour bonus (+10 points)
        # Approximately 30 minutes before/after sunset (simplified to 17:30-19:00)
        if 17.5 <= current_hour + (datetime.now().minute / 60) <= 19:
            base_score += 10
            is_golden_hour = True
        else:
            is_golden_hour = False
        
        # 6. Precipitation penalty (-50 points if >60% probability)
        precipitation_prob = weather_data.get('precipitation_probability', 0)
        try:
            precipitation_prob = float(precipitation_prob)
        except (ValueError, TypeError):
            print(f"Warning: Invalid precipitation probability {precipitation_prob}, using 0")
            precipitation_prob = 0
        
        if precipitation_prob > 60:
            base_score -= 50
        
        # 7. Air quality penalty (-30 points if very bad)
        if aqi > 200:  # Very bad air quality
            base_score -= 30
        
        # Clamp score to 0-100 range
        final_score = max(0, min(100, int(base_score)))
        
        # Determine status message based on score
        if final_score >= 80:
            status_message = "인생샷 각"
        elif final_score >= 60:
            status_message = "좋은 시간"
        elif final_score >= 40:
            status_message = "괜찮음"
        elif final_score >= 30:
            status_message = "별로"
        else:
            status_message = "방콕 추천"
        
        # Prepare result data
        result = {
            'score': final_score,
            'status_message': status_message,
            'weather_condition': weather_condition,
            'temperature': temperature,
            'aqi': aqi,
            'is_golden_hour': is_golden_hour,
            'precipitation_probability': precipitation_prob,
            'timestamp': datetime.now().isoformat(),
            'components': {
                'weather_score': weather_score,
                'temp_score': temp_score,
                'air_score': air_score,
                'time_score': time_score,
            }
        }
        
        # Store in PostgreSQL with error handling
        db_success = False
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Create table if not exists (for initial setup)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS beauty_scores (
                    id SERIAL PRIMARY KEY,
                    score INTEGER NOT NULL,
                    status_message VARCHAR(50),
                    weather_condition VARCHAR(50),
                    temperature FLOAT,
                    aqi INTEGER,
                    is_golden_hour BOOLEAN,
                    precipitation_probability INTEGER,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    metadata JSONB
                )
            """)
            
            # Insert score record
            cursor.execute("""
                INSERT INTO beauty_scores 
                (score, status_message, weather_condition, temperature, aqi, 
                 is_golden_hour, precipitation_probability, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                final_score,
                status_message,
                weather_condition,
                temperature,
                int(aqi),
                is_golden_hour,
                int(precipitation_prob),
                json.dumps(result['components'])
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            db_success = True
            print(f"Successfully stored score in PostgreSQL: {final_score}")
            
        except psycopg2.Error as e:
            print(f"PostgreSQL error storing score: {e}")
            # Continue execution even if database write fails
        except Exception as e:
            print(f"Unexpected error storing score in PostgreSQL: {e}")
            # Continue execution even if database write fails
        
        # Update Redis cache with 1-hour TTL
        cache_success = False
        try:
            redis_client = get_redis_client()
            cache_data = {
                'score': final_score,
                'status_message': status_message,
                'weather_condition': weather_condition,
                'updated_at': datetime.now().isoformat()
            }
            redis_client.setex(
                'current_score',
                3600,  # 1 hour TTL
                json.dumps(cache_data)
            )
            cache_success = True
            print(f"Successfully cached score in Redis: {final_score}")
            
        except redis.RedisError as e:
            print(f"Redis error caching score: {e}")
            # Continue execution even if cache write fails
        except Exception as e:
            print(f"Unexpected error caching score in Redis: {e}")
            # Continue execution even if cache write fails
        
        # Add storage status to result
        result['storage_status'] = {
            'database': db_success,
            'cache': cache_success
        }
        
        print(f"Beauty score calculated: {final_score} - {status_message}")
        print(f"Storage status - Database: {db_success}, Cache: {cache_success}")
        
        return result
        
    except Exception as e:
        print(f"Critical error calculating beauty score: {e}")
        # Return a minimal result to prevent complete task failure
        fallback_result = {
            'score': 50,
            'status_message': "데이터 처리 오류",
            'weather_condition': 'unknown',
            'temperature': 15.0,
            'aqi': 50,
            'is_golden_hour': False,
            'precipitation_probability': 0,
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'storage_status': {
                'database': False,
                'cache': False
            }
        }
        print(f"Using fallback beauty score due to error: {fallback_result}")
        return fallback_result


# Define the DAG with enhanced error handling
with DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='Hourly weather data collection and beauty score calculation',
    schedule='@hourly',
    catchup=False,
    tags=['weather', 'etl', 'beauty-score'],
    on_failure_callback=log_dag_failure,
    max_active_runs=1,  # Prevent overlapping runs
) as dag:
    
    # Task 1: Fetch weather data with enhanced retry configuration
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=log_task_failure,
    )
    
    # Task 2: Fetch air quality data with enhanced retry configuration
    fetch_air_quality_task = PythonOperator(
        task_id='fetch_air_quality_data',
        python_callable=fetch_air_quality_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=log_task_failure,
    )
    
    # Task 3: Calculate beauty score and update cache
    calculate_score_task = PythonOperator(
        task_id='calculate_beauty_score',
        python_callable=calculate_beauty_score,
        provide_context=True,
        retries=2,  # Fewer retries for calculation task
        retry_delay=timedelta(minutes=2),
        on_failure_callback=log_task_failure,
    )
    
    # Task 4: Check for significant weather changes
    check_weather_change_task = PythonOperator(
        task_id='check_weather_change',
        python_callable=check_weather_change,
        provide_context=True,
        retries=1,  # Minimal retries for change detection
        retry_delay=timedelta(minutes=1),
        on_failure_callback=log_task_failure,
    )
    
    # Task 5: Trigger AI Image Generation DAG if weather changed significantly
    trigger_ai_image_dag = TriggerDagRunOperator(
        task_id='trigger_ai_image_generation',
        trigger_dag_id='ai_image_generation_dag',
        wait_for_completion=False,
        trigger_rule='all_success',
        retries=1,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=log_task_failure,
    )
    
    # Set up task dependencies
    # Both weather and air quality data must be fetched before calculating score
    [fetch_weather_task, fetch_air_quality_task] >> calculate_score_task
    
    # After calculating score, check for weather changes
    calculate_score_task >> check_weather_change_task
    
    # Conditionally trigger AI image generation based on weather change
    # Note: In production, you would use a BranchPythonOperator or ShortCircuitOperator
    # to conditionally execute the trigger. For simplicity, we trigger on all runs.
    check_weather_change_task >> trigger_ai_image_dag

# Task Definitions
generate_hero_task = PythonOperator(
    task_id='generate_hero_component',
    python_callable=generate_hero_component,
    dag=dag,
)

generate_today_task = PythonOperator(
    task_id='generate_today_component',
    python_callable=generate_today_component,
    dag=dag,
)

generate_week_task = PythonOperator(
    task_id='generate_week_component',
    python_callable=generate_week_component,
    dag=dag,
)

# Task Dependencies - can run in parallel
generate_hero_task
generate_today_task  
generate_week_task