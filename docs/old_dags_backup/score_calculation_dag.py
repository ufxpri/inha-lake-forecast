"""
Score Calculation DAG for Ingyeongho Beauty Score System.

This DAG is triggered by the data collection DAG to:
1. Calculate beauty scores based on collected weather and air quality data
2. Generate hourly and daily predictions
3. Store results in PostgreSQL and Redis
4. Trigger component generation DAG when scores are updated
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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


def calculate_beauty_score(**context) -> Dict[str, Any]:
    """
    Calculate beauty score based on weather and air quality data.
    
    Algorithm:
    - Weather state: 40% weight
    - Temperature comfort: 30% weight  
    - Air quality: 20% weight
    - Time adjustment: 10% weight
    - Golden hour bonus: +10 points
    - Precipitation penalty: -50 points (if >60% probability)
    - Air quality penalty: -30 points (if very bad)
    """
    try:
        # Get latest weather and air quality data from Redis
        redis_client = get_redis_client()
        
        weather_data_str = redis_client.get('current_weather_data')
        air_quality_data_str = redis_client.get('current_air_quality_data')
        
        if not weather_data_str or not air_quality_data_str:
            print("Warning: No current data available, fetching from database")
            # Fallback to database
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT temperature, weather_condition, precipitation_probability, humidity, wind_speed
                FROM weather_data 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            weather_row = cursor.fetchone()
            
            cursor.execute("""
                SELECT aqi, pm10, pm25, grade
                FROM air_quality_data 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            air_quality_row = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if weather_row and air_quality_row:
                weather_data = {
                    'temperature': weather_row[0],
                    'weather_condition': weather_row[1],
                    'precipitation_probability': weather_row[2],
                    'humidity': weather_row[3],
                    'wind_speed': weather_row[4]
                }
                air_quality_data = {
                    'aqi': air_quality_row[0],
                    'pm10': air_quality_row[1],
                    'pm25': air_quality_row[2],
                    'grade': air_quality_row[3]
                }
            else:
                raise ValueError("No weather or air quality data available")
        else:
            weather_data = json.loads(weather_data_str)
            air_quality_data = json.loads(air_quality_data_str)
        
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
        temperature = float(weather_data.get('temperature', 15))
        
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
        aqi = float(air_quality_data.get('aqi', 50))
        
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
        current_time = current_hour + (datetime.now().minute / 60)
        if 17.5 <= current_time <= 19:
            base_score += 10
            is_golden_hour = True
        else:
            is_golden_hour = False
        
        # 6. Precipitation penalty (-50 points if >60% probability)
        precipitation_prob = float(weather_data.get('precipitation_probability', 0))
        if precipitation_prob > 60:
            base_score -= 50
        
        # 7. Air quality penalty (-30 points if very bad)
        if aqi > 200:
            base_score -= 30
        
        # Clamp score to 0-100 range
        final_score = max(0, min(100, int(base_score)))
        
        # Determine status message and emoji based on score
        if final_score >= 80:
            status_message = "최고예요"
            emoji = "😆"
        elif final_score >= 60:
            status_message = "좋아요"
            emoji = "😊"
        elif final_score >= 40:
            status_message = "괜찮아요"
            emoji = "🙂"
        elif final_score >= 30:
            status_message = "별로예요"
            emoji = "😐"
        else:
            status_message = "안좋아요"
            emoji = "😞"
        
        # Prepare result data
        result = {
            'score': final_score,
            'status_message': status_message,
            'emoji': emoji,
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
        
        print(f"Beauty score calculated: {final_score} - {status_message} {emoji}")
        return result
        
    except Exception as e:
        print(f"Critical error calculating beauty score: {e}")
        # Return fallback result
        return {
            'score': 50,
            'status_message': "데이터 처리 오류",
            'emoji': "😐",
            'weather_condition': 'unknown',
            'temperature': 15.0,
            'aqi': 50,
            'is_golden_hour': False,
            'precipitation_probability': 0,
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }


def generate_hourly_predictions(**context) -> List[Dict[str, Any]]:
    """
    Generate hourly beauty score predictions for the next 24 hours.
    """
    try:
        # Get current score data
        ti = context['task_instance']
        current_score_data = ti.xcom_pull(key='return_value', task_ids='calculate_beauty_score')
        
        if not current_score_data:
            raise ValueError("No current score data available")
        
        hourly_predictions = []
        current_hour = datetime.now().hour
        
        # Generate predictions for next 24 hours
        for i in range(24):
            hour = (current_hour + i) % 24
            
            # Base score from current conditions
            base_score = current_score_data['score']
            
            # Adjust based on time of day
            if 6 <= hour <= 8 or 17 <= hour <= 19:  # Golden hours
                time_adjustment = 10
                condition = "황금시간"
            elif 9 <= hour <= 16:  # Daytime
                time_adjustment = 5
                condition = "주간"
            elif 20 <= hour <= 22:  # Evening
                time_adjustment = 0
                condition = "저녁"
            else:  # Night
                time_adjustment = -15
                condition = "야간"
            
            # Add some variation based on weather patterns
            weather_variation = (i % 3) * 2 - 2  # -2, 0, 2 pattern
            
            predicted_score = max(0, min(100, base_score + time_adjustment + weather_variation))
            
            # Determine emoji based on score
            if predicted_score >= 80:
                emoji = "😆"
            elif predicted_score >= 60:
                emoji = "😊"
            elif predicted_score >= 40:
                emoji = "🙂"
            else:
                emoji = "😐"
            
            hourly_predictions.append({
                'hour': hour,
                'score': predicted_score,
                'condition': condition,
                'emoji': emoji,
                'timestamp': (datetime.now() + timedelta(hours=i)).isoformat()
            })
        
        print(f"Generated {len(hourly_predictions)} hourly predictions")
        return hourly_predictions
        
    except Exception as e:
        print(f"Error generating hourly predictions: {e}")
        # Return minimal predictions
        return [{
            'hour': (datetime.now().hour + i) % 24,
            'score': 50,
            'condition': "예측불가",
            'emoji': "😐",
            'timestamp': (datetime.now() + timedelta(hours=i)).isoformat()
        } for i in range(24)]


def store_scores(**context):
    """
    Store calculated scores and predictions in PostgreSQL and Redis.
    """
    try:
        # Get data from XCom
        ti = context['task_instance']
        score_data = ti.xcom_pull(key='return_value', task_ids='calculate_beauty_score')
        hourly_predictions = ti.xcom_pull(key='return_value', task_ids='generate_hourly_predictions')
        
        if not score_data:
            raise ValueError("No score data to store")
        
        # Store in PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create beauty_scores table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS beauty_scores (
                id SERIAL PRIMARY KEY,
                score INTEGER NOT NULL,
                status_message VARCHAR(50),
                emoji VARCHAR(10),
                weather_condition VARCHAR(50),
                temperature FLOAT,
                aqi INTEGER,
                is_golden_hour BOOLEAN,
                precipitation_probability INTEGER,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Create hourly_predictions table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_predictions (
                id SERIAL PRIMARY KEY,
                hour INTEGER,
                score INTEGER,
                condition VARCHAR(50),
                emoji VARCHAR(10),
                prediction_timestamp TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Insert current score
        cursor.execute("""
            INSERT INTO beauty_scores 
            (score, status_message, emoji, weather_condition, temperature, aqi, 
             is_golden_hour, precipitation_probability, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            score_data['score'],
            score_data['status_message'],
            score_data['emoji'],
            score_data['weather_condition'],
            score_data['temperature'],
            int(score_data['aqi']),
            score_data['is_golden_hour'],
            int(score_data['precipitation_probability']),
            json.dumps(score_data.get('components', {}))
        ))
        
        # Clear old hourly predictions and insert new ones
        cursor.execute("DELETE FROM hourly_predictions WHERE created_at < NOW() - INTERVAL '1 day'")
        
        if hourly_predictions:
            for prediction in hourly_predictions:
                cursor.execute("""
                    INSERT INTO hourly_predictions 
                    (hour, score, condition, emoji, prediction_timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    prediction['hour'],
                    prediction['score'],
                    prediction['condition'],
                    prediction['emoji'],
                    prediction['timestamp']
                ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Store in Redis cache
        redis_client = get_redis_client()
        
        # Cache current score with 1-hour TTL
        redis_client.setex(
            'current_beauty_score',
            3600,
            json.dumps(score_data)
        )
        
        # Cache hourly predictions with 2-hour TTL
        if hourly_predictions:
            redis_client.setex(
                'hourly_predictions',
                7200,
                json.dumps(hourly_predictions)
            )
        
        print("✅ Successfully stored beauty scores and predictions")
        return {
            'status': 'success',
            'score': score_data['score'],
            'predictions_count': len(hourly_predictions) if hourly_predictions else 0,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error storing scores: {e}")
        raise


# Default arguments
default_args = {
    'owner': 'ingyeongho-score-team',
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
    'score_calculation_dag',
    default_args=default_args,
    description='Calculate beauty scores and generate predictions',
    schedule_interval=None,  # Triggered by data collection DAG
    catchup=False,
    tags=['score-calculation', 'predictions', 'beauty-score'],
    max_active_runs=1,
) as dag:
    
    # Task 1: Calculate current beauty score
    calculate_score_task = PythonOperator(
        task_id='calculate_beauty_score',
        python_callable=calculate_beauty_score,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 2: Generate hourly predictions
    generate_predictions_task = PythonOperator(
        task_id='generate_hourly_predictions',
        python_callable=generate_hourly_predictions,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 3: Store scores and predictions
    store_scores_task = PythonOperator(
        task_id='store_scores',
        python_callable=store_scores,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Task 4: Trigger component generation DAG
    trigger_component_generation = TriggerDagRunOperator(
        task_id='trigger_component_generation',
        trigger_dag_id='component_generation_dag',
        wait_for_completion=False,
        trigger_rule='all_success',
    )
    
    # Set up task dependencies
    calculate_score_task >> generate_predictions_task >> store_scores_task >> trigger_component_generation