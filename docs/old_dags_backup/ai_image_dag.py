"""
AI Image Generation DAG for Ingyeongho Beauty Score System.

This DAG:
1. Runs every 3 hours as a fallback schedule
2. Can be triggered externally by Weather ETL DAG on significant weather changes
3. Generates weather-appropriate AI prompts based on current conditions
4. Calls Stable Diffusion or OpenAI DALL-E API to create background images
5. Stores generated images and updates Redis cache
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import requests
import psycopg2
import redis
import time
from typing import Dict, Any, Optional


# Error handling and logging utilities
def log_task_failure(context):
    """
    Callback function for task failures in AI Image DAG.
    Logs detailed error information and can be extended for alerting.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    error_message = f"""
    AI Image DAG Failure Alert:
    - DAG ID: {dag_id}
    - Task ID: {task_id}
    - Execution Date: {execution_date}
    - Exception: {str(exception) if exception else 'Unknown error'}
    - Log URL: {task_instance.log_url}
    """
    
    print(f"AI IMAGE TASK FAILURE: {error_message}")


def log_dag_failure(context):
    """
    Callback function for AI Image DAG failures.
    """
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    error_message = f"""
    AI Image DAG Failure Alert:
    - DAG ID: {dag_id}
    - Execution Date: {execution_date}
    - AI image generation pipeline failed
    """
    
    print(f"AI IMAGE DAG FAILURE: {error_message}")


# Default arguments for the DAG with enhanced error handling
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 11, 20),
    'on_failure_callback': log_task_failure,
}


# Database and Redis connection utilities
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


def generate_weather_prompt(**context) -> str:
    """
    Generate AI image prompt based on current weather conditions, season, and time of day.
    
    Requirements: 9.2
    
    Returns:
        String containing the generated prompt for AI image generation
    """
    try:
        # Get current weather data from Redis cache
        redis_client = get_redis_client()
        score_data_str = redis_client.get('current_score')
        
        if score_data_str:
            score_data = json.loads(score_data_str)
            weather_condition = score_data.get('weather_condition', 'clear')
        else:
            # Fallback to default
            weather_condition = 'clear'
        
        # Determine current season based on month
        current_month = datetime.now().month
        if current_month in [3, 4, 5]:
            season = 'spring'
            seasonal_element = 'cherry blossoms and fresh green leaves'
        elif current_month in [6, 7, 8]:
            season = 'summer'
            seasonal_element = 'lush greenery and vibrant flowers'
        elif current_month in [9, 10, 11]:
            season = 'autumn'
            seasonal_element = 'colorful autumn leaves and golden foliage'
        else:  # 12, 1, 2
            season = 'winter'
            seasonal_element = 'snow-covered landscape and bare trees'
        
        # Determine time of day
        current_hour = datetime.now().hour
        if 5 <= current_hour < 9:
            time_of_day = 'morning'
            lighting = 'soft morning light with golden sunrise'
        elif 9 <= current_hour < 17:
            time_of_day = 'afternoon'
            lighting = 'bright daylight with clear sky'
        elif 17 <= current_hour < 20:
            time_of_day = 'evening'
            lighting = 'warm golden hour light with sunset colors'
        else:
            time_of_day = 'night'
            lighting = 'moonlight with starry sky'
        
        # Weather-specific descriptions
        weather_descriptions = {
            'clear': 'clear blue sky with few clouds',
            'partly_cloudy': 'partly cloudy sky with scattered clouds',
            'cloudy': 'overcast sky with soft diffused light',
            'overcast': 'gray cloudy sky with muted colors',
            'rain': 'rainy atmosphere with water droplets and reflections',
            'snow': 'snowy weather with falling snowflakes',
            'unknown': 'atmospheric conditions'
        }
        weather_desc = weather_descriptions.get(weather_condition, 'beautiful weather')
        
        # Construct the prompt
        prompt = f"""A serene and beautiful Korean university lake landscape called Ingyeongho, 
{season} season with {seasonal_element}, 
{time_of_day} time with {lighting}, 
{weather_desc}, 
peaceful water reflections, 
natural scenery, 
photorealistic, 
high quality, 
cinematic composition, 
8k resolution"""
        
        # Clean up the prompt (remove extra whitespace and newlines)
        prompt = ' '.join(prompt.split())
        
        print(f"Generated prompt: {prompt}")
        
        # Store prompt in XCom for downstream tasks
        context['task_instance'].xcom_push(key='ai_prompt', value=prompt)
        context['task_instance'].xcom_push(key='season', value=season)
        context['task_instance'].xcom_push(key='time_of_day', value=time_of_day)
        context['task_instance'].xcom_push(key='weather_condition', value=weather_condition)
        
        return prompt
        
    except Exception as e:
        print(f"Error generating weather prompt: {e}")
        # Return a default prompt on error
        default_prompt = "A serene Korean university lake landscape, beautiful natural scenery, photorealistic, high quality"
        context['task_instance'].xcom_push(key='ai_prompt', value=default_prompt)
        return default_prompt


def generate_ai_image(**context) -> Dict[str, Any]:
    """
    Call AI image generation API (Stable Diffusion or OpenAI DALL-E) to create background image.
    Implements graceful degradation when AI services fail.
    
    Requirements: 9.3
    
    Returns:
        Dict containing image URL and metadata
    """
    max_retries = 2
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Retrieve prompt from XCom
            ti = context['task_instance']
            prompt = ti.xcom_pull(key='ai_prompt', task_ids='generate_prompt')
            
            if not prompt:
                print("Warning: No prompt available from upstream task, using default")
                prompt = "A serene Korean university lake landscape, beautiful natural scenery, photorealistic, high quality"
            
            # Determine which AI service to use based on environment variable
            ai_service = os.getenv('AI_IMAGE_SERVICE', 'mock')  # 'openai', 'stability', or 'mock'
            
            if ai_service == 'openai':
                # OpenAI DALL-E API with error handling
                api_key = os.getenv('OPENAI_API_KEY')
                if not api_key:
                    print("Warning: OPENAI_API_KEY not set, falling back to mock service")
                    ai_service = 'mock'
                else:
                    response = requests.post(
                        'https://api.openai.com/v1/images/generations',
                        headers={
                            'Authorization': f'Bearer {api_key}',
                            'Content-Type': 'application/json'
                        },
                        json={
                            'model': 'dall-e-3',
                            'prompt': prompt,
                            'n': 1,
                            'size': '1792x1024',
                            'quality': 'standard'
                        },
                        timeout=120  # Increased timeout for AI services
                    )
                    response.raise_for_status()
                    result = response.json()
                    image_url = result['data'][0]['url']
                
            elif ai_service == 'stability':
                # Stable Diffusion API with error handling
                api_key = os.getenv('STABILITY_API_KEY')
                if not api_key:
                    print("Warning: STABILITY_API_KEY not set, falling back to mock service")
                    ai_service = 'mock'
                else:
                    response = requests.post(
                        'https://api.stability.ai/v1/generation/stable-diffusion-xl-1024-v1-0/text-to-image',
                        headers={
                            'Authorization': f'Bearer {api_key}',
                            'Content-Type': 'application/json'
                        },
                        json={
                            'text_prompts': [{'text': prompt, 'weight': 1}],
                            'cfg_scale': 7,
                            'height': 1024,
                            'width': 1792,
                            'samples': 1,
                            'steps': 30
                        },
                        timeout=120  # Increased timeout for AI services
                    )
                    response.raise_for_status()
                    result = response.json()
                    # Stability AI returns base64 encoded image
                    image_data = result['artifacts'][0]['base64']
                    image_url = f"data:image/png;base64,{image_data}"
            
            if ai_service == 'mock':
                # Mock mode for development/testing or fallback
                print("Using mock AI image generation (no actual API call)")
                # Use a placeholder image service with seed based on prompt hash
                import hashlib
                seed = abs(hash(prompt)) % 10000
                image_url = f"https://picsum.photos/seed/{seed}/1792/1024"
            
            # Store result in XCom
            result_data = {
                'image_url': image_url,
                'prompt': prompt,
                'service': ai_service,
                'generated_at': datetime.now().isoformat(),
                'retry_count': retry_count
            }
            
            ti.xcom_push(key='ai_image_result', value=result_data)
            
            print(f"Successfully generated AI image using {ai_service} (attempt {retry_count + 1})")
            return result_data
            
        except requests.exceptions.Timeout as e:
            retry_count += 1
            print(f"Timeout error calling AI API (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                break
            time.sleep(10 * retry_count)  # Longer backoff for AI services
            
        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"Request error calling AI API (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                break
            time.sleep(10 * retry_count)
            
        except Exception as e:
            print(f"Unexpected error in generate_ai_image: {e}")
            break
    
    # Final fallback: use mock service
    print("All AI service attempts failed, using mock fallback")
    try:
        ti = context['task_instance']
        prompt = ti.xcom_pull(key='ai_prompt', task_ids='generate_prompt') or "lake landscape"
        
        import hashlib
        seed = abs(hash(prompt)) % 10000
        image_url = f"https://picsum.photos/seed/{seed}/1792/1024"
        
        fallback_data = {
            'image_url': image_url,
            'prompt': prompt,
            'service': 'mock_fallback',
            'generated_at': datetime.now().isoformat(),
            'retry_count': retry_count,
            'error': 'AI services unavailable, using fallback'
        }
        
        ti.xcom_push(key='ai_image_result', value=fallback_data)
        print(f"Using fallback mock image generation: {fallback_data}")
        return fallback_data
        
    except Exception as fallback_error:
        print(f"Even fallback failed: {fallback_error}")
        raise Exception("All image generation methods failed")


def save_and_cache_image(**context) -> Dict[str, Any]:
    """
    Download and save generated image to static files directory.
    Store image metadata in PostgreSQL and update Redis cache.
    Implements error handling for file operations and storage failures.
    
    Requirements: 9.3, 9.4
    
    Returns:
        Dict containing saved file path and metadata
    """
    file_path = None
    try:
        # Retrieve image data from XCom
        ti = context['task_instance']
        image_result = ti.xcom_pull(key='ai_image_result', task_ids='generate_image')
        prompt = ti.xcom_pull(key='ai_prompt', task_ids='generate_prompt')
        season = ti.xcom_pull(key='season', task_ids='generate_prompt')
        time_of_day = ti.xcom_pull(key='time_of_day', task_ids='generate_prompt')
        weather_condition = ti.xcom_pull(key='weather_condition', task_ids='generate_prompt')
        
        if not image_result or not image_result.get('image_url'):
            raise ValueError("No image URL available from upstream task")
        
        image_url = image_result['image_url']
        
        # Create directory if it doesn't exist with error handling
        static_dir = '/opt/airflow/dags/../app/static/generated'
        try:
            os.makedirs(static_dir, exist_ok=True)
        except OSError as e:
            print(f"Error creating static directory: {e}")
            # Try alternative path
            static_dir = '/tmp/generated_images'
            os.makedirs(static_dir, exist_ok=True)
            print(f"Using alternative directory: {static_dir}")
        
        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bg_{timestamp}.jpg"
        file_path = os.path.join(static_dir, filename)
        
        # Download and save image with retry logic
        max_download_retries = 3
        download_success = False
        
        for attempt in range(max_download_retries):
            try:
                if image_url.startswith('data:image'):
                    # Handle base64 encoded image (from Stability AI)
                    import base64
                    try:
                        image_data = image_url.split(',')[1]
                        decoded_data = base64.b64decode(image_data)
                        with open(file_path, 'wb') as f:
                            f.write(decoded_data)
                        download_success = True
                        break
                    except Exception as b64_error:
                        print(f"Error decoding base64 image (attempt {attempt + 1}): {b64_error}")
                        if attempt == max_download_retries - 1:
                            raise
                else:
                    # Download from URL (from OpenAI or placeholder)
                    response = requests.get(image_url, timeout=60, stream=True)
                    response.raise_for_status()
                    
                    # Check content type
                    content_type = response.headers.get('content-type', '')
                    if not content_type.startswith('image/'):
                        print(f"Warning: Unexpected content type: {content_type}")
                    
                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    download_success = True
                    break
                    
            except requests.exceptions.Timeout as e:
                print(f"Timeout downloading image (attempt {attempt + 1}): {e}")
                if attempt < max_download_retries - 1:
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                else:
                    raise
            except requests.exceptions.RequestException as e:
                print(f"Request error downloading image (attempt {attempt + 1}): {e}")
                if attempt < max_download_retries - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    raise
            except Exception as e:
                print(f"Unexpected error downloading image (attempt {attempt + 1}): {e}")
                if attempt == max_download_retries - 1:
                    raise
        
        if not download_success:
            raise Exception("Failed to download image after all retry attempts")
        
        print(f"Image saved to: {file_path}")
        
        # Prepare metadata with defaults for missing values
        web_path = f"/static/generated/{filename}"
        metadata = {
            'prompt': prompt or 'No prompt available',
            'season': season or 'unknown',
            'time_of_day': time_of_day or 'unknown',
            'weather_condition': weather_condition or 'unknown',
            'service': image_result.get('service', 'unknown'),
            'generated_at': datetime.now().isoformat()
        }
        
        # Store metadata in PostgreSQL with error handling
        db_success = False
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ai_generated_images (
                    id SERIAL PRIMARY KEY,
                    file_path VARCHAR(255) NOT NULL,
                    prompt TEXT,
                    season VARCHAR(20),
                    time_of_day VARCHAR(20),
                    weather_condition VARCHAR(50),
                    service VARCHAR(50),
                    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    metadata JSONB
                )
            """)
            
            # Insert image record
            cursor.execute("""
                INSERT INTO ai_generated_images 
                (file_path, prompt, season, time_of_day, weather_condition, service, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                web_path,
                metadata['prompt'],
                metadata['season'],
                metadata['time_of_day'],
                metadata['weather_condition'],
                metadata['service'],
                json.dumps(metadata)
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            db_success = True
            print(f"Image metadata stored in PostgreSQL")
            
        except psycopg2.Error as e:
            print(f"PostgreSQL error storing image metadata: {e}")
            # Continue even if database write fails
        except Exception as e:
            print(f"Unexpected error storing image metadata in PostgreSQL: {e}")
            # Continue even if database write fails
        
        # Update Redis cache with latest image path
        cache_success = False
        try:
            redis_client = get_redis_client()
            cache_data = {
                'image_path': web_path,
                'prompt': metadata['prompt'],
                'generated_at': datetime.now().isoformat()
            }
            # No expiration - keep until next image is generated
            redis_client.set('latest_bg_image', json.dumps(cache_data))
            cache_success = True
            print(f"Redis cache updated with new background image: {web_path}")
            
        except redis.RedisError as e:
            print(f"Redis error updating cache: {e}")
            # Continue even if cache update fails
        except Exception as e:
            print(f"Unexpected error updating Redis cache: {e}")
            # Continue even if cache update fails
        
        result = {
            'file_path': web_path,
            'local_path': file_path,
            'metadata': metadata,
            'storage_status': {
                'database': db_success,
                'cache': cache_success
            }
        }
        
        print(f"AI image generation complete: {web_path}")
        print(f"Storage status - Database: {db_success}, Cache: {cache_success}")
        return result
        
    except Exception as e:
        print(f"Critical error saving and caching image: {e}")
        
        # Clean up file if it was created but process failed
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"Cleaned up partial file: {file_path}")
            except Exception as cleanup_error:
                print(f"Error cleaning up file: {cleanup_error}")
        
        # Return a minimal result to prevent complete task failure
        fallback_result = {
            'file_path': None,
            'local_path': None,
            'metadata': {
                'error': str(e),
                'generated_at': datetime.now().isoformat()
            },
            'storage_status': {
                'database': False,
                'cache': False
            }
        }
        print(f"Using fallback result due to error: {fallback_result}")
        return fallback_result


# Define the DAG with enhanced error handling
with DAG(
    'ai_image_generation_dag',
    default_args=default_args,
    description='AI-powered background image generation based on weather conditions',
    schedule='0 */3 * * *',  # Every 3 hours at minute 0
    catchup=False,
    tags=['ai', 'image-generation', 'background'],
    on_failure_callback=log_dag_failure,
    max_active_runs=1,  # Prevent overlapping runs
) as dag:
    
    # Task 1: Generate weather-based prompt
    generate_prompt_task = PythonOperator(
        task_id='generate_prompt',
        python_callable=generate_weather_prompt,
        provide_context=True,
        retries=1,  # Minimal retries for prompt generation
        retry_delay=timedelta(minutes=2),
        on_failure_callback=log_task_failure,
    )
    
    # Task 2: Call AI image generation API with enhanced retry configuration
    generate_image_task = PythonOperator(
        task_id='generate_image',
        python_callable=generate_ai_image,
        provide_context=True,
        retries=2,  # More retries for external API calls
        retry_delay=timedelta(minutes=10),
        on_failure_callback=log_task_failure,
    )
    
    # Task 3: Save image and update cache
    save_image_task = PythonOperator(
        task_id='save_and_cache_image',
        python_callable=save_and_cache_image,
        provide_context=True,
        retries=2,  # Retries for file operations
        retry_delay=timedelta(minutes=5),
        on_failure_callback=log_task_failure,
    )
    
    # Set up task dependencies
    generate_prompt_task >> generate_image_task >> save_image_task
