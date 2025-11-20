"""
Utility functions for generating HTML components.
Used by Airflow DAGs to create static HTML files for Nginx.
"""
import os
from datetime import datetime
from typing import Dict, Any


def write_component_html(component_name: str, html_content: str) -> str:
    """
    Write HTML content to component file in shared volume.
    
    Args:
        component_name: Name of component (hero, today, week)
        html_content: HTML content to write
        
    Returns:
        Path to written file
    """
    output_path = f"/opt/airflow/nginx_html/components/{component_name}.html"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write HTML content
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Generated {component_name}.html at {output_path}")
    return output_path


def get_weather_data() -> Dict[str, Any]:
    """
    Fetch current weather data from APIs.
    Replace this with actual API calls to KMA.
    
    Returns:
        Dictionary with weather data
    """
    # Mock data - replace with real API calls
    return {
        "temperature": 23,
        "condition": "맑음",
        "icon": "☀️",
        "humidity": 65,
        "wind_speed": 2.1,
        "description": "완벽한 인경호 관람 날씨입니다"
    }


def calculate_beauty_score(weather_data: Dict[str, Any]) -> int:
    """
    Calculate beauty score based on weather conditions.
    
    Args:
        weather_data: Weather data dictionary
        
    Returns:
        Beauty score (0-100)
    """
    # Simple scoring algorithm - replace with your actual algorithm
    base_score = 50
    
    # Temperature scoring (optimal around 20-25°C)
    temp = weather_data.get('temperature', 20)
    if 20 <= temp <= 25:
        temp_score = 30
    elif 15 <= temp <= 30:
        temp_score = 20
    else:
        temp_score = 10
    
    # Weather condition scoring
    condition = weather_data.get('condition', '').lower()
    if '맑' in condition:
        condition_score = 20
    elif '구름' in condition:
        condition_score = 15
    else:
        condition_score = 5
    
    return min(100, base_score + temp_score + condition_score)


def get_weather_icon(condition: str) -> str:
    """
    Get appropriate emoji icon for weather condition.
    
    Args:
        condition: Weather condition string
        
    Returns:
        Emoji icon
    """
    condition_lower = condition.lower()
    
    if '맑' in condition_lower:
        return "☀️"
    elif '구름' in condition_lower:
        return "🌤️"
    elif '흐림' in condition_lower:
        return "☁️"
    elif '비' in condition_lower:
        return "🌧️"
    elif '눈' in condition_lower:
        return "❄️"
    else:
        return "🌤️"


def format_time(hour: int) -> str:
    """
    Format hour as time string.
    
    Args:
        hour: Hour (0-23)
        
    Returns:
        Formatted time string (HH:MM)
    """
    return f"{hour:02d}:00"


def get_day_name(weekday: int) -> str:
    """
    Get Korean day name from weekday number.
    
    Args:
        weekday: Weekday number (0=Monday, 6=Sunday)
        
    Returns:
        Korean day name
    """
    days = ["월", "화", "수", "목", "금", "토", "일"]
    return days[weekday] + "요일"