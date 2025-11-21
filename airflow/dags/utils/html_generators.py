"""
HTML component generators for nginx htmx endpoints
"""
from datetime import datetime
from typing import List, Dict, Any


def generate_current_status(weather_data: Dict[str, Any]) -> str:
    """
    Generate current-status component
    
    Args:
        weather_data: Dict with keys like 'condition', 'emoji'
    
    Returns:
        HTML string for current status
    """
    condition = weather_data.get('condition', '맑음')
    emoji = weather_data.get('emoji', '☀️')
    
    return f"{condition} {emoji}"


def generate_hero_image(image_path: str = "/images/latest.jpg") -> str:
    """
    Generate hero-image component
    
    Args:
        image_path: Path to the image file
    
    Returns:
        HTML string for hero image
    """
    return f'''<img src="{image_path}" alt="인경호 현재 모습" class="hero-img" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
<div style="display:none; width:100%; height:100%; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); align-items:center; justify-content:center; color:white; font-size:24px; font-weight:bold;">
    📷 인경호 이미지
</div>
<div class="user-badge">👤</div>'''


def generate_best_time(best_hour: int, best_minute: int) -> str:
    """
    Generate best-time component
    
    Args:
        best_hour: Hour (0-23)
        best_minute: Minute (0-59)
    
    Returns:
        HTML string for best time
    """
    return f"{best_hour:02d}:{best_minute:02d}"


def generate_hourly_chart(hourly_data: List[Dict[str, Any]]) -> str:
    """
    Generate hourly-chart component for 24 hours (00:00 to 23:00)
    
    Args:
        hourly_data: List of 24 dicts with keys: 'hour', 'score', 'emoji'
                     Example: [{'hour': 0, 'score': 40, 'emoji': '😴'}, ...]
                     hour should be 0-23
    
    Returns:
        HTML string for hourly chart
    """
    if not hourly_data or len(hourly_data) != 24:
        # Default fallback - generate 24 hours with sample data
        import random
        emoji_map = {
            range(0, 6): '😴',    # Night
            range(6, 9): '🌅',    # Early morning
            range(9, 12): '😊',   # Morning
            range(12, 15): '🙂',  # Noon
            range(15, 18): '😆',  # Afternoon
            range(18, 21): '🌆',  # Evening
            range(21, 24): '😌',  # Night
        }
        
        def get_emoji(hour):
            for hour_range, emoji in emoji_map.items():
                if hour in hour_range:
                    return emoji
            return '😊'
        
        hourly_data = [
            {'hour': h, 'score': random.randint(30, 100), 'emoji': get_emoji(h)}
            for h in range(24)
        ]
    
    # Find the best score for highlighting
    max_score = max(item['score'] for item in hourly_data)
    
    chart_items = []
    for item in hourly_data:
        score = item['score']
        hour = item['hour']
        emoji = item['emoji']
        is_best = score == max_score
        
        # Format time as HH:00
        time_str = f"{hour:02d}:00"
        
        # Choose color based on whether it's the best
        color = '#3D6AFE' if is_best else '#ccc'
        active_class = ' active' if is_best else ''
        
        chart_item = f'''<div class="chart-item{active_class}">
    <div class="emoji-face">{emoji}</div>
    <div class="bar" style="height: {score}%; background-color: {color};"></div>
    <div class="time-label">{time_str}</div>
</div>'''
        chart_items.append(chart_item)
    
    return '\n'.join(chart_items)


def generate_best_date(month: int, day: int) -> str:
    """
    Generate best-date component
    
    Args:
        month: Month number (1-12)
        day: Day number (1-31)
    
    Returns:
        HTML string for best date
    """
    return f"{month}월 {day}일"


def generate_monthly_calendar(year: int, month: int, special_dates: List[Dict[str, Any]]) -> str:
    """
    Generate monthly-calendar component
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        special_dates: List of dicts with keys: 'day', 'emoji'
                       Example: [{'day': 14, 'emoji': '✨'}, {'day': 20, 'emoji': '🥰'}]
    
    Returns:
        HTML string for monthly calendar
    """
    from calendar import monthcalendar, month_name
    import locale
    
    # Get calendar data
    cal = monthcalendar(year, month)
    
    # Create special dates lookup
    special_lookup = {item['day']: item['emoji'] for item in special_dates}
    
    # Month names in Korean
    month_names_kr = ['', '1월', '2월', '3월', '4월', '5월', '6월', 
                      '7월', '8월', '9월', '10월', '11월', '12월']
    
    html = f'<div class="calendar-header">{month_names_kr[month]}</div>\n'
    html += '<div class="calendar-grid">\n'
    
    # Day names
    day_names = [
        '<div class="day-name sun">일</div>',
        '<div class="day-name">월</div>',
        '<div class="day-name">화</div>',
        '<div class="day-name">수</div>',
        '<div class="day-name">목</div>',
        '<div class="day-name">금</div>',
        '<div class="day-name sat">토</div>',
    ]
    html += '    ' + '\n    '.join(day_names) + '\n    \n'
    
    # Get current day for highlighting
    today = datetime.now()
    current_day = today.day if today.year == year and today.month == month else None
    
    # Generate calendar dates
    for week in cal:
        week_html = []
        for day_idx, day in enumerate(week):
            if day == 0:
                # Empty cell (previous/next month)
                week_html.append('<div class="date faded"></div>')
            else:
                classes = ['date']
                
                # Add day-of-week classes
                if day_idx == 0:  # Sunday
                    classes.append('sun')
                elif day_idx == 6:  # Saturday
                    classes.append('sat')
                
                # Check if it's a special date
                if day in special_lookup:
                    classes.append('special')
                    emoji = special_lookup[day]
                    week_html.append(f'<div class="{" ".join(classes)}">{day}<span>{emoji}</span></div>')
                else:
                    week_html.append(f'<div class="{" ".join(classes)}">{day}</div>')
        
        html += '    ' + '\n    '.join(week_html) + '\n'
    
    html += '</div>'
    
    return html


def generate_special_dates(dates_data: List[Dict[str, Any]]) -> str:
    """
    Generate special-dates component
    
    Args:
        dates_data: List of dicts with keys: 'month', 'day', 'description', 'color'
                    Example: [{'month': 11, 'day': 14, 'description': '가장 맑은 날 ✨', 'color': 'yellow'}, ...]
    
    Returns:
        HTML string for special dates list
    """
    if not dates_data:
        # Default fallback
        dates_data = [
            {'month': 11, 'day': 14, 'description': '가장 맑은 날 ✨', 'color': 'yellow'},
            {'month': 11, 'day': 23, 'description': '가장 구름 없는 날 🪁', 'color': 'pink'},
            {'month': 11, 'day': 20, 'description': '가장 따뜻한 날 😍', 'color': 'yellow'},
        ]
    
    cards = []
    for date_info in dates_data:
        month = date_info['month']
        day = date_info['day']
        description = date_info['description']
        color = date_info.get('color', 'yellow')
        
        card = f'''<div class="info-card {color}">
    <span class="card-date">{month}월 {day}일</span>
    <span class="card-desc">{description}</span>
</div>'''
        cards.append(card)
    
    return '\n\n'.join(cards)


def write_component(component_name: str, content: str, output_dir: str = "/usr/share/nginx/html/components"):
    """
    Write component HTML to file
    
    Args:
        component_name: Name of the component (e.g., 'current-status')
        content: HTML content to write
        output_dir: Directory to write to
    """
    import os
    
    # Ensure directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Write file
    file_path = os.path.join(output_dir, component_name)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✓ Generated {component_name}")
