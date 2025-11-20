"""
Utility functions for Ingyeongho Beauty Score Service.
"""
from datetime import date, timedelta
from calendar import monthrange
from typing import Dict, List, Optional


def get_status_message(score: int) -> str:
    """
    Map beauty score to appropriate status message.
    
    Requirements 1.2: Status message based on score ranges
    - 80-100: "인생샷 각"
    - 0-30: "방콕 추천"
    - Other ranges: intermediate messages
    
    Args:
        score: Beauty score integer between 0 and 100
        
    Returns:
        Status message string
    """
    if not isinstance(score, int):
        raise TypeError(f"Score must be an integer, got {type(score).__name__}")
    
    if score < 0 or score > 100:
        raise ValueError(f"Score must be between 0 and 100, got {score}")
    
    if 80 <= score <= 100:
        return "인생샷 각"
    elif 60 <= score < 80:
        return "좋은 시간"
    elif 40 <= score < 60:
        return "괜찮은 편"
    elif 30 <= score < 40:
        return "그저 그래요"
    else:  # 0 <= score < 30
        return "방콕 추천"


def get_score_color_class(score: int) -> str:
    """
    Map beauty score to Tailwind CSS color class for calendar display.
    
    Requirements 3.1: Color intensity mapping based on score ranges
    - 0-30: red (low score)
    - 31-50: orange (below average)
    - 51-70: yellow (average)
    - 71-79: lime (good)
    - 80-100: green (excellent)
    
    Args:
        score: Beauty score integer between 0 and 100
        
    Returns:
        Tailwind CSS color class string
    """
    if score >= 80:
        return "bg-green-500 text-white"
    elif score >= 71:
        return "bg-lime-400 text-gray-900"
    elif score >= 51:
        return "bg-yellow-400 text-gray-900"
    elif score >= 31:
        return "bg-orange-400 text-white"
    else:  # 0-30
        return "bg-red-500 text-white"


def get_seasonal_icon(target_date: date) -> Optional[str]:
    """
    Assign seasonal icon based on date ranges.
    
    Requirements 3.4: Seasonal icon assignment logic
    - Spring (March 20 - April 10): Cherry blossoms 🌸
    - Autumn (October 15 - November 15): Autumn leaves 🍂
    - Winter (November - February): Ducks 🦆
    
    Args:
        target_date: Date to check for seasonal features
        
    Returns:
        Emoji icon string or None if no special season
    """
    month = target_date.month
    day = target_date.day
    
    # Cherry blossom season (Spring)
    if (month == 3 and day >= 20) or (month == 4 and day <= 10):
        return "🌸"
    
    # Autumn leaves season
    if (month == 10 and day >= 15) or (month == 11 and day <= 15):
        return "🍂"
    
    # Duck season (Winter)
    if month in [11, 12, 1, 2]:
        return "🦆"
    
    return None


def generate_monthly_calendar_grid(year: int, month: int, scores_data: Dict[str, int]) -> List[List[Optional[Dict]]]:
    """
    Generate monthly calendar grid structure with scores and styling.
    
    Requirements 3.1, 3.4: Calendar generation with color coding and seasonal icons
    
    Args:
        year: Year for the calendar
        month: Month for the calendar (1-12)
        scores_data: Dictionary mapping date strings (YYYY-MM-DD) to scores
        
    Returns:
        List of weeks, each week is a list of day dictionaries or None for empty cells
        Each day dict contains: date, score, color_class, icon, day_number
    """
    # Get first day of month and number of days
    first_day_weekday, num_days = monthrange(year, month)
    
    # Create calendar grid (weeks x 7 days)
    calendar_grid = []
    current_week = []
    
    # Add empty cells for days before month starts
    for _ in range(first_day_weekday):
        current_week.append(None)
    
    # Add all days of the month
    for day in range(1, num_days + 1):
        target_date = date(year, month, day)
        date_str = target_date.isoformat()
        
        # Get score for this date (default to None if not available)
        score = scores_data.get(date_str)
        
        # Build day data
        day_data = {
            "date": date_str,
            "day_number": day,
            "score": score,
            "color_class": get_score_color_class(score) if score is not None else "bg-gray-200 text-gray-600",
            "icon": get_seasonal_icon(target_date)
        }
        
        current_week.append(day_data)
        
        # Start new week on Sunday (weekday 6)
        if len(current_week) == 7:
            calendar_grid.append(current_week)
            current_week = []
    
    # Add empty cells for remaining days in last week
    if current_week:
        while len(current_week) < 7:
            current_week.append(None)
        calendar_grid.append(current_week)
    
    return calendar_grid


def generate_calendar_html(year: int, month: int, scores_data: Dict[str, int]) -> str:
    """
    Generate pre-rendered calendar HTML for caching.
    
    Requirements 3.1: Monthly calendar with color-coded scores
    
    Args:
        year: Year for the calendar
        month: Month for the calendar (1-12)
        scores_data: Dictionary mapping date strings to scores
        
    Returns:
        HTML string for calendar grid
    """
    calendar_grid = generate_monthly_calendar_grid(year, month, scores_data)
    
    # Month name in Korean
    month_names = ["", "1월", "2월", "3월", "4월", "5월", "6월", 
                   "7월", "8월", "9월", "10월", "11월", "12월"]
    
    html_parts = []
    
    # Calendar header
    html_parts.append(f'''
    <div class="mb-4">
        <h3 class="text-xl font-bold text-gray-800 text-center">{year}년 {month_names[month]}</h3>
    </div>
    ''')
    
    # Weekday headers
    html_parts.append('''
    <div class="grid grid-cols-7 gap-1 mb-2">
        <div class="text-center text-sm font-semibold text-red-600 py-2">일</div>
        <div class="text-center text-sm font-semibold text-gray-700 py-2">월</div>
        <div class="text-center text-sm font-semibold text-gray-700 py-2">화</div>
        <div class="text-center text-sm font-semibold text-gray-700 py-2">수</div>
        <div class="text-center text-sm font-semibold text-gray-700 py-2">목</div>
        <div class="text-center text-sm font-semibold text-gray-700 py-2">금</div>
        <div class="text-center text-sm font-semibold text-blue-600 py-2">토</div>
    </div>
    ''')
    
    # Calendar grid
    html_parts.append('<div class="grid grid-cols-7 gap-1">')
    
    for week in calendar_grid:
        for day_data in week:
            if day_data is None:
                # Empty cell
                html_parts.append('<div class="aspect-square"></div>')
            else:
                # Day cell with score
                date_str = day_data["date"]
                day_num = day_data["day_number"]
                score = day_data["score"]
                color_class = day_data["color_class"]
                icon = day_data["icon"]
                
                # Build cell content
                cell_html = f'''
                <div 
                    class="aspect-square {color_class} rounded-lg p-2 cursor-pointer hover:opacity-80 transition flex flex-col items-center justify-center"
                    hx-get="/calendar/detail?date={date_str}"
                    hx-target="#calendar-modal"
                    hx-swap="innerHTML"
                >
                    <div class="text-xs font-semibold">{day_num}</div>
                '''
                
                if score is not None:
                    cell_html += f'<div class="text-lg font-bold">{score}</div>'
                else:
                    cell_html += '<div class="text-xs">-</div>'
                
                if icon:
                    cell_html += f'<div class="text-sm">{icon}</div>'
                
                cell_html += '</div>'
                html_parts.append(cell_html)
    
    html_parts.append('</div>')
    
    # Modal container
    html_parts.append('<div id="calendar-modal" class="mt-4"></div>')
    
    return ''.join(html_parts)
