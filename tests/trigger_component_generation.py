#!/usr/bin/env python3
"""
Manual script to trigger component generation.
Useful for testing without waiting for Airflow schedule.
"""
import os
import sys
from datetime import datetime

# Add the utils directory to path
sys.path.append('airflow/dags/utils')

try:
    from component_generator import write_component_html, get_weather_data, calculate_beauty_score, get_weather_icon
except ImportError:
    print("❌ Could not import component_generator. Make sure you're in the project root.")
    sys.exit(1)


def generate_all_components():
    """Generate all three components manually."""
    print("🚀 Generating HTML components...")
    
    # Ensure output directory exists
    os.makedirs("nginx/html/components", exist_ok=True)
    
    # Generate Hero Component
    weather_data = get_weather_data()
    hero_html = f"""<!-- Hero Component - Current Weather Conditions -->
<!-- Generated manually at {datetime.now().isoformat()} -->
<div class="bg-gradient-to-br from-blue-500 to-indigo-600 text-white rounded-xl p-6 shadow-lg">
    <div class="flex items-center justify-between mb-4">
        <h3 class="text-xl font-bold">🌤️ 현재 날씨</h3>
        <span class="text-sm opacity-80">{datetime.now().strftime("%H:%M")}</span>
    </div>
    
    <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
            <div class="text-4xl">{weather_data['icon']}</div>
            <div>
                <div class="text-3xl font-bold">{weather_data['temperature']}°C</div>
                <div class="text-lg opacity-90">{weather_data['condition']}</div>
            </div>
        </div>
        
        <div class="text-right">
            <div class="text-sm opacity-80 mb-1">습도: {weather_data['humidity']}%</div>
            <div class="text-sm opacity-80">바람: {weather_data['wind_speed']}m/s</div>
        </div>
    </div>
    
    <div class="mt-4 pt-4 border-t border-white/20">
        <p class="text-sm opacity-90">{weather_data['description']}</p>
    </div>
</div>"""
    
    with open("nginx/html/components/hero.html", 'w', encoding='utf-8') as f:
        f.write(hero_html)
    print("✅ Generated hero.html")
    
    # Generate Today Component
    today_html = f"""<!-- Today Component - Best Time Today -->
<!-- Generated manually at {datetime.now().isoformat()} -->
<div class="bg-gradient-to-br from-green-500 to-emerald-600 text-white rounded-xl p-6 shadow-lg">
    <div class="flex items-center justify-between mb-4">
        <h3 class="text-xl font-bold">⭐ 오늘의 베스트 타임</h3>
        <div class="bg-white/20 px-3 py-1 rounded-full text-sm font-semibold">
            BEST
        </div>
    </div>
    
    <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
            <div class="text-4xl">🌅</div>
            <div>
                <div class="text-3xl font-bold">06:30</div>
                <div class="text-lg opacity-90">일출</div>
            </div>
        </div>
        
        <div class="text-right">
            <div class="text-2xl font-bold text-yellow-200">95점</div>
            <div class="text-sm opacity-80">18°C</div>
        </div>
    </div>
    
    <div class="mt-4 pt-4 border-t border-white/20">
        <p class="text-sm opacity-90">06:30에 가장 아름다운 인경호를 만날 수 있습니다</p>
    </div>
</div>"""
    
    with open("nginx/html/components/today.html", 'w', encoding='utf-8') as f:
        f.write(today_html)
    print("✅ Generated today.html")
    
    # Generate Week Component
    week_html = f"""<!-- Week Component - Best Day This Week -->
<!-- Generated manually at {datetime.now().isoformat()} -->
<div class="bg-gradient-to-br from-purple-500 to-pink-600 text-white rounded-xl p-6 shadow-lg">
    <div class="flex items-center justify-between mb-4">
        <h3 class="text-xl font-bold">🏆 이번 주 베스트 데이</h3>
        <div class="bg-white/20 px-3 py-1 rounded-full text-sm font-semibold">
            WEEK BEST
        </div>
    </div>
    
    <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
            <div class="text-4xl">🌤️</div>
            <div>
                <div class="text-2xl font-bold">11월 22일</div>
                <div class="text-xl font-semibold">금요일</div>
                <div class="text-lg opacity-90">구름 조금</div>
            </div>
        </div>
        
        <div class="text-right">
            <div class="text-2xl font-bold text-yellow-200">92점</div>
            <div class="text-sm opacity-80">21°C</div>
        </div>
    </div>
    
    <div class="mt-4 pt-4 border-t border-white/20">
        <p class="text-sm opacity-90">이번 주 금요일이 가장 아름다운 인경호를 볼 수 있는 날입니다</p>
    </div>
</div>"""
    
    with open("nginx/html/components/week.html", 'w', encoding='utf-8') as f:
        f.write(week_html)
    print("✅ Generated week.html")
    
    print("\n🎉 All components generated successfully!")
    print("📁 Files created in: nginx/html/components/")
    print("🌐 Test at: http://localhost:8082")


if __name__ == "__main__":
    generate_all_components()