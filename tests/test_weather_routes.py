#!/usr/bin/env python3
"""
Simple test script to verify the simplified architecture is working.
Tests the 5 main endpoints: main page, 3 components, and upload.
"""

import requests
import sys

def test_simplified_routes():
    """Test the 5 main routes in simplified architecture."""
    nginx_url = "http://localhost:8082"  # Nginx port
    fastapi_url = "http://localhost:8001"  # FastAPI port (direct)
    
    routes_to_test = [
        # Nginx-served routes
        (nginx_url, "/", "Main page (Nginx)"),
        (nginx_url, "/components/hero.html", "Hero component (Nginx)"),
        (nginx_url, "/components/today.html", "Today component (Nginx)"),
        (nginx_url, "/components/week.html", "Week component (Nginx)"),
        
        # FastAPI-only route (via Nginx proxy)
        (nginx_url, "/upload/image", "Upload endpoint (FastAPI via Nginx)"),
    ]
    
    print("Testing simplified architecture (5 endpoints)...")
    print("=" * 60)
    
    for base_url, route, description in routes_to_test:
        try:
            if route == "/upload/image":
                # Test POST endpoint with empty request (should fail gracefully)
                response = requests.post(f"{base_url}{route}", timeout=10)
                # Expect 422 (validation error) for missing file
                status = "✅ PASS" if response.status_code == 422 else f"❌ FAIL ({response.status_code})"
            else:
                # Test GET endpoints
                response = requests.get(f"{base_url}{route}", timeout=10)
                status = "✅ PASS" if response.status_code == 200 else f"❌ FAIL ({response.status_code})"
            
            print(f"{description:<35} {status}")
            
            if response.status_code not in [200, 422]:
                print(f"   Error: {response.text[:100]}...")
                
        except requests.exceptions.RequestException as e:
            print(f"{description:<35} ❌ ERROR - {str(e)}")
    
    print("=" * 60)
    print("✅ Architecture test completed!")
    print("\n📋 Summary:")
    print("- Main page: Served by Nginx")
    print("- 3 Components: Static HTML files served by Nginx")
    print("- Upload: Only FastAPI functionality, proxied by Nginx")
    print("- Data processing: Handled by Airflow DAGs")

if __name__ == "__main__":
    try:
        test_simplified_routes()
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(1)