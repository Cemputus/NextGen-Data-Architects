"""Test API endpoints"""
import requests
import json

BASE_URL = "http://localhost:5000"

def test_login():
    """Test login endpoint"""
    print("[1] Testing Login Endpoint...")
    try:
        # Test admin login
        response = requests.post(
            f"{BASE_URL}/api/login",
            json={"username": "admin", "password": "admin123"},
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            print(f"  [OK] Login successful! Token received: {data.get('access_token', '')[:20]}...")
            return data.get('access_token')
        else:
            print(f"  [ERROR] Login failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"  [ERROR] Login test failed: {e}")
        return None

def test_protected_endpoint(token):
    """Test protected endpoint"""
    print("\n[2] Testing Protected Endpoint (Dashboard Stats)...")
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(
            f"{BASE_URL}/api/dashboard/stats",
            headers=headers,
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            print(f"  [OK] Stats retrieved successfully!")
            print(f"  -> Total Students: {data.get('total_students', 0)}")
            print(f"  -> Total Courses: {data.get('total_courses', 0)}")
            print(f"  -> Total Enrollments: {data.get('total_enrollments', 0)}")
            print(f"  -> Average Grade: {data.get('avg_grade', 0)}")
            return True
        else:
            print(f"  [ERROR] Stats request failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"  [ERROR] Stats test failed: {e}")
        return False

def test_frontend():
    """Test frontend is accessible"""
    print("\n[3] Testing Frontend...")
    try:
        response = requests.get("http://localhost:3000", timeout=5)
        if response.status_code == 200:
            print(f"  [OK] Frontend is accessible!")
            return True
        else:
            print(f"  [ERROR] Frontend returned: {response.status_code}")
            return False
    except Exception as e:
        print(f"  [ERROR] Frontend test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("UCU System API Testing")
    print("=" * 60)
    
    # Test login
    token = test_login()
    
    if token:
        # Test protected endpoint
        test_protected_endpoint(token)
    
    # Test frontend
    test_frontend()
    
    print("\n" + "=" * 60)
    print("API Testing Complete!")
    print("=" * 60)

