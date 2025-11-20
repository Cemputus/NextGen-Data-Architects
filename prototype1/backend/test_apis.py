"""
Test all API endpoints to ensure they work correctly
"""
import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:5000"

def test_login_api():
    """Test login API with various user types"""
    print("\n" + "="*80)
    print("TESTING LOGIN API")
    print("="*80)
    
    # Test cases
    test_cases = [
        {
            "name": "Student Login (Access Number)",
            "data": {"identifier": "A26143", "password": "A26143@ucu"},
            "expected_role": "student"
        },
        {
            "name": "Admin Login",
            "data": {"identifier": "admin", "password": "admin123"},
            "expected_role": "sysadmin"
        },
        {
            "name": "Analyst Login",
            "data": {"identifier": "analyst", "password": "analyst123"},
            "expected_role": "analyst"
        },
        {
            "name": "Invalid Credentials",
            "data": {"identifier": "admin", "password": "wrong"},
            "expected_role": None,
            "should_fail": True
        },
        {
            "name": "Missing Password",
            "data": {"identifier": "admin"},
            "expected_role": None,
            "should_fail": True
        }
    ]
    
    results = []
    for test in test_cases:
        try:
            response = requests.post(f"{BASE_URL}/api/auth/login", json=test["data"], timeout=5)
            
            if test.get("should_fail"):
                if response.status_code in [400, 401]:
                    print(f"✓ {test['name']}: Correctly rejected (Status: {response.status_code})")
                    results.append(True)
                else:
                    print(f"✗ {test['name']}: Should have failed but got Status: {response.status_code}")
                    results.append(False)
            else:
                if response.status_code == 200:
                    data = response.json()
                    # Check role from user object or direct role field
                    role = data.get("role") or (data.get("user", {}).get("role") if data.get("user") else None)
                    if role == test["expected_role"]:
                        token_preview = data.get('access_token', '')[:20] if data.get('access_token') else 'N/A'
                        print(f"✓ {test['name']}: Success - Role: {role}, Token: {token_preview}...")
                        results.append(True)
                    else:
                        print(f"✗ {test['name']}: Wrong role. Expected: {test['expected_role']}, Got: {role}")
                        print(f"   Full response: {json.dumps(data, indent=2)[:200]}...")
                        results.append(False)
                else:
                    print(f"✗ {test['name']}: Failed with Status: {response.status_code}, Error: {response.text}")
                    results.append(False)
        except requests.exceptions.ConnectionError:
            print(f"✗ {test['name']}: Cannot connect to server. Is the backend running?")
            results.append(False)
        except Exception as e:
            print(f"✗ {test['name']}: Error - {str(e)}")
            results.append(False)
    
    return all(results)

def test_protected_endpoints():
    """Test protected endpoints with authentication"""
    print("\n" + "="*80)
    print("TESTING PROTECTED ENDPOINTS")
    print("="*80)
    
    # First login to get token
    try:
        login_response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"identifier": "admin", "password": "admin123"},
            timeout=5
        )
        
        if login_response.status_code != 200:
            print("✗ Cannot test protected endpoints - Login failed")
            return False
        
        token = login_response.json().get("access_token")
        headers = {"Authorization": f"Bearer {token}"}
        
        # Test endpoints
        endpoints = [
            ("/api/dashboard/stats", "Dashboard Stats"),
            ("/api/dashboard/students-by-department", "Students by Department"),
            ("/api/dashboard/grades-over-time", "Grades Over Time"),
            ("/api/dashboard/payment-status", "Payment Status"),
            ("/api/analytics/filter-options", "Filter Options"),
        ]
        
        results = []
        for endpoint, name in endpoints:
            try:
                response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, timeout=5)
                if response.status_code == 200:
                    print(f"✓ {name}: Success")
                    results.append(True)
                else:
                    print(f"✗ {name}: Failed with Status: {response.status_code}")
                    results.append(False)
            except Exception as e:
                print(f"✗ {name}: Error - {str(e)}")
                results.append(False)
        
        return all(results)
        
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to server. Is the backend running?")
        return False
    except Exception as e:
        print(f"✗ Error testing protected endpoints: {str(e)}")
        return False

def test_export_endpoints():
    """Test export endpoints"""
    print("\n" + "="*80)
    print("TESTING EXPORT ENDPOINTS")
    print("="*80)
    
    try:
        # Login first
        login_response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"identifier": "admin", "password": "admin123"},
            timeout=5
        )
        
        if login_response.status_code != 200:
            print("✗ Cannot test export endpoints - Login failed")
            return False
        
        token = login_response.json().get("access_token")
        headers = {"Authorization": f"Bearer {token}"}
        
        # Test Excel export
        try:
            response = requests.get(
                f"{BASE_URL}/api/export/excel",
                headers=headers,
                params={"type": "dashboard"},
                timeout=10
            )
            if response.status_code == 200 and response.headers.get('content-type', '').startswith('application'):
                print("✓ Excel Export: Success")
                return True
            else:
                print(f"✗ Excel Export: Failed with Status: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Excel Export: Error - {str(e)}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to server. Is the backend running?")
        return False
    except Exception as e:
        print(f"✗ Error testing export endpoints: {str(e)}")
        return False

if __name__ == "__main__":
    print("\n" + "="*80)
    print("API TESTING SUITE")
    print("="*80)
    print(f"Testing against: {BASE_URL}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run tests
    login_ok = test_login_api()
    protected_ok = test_protected_endpoints()
    export_ok = test_export_endpoints()
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Login API: {'✓ PASS' if login_ok else '✗ FAIL'}")
    print(f"Protected Endpoints: {'✓ PASS' if protected_ok else '✗ FAIL'}")
    print(f"Export Endpoints: {'✓ PASS' if export_ok else '✗ FAIL'}")
    
    if all([login_ok, protected_ok, export_ok]):
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed. Please check the output above.")

