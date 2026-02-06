from app import app

def test_home():
    """Test the home endpoint returns correct response."""
    with app.app_context():  # Required for Flask app context
        with app.test_client() as client:  # Test client context manager
            response = client.get('/')
            
            assert response.status_code == 200
            assert response.data == b"Hello, World!"
