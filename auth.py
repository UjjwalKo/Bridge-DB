from authlib.integrations.starlette_client import OAuth, StarletteIntegration
from authlib.integrations.base_client import BaseOAuth
from fastapi import Request, HTTPException, Depends
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import os
from typing import Optional
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bridgedb.auth")

# Load environment variables
load_dotenv()

# Configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
SECRET_KEY = os.getenv("SECRET_KEY")
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SECRET_KEY]):
    raise ValueError("Missing required environment variables for authentication")

# Custom OAuth class to handle URL issues
class CustomOAuth(OAuth):
    def __init__(self, base_url=None, **kwargs):
        super().__init__(**kwargs)
        self.base_url = base_url

    def register(self, name, **kwargs):
        """Override register to inject base_url into the client"""
        client = super().register(name, **kwargs)
        if hasattr(client, '_get_access_token_params'):
            original_method = client._get_access_token_params
            
            def patched_method(request, params, **kwargs):
                result = original_method(request, params, **kwargs)
                # Ensure redirect_uri has proper protocol
                if 'redirect_uri' in result and not result['redirect_uri'].startswith(('http://', 'https://')):
                    result['redirect_uri'] = f"{self.base_url}{result['redirect_uri']}"
                return result
                
            client._get_access_token_params = patched_method
        return client

# OAuth setup
oauth = CustomOAuth(base_url=BASE_URL)
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={
        "scope": "openid email profile",
        "prompt": "select_account"
    },
    # Explicitly set these URLs as fallbacks if metadata discovery fails
    access_token_url="https://oauth2.googleapis.com/token",
    authorize_url="https://accounts.google.com/o/oauth2/auth",
    userinfo_endpoint="https://openidconnect.googleapis.com/v1/userinfo",
)

# JWT functions
def create_access_token(data: dict) -> str:
    try:
        to_encode = data.copy()
        # Use timezone-aware datetime
        expire = datetime.now(timezone.utc) + timedelta(hours=24)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
        return encoded_jwt
    except Exception as e:
        logger.error(f"Error creating access token: {str(e)}")
        raise

def verify_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except JWTError as e:
        logger.error(f"JWT error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error verifying token: {str(e)}")
        return None

# Auth dependency
async def get_current_user(request: Request) -> Optional[dict]:
    # First check for JWT in cookies
    token = request.cookies.get("access_token")
    if token:
        user = verify_token(token)
        if user:
            return user
    
    # Fallback to session
    user = request.session.get("user")
    if user:
        return user
    
    return None

# Check if user is authenticated
async def require_user(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return user

# Initialize session middleware
def get_session_middleware():
    return SessionMiddleware(None, secret_key=SECRET_KEY, max_age=86400)  