import os
import json
import logging
from typing import Optional, Dict, Any
import httpx
from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse
from google.oauth2 import id_token
from google.auth.transport import requests
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bridgedb.auth")

# Configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
SECRET_KEY = os.getenv("SECRET_KEY")
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
REDIRECT_URI = f"{BASE_URL}/auth/callback"

if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SECRET_KEY]):
    raise ValueError("Missing required environment variables for authentication")

# JWT functions
def create_access_token(data: dict) -> str:
    try:
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + timedelta(hours=24)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
        return encoded_jwt
    except Exception as e:
        logger.error(f"Error creating access token: {str(e)}")
        raise

def verify_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except JWTError as e:
        logger.error(f"JWT error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error verifying token: {str(e)}")
        return None

# Google Auth functions
def get_google_auth_url() -> str:
    """Generate Google OAuth authorization URL"""
    return (
        "https://accounts.google.com/o/oauth2/auth"
        f"?client_id={GOOGLE_CLIENT_ID}"
        "&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=email+profile+openid"
        "&access_type=offline"
        "&prompt=consent"
    )

async def exchange_code_for_token(code: str) -> Dict[str, Any]:
    """Exchange authorization code for tokens"""
    try:
        logger.info(f"Exchanging code for token with redirect_uri: {REDIRECT_URI}")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": REDIRECT_URI
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            if response.status_code != 200:
                logger.error(f"Token exchange failed: {response.status_code} - {response.text}")
                raise HTTPException(status_code=400, detail="Failed to exchange code for token")
            
            return response.json()
    except Exception as e:
        logger.error(f"Error exchanging code for token: {str(e)}")
        raise

async def get_user_info(token_data: Dict[str, Any]) -> Dict[str, Any]:
    """Get user info from Google"""
    try:
        # Try using ID token first
        if "id_token" in token_data:
            try:
                # Verify the token
                idinfo = id_token.verify_oauth2_token(
                    token_data["id_token"], 
                    requests.Request(),
                    GOOGLE_CLIENT_ID
                )
                
                return {
                    "email": idinfo["email"],
                    "name": idinfo.get("name", idinfo["email"]),
                    "picture": idinfo.get("picture", "")
                }
            except Exception as e:
                logger.error(f"Error verifying ID token: {str(e)}")
        
        # Fallback to userinfo endpoint
        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError("No access token available")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://www.googleapis.com/oauth2/v3/userinfo",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if response.status_code != 200:
                logger.error(f"User info request failed: {response.status_code} - {response.text}")
                raise HTTPException(status_code=400, detail="Failed to get user info")
            
            user_data = response.json()
            return {
                "email": user_data["email"],
                "name": user_data.get("name", user_data["email"]),
                "picture": user_data.get("picture", "")
            }
    except Exception as e:
        logger.error(f"Error getting user info: {str(e)}")
        raise

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