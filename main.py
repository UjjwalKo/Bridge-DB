from fastapi import FastAPI, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect, Form, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import uvicorn
import json
import uuid
import logging
import os
import traceback
from dotenv import load_dotenv

# Import our modules - Use new google_auth instead of auth
from google_auth import (
    get_google_auth_url, exchange_code_for_token, get_user_info,
    get_current_user, require_user, create_access_token
)
from db import DatabaseConnector, SchemaInspector, DatabaseMigrator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("bridgedb")

# Get environment variables
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
SECRET_KEY = os.getenv("SECRET_KEY")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

# Define lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code (runs before app starts)
    logger.info(f"Starting BridgeDB application in {ENVIRONMENT} environment")
    logger.info(f"Google OAuth configured: Client ID ending in ...{GOOGLE_CLIENT_ID[-6:] if GOOGLE_CLIENT_ID else 'NOT CONFIGURED'}")
    logger.info(f"Base URL configured as: {BASE_URL}")
    
    yield  # This is where the application runs
    
    # Shutdown code (runs when app is shutting down)
    logger.info("Application shutdown: closing database connections")
    db_connector.disconnect("source")
    db_connector.disconnect("target")
    logger.info("Database connections closed successfully")

# Initialize FastAPI app with the lifespan
app = FastAPI(
    title="BridgeDB", 
    description="Database Migration Tool",
    lifespan=lifespan
)

# Configure templates
templates = Jinja2Templates(directory="templates")

# Add session middleware
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize database components
db_connector = DatabaseConnector()
schema_inspector = SchemaInspector(db_connector)
migrator = DatabaseMigrator(db_connector, schema_inspector)

# Store WebSocket connections
active_connections: Dict[str, List[WebSocket]] = {}

# Routes
@app.get("/", response_class=HTMLResponse)
async def root(request: Request, user: Optional[dict] = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, user: Optional[dict] = Depends(get_current_user)):
    if user:
        return RedirectResponse(url="/")
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/login/google")
async def login_google():
    """Redirect to Google OAuth login"""
    try:
        auth_url = get_google_auth_url()
        logger.info(f"Redirecting to Google auth URL: {auth_url}")
        return RedirectResponse(url=auth_url)
    except Exception as e:
        logger.error(f"Error in Google login: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": "Authentication error", "details": str(e)}
        )

@app.get("/auth/callback")
async def auth_callback(request: Request, code: str = None, error: str = None):
    """Handle OAuth callback from Google"""
    try:
        logger.info("Received callback from Google OAuth")
        logger.debug(f"Query params: code={bool(code)}, error={error}")
        
        if error:
            logger.error(f"OAuth error returned: {error}")
            return JSONResponse(
                status_code=400,
                content={"error": "Authentication failed", "details": error}
            )
        
        if not code:
            logger.error("No authorization code received")
            return JSONResponse(
                status_code=400,
                content={"error": "No authorization code received"}
            )
        
        # Exchange code for token
        token_data = await exchange_code_for_token(code)
        logger.info("Successfully exchanged code for token")
        
        # Get user info
        user_info = await get_user_info(token_data)
        logger.info(f"Authenticated user: {user_info.get('email')}")
        
        # Store user info in session
        request.session["user"] = user_info
        
        # Create JWT token
        access_token = create_access_token({"sub": user_info["email"]})
        
        # Redirect to dashboard with JWT as cookie
        response = RedirectResponse(url="/")
        response.set_cookie(
            key="access_token",
            value=access_token,
            httponly=True,
            max_age=86400,  # 24 hours
            secure=ENVIRONMENT == "production",
            samesite="lax"
        )
        
        return response
    except Exception as e:
        logger.error(f"Error in auth callback: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": "Authentication callback error", "details": str(e)}
        )

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    response = RedirectResponse(url="/login")
    response.delete_cookie("access_token")
    return response

# API Routes
@app.post("/api/connect")
async def connect_database(
    request: Request,
    user: dict = Depends(require_user)
):
    """Connect to a database"""
    try:
        data = await request.json()
        connection_id = data.get("connection_id", "default")
        db_type = data.get("db_type")
        config = data.get("config", {})
        
        if not db_type or not config:
            raise HTTPException(status_code=400, detail="Missing required parameters")
        
        result = await db_connector.connect(db_type, config, connection_id)
        return result
    except Exception as e:
        logger.error(f"Connection error: {str(e)}")
        return {"status": "error", "message": str(e)}

# The rest of main.py remains the same...
# ... (other routes and WebSocket code)

# Health check endpoint
@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy", 
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0"
    }

# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}")
    logger.error(traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "details": str(exc)}
    )

if __name__ == "__main__":
    host = os.getenv("APP_HOST", "localhost")
    port = int(os.getenv("APP_PORT", 8000))
    uvicorn.run(app, host=host, port=port)