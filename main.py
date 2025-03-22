"""
OANDA TradingView Bot - Main Application
========================================
This application receives TradingView alerts via webhooks, processes them,
and executes trades on OANDA platform.

Author: [Your Name]
Date: March 21, 2025
"""

import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import time
import json
import signal
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, List, Tuple, Callable, TypeVar, Union
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, Field
from functools import wraps
from enum import Enum

# Type variables for type hints
P = TypeVar('P', bound=Callable)
T = TypeVar('T')

# Basic logging setup (must come first)
def setup_basic_logging():
    """Initial basic logging before config is loaded"""
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
        
    root_logger.addHandler(console_handler)

setup_basic_logging()
logger = logging.getLogger('trading_bot')

class Settings(BaseModel):
    """Application settings loaded from environment variables"""
    oanda_account: str = Field(..., env="OANDA_ACCOUNT_ID")
    oanda_token: str = Field(..., env="OANDA_API_TOKEN")
    oanda_api_url: str = Field("https://api-fxpractice.oanda.com/v3", env="OANDA_API_URL")
    oanda_environment: str = Field("practice", env="OANDA_ENVIRONMENT")
    allowed_origins: str = Field("*", env="ALLOWED_ORIGINS")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    connect_timeout: int = Field(10, env="CONNECT_TIMEOUT")
    read_timeout: int = Field(30, env="READ_TIMEOUT")
    total_timeout: int = Field(45, env="TOTAL_TIMEOUT")
    max_retries: int = Field(3, env="MAX_RETRIES")
    base_delay: float = Field(1.0, env="BASE_DELAY")
    base_position: int = Field(5000, env="BASE_POSITION")
    max_daily_loss: float = Field(0.20, env="MAX_DAILY_LOSS")
    trade_24_7: bool = Field(False, env="TRADE_24_7")

    @validator('oanda_account', 'oanda_token', pre=True)
    def validate_credentials(cls, v):
        if not v:
            raise ValidationError("Missing required OANDA credentials")
        return v

    @validator('oanda_api_url')
    def validate_api_url(cls, v, values):
        environment = values.get('oanda_environment', 'practice')
        if environment == 'practice' and 'fxpractice' not in v:
            raise ValidationError("Practice environment requires fxpractice API URL")
        if environment == 'live' and 'fxtrade' not in v:
            raise ValidationError("Live environment requires fxtrade API URL")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = True
        env_file_encoding = "utf-8"

def load_settings() -> Settings:
    """Load settings with basic logging"""
    try:
        return Settings()
    except Exception as e:
        logger.critical(f"Configuration error: {str(e)}")
        raise

config = load_settings()

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, 'request_id', None),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        })

def setup_full_logging():
    """Proper logging setup with config values"""
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception:
        log_file = 'trading_bot.log'
    
    formatter = JSONFormatter()
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
    
    # Clear existing handlers
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
        
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

setup_full_logging()

# Log configuration details
logger.info(f"OANDA Account: {config.oanda_account}")
logger.info(f"API URL: {config.oanda_api_url}")
logger.info(f"Environment: {config.oanda_environment}")

class TradingError(Exception):
    """Base exception for trading-related errors"""
    pass

class MarketError(TradingError):
    """Errors related to market conditions"""
    pass

class OrderError(TradingError):
    """Errors related to order execution"""
    pass

class ValidationError(TradingError):
    """Errors related to data validation"""
    pass

def handle_async_errors(func: Callable) -> Callable:
    """Decorator for handling errors in async functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except TradingError as e:
            logger.error(f"Trading error in {func.__name__}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise TradingError(f"Internal error in {func.__name__}: {str(e)}") from e
    return wrapper

def handle_sync_errors(func: Callable) -> Callable:
    """Decorator for handling errors in synchronous functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TradingError as e:
            logger.error(f"Trading error in {func.__name__}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise TradingError(f"Internal error in {func.__name__}: {str(e)}") from e
    return wrapper

class OrderType(str, Enum):
    """Order types supported by OANDA"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    MARKET_IF_TOUCHED = "MARKET_IF_TOUCHED"

class TimeInForce(str, Enum):
    """Time in force options for orders"""
    GTC = "GTC"  # Good Till Cancelled
    GTD = "GTD"  # Good Till Date
    FOK = "FOK"  # Fill Or Kill
    IOC = "IOC"  # Immediate Or Cancel

class AlertAction(str, Enum):
    """Supported trading actions"""
    BUY = "BUY"
    SELL = "SELL"
    CLOSE = "CLOSE"
    CLOSE_LONG = "CLOSE_LONG"
    CLOSE_SHORT = "CLOSE_SHORT"

class AlertData(BaseModel):
    """Alert data model with validation"""
    symbol: str
    action: AlertAction
    timeframe: str = "1H"
    orderType: OrderType = OrderType.MARKET
    timeInForce: TimeInForce = TimeInForce.FOK
    percentage: float = 15.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None

    @validator('timeframe', pre=True)
    def validate_timeframe(cls, v):
        """Validate and standardize timeframe format"""
        if v is None:
            return "15M"  # Default
        if not isinstance(v, str):
            v = str(v)
        if v.isdigit():
            mapping = {1: "1H", 4: "4H", 12: "12H", 5: "5M", 15: "15M", 30: "30M"}
            try:
                num = int(v)
                v = mapping.get(num, f"{v}M")
            except ValueError:
                raise ValueError("Invalid timeframe value")
        pattern = re.compile(r'^(\d+)([mMhHdD])$')
        match = pattern.match(v)
        if not match:
            if v.isdigit():
                return f"{v}M"
            raise ValueError("Invalid timeframe format. Use '15M' or '1H' format")
        value, unit = match.groups()
        value = int(value)
        if unit.upper() == 'H':
            if value > 24:
                raise ValueError("Maximum timeframe is 24H")
            return f"{value}H"
        elif unit.upper() == 'M':
            if value > 1440:
                raise ValueError("Maximum timeframe is 1440M (24H)")
            return f"{value}M"
        elif unit.upper() == 'D':
            if value > 30:
                raise ValueError("Maximum timeframe is 30D")
            return f"{value}D"
        raise ValueError("Invalid timeframe format")

    @validator('percentage')
    def validate_percentage(cls, v):
        """Validate percentage is within bounds"""
        if v is None:
            return 15.0
        if not 0 < v <= 100:
            raise ValueError("Percentage must be between 0 and 100")
        return float(v)

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"

_session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create aiohttp session with proper headers"""
    global _session
    try:
        if not config.oanda_token:
            raise TradingError("Missing OANDA API token in configuration")
            
        if _session is None or _session.closed or force_new:
            if _session and not _session.closed:
                await _session.close()
            
            _session = aiohttp.ClientSession(
                timeout=HTTP_REQUEST_TIMEOUT,
                headers={
                    "Authorization": f"Bearer {config.oanda_token}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "RFC3339"
                }
            )
        return _session
    except Exception as e:
        logger.error(f"Session creation error: {str(e)}")
        raise

async def cleanup_session():
    """Close the aiohttp session"""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None

# Set up HTTP session timeouts
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

# Market session configuration
MARKET_SESSIONS = {
    "FOREX": {
        "hours": "24/5",
        "timezone": "UTC",
        "holidays": "US"
    },
    "XAU_USD": {
        "hours": "23:00-21:59",
        "timezone": "UTC",
        "holidays": []
    },
    "CRYPTO": {
        "hours": "24/7",
        "timezone": "UTC",
        "holidays": []
    }
}

# Instrument leverage based on Singapore MAS regulations
INSTRUMENT_LEVERAGES = {
    # Forex - 20:1 leverage for major and minor pairs
    "USD_CHF": 20, "EUR_USD": 20, "GBP_USD": 20,
    "USD_JPY": 20, "AUD_USD": 20, "USD_THB": 20,
    "CAD_CHF": 20, "NZD_USD": 20, "AUD_CAD": 20,
    "AUD_JPY": 20, "USD_SGD": 20, "EUR_JPY": 20,
    "GBP_JPY": 20, "USD_CAD": 20,
    # Crypto - 2:1 leverage
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2, "BTCUSD": 2,
    # Gold - 10:1 leverage
    "XAU_USD": 10
}

# TradingView field mapping
TV_FIELD_MAP = {
    'symbol': 'symbol',           # TradingView symbol field
    'action': 'action',           # Buy, Sell, etc.
    'timeframe': 'timeframe',     # Chart timeframe
    'orderType': 'orderType',     # Market, Limit, etc.
    'timeInForce': 'timeInForce', # Good til canceled, etc.
    'percentage': 'percentage',   # Risk percentage
    'account': 'account',         # OANDA account ID
    'id': 'id',                   # Alert ID
    'comment': 'comment'          # Additional comments
}

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade on OANDA based on alert data"""
    request_id = str(uuid.uuid4())
    instrument = standardize_symbol(alert_data['symbol']).upper()
    
    try:
        # Get account balance and calculate position size
        balance = await get_account_balance(alert_data.get('account', config.oanda_account))
        units, precision = await calculate_trade_size(instrument, alert_data['percentage'], balance)
        
        # Adjust units for sell orders
        if alert_data['action'].upper() == 'SELL':
            units = -abs(units)
            
        # Prepare order data
        order_data = {
            "order": {
                "type": alert_data['orderType'],
                "instrument": instrument,
                "units": str(units),
                "timeInForce": alert_data['timeInForce'],
                "positionFill": "DEFAULT"
            }
        }
        
        # Add client extensions if comment is present
        if alert_data.get('comment'):
            order_data["order"]["clientExtensions"] = {
                "comment": alert_data['comment'][:128]  # OANDA limits to 128 chars
            }
        
        # Get session and submit order
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        # Implement retry logic
        retries = 0
        max_retries = config.max_retries
        while retries < max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Trade executed successfully: {result}")
                        return True, result
                    
                    # Handle specific error cases
                    if "RATE_LIMIT" in response_text:
                        logger.warning(f"[{request_id}] Rate limit hit, retrying in 60s")
                        await asyncio.sleep(60)
                    elif "MARKET_HALTED" in response_text:
                        logger.error(f"[{request_id}] Market is halted")
                        return False, {"error": "Market is halted"}
                    else:
                        logger.warning(f"[{request_id}] Order error (code: {response.status}): {response_text}")
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    retries += 1
                    logger.warning(f"[{request_id}] Retry {retries}/{max_retries}")
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error: {str(e)}")
                if retries < max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}")
        return False, {"error": str(e)}

@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close an open position on OANDA"""
    request_id = str(uuid.uuid4())
    try:
        # Standardize the instrument name
        instrument = standardize_symbol(alert_data['symbol']).upper()
        account_id = alert_data.get('account', config.oanda_account)
        
        # Fetch current position details
        success, position_data = await get_open_positions(account_id)
        if not success:
            return False, position_data
            
        # Find the position to close
        position = next(
            (p for p in position_data.get('positions', [])
             if p['instrument'] == instrument),
            None
        )
        
        if not position:
            logger.warning(f"[{request_id}] No position found for {instrument}")
            return False, {"error": f"No open position for {instrument}"}
            
        # Determine units to close based on position type
        long_units = float(position['long'].get('units', '0'))
        short_units = float(position['short'].get('units', '0'))
        
        close_data = {
            "longUnits": "ALL" if long_units > 0 else "NONE",
            "shortUnits": "ALL" if short_units < 0 else "NONE"
        }
        
        # Execute the close
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/positions/{instrument}/close"
        
        async with session.put(url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"[{request_id}] Failed to close position: {error_text}")
                return False, {"error": f"Close failed: {error_text}"}
                
            result = await response.json()
            logger.info(f"[{request_id}] Position closed successfully: {result}")
            
            # Extract P&L
            pnl = 0.0
            try:
                if 'longOrderFillTransaction' in result and result['longOrderFillTransaction']:
                    pnl += float(result['longOrderFillTransaction'].get('pl', 0))
                
                if 'shortOrderFillTransaction' in result and result['shortOrderFillTransaction']:
                    pnl += float(result['shortOrderFillTransaction'].get('pl', 0))
                
                logger.info(f"[{request_id}] Position P&L: {pnl}")
            except Exception as e:
                logger.error(f"[{request_id}] Error calculating P&L: {str(e)}")
            
            return True, result
                
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}")
        return False, {"error": str(e)}

# Initialize global variables
alert_handler = None

def create_error_response(status_code: int, message: str, request_id: str) -> JSONResponse:
    """Helper to create consistent error responses"""
    return JSONResponse(
        status_code=status_code,
        content={"error": message, "request_id": request_id}
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper initialization and cleanup"""
    logger.info("Initializing application...")
    global alert_handler
    
    try:
        # Initialize session
        await get_session(force_new=True)
        
        # Initialize alert handler
        alert_handler = AlertHandler()
        await alert_handler.start()
        
        # Set up signal handlers
        def handle_signals():
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: loop.create_task(shutdown(s))
                )
                
        async def shutdown(sig):
            logger.info(f"Received exit signal {sig.name}")
            await cleanup()
        
        handle_signals()
        logger.info("Services initialized successfully")
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup()
        logger.info("Shutdown complete")

async def cleanup():
    """Clean up application resources"""
    tasks = []
    
    # Stop alert handler
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    
    # Close HTTP session
    tasks.append(cleanup_session())
    
    # Wait for all cleanup tasks
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# Create FastAPI app with lifespan
app = FastAPI(
    title="OANDA TradingView Bot",
    description="Trading bot for OANDA using TradingView signals",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log requests with timing information"""
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Add request_id to logging context
    logger.info(f"[{request_id}] {request.method} {request.url.path}")
    
    # Process request
    response = await call_next(request)
    
    # Log response time
    process_time = time.time() - start_time
    logger.info(f"[{request_id}] Completed in {process_time:.3f}s - Status: {response.status_code}")
    
    return response

##############################################################################
# API Endpoints
##############################################################################

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "time": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }

@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    """Handle incoming webhook from TradingView"""
    request_id = str(uuid.uuid4())
    try:
        # Parse JSON body
        body = await request.json()
        logger.info(f"[{request_id}] Received TradingView webhook: {json.dumps(body, indent=2)}")
        
        # Translate TradingView format to our alert format
        cleaned_data = translate_tradingview_signal(body)
        
        # Process the alert
        success, result = await alert_handler.process_alert(cleaned_data)
        
        if success:
            return JSONResponse(
                status_code=200,
                content={"status": "success", "request_id": request_id, "result": result}
            )
        else:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "request_id": request_id, "error": result}
            )
            
    except json.JSONDecodeError as e:
        logger.error(f"[{request_id}] Invalid JSON: {str(e)}")
        return create_error_response(400, f"Invalid JSON format: {str(e)}", request_id)
    except ValueError as e:
        logger.error(f"[{request_id}] Validation error: {str(e)}")
        return create_error_response(422, str(e), request_id)
    except TradingError as e:
        logger.error(f"[{request_id}] Trading error: {str(e)}")
        return create_error_response(400, str(e), request_id)
    except Exception as e:
        logger.error(f"[{request_id}] Unexpected error: {str(e)}", exc_info=True)
        return create_error_response(500, "Internal server error", request_id)

@app.post("/alerts")
async def handle_direct_alert(alert: AlertData):
    """Handle direct alert in our format"""
    request_id = str(uuid.uuid4())
    try:
        # Process the alert
        success, result = await alert_handler.process_alert(alert.dict())
        
        if success:
            return JSONResponse(
                status_code=200,
                content={"status": "success", "request_id": request_id, "result": result}
            )
        else:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "request_id": request_id, "error": result}
            )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error in direct alert: {str(e)}", exc_info=True)
        return create_error_response(500, "Internal server error", request_id)

@app.get("/positions")
async def get_positions():
    """Get all currently tracked positions"""
    try:
        positions = await alert_handler.position_tracker.get_all_positions()
        return {"status": "success", "positions": positions}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": "Failed to fetch positions"}
        )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "OANDA TradingView Bot",
        "status": "online",
        "version": "1.0.0",
        "endpoints": ["/tradingview", "/alerts", "/health", "/positions"]
    }

##############################################################################
# Main Entry Point
##############################################################################

if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment with fallback
    port = int(os.getenv("PORT", 8000))
    
    # Configure uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_config=None,  # Use our custom logging
        timeout_keep_alive=65,
        reload=False
    )
