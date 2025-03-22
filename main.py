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

##############################################################################
# Core Configuration
##############################################################################

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
    """Load settings with graceful fallback to defaults"""
    try:
        config = Settings()
        # Add these logging statements here
        logger.info(f"OANDA Account: {config.oanda_account}")
        logger.info(f"API URL: {config.oanda_api_url}")
        logger.info(f"Environment: {config.oanda_environment}")
        return config
    except Exception as e:
        logger.warning(f"Error loading settings: {str(e)}. Using defaults.")
        return Settings(
            oanda_account=os.getenv("OANDA_ACCOUNT_ID", ""),
            oanda_token=os.getenv("OANDA_API_TOKEN", "")
        )

config = load_settings()

# Set up HTTP session timeouts
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

##############################################################################
# Instrument Configuration
##############################################################################

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

##############################################################################
# Exception Handling
##############################################################################

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

##############################################################################
# Logging Configuration
##############################################################################

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

def setup_logging() -> logging.Logger:
    """Configure logging with rotation and structured output"""
    try:
        # Try to create log directory if on Render
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception:
        # Fallback to local log file
        log_file = 'trading_bot.log'
    
    # Create structured JSON formatter
    formatter = JSONFormatter()
    
    # Configure file handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Add console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers and add our handlers
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
        
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Create app-specific logger
    app_logger = logging.getLogger('trading_bot')
    return app_logger

logger = setup_logging()

##############################################################################
# Data Models
##############################################################################

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

        # Handle simple numeric values
        if v.isdigit():
            mapping = {1: "1H", 4: "4H", 12: "12H", 5: "5M", 15: "15M", 30: "30M"}
            try:
                num = int(v)
                v = mapping.get(num, f"{v}M")
            except ValueError:
                raise ValueError("Invalid timeframe value")

        # Validate format
        pattern = re.compile(r'^(\d+)([mMhHdD])$')
        match = pattern.match(v)
        
        if not match:
            # Handle plain numbers
            if v.isdigit():
                return f"{v}M"
            raise ValueError("Invalid timeframe format. Use '15M' or '1H' format")
        
        value, unit = match.groups()
        value = int(value)
        
        # Normalize to minutes
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

##############################################################################
# Session Management
##############################################################################

_session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create aiohttp session with proper headers"""
    global _session
    try:
        # Add this token check first
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

##############################################################################
# Market Utilities
##############################################################################

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format for OANDA compatibility"""
    if not symbol:
        return symbol
        
    # Convert to uppercase and replace hyphen with underscore
    symbol_upper = symbol.upper().replace('-', '_')
    
    # Map common symbol formats
    crypto_symbol_map = {
        "BTCUSD": "BTC_USD",
        "ETHUSD": "ETH_USD",
        "LTCUSD": "LTC_USD",
        "XRPUSD": "XRP_USD"
    }
    
    forex_map = {
        "EURUSD": "EUR_USD",
        "GBPUSD": "GBP_USD",
        "USDJPY": "USD_JPY",
        "AUDUSD": "AUD_USD",
        "USDCAD": "USD_CAD",
        "USDCHF": "USD_CHF",
        "NZDUSD": "NZD_USD",
        "XAUUSD": "XAU_USD"  # Gold
    }
    
    # Check direct mappings first
    if symbol_upper in crypto_symbol_map:
        return crypto_symbol_map[symbol_upper]
    
    if symbol_upper in forex_map:
        return forex_map[symbol_upper]
    
    # If already in correct format, return as is
    if "_" in symbol_upper:
        return symbol_upper
    
    # For 6-character forex symbols
    if len(symbol_upper) == 6 and all(c.isalpha() for c in symbol_upper):
        return f"{symbol_upper[:3]}_{symbol_upper[3:]}"
            
    # For crypto with USD suffix
    for prefix in ["BTC", "ETH", "LTC", "XRP"]:
        if symbol_upper.startswith(prefix) and "USD" in symbol_upper:
            return f"{prefix}_USD"
            
    # Return original as fallback
    return symbol_upper

@handle_sync_errors
def check_market_hours(session_config: dict) -> bool:
    """Check if the market is currently open based on session config"""
    # Always trade if 24/7 mode is enabled
    if config.trade_24_7:
        return True
    
    # Get current time in the market's timezone
    tz = timezone(session_config['timezone'])
    now = datetime.now(tz)
    
    # Special cases for continuous trading
    if "24/7" in session_config['hours']:
        return True
    if "24/5" in session_config['hours']:
        return now.weekday() < 5  # Monday-Friday
    
    # Handle specified time ranges
    time_ranges = session_config['hours'].split('|')
    for time_range in time_ranges:
        start_str, end_str = time_range.split('-')
        start = datetime.strptime(start_str, "%H:%M").time()
        end = datetime.strptime(end_str, "%H:%M").time()
        
        # Check if current time is within range
        if start <= end:
            if start <= now.time() <= end:
                return True
        else:
            # For ranges crossing midnight
            if now.time() >= start or now.time() <= end:
                return True
    
    return False

def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable"""
    try:
        # Determine session type
        if any(c in instrument for c in ["BTC","ETH","XRP","LTC"]):
            session_type = "CRYPTO"
        elif "XAU" in instrument:
            session_type = "XAU_USD"
        else:
            session_type = "FOREX"
        
        # Verify session exists
        if session_type not in MARKET_SESSIONS:
            return False, f"Unknown session type for instrument {instrument}"
            
        # Check market hours
        if check_market_hours(MARKET_SESSIONS[session_type]):
            return True, "Market open"
        
        return False, f"Instrument {instrument} outside market hours"
        
    except Exception as e:
        logger.error(f"Error checking instrument tradeable status: {str(e)}")
        return False, f"Error checking trading status: {str(e)}"

@handle_async_errors
async def get_current_price(instrument: str, action: str) -> float:
    """Get current instrument price from OANDA"""
    try:
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account}/pricing"
        params = {"instruments": instrument}
        
        async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                raise ValueError(f"Price fetch failed: {error_text}")
            
            data = await response.json()
            if not data.get('prices'):
                raise ValueError("No price data received")
            
            # Get bid/ask prices
            bid = float(data['prices'][0]['bids'][0]['price'])
            ask = float(data['prices'][0]['asks'][0]['price'])
            
            # Return ask for buys, bid for sells
            return ask if action == 'BUY' else bid
            
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting price for {instrument}")
        raise
    except Exception as e:
        logger.error(f"Error getting price for {instrument}: {str(e)}")
        raise

##############################################################################
# Account and Position Management
##############################################################################

@handle_async_errors
async def get_account_balance(account_id: str = None) -> float:
    """Fetch account balance from OANDA"""
    try:
        account_id = account_id or config.oanda_account
        
        # Add this validation check
        if account_id != config.oanda_account:
            raise ValidationError("Account ID does not match configured account")
        
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/summary"
        
        async with session.get(url) as response:
            if response.status != 200:
                error_text = await response.text()
                raise ValueError(f"Account balance fetch failed: {error_text}")
                
            data = await response.json()
            return float(data['account']['balance'])
            
    except Exception as e:
        logger.error(f"Error fetching account balance: {str(e)}")
        raise

@handle_async_errors
async def get_open_positions(account_id: str = None) -> Tuple[bool, Dict[str, Any]]:
    """Fetch all open positions from OANDA"""
    try:
        account_id = account_id or config.oanda_account
        session = await get_session()
        
        url = f"{config.oanda_api_url}/accounts/{account_id}/openPositions"
        
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to fetch positions: {error_text}")
                return False, {"error": f"Position fetch failed: {error_text}"}
                
            positions_data = await response.json()
            return True, positions_data
            
    except asyncio.TimeoutError:
        logger.error("Timeout fetching positions")
        return False, {"error": "Request timeout"}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        return False, {"error": str(e)}

@handle_async_errors
async def calculate_trade_size(instrument: str, risk_percentage: float, balance: float) -> Tuple[float, int]:
    """Calculate trade size based on risk percentage and account balance"""
    if risk_percentage <= 0 or risk_percentage > 100:
        raise ValueError("Invalid percentage value")
        
    # Normalize the instrument symbol
    normalized_instrument = standardize_symbol(instrument)
    
    # Default minimum sizes for common instruments
    MIN_SIZES = {
        "BTC_USD": 0.0001,
        "ETH_USD": 0.002,
        "LTC_USD": 0.05,
        "XRP_USD": 0.2,
        "XAU_USD": 0.01,
        # Add forex defaults if needed
    }
    
    try:
        # Calculate the equity amount to risk
        equity_percentage = risk_percentage / 100
        equity_amount = balance * equity_percentage
        
        # Get leverage based on instrument
        leverage = INSTRUMENT_LEVERAGES.get(normalized_instrument, 20)  # Default to 20:1
        position_value = equity_amount * leverage
        
        # Handle different instrument types
        if 'XAU' in normalized_instrument:  # Gold
            precision = 2
            min_size = 0.01
            # Get current gold price
            price = await get_current_price(normalized_instrument, 'BUY')
            trade_size = position_value / price
            
        elif any(crypto in normalized_instrument for crypto in ["BTC", "ETH", "LTC", "XRP"]):  # Crypto
            # Extract crypto symbol
            crypto_symbol = next((c for c in ["BTC", "ETH", "LTC", "XRP"] if c in normalized_instrument), None)
            
            # Set precision based on crypto type
            precision = 8 if crypto_symbol == "BTC" else 6
            min_size = MIN_SIZES.get(f"{crypto_symbol}_USD", 0.0001)
            
            # Get current crypto price
            price = await get_current_price(normalized_instrument, 'BUY')
            trade_size = position_value / price
            
        else:  # Forex
            precision = 0
            min_size = 1
            trade_size = position_value
        
        # Apply minimum size constraint
        trade_size = max(min_size, trade_size)
        
        # Round to appropriate precision
        if precision > 0:
            trade_size = round(trade_size, precision)
        else:
            trade_size = int(round(trade_size))
        
        logger.info(f"Using {risk_percentage}% of equity with {leverage}:1 leverage. " 
                  f"Calculated trade size: {trade_size} for {normalized_instrument}")
                  
        return trade_size, precision
        
    except Exception as e:
        logger.error(f"Error calculating trade size: {str(e)}")
        raise

##############################################################################
# Trade Execution
##############################################################################

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

##############################################################################
# Alert Processing
##############################################################################

def translate_tradingview_signal(data: Dict[str, Any]) -> Dict[str, Any]:
    """Translate TradingView webhook data to our alert format"""
    logger.info(f"Incoming TradingView data: {json.dumps(data, indent=2)}")
    logger.info(f"Using field mapping: {json.dumps(TV_FIELD_MAP, indent=2)}")
    
    translated = {}
    missing_fields = []
    
    # Map fields according to TV_FIELD_MAP
    for k, v in TV_FIELD_MAP.items():
        logger.debug(f"Looking for key '{k}' mapped from '{v}'")
        value = data.get(v)
        if value is not None:
            translated[k] = value
            logger.debug(f"Found value for '{k}': {value}")
        elif k in ['symbol', 'action']:  # These are required fields
            missing_fields.append(f"{k} (mapped from '{v}')")
            logger.debug(f"Missing required field '{k}' mapped from '{v}'")
    
    # Log the translation process
    logger.info(f"Translated data: {json.dumps(translated, indent=2)}")
    
    if missing_fields:
        error_msg = f"Missing required fields: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
            
    # Apply defaults for optional fields
    translated.setdefault('timeframe', "15M")
    translated.setdefault('orderType', "MARKET")
    translated.setdefault('timeInForce', "FOK")
    translated.setdefault('percentage', 15)
    
    logger.info(f"Final data with defaults: {json.dumps(translated, indent=2)}")
    return translated

class PositionTracker:
    """Track open positions and reconcile with broker data"""
    def __init__(self):
        self.positions = {}  # Symbol -> position data
        self._lock = asyncio.Lock()
        self._running = False
        self._initialized = False
        self.daily_pnl = 0.0
        self.pnl_reset_date = datetime.now().date()

    async def start(self):
        """Initialize and start the position tracker"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:  # Double-check pattern
                    self._running = True
                    self.reconciliation_task = asyncio.create_task(self.reconcile_positions())
                    self._initialized = True
                    logger.info("Position tracker started")
    
    async def stop(self):
        """Gracefully stop the position tracker"""
        self._running = False
        if hasattr(self, 'reconciliation_task'):
            self.reconciliation_task.cancel()
            try:
                await self.reconciliation_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error stopping reconciliation task: {str(e)}")
        logger.info("Position tracker stopped")
    
    @handle_async_errors
    async def reconcile_positions(self):
        """Reconcile positions with broker data periodically"""
        while self._running:
            try:
                # Wait between reconciliation attempts
                await asyncio.sleep(900)  # Every 15 minutes
                
                logger.info("Starting position reconciliation")
                async with self._lock:
                    success, positions_data = await get_open_positions()
                    
                    if not success:
                        logger.error("Failed to fetch positions for reconciliation")
                        continue
                    
                    # Convert Oanda positions to a set for efficient lookup
                    oanda_positions = {
                        p['instrument'] for p in positions_data.get('positions', [])
                    }
                    
                    # Check each tracked position
                    for symbol in list(self.positions.keys()):
                        try:
                            if symbol not in oanda_positions:
                                # Position closed externally
                                old_data = self.positions.pop(symbol, None)
                                logger.warning(
                                    f"Removing stale position for {symbol}. "
                                    f"Old data: {old_data}"
                                )
                        except Exception as e:
                            logger.error(
                                f"Error reconciling position for {symbol}: {str(e)}"
                            )
                    
                    logger.info(
                        f"Reconciliation complete. Active positions: "
                        f"{list(self.positions.keys())}"
                    )
                    
            except asyncio.TimeoutError:
                logger.error("Position reconciliation timed out, will retry in next cycle")
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying on unexpected errors
    
    @handle_async_errors
    async def record_position(self, symbol: str, action: str, timeframe: str) -> bool:
        """Record a new position"""
        try:
            async with self._lock:
                current_time = datetime.now()
                
                position_data = {
                    'entry_time': current_time,
                    'position_type': 'LONG' if action.upper() == 'BUY' else 'SHORT',
                    'timeframe': timeframe,
                    'last_update': current_time
                }
                
                self.positions[symbol] = position_data
                logger.info(f"Recorded position for {symbol}: {position_data}")
                return True
                
        except Exception as e:
            logger.error(f"Error recording position for {symbol}: {str(e)}")
            return False
    
    @handle_async_errors
    async def clear_position(self, symbol: str) -> bool:
        """Clear a position from tracking"""
        try:
            async with self._lock:
                if symbol in self.positions:
                    position_data = self.positions.pop(symbol)
                    logger.info(f"Cleared position for {symbol}: {position_data}")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error clearing position for {symbol}: {str(e)}")
            return False
    
    async def get_position_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current position information for a symbol"""
        async with self._lock:
            return self.positions.get(symbol)
    
    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all current positions"""
        async with self._lock:
            return self.positions.copy()

    @handle_async_errors
    async def record_trade_pnl(self, pnl: float) -> None:
        """Record P&L from a trade and reset daily if needed"""
        async with self._lock:
            current_date = datetime.now().date()
            
            # Reset daily P&L if it's a new day
            if current_date != self.pnl_reset_date:
                logger.info(f"Resetting daily P&L (was {self.daily_pnl}) for new day: {current_date}")
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            
            # Add the P&L to today's total
            self.daily_pnl += pnl
            logger.info(f"Updated daily P&L: {self.daily_pnl}")
    
    async def get_daily_pnl(self) -> float:
        """Get current daily P&L"""
        async with self._lock:
            # Reset if it's a new day
            current_date = datetime.now().date()
            if current_date != self.pnl_reset_date:
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            
            return self.daily_pnl
    
    async def check_max_daily_loss(self, account_balance: float) -> Tuple[bool, float]:
        """Check if max daily loss has been reached"""
        daily_pnl = await self.get_daily_pnl()
        loss_percentage = abs(min(0, daily_pnl)) / account_balance
        
        if loss_percentage >= config.max_daily_loss:
            logger.warning(f"Max daily loss reached: {loss_percentage:.2%} (limit: {config.max_daily_loss:.2%})")
            return False, loss_percentage
        
        return True, loss_percentage

class AlertHandler:
    """Handler for processing trading alerts"""
    def __init__(self):
        self.position_tracker = PositionTracker()
        self._lock = asyncio.Lock()
        self._initialized = False
    
    async def start(self):
        """Initialize the handler only once"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:  # Double-check pattern
                    await self.position_tracker.start()
                    self._initialized = True
                    logger.info("Alert handler initialized")
    
    async def stop(self):
        """Stop the alert handler and cleanup resources"""
        try:
            await self.position_tracker.stop()
            logger.info("Alert handler stopped")
        except Exception as e:
            logger.error(f"Error stopping alert handler: {str(e)}")

    @handle_async_errors
    async def process_alert(self, alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Process trading alerts with validation and execution"""
        request_id = str(uuid.uuid4())
        logger.info(f"[{request_id}] Processing alert: {json.dumps(alert_data, indent=2)}")
    
        try:
            if not alert_data:
                logger.error(f"[{request_id}] Empty alert data received")
                return False, {"error": "Empty alert data"}
    
            async with self._lock:
                action = alert_data['action'].upper()
                symbol = alert_data['symbol']
                
                # Standardize the symbol
                instrument = standardize_symbol(symbol)
                logger.info(f"[{request_id}] Standardized instrument: {instrument}")
                
                # Get account balance
                account_id = alert_data.get('account', config.oanda_account)
                balance = await get_account_balance(account_id)
                
                # Check max daily loss (skip for CLOSE actions)
                if action not in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                    can_trade, loss_pct = await self.position_tracker.check_max_daily_loss(balance)
                    if not can_trade:
                        logger.error(f"[{request_id}] Max daily loss reached ({loss_pct:.2%}), rejecting trade")
                        return False, {"error": "Max daily loss limit reached"}
                    
                # Market condition check
                tradeable, reason = is_instrument_tradeable(instrument)
                if not tradeable:
                    logger.warning(f"[{request_id}] Market check failed: {reason}")
                    return False, {"error": reason}
    
                # Fetch current positions
                success, positions_data = await get_open_positions(account_id)
                if not success:
                    logger.error(f"[{request_id}] Position check failed: {positions_data}")
                    return False, positions_data
    
                # Position closure logic
                if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                    logger.info(f"[{request_id}] Processing close request")
                    success, result = await close_position(alert_data)
                    if success:
                        await self.position_tracker.clear_position(symbol)
                        return True, result
                    return False, result
    
                # Find existing position
                position = next(
                    (p for p in positions_data.get('positions', [])
                     if p['instrument'] == instrument),
                    None
                )
    
                # Close opposite positions if needed
                if position:
                    has_long = float(position['long'].get('units', '0')) > 0
                    has_short = float(position['short'].get('units', '0')) < 0
                    
                    if (action == 'BUY' and has_short) or (action == 'SELL' and has_long):
                        logger.info(f"[{request_id}] Closing opposite position")
                        close_data = {**alert_data, 'action': 'CLOSE'}
                        success, result = await close_position(close_data)
                        if not success:
                            logger.error(f"[{request_id}] Failed to close opposite position")
                            return False, {"error": "Failed to close opposite position", "details": result}
                        await self.position_tracker.clear_position(symbol)
    
                # Execute new trade
                logger.info(f"[{request_id}] Executing new trade")
                success, result = await execute_trade(alert_data)
                
                if success:
                    await self.position_tracker.record_position(
                        symbol,
                        action,
                        alert_data['timeframe']
                    )
                    return True, result
                
                return False, result
                
        except Exception as e:
            logger.error(f"[{request_id}] Critical error: {str(e)}", exc_info=True)
            return False, {"error": str(e)}

##############################################################################
# API Setup
##############################################################################

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
