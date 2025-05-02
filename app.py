from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Literal, Union, Any
import redis
from redis.exceptions import RedisError
import json
import uuid
import logging
import os
from enum import Enum
from datetime import datetime
import time
import math
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import requests
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("prediction_market")

# Constants for market operations
class OptionType(str, Enum):
    YES = "YES"
    NO = "NO"

class OrderType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(str, Enum):
    OPEN = "UNMATCHED"
    PARTIAL = "PARTIAL"
    FILLED = "MATCHED"
    CANCELLED = "CANCELED"
    SETTLED = "SETTLED"

class MarketStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    RESOLVED = "RESOLVED"

# Market state and configuration
VALID_PRICES = [round(0.5 + i * 0.1, 1) for i in range(91)]  # 0.5 to 9.5 in 0.5 increments
DEFAULT_MARKET_PRICE = 5.0  # Default price for both YES and NO
TOTAL_PAYOUT = 10.0  # Total payout per contract

FRAPPE_API_URL = os.environ.get('FRAPPE_API_URL')
FRAPPE_API_KEY = os.environ.get('FRAPPE_API_KEY')

# Redis connection manager
class RedisManager:
    def __init__(self, 
                 host=os.environ.get('REDIS_HOST', '127.0.0.1'),
                 port=int(os.environ.get('REDIS_PORT', 6379)),
                 db=0, 
                 pool_size=10):
        self.connection_pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            max_connections=pool_size
        )
    
    def get_connection(self):
        return redis.Redis(connection_pool=self.connection_pool)
    
    async def health_check(self):
        try:
            client = self.get_connection()
            return client.ping()
        except RedisError as e:
            logger.error(f"Redis health check failed: {str(e)}")
            return False

# Create the Redis manager
redis_manager = RedisManager()

# Request and response models
class OrderRequest(BaseModel):
    user_id: str = Field(..., description="User ID of the order maker")
    market_id: str = Field(..., description="Market ID for the order")
    option_type: OptionType = Field(..., description="Option type (YES or NO)")
    price: float = Field(..., description="Order price (0.5-9.5 in 0.5 increments)")
    quantity: int = Field(..., gt=0, description="Order quantity")
    order_type: OrderType = Field(..., description="Order type (BUY or SELL)")
    filled_quantity: int = Field(..., ge=0, description="Order quantity")
    order_id: Optional[str] = None
    status: Optional[str] = None
    linked_order_id: Optional[str] = None  # For SELL orders, link to original BUY order

    @validator('price')
    def validate_price(cls, value):
        # Validate price is within range and a valid increment
        if value not in VALID_PRICES:
            valid_prices_str = ", ".join([str(p) for p in VALID_PRICES])
            raise ValueError(f"Price must be one of the following values: {valid_prices_str}")
        return value
class MarketRequest(BaseModel):
    market_id: str = Field(..., description="Unique identifier for market")
    question: str = Field(..., description="Question being predicted")
    closing_time: str = Field(..., description="ISO formatted closing time")
    status: MarketStatus = Field(default=MarketStatus.OPEN)

class UpdateOrderRequest(BaseModel):
    order_id: str = Field(..., description="ID of the order to update")
    new_quantity: int = Field(..., gt=0, description="New order quantity (must be less than original quantity)")
 
class OrderResponse(BaseModel):
    order_id: str
    user_id: str
    market_id: str
    option_type: OptionType
    price: float
    quantity: int
    order_type: OrderType
    status: OrderStatus
    created_at: str
    updated_at: str
    filled_quantity: int
    linked_order_id: Optional[str] = None

class TradeResponse(BaseModel):
    trade_id: str
    market_id: str
    yes_order_id: str
    no_order_id: str
    yes_user_id: str
    no_user_id: str
    yes_price: float
    no_price: float
    quantity: int
    executed_at: str

class MarketPriceResponse(BaseModel):
    market_id: str
    last_updated: str
    YES: Dict[str, float]
    NO: Dict[str, float]

class ErrorResponse(BaseModel):
    detail: str

# App startup and shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting prediction market service")
    
    # Check Redis connection
    if not await redis_manager.health_check():
        logger.error("Failed to connect to Redis")
    
    yield  # App running
    
    # Shutdown
    logger.info("Shutting down prediction market service")

# Initialize FastAPI app
app = FastAPI(
    title="Prediction Market API",
    description="A prediction market platform that integrates with Frappe",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Specify allowed origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency to get Redis client
def get_redis():
    try:
        client = redis_manager.get_connection()
        return client
    except RedisError as e:
        logger.error(f"Redis connection error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection error"
        )

# Helper function to generate a unique ID with timestamp prefix
def generate_id(prefix):
    timestamp = int(time.time())
    unique_id = str(uuid.uuid4()).replace('-', '')[:12]
    return f"{prefix}_{timestamp}_{unique_id}"


# Frappe integration helpers
def send_updated_market_price(market_id, yes_price, no_price):
    """Update market in Frappe"""
    try:
        payload = {
            "market_id": market_id,
            "yes_price": yes_price,
            "no_price": no_price
        }
        
        headers = {
            "Authorization": f"Token {FRAPPE_API_KEY}"
        }
        
        response = requests.post(
            f"{FRAPPE_API_URL}/rewardapp.engine.update_market_price",
            json=payload,
            headers=headers,
            timeout=5
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to update market price: {response.text}")
            return False
        else:
            logger.info(f"market updated for market ID {market_id}")
            return True
            
    except Exception as e:
        logger.error(f"Error in updating market: {str(e)}")
        return False


def send_order_update_to_frappe(order):
    """Send order status update to Frappe asynchronously"""
    def _send_update():
        try:
            payload = {
                "order_id": order["order_id"],
                "user_id": order["user_id"],
                "market_id": order["market_id"],
                "option_type": order["option_type"],
                "price": order["price"],
                "quantity": order["quantity"],
                "filled_quantity": order["filled_quantity"],
                "order_type": order["order_type"],
                "status": order["status"]
            }
            
            headers = {
                "Authorization": f"Token {FRAPPE_API_KEY}"
            }
            
            response = requests.post(
                f"{FRAPPE_API_URL}/rewardapp.engine.update_order",
                json=payload,
                headers=headers,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to update order in Frappe: {response.text}")
                return False
            else:
                logger.info(f"Order {order['order_id']} status update sent to Frappe")
                return True
                
        except Exception as e:
            logger.error(f"Error sending order update to Frappe: {str(e)}")
            return False
    
    # Start the request in a separate thread
    import threading
    thread = threading.Thread(target=_send_update)
    thread.daemon = True
    thread.start()
    return True

def send_trades_to_frappe(trades):
    """Send executed trades to Frappe"""
    if not trades:
        return True
        
    try:
        trade_data = {
            "trades": [
                {
                    "trade_id": trade["trade_id"],
                    "first_user_order_id": trade["first_user_order_id"],
                    "second_user_order_id": trade["second_user_order_id"],
                    "market_id": trade["market_id"],
                    "first_user_id": trade["first_user_id"],
                    "second_user_id": trade["second_user_id"],
                    "first_user_price": trade["first_user_price"],
                    "second_user_price": trade["second_user_price"],
                    "first_user_option": trade["first_user_option"],
                    "second_user_option": trade["second_user_option"],
                    "quantity": trade["quantity"],
                    "executed_at": trade["executed_at"]
                }
                for trade in trades
            ]
        }
        
        headers = {
            "Authorization": f"Token {FRAPPE_API_KEY}"
        }
        
        response = requests.post(
            f"{FRAPPE_API_URL}/rewardapp.engine.trades",
            json=trade_data,
            headers=headers,
            timeout=5
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to send trades to Frappe: {response.text}")
            return False
        else:
            logger.info(f"Sent {len(trades)} trades to Frappe")
            return True
            
    except Exception as e:
        logger.error(f"Error sending trades to Frappe: {str(e)}")
        return False


# Market operations
class MarketManager:
    @staticmethod
    def initialize_market(redis_client, market_data):
        """Initialize a new market with default prices"""
        market_id = market_data["market_id"]
        
        # Prepare market record
        market_record = {
            "market_id": market_id,
            "question": market_data["question"],
            "closing_time": market_data["closing_time"],
            "status": market_data.get("status", MarketStatus.OPEN),
            "yes_price": DEFAULT_MARKET_PRICE,
            "no_price": DEFAULT_MARKET_PRICE,
            "yes_demand": 0,
            "no_demand": 0,
            "last_updated": datetime.utcnow().isoformat()
        }
        
        market_key = f"market:{market_id}:data"
        if not redis_client.get(market_key):
            # Store market data
            redis_client.set(market_key, json.dumps(market_record))
            
            # Add to open markets set
            redis_client.sadd("markets:open", market_id)
            
            logger.info(f"Initialized new market: {market_id}")
            return market_record
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Market already exist"
            )
    
    @staticmethod
    def get_market_data(redis_client, market_id):
        """Get market data including current prices"""
        market_key = f"market:{market_id}:data"
        
        # Get market data
        market_data_json = redis_client.get(market_key)
        if not market_data_json:
            logger.warning(f"Market {market_id} not found")
            return None
        
        return json.loads(market_data_json)
    
    
    @staticmethod
    def close_market(redis_client, market_id):
        """
        1. Update market status
        """
        try:
            logger.info(f"Starting market closure process for {market_id}")
            
            # Get market data
            market_key = f"market:{market_id}:data"
            market_data_json = redis_client.get(market_key)
            if not market_data_json:
                logger.error(f"Market {market_id} not found")
                return False
                
            market_data = json.loads(market_data_json)
            
            # Check if market is already closed or resolved
            if market_data["status"] != MarketStatus.OPEN:
                logger.warning(f"Market {market_id} is already in {market_data['status']} state")
                return False
            
            # Update status
            market_data["status"] = MarketStatus.CLOSED
            market_data["closed_at"] = datetime.utcnow().isoformat()
            
            # Save updates
            redis_client.set(market_key, json.dumps(market_data))
            
            # Move from open to closed set
            redis_client.srem("markets:open", market_id)
            redis_client.sadd("markets:closed", market_id)

            logger.info(f"Market {market_id} closed")
            return True
        
        except Exception as e:
            logger.error(f"Error closing market: {str(e)}")
            return False
    
# Order book operations
class OrderBook:

    @staticmethod
    def add_order(redis_client, order_data):
        """Add an order to the order book"""
        # Generate order ID if not provided
        if not order_data.get("order_id"):
            order_data["order_id"] = generate_id("order")
        
        # Set default status and other fields
        if not order_data.get("status"):
            order_data["status"] = OrderStatus.OPEN
            
        created_at = datetime.utcnow().isoformat()
        
        # Complete order record
        full_order = {
            **order_data,
            "created_at": order_data.get("created_at", created_at),
            "updated_at": order_data.get("updated_at", created_at)
        }

        market_id = order_data["market_id"]
        option_type = order_data["option_type"]
        order_type = order_data["order_type"]
        price = order_data["price"]
        order_id = order_data["order_id"]
        
        # Check if market exists and is open
        market_data = MarketManager.get_market_data(redis_client, market_id)
        if not market_data:
            logger.error(f"Market {market_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Market {market_id} not found"
            )
            
        if market_data["status"] != MarketStatus.OPEN:
            logger.error(f"Market {market_id} is not open for orders")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Market {market_id} is not open for orders"
            )

        # Keys for sorted sets
        order_book_key = f"order_book:{market_id}:{option_type}:{order_type}"
        
        # Store the individual order with all details
        order_key = f"order:{order_id}"
        
        try:
            # Start a transaction
            pipe = redis_client.pipeline()
            
            # Add to order book (price is the score)
            # For BUY orders, we want highest price first, so we negate the score
            score = -price if order_type == OrderType.BUY else price
            pipe.zadd(order_book_key, {order_id: score})
            
            # Store full order data
            pipe.set(order_key, json.dumps(full_order))
            
            # Add to user orders index
            pipe.sadd(f"user:{order_data['user_id']}:orders", order_id)
            
            # Add to market orders index
            pipe.sadd(f"market:{market_id}:{order_type}:orders", order_id)
            
            # Execute transaction
            pipe.execute()

            return full_order
        except RedisError as e:
            logger.error(f"Redis error adding order: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Failed to place order"
            )
        
    @staticmethod
    def get_order(redis_client, order_id):
        """Get order details by ID"""
        order_key = f"order:{order_id}"
        try:
            order_json = redis_client.get(order_key)
            if not order_json:
                return None
            return json.loads(order_json)
        except (RedisError, json.JSONDecodeError) as e:
            logger.error(f"Error retrieving order {order_id}: {str(e)}")
            return None
    
    @staticmethod
    def update_order(redis_client, order_id, updates):
        """Update an existing order"""
        order_key = f"order:{order_id}"
        try:
            # Get current order
            order_json = redis_client.get(order_key)
            if not order_json:
                return None
                
            order = json.loads(order_json)
            
            # Apply updates
            order.update(updates)
            order["updated_at"] = datetime.utcnow().isoformat()
            
            # Save updated order
            redis_client.set(order_key, json.dumps(order))
            
            # Notify Frappe about the update
            # send_order_update_to_frappe(order)
            
            return order
        except (RedisError, json.JSONDecodeError) as e:
            logger.error(f"Error updating order {order_id}: {str(e)}")
            return None
    
    @staticmethod
    def remove_order_from_book(redis_client, order):
        """Remove an order from the order book sorted set"""
        order_book_key = f"order_book:{order['market_id']}:{order['option_type']}:{order['order_type']}"
        try:
            redis_client.zrem(order_book_key, order["order_id"])
        except RedisError as e:
            logger.error(f"Error removing order from book: {str(e)}")
            raise
    
    @staticmethod
    def match_orders(redis_client, market_id):
        """Match orders for a market"""
        matched_trades = []
        
        # Get market data
        market_data = MarketManager.get_market_data(redis_client, market_id)
        if not market_data:
            logger.warning(f"Market {market_id} not found")
            return []
            
        # Only match orders for open markets
        if market_data["status"] != MarketStatus.OPEN:
            logger.info(f"Market {market_id} is not open, skipping matching")
            return []
        
        # First match BUY YES with BUY NO (new positions)
        buy_vs_buy_trades = OrderBook._match_buy_orders(redis_client, market_id)
        matched_trades.extend(buy_vs_buy_trades)
        
        # Then match SELL with BUY of same type (closing positions)
        sell_vs_buy_trades = OrderBook._match_sell_orders(redis_client, market_id)
        matched_trades.extend(sell_vs_buy_trades)
        
        # Update market prices after matching
        if matched_trades:
            # Get the updated market data to send the latest prices
            updated_market_data = MarketManager.get_market_data(redis_client, market_id)
            if updated_market_data:
                # Send the prices that were updated during matching
                send_updated_market_price(market_id, 
                                        updated_market_data["yes_price"], 
                                        updated_market_data["no_price"])
            # Send trades to Frappe
            send_trades_to_frappe([t for t in matched_trades if isinstance(t, dict)])
        
        return matched_trades

    @staticmethod
    def _match_buy_orders(redis_client, market_id):
        """Match BUY YES orders with BUY NO orders (creating new positions)"""
        matched_trades = []
        
        # Get all buy orders for YES
        yes_buy_key = f"order_book:{market_id}:{OptionType.YES}:{OrderType.BUY}"
        yes_buy_ids = redis_client.zrange(yes_buy_key, 0, -1, withscores=True)
        
        # Get all buy orders for NO
        no_buy_key = f"order_book:{market_id}:{OptionType.NO}:{OrderType.BUY}"
        no_buy_ids = redis_client.zrange(no_buy_key, 0, -1, withscores=True)
        
        logger.info(f"Attempting to match: {len(yes_buy_ids)} YES buy orders with {len(no_buy_ids)} NO buy orders")
        
        # Collect all valid YES orders
        yes_orders = []
        for yes_order_id, neg_score in yes_buy_ids:
            order = OrderBook.get_order(redis_client, yes_order_id)
            if order and order["status"] not in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
                filled_quantity = order.get("filled_quantity", 0)
                if filled_quantity is None:
                    filled_quantity = 0
                    
                remaining = order["quantity"] - filled_quantity
                if remaining > 0:
                    yes_orders.append({
                        "order_id": yes_order_id,
                        "order": order,
                        "price": -neg_score,
                        "remaining": remaining,
                        "filled_quantity": filled_quantity
                    })
        
        # Collect all valid NO orders
        no_orders = []
        for no_order_id, neg_score in no_buy_ids:
            order = OrderBook.get_order(redis_client, no_order_id)
            if order and order["status"] not in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
                filled_quantity = order.get("filled_quantity", 0)
                if filled_quantity is None:
                    filled_quantity = 0
                    
                remaining = order["quantity"] - filled_quantity
                if remaining > 0:
                    no_orders.append({
                        "order_id": no_order_id,
                        "order": order,
                        "price": -neg_score,
                        "remaining": remaining,
                        "filled_quantity": filled_quantity
                    })
                    
        logger.info(f"Valid orders: {len(yes_orders)} YES and {len(no_orders)} NO")
        
        # Process all YES orders
        yes_idx = 0
        while yes_idx < len(yes_orders):
            yes_data = yes_orders[yes_idx]
            yes_order_id = yes_data["order_id"]
            yes_order = yes_data["order"]
            yes_price = yes_data["price"]
            yes_remaining = yes_data["remaining"]
            yes_filled = yes_data["filled_quantity"]
            
            if yes_remaining <= 0:
                yes_idx += 1
                continue
                
            # Calculate complementary price
            complementary_no_price = TOTAL_PAYOUT - yes_price
            
            # Track if we found a match in this iteration
            match_found = False
            
            # Try to match with compatible NO orders
            no_idx = 0
            while no_idx < len(no_orders):
                no_data = no_orders[no_idx]
                no_order_id = no_data["order_id"]
                no_order = no_data["order"]
                no_price = no_data["price"]
                no_remaining = no_data["remaining"]
                no_filled = no_data["filled_quantity"]
                
                if no_remaining <= 0:
                    no_idx += 1
                    continue
                
                # Check that orders aren't from the same user
                if yes_order["user_id"] == no_order["user_id"]:
                    logger.info(f"Skipping match between orders from same user {yes_order['user_id']}")
                    no_idx += 1
                    continue
                    
                # Check if prices are compatible
                if no_price == complementary_no_price:
                    # Calculate the match quantity (smaller of the two remaining quantities)
                    match_quantity = min(yes_remaining, no_remaining)
                    
                    if match_quantity <= 0:
                        no_idx += 1
                        continue
                    
                    logger.info(f"Found match: YES order {yes_order_id} ({yes_remaining} units) with " +
                            f"NO order {no_order_id} ({no_remaining} units). " +
                            f"Will match {match_quantity} units.")
                    
                    # Get fresh copies of orders from Redis to ensure accurate state
                    yes_order_fresh = OrderBook.get_order(redis_client, yes_order_id)
                    no_order_fresh = OrderBook.get_order(redis_client, no_order_id)
                    
                    # Skip if orders have been modified or removed
                    if not yes_order_fresh or not no_order_fresh:
                        no_idx += 1
                        continue
                    
                    # Get current filled quantities from fresh data
                    fresh_yes_filled = yes_order_fresh.get("filled_quantity", 0)
                    if fresh_yes_filled is None:
                        fresh_yes_filled = 0
                        
                    fresh_no_filled = no_order_fresh.get("filled_quantity", 0)
                    if fresh_no_filled is None:
                        fresh_no_filled = 0
                    
                    # Check if filled quantities have changed
                    if fresh_yes_filled != yes_filled or fresh_no_filled != no_filled:
                        # Update our tracking with fresh data
                        yes_filled = fresh_yes_filled
                        yes_remaining = yes_order_fresh["quantity"] - yes_filled
                        
                        no_filled = fresh_no_filled
                        no_remaining = no_order_fresh["quantity"] - no_filled
                        
                        # Update our local data structures
                        yes_orders[yes_idx]["filled_quantity"] = yes_filled
                        yes_orders[yes_idx]["remaining"] = yes_remaining
                        
                        no_orders[no_idx]["filled_quantity"] = no_filled
                        no_orders[no_idx]["remaining"] = no_remaining
                        
                        # Skip if no longer matchable
                        if yes_remaining <= 0 or no_remaining <= 0:
                            no_idx += 1
                            continue
                        
                        # Recalculate match quantity
                        match_quantity = min(yes_remaining, no_remaining)
                    
                    # Calculate correct filled quantities based on fresh data
                    new_yes_filled = yes_filled + match_quantity
                    new_no_filled = no_filled + match_quantity
                    
                    # Verify within limits
                    if new_yes_filled > yes_order_fresh["quantity"]:
                        logger.error(f"New YES filled qty {new_yes_filled} exceeds order qty {yes_order_fresh['quantity']}")
                        match_quantity = yes_order_fresh["quantity"] - yes_filled
                        new_yes_filled = yes_order_fresh["quantity"]
                        
                    if new_no_filled > no_order_fresh["quantity"]:
                        logger.error(f"New NO filled qty {new_no_filled} exceeds order qty {no_order_fresh['quantity']}")
                        match_quantity = min(match_quantity, no_order_fresh["quantity"] - no_filled)
                        new_no_filled = no_filled + match_quantity
                        new_yes_filled = yes_filled + match_quantity
                    
                    # Final check on match quantity
                    if match_quantity <= 0:
                        no_idx += 1
                        continue
                    
                    # Create trade record
                    trade = {
                        "trade_id": generate_id("trade"),
                        "market_id": market_id,
                        "first_user_order_id": yes_order_id,
                        "second_user_order_id": no_order_id,
                        "first_user_id": yes_order["user_id"],
                        "second_user_id": no_order["user_id"],
                        "first_user_option": yes_order["option_type"],
                        "second_user_option": no_order["option_type"],
                        "first_user_price": yes_price,
                        "second_user_price": no_price,
                        "quantity": match_quantity,
                        "executed_at": datetime.utcnow().isoformat()
                    }

                    # Store the trade in Redis
                    redis_client.set(f"trade:{trade['trade_id']}", json.dumps(trade))
                    redis_client.sadd(f"market:{market_id}:trades", trade["trade_id"])
                    
                    # redis_client.sadd(f"order:{yes_order_id}:trades", trade['trade_id'])
                    # redis_client.sadd(f"order:{no_order_id}:trades", trade['trade_id'])

                    # Update orders in transaction
                    pipe = redis_client.pipeline()
                    
                    market_key = f"market:{market_id}:data"
                    market_data_json = redis_client.get(market_key)
                    if market_data_json:
                        market_data = json.loads(market_data_json)
                        market_data["yes_price"] = yes_price
                        market_data["no_price"] = no_price
                        market_data["last_updated"] = datetime.utcnow().isoformat()
                        market_data["last_trade_id"] = trade["trade_id"]
                        pipe.set(market_key, json.dumps(market_data))

                    # Determine correct YES order status
                    yes_status = OrderStatus.FILLED if new_yes_filled >= yes_order_fresh["quantity"] else OrderStatus.PARTIAL
                    
                    # Update YES order
                    yes_order_updated = {
                        **yes_order_fresh,
                        "filled_quantity": new_yes_filled,
                        "status": yes_status,
                        "updated_at": datetime.utcnow().isoformat()
                    }
                    pipe.set(f"order:{yes_order_id}", json.dumps(yes_order_updated))
                    
                    # Remove from order book if fully filled
                    if yes_status == OrderStatus.FILLED:
                        pipe.zrem(yes_buy_key, yes_order_id)
                    
                    # Determine correct NO order status
                    no_status = OrderStatus.FILLED if new_no_filled >= no_order_fresh["quantity"] else OrderStatus.PARTIAL
                    
                    # Update NO order
                    no_order_updated = {
                        **no_order_fresh,
                        "filled_quantity": new_no_filled,
                        "status": no_status,
                        "updated_at": datetime.utcnow().isoformat()
                    }
                    pipe.set(f"order:{no_order_id}", json.dumps(no_order_updated))
                    
                    # Remove from order book if fully filled
                    if no_status == OrderStatus.FILLED:
                        pipe.zrem(no_buy_key, no_order_id)
                    
                    # Execute all updates atomically
                    pipe.execute()
                    
                    # Send order updates to Frappe
                    send_order_update_to_frappe(yes_order_updated)
                    send_order_update_to_frappe(no_order_updated)
                    
                    # Add trade to results
                    matched_trades.append(trade)
                    
                    # Update remaining quantities for next iterations
                    yes_remaining -= match_quantity
                    no_remaining -= match_quantity
                    
                    # Update our tracking data structures
                    yes_orders[yes_idx]["remaining"] = yes_remaining
                    yes_orders[yes_idx]["filled_quantity"] = new_yes_filled
                    
                    no_orders[no_idx]["remaining"] = no_remaining
                    no_orders[no_idx]["filled_quantity"] = new_no_filled
                    
                    logger.info(f"Matched {match_quantity} units. YES remaining: {yes_remaining}, NO remaining: {no_remaining}")
                    logger.info(f"YES order status: {yes_status}, NO order status: {no_status}")
                    
                    # If YES or NO order is completely matched, move to next
                    if no_remaining <= 0:
                        no_idx += 1
                    
                    # If YES order is completely matched, break the inner loop
                    if yes_remaining <= 0:
                        break
                        
                    # Mark that we found a match
                    match_found = True
                else:
                    # Prices not compatible, move to next NO order
                    no_idx += 1
            
            # If no match was found or YES order is filled, move to the next YES order
            if not match_found or yes_remaining <= 0:
                yes_idx += 1
        
        return matched_trades


    @staticmethod
    def _match_sell_orders(redis_client, market_id):
        """
        Match SELL orders with BUY orders of the same option type.
        Simple trade matching without requiring linked BUY orders.
        """
        matched_trades = []
        
        # Process both YES and NO options
        for option_type in [OptionType.YES, OptionType.NO]:
            # Get all SELL orders for this option type
            sell_key = f"order_book:{market_id}:{option_type}:{OrderType.SELL}"
            sell_ids = redis_client.zrange(sell_key, 0, -1, withscores=True)
            
            # Get all BUY orders for this option type
            buy_key = f"order_book:{market_id}:{option_type}:{OrderType.BUY}"
            buy_ids = redis_client.zrange(buy_key, 0, -1, withscores=True)
            
            logger.info(f"Attempting to match {len(sell_ids)} {option_type} SELL orders with {len(buy_ids)} {option_type} BUY orders")
            
            # Collect valid SELL orders
            sell_orders = []
            for sell_order_id, score in sell_ids:
                order = OrderBook.get_order(redis_client, sell_order_id)
                if order and order["status"] not in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
                    # Make sure filled_quantity is valid
                    filled_quantity = order.get("filled_quantity", 0)
                    if filled_quantity is None:
                        filled_quantity = 0
                        
                    remaining = order["quantity"] - filled_quantity
                    if remaining > 0:
                        sell_orders.append({
                            "order_id": sell_order_id,
                            "order": order,
                            "price": score,  # Already ordered correctly for SELLs
                            "remaining": remaining,
                            "filled_quantity": filled_quantity  # Track current filled quantity
                        })
            
            # Collect valid BUY orders
            buy_orders = []
            for buy_order_id, neg_score in buy_ids:
                order = OrderBook.get_order(redis_client, buy_order_id)
                if order and order["status"] not in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
                    # Make sure filled_quantity is valid
                    filled_quantity = order.get("filled_quantity", 0)
                    if filled_quantity is None:
                        filled_quantity = 0
                        
                    remaining = order["quantity"] - filled_quantity
                    if remaining > 0:
                        buy_orders.append({
                            "order_id": buy_order_id,
                            "order": order,
                            "price": -neg_score,  # Convert back from negated score
                            "remaining": remaining,
                            "filled_quantity": filled_quantity  # Track current filled quantity
                        })
            
            logger.info(f"Valid orders: {len(sell_orders)} {option_type} SELL and {len(buy_orders)} {option_type} BUY")
            
            # Process SELL orders
            sell_idx = 0
            while sell_idx < len(sell_orders):
                sell_data = sell_orders[sell_idx]
                sell_order_id = sell_data["order_id"]
                sell_order = sell_data["order"]
                sell_price = sell_data["price"]
                sell_remaining = sell_data["remaining"]
                sell_filled = sell_data["filled_quantity"]
                
                if sell_remaining <= 0:
                    sell_idx += 1
                    continue
                
                # Try to match with compatible BUY orders
                # For SELL orders, we want to match with BUYs at EXACTLY the same price
                buy_idx = 0
                match_found = False
                
                while buy_idx < len(buy_orders):
                    buy_data = buy_orders[buy_idx]
                    buy_order_id = buy_data["order_id"]
                    buy_order = buy_data["order"]
                    buy_price = buy_data["price"]
                    buy_remaining = buy_data["remaining"]
                    buy_filled = buy_data["filled_quantity"]
                    
                    if buy_remaining <= 0:
                        buy_idx += 1
                        continue
                    
                    # Check that orders aren't from the same user
                    if sell_order["user_id"] == buy_order["user_id"]:
                        logger.info(f"Skipping match between orders from same user {sell_order['user_id']}")
                        buy_idx += 1
                        continue
                    
                    # Check if prices are EXACTLY the same for SELL and BUY
                    if buy_price == sell_price:
                        # Calculate match quantity (smaller of the remaining quantities)
                        match_quantity = min(sell_remaining, buy_remaining)
                        
                        if match_quantity <= 0:
                            buy_idx += 1
                            continue
                        
                        logger.info(f"Found SELL match: {option_type} SELL order {sell_order_id} ({sell_remaining} units) with " +
                                f"{option_type} BUY order {buy_order_id} ({buy_remaining} units). " +
                                f"Will match {match_quantity} units at price {sell_price}.")
                        
                        # Get fresh copies of orders from Redis
                        sell_order_fresh = OrderBook.get_order(redis_client, sell_order_id)
                        buy_order_fresh = OrderBook.get_order(redis_client, buy_order_id)
                        
                        # Skip if orders have been modified or removed
                        if not sell_order_fresh or not buy_order_fresh:
                            logger.warning(f"Order disappeared during matching: SELL={sell_order_id}, BUY={buy_order_id}")
                            buy_idx += 1
                            continue
                        
                        # Verify fresh order quantities and statuses
                        fresh_sell_filled = sell_order_fresh.get("filled_quantity", 0)
                        if fresh_sell_filled is None:
                            fresh_sell_filled = 0
                            
                        fresh_buy_filled = buy_order_fresh.get("filled_quantity", 0)
                        if fresh_buy_filled is None:
                            fresh_buy_filled = 0
                        
                        # Check if the filled quantities changed since we started matching
                        if fresh_sell_filled != sell_filled or fresh_buy_filled != buy_filled:
                            logger.warning(f"Order quantities changed during matching process. Recomputing.")
                            # Update our local tracking with the fresh data
                            sell_filled = fresh_sell_filled
                            buy_filled = fresh_buy_filled
                            
                            # Recalculate remaining quantities
                            sell_remaining = sell_order_fresh["quantity"] - sell_filled
                            buy_remaining = buy_order_fresh["quantity"] - buy_filled
                            
                            # Update our local data structures
                            sell_orders[sell_idx]["filled_quantity"] = sell_filled
                            sell_orders[sell_idx]["remaining"] = sell_remaining
                            buy_orders[buy_idx]["filled_quantity"] = buy_filled
                            buy_orders[buy_idx]["remaining"] = buy_remaining
                            
                            # Skip if no longer matchable
                            if sell_remaining <= 0 or buy_remaining <= 0:
                                logger.info(f"Orders no longer matchable after refresh")
                                buy_idx += 1
                                continue
                            
                            # Recalculate match quantity with fresh data
                            match_quantity = min(sell_remaining, buy_remaining)
                        
                        # Validate that the match quantity doesn't exceed the available quantity
                        if match_quantity > sell_remaining or match_quantity > buy_remaining:
                            logger.error(f"Invalid match quantity: {match_quantity}. Sell remaining: {sell_remaining}, Buy remaining: {buy_remaining}")
                            buy_idx += 1
                            continue
                            
                        # Calculate correct filled quantities based on fresh data
                        new_sell_filled = sell_filled + match_quantity
                        new_buy_filled = buy_filled + match_quantity
                        
                        # Verify new filled quantities don't exceed order quantities
                        if new_sell_filled > sell_order_fresh["quantity"]:
                            logger.error(f"New sell filled quantity {new_sell_filled} exceeds order quantity {sell_order_fresh['quantity']}")
                            # Adjust match quantity to avoid exceeding
                            match_quantity = sell_order_fresh["quantity"] - sell_filled
                            new_sell_filled = sell_order_fresh["quantity"]
                            
                        if new_buy_filled > buy_order_fresh["quantity"]:
                            logger.error(f"New buy filled quantity {new_buy_filled} exceeds order quantity {buy_order_fresh['quantity']}")
                            # Use the smaller of the two adjustments
                            match_quantity = min(match_quantity, buy_order_fresh["quantity"] - buy_filled)
                            new_buy_filled = buy_filled + match_quantity
                            new_sell_filled = sell_filled + match_quantity
                        
                        # Final validation of match quantity
                        if match_quantity <= 0:
                            logger.warning(f"Match quantity became zero or negative after adjustments")
                            buy_idx += 1
                            continue
                        
                        # Start a Redis transaction
                        pipe = redis_client.pipeline()
                        
                        # Create a trade record
                        trade = {
                            "trade_id": generate_id("trade"),
                            "market_id": market_id,
                            "first_user_order_id": sell_order_id,
                            "second_user_order_id": buy_order_id,
                            "first_user_id": sell_order["user_id"],
                            "second_user_id": buy_order["user_id"],
                            "first_user_option": option_type,
                            "second_user_option": option_type,
                            "first_user_price": sell_price,
                            "second_user_price": buy_price,
                            "quantity": match_quantity,
                            "executed_at": datetime.utcnow().isoformat()
                        }
                        
                        # Store the trade in Redis
                        pipe.set(f"trade:{trade['trade_id']}", json.dumps(trade))
                        pipe.sadd(f"market:{market_id}:trades", trade["trade_id"])
                        
                        # Add to order-trade indices
                        pipe.sadd(f"order:{sell_order_id}:trades", trade["trade_id"])
                        pipe.sadd(f"order:{buy_order_id}:trades", trade["trade_id"])
                        
                        market_key = f"market:{market_id}:data"
                        market_data_json = redis_client.get(market_key)
                        if market_data_json:
                            market_data = json.loads(market_data_json)
                            
                            # For sell orders of the same type, we use the trade price directly
                            if option_type == OptionType.YES:
                                yes_price = sell_price
                                no_price = TOTAL_PAYOUT - yes_price
                            else:  # NO
                                no_price = sell_price
                                yes_price = TOTAL_PAYOUT - no_price
                                
                            market_data["yes_price"] = yes_price
                            market_data["no_price"] = no_price
                            market_data["last_updated"] = datetime.utcnow().isoformat()
                            market_data["last_trade_id"] = trade["trade_id"]
                            pipe.set(market_key, json.dumps(market_data))

                        # Update SELL order
                        sell_status = OrderStatus.FILLED if new_sell_filled >= sell_order_fresh["quantity"] else OrderStatus.PARTIAL
                        sell_order_updated = {
                            **sell_order_fresh,
                            "filled_quantity": new_sell_filled,
                            "status": sell_status,
                            "updated_at": datetime.utcnow().isoformat()
                        }
                        pipe.set(f"order:{sell_order_id}", json.dumps(sell_order_updated))
                        
                        # Remove from order book if fully filled
                        if sell_status == OrderStatus.FILLED:
                            pipe.zrem(sell_key, sell_order_id)
                        
                        # Update BUY order
                        buy_status = OrderStatus.FILLED if new_buy_filled >= buy_order_fresh["quantity"] else OrderStatus.PARTIAL
                        buy_order_updated = {
                            **buy_order_fresh,
                            "filled_quantity": new_buy_filled,
                            "status": buy_status,
                            "updated_at": datetime.utcnow().isoformat()
                        }
                        pipe.set(f"order:{buy_order_id}", json.dumps(buy_order_updated))
                        
                        # Remove from order book if fully filled
                        if buy_status == OrderStatus.FILLED:
                            pipe.zrem(buy_key, buy_order_id)
                        
                        # Execute all updates atomically
                        pipe.execute()
                        
                        # Send order updates to Frappe
                        send_order_update_to_frappe(sell_order_updated)
                        send_order_update_to_frappe(buy_order_updated)
                        
                        # Add trade to results
                        matched_trades.append(trade)
                        
                        # Update remaining quantities for next iterations
                        sell_remaining -= match_quantity
                        buy_remaining -= match_quantity
                        
                        # Update our local data structures for next iterations
                        sell_orders[sell_idx]["filled_quantity"] = new_sell_filled
                        sell_orders[sell_idx]["remaining"] = sell_remaining
                        buy_orders[buy_idx]["filled_quantity"] = new_buy_filled
                        buy_orders[buy_idx]["remaining"] = buy_remaining
                        
                        logger.info(f"Matched {match_quantity} units. SELL remaining: {sell_remaining}, BUY remaining: {buy_remaining}")
                        logger.info(f"SELL order status: {sell_status}, BUY order status: {buy_status}")
                        
                        # Mark that we found a match
                        match_found = True
                        
                        # If BUY order is completely matched, move to the next one
                        if buy_remaining <= 0:
                            buy_idx += 1
                        
                        # If SELL order is completely matched, break the inner loop
                        if sell_remaining <= 0:
                            break
                    else:
                        # Prices don't match, move to the next BUY order
                        buy_idx += 1
                
                # If no match was found or the SELL order is fully matched, move to the next SELL order
                if not match_found or sell_remaining <= 0:
                    sell_idx += 1

        return matched_trades
# API endpoints
@app.post("/markets/", status_code=status.HTTP_201_CREATED)
async def create_market(market: MarketRequest, redis_client = Depends(get_redis)):
    """Create a new prediction market"""
    try:
        # Initialize the market
        market_data = MarketManager.initialize_market(redis_client, market.dict())
        return market_data
    except Exception as e:
        logger.error(f"Error creating market: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create market: {str(e)}"
        )

@app.post("/markets/{market_id}/close", status_code=status.HTTP_200_OK)
async def close_market(
    market_id: str, 
    redis_client = Depends(get_redis)
):
    """Close a market with resolution information and automate settlement"""
    try:
        # Execute the full closure workflow
        
        # Step 0: Check if market exists and is in OPEN state
        market_data = MarketManager.get_market_data(redis_client, market_id)
        if not market_data:
            logger.error(f"Market {market_id} not found")
            return {
                "success": False,
                "stage": "market_check",
                "message": f"Market {market_id} not found"
            }
            
        if market_data["status"] != MarketStatus.OPEN:
            logger.error(f"Market {market_id} is not in OPEN state (current state: {market_data['status']})")
            return {
                "success": False,
                "stage": "market_check",
                "message": f"Cannot close market - it is already in {market_data['status']} state"
            }
        
        # Step 1: Close the market
        close_success = MarketManager.close_market(redis_client, market_id)
        if not close_success:
            logger.error(f"Failed to close market {market_id}")
            return {
                "success": False,
                "stage": "market_closure",
                "message": "Failed to close market"
            }
        
        return {
            "message":"Market closed successfully"
        }    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in close market workflow: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process market closure: {str(e)}"
        )

@app.post("/orders/", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def place_order(order: OrderRequest, background_tasks: BackgroundTasks, redis_client = Depends(get_redis)):
    """Place a new order in the market"""
    try:
        new_order = OrderBook.add_order(redis_client, order.dict())

        # Match orders in the background
        background_tasks.add_task(OrderBook.match_orders, redis_client, order.market_id)
        
        return new_order
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error placing order: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process order: {str(e)}"
        )
   
@app.put("/orders/update_quantity", response_model=OrderResponse, status_code=status.HTTP_200_OK)
async def update_order_quantity(update_req: UpdateOrderRequest, redis_client = Depends(get_redis)):
    """
    Update an existing order's quantity (only supports reducing quantity)
    """
    order_id = update_req.order_id
    new_quantity = update_req.new_quantity
    
    # Get the order
    order = OrderBook.get_order(redis_client, order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    # Check if order can be updated
    if order["status"] in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot update an order with status: {order['status']}"
        )
    
    # Get market data to check status
    market_data = MarketManager.get_market_data(redis_client, order["market_id"])
    if not market_data or market_data["status"] != MarketStatus.OPEN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update order - market is not open"
        )
    
    # Check if new quantity is valid (less than original and greater than filled)
    original_quantity = order["quantity"]
    filled_quantity = order.get("filled_quantity", 0)
    if filled_quantity is None:
        filled_quantity = 0
    
    if new_quantity >= original_quantity:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New quantity must be less than original quantity"
        )
    
    if new_quantity < filled_quantity:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"New quantity cannot be less than filled quantity ({filled_quantity})"
        )
    
    try:
        # Start a transaction
        pipe = redis_client.pipeline()
        
        # Update the order
        updated_order = {
            **order,
            "quantity": new_quantity,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # If order was PARTIAL and new quantity equals filled quantity, mark as FILLED
        if order["status"] == OrderStatus.PARTIAL and new_quantity == filled_quantity:
            updated_order["status"] = OrderStatus.FILLED
            # Remove from order book if fully filled
            order_book_key = f"order_book:{order['market_id']}:{order['option_type']}:{order['order_type']}"
            pipe.zrem(order_book_key, order_id)
        
        # Save the updated order
        pipe.set(f"order:{order_id}", json.dumps(updated_order))
        
        # Execute transaction
        pipe.execute()
        
        return updated_order
        
    except Exception as e:
        logger.error(f"Error updating order quantity: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update order"
        )

@app.delete("/orders/{order_id}", status_code=status.HTTP_200_OK)
async def cancel_order(order_id: str, redis_client = Depends(get_redis)):
    """Cancel an existing order"""
    order = OrderBook.get_order(redis_client, order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    if order["status"] in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel an order with status: {order['status']}"
        )
    
    # Get market data to check status
    market_data = MarketManager.get_market_data(redis_client, order["market_id"])
    if not market_data or market_data["status"] != MarketStatus.OPEN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel order - market is not open"
        )
    
    try:
        # Start a transaction
        pipe = redis_client.pipeline()
    
        # Remove from order book
        OrderBook.remove_order_from_book(redis_client, order)
        
        # Update order status
        updated_order = {
            **order,
            "status": OrderStatus.CANCELLED,
            "updated_at": datetime.utcnow().isoformat()
        }
        pipe.set(f"order:{order_id}", json.dumps(updated_order))
        
        # Execute the transaction
        pipe.execute()
        # Return appropriate response based on order type
        if order["order_type"] == OrderType.SELL:
            return {
                "message": "Order cancelled successfully. The linked BUY order has been updated.", 
                "order": updated_order
            }
        else:
            return {
                "message": "Order cancelled successfully and funds have been refunded.",
                "order": updated_order
            }
        
    except Exception as e:
        logger.error(f"Error cancelling order: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel order"
        )

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    """Check API health status"""
    redis_ok = await redis_manager.health_check()
    
    if not redis_ok:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "redis": "not connected"}
        )
    
    return {"status": "healthy", "redis": "connected"}

@app.get("/available_prices", status_code=status.HTTP_200_OK)
async def get_available_prices():
    """Get list of valid price increments"""
    return {"valid_prices": VALID_PRICES}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8086)