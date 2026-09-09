try:
    from jose import JWTError, jwt
except ImportError:
    import jwt as _pyjwt
    try:
        JWTError = _pyjwt.exceptions.PyJWTError
    except Exception:
        class JWTError(Exception): pass
    jwt = _pyjwt

import json
import asyncio
import os
import re
import random
import string
import hashlib
import importlib
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Dict, Optional

try:
    bcrypt = importlib.import_module("bcrypt")
except ImportError:
    bcrypt = None

from fastapi import (FastAPI, Depends, HTTPException, status, WebSocket,
                     WebSocketDisconnect, Query)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, field_validator
from sqlalchemy import (create_engine, Column, Integer, BigInteger, String,
                       Float, Boolean, DateTime, ForeignKey, text, or_, desc)
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from sqlalchemy.engine import URL

# --- SYSTEM CONFIGURATION ---
SECRET_KEY = os.environ["SECRET_KEY"]
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

ADMIN_USERNAME = os.environ["ADMIN_USERNAME"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
ADMIN_PASSWORD_HASH = hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "https://assetcore-et.vercel.app")

DEFAULT_ORIGINS = [
    "https://assetcore-et.vercel.app",
    "https://projectdeployment-1-yuwq.onrender.com",
    "https://web.telegram.org",
]
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", ",".join(DEFAULT_ORIGINS)).split(",") if o.strip()]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/admin/login")

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

SQLALCHEMY_DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- WEBSOCKET CONNECTION MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        key = str(user_id)
        old = self.active_connections.get(key)
        if old is not None:
            try: await old.close(code=1000, reason="Replaced by new connection")
            except Exception: pass
        self.active_connections[key] = websocket

    def disconnect(self, user_id: str):
        self.active_connections.pop(str(user_id), None)

    async def send_personal_message(self, message: str, user_id: str):
        key = str(user_id)
        ws = self.active_connections.get(key)
        if ws is None: return
        try: await ws.send_text(message)
        except Exception: self.disconnect(key)

manager = ConnectionManager()

# --- DATABASE MODELS ---
class User(Base):
    __tablename__ = "users"
    telegram_id = Column(BigInteger, primary_key=True, index=True)
    telegram_username = Column(String, nullable=True)
    phone_number = Column(String, unique=True, nullable=True)
    password_hash = Column(String, nullable=False)
    invitation_code = Column(String, unique=True, nullable=False)
    invitation_link = Column(String, nullable=False)

    balance = relationship("UserBalance", uselist=False, back_populates="user", cascade="all, delete-orphan")
    products = relationship("UserProduct", back_populates="user", cascade="all, delete-orphan")
    deposits = relationship("Deposit", back_populates="user", cascade="all, delete-orphan")
    withdrawals = relationship("Withdrawal", back_populates="user", cascade="all, delete-orphan")
    referrals = relationship("TeamReferral", foreign_keys="TeamReferral.telegram_id", back_populates="user", cascade="all, delete-orphan")
    inviter_referrals = relationship("TeamReferral", foreign_keys="TeamReferral.inviter_id", back_populates="inviter", cascade="all, delete-orphan")

class UserBalance(Base):
    __tablename__ = "user_balances"
    telegram_id = Column(BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), primary_key=True)
    registration_bonus = Column(Float, default=0.0)
    daily_income_balance = Column(Float, default=0.0)
    invitation_income = Column(Float, default=0.0)
    total_balance = Column(Float, default=0.0)
    available_balance = Column(Float, default=0.0) # NEW: Tracks withdrawable funds
    last_checkin_at = Column(DateTime, nullable=True)
    user = relationship("User", back_populates="balance")

class TeamReferral(Base):
    __tablename__ = "team_referrals"
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), nullable=False)
    inviter_id = Column(BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), nullable=False)
    commission_earned = Column(Float, default=0.0)
    is_valid_depositor = Column(Boolean, default=False)
    user = relationship("User", foreign_keys=[telegram_id], back_populates="referrals")
    inviter = relationship("User", foreign_keys=[inviter_id], back_populates="inviter_referrals")

class UserProduct(Base):
    __tablename__ = "user_products"
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), nullable=False)
    product_name = Column(String, nullable=False)
    product_price = Column(Float, nullable=False)
    daily_income = Column(Float, nullable=False)
    purchased_at = Column(DateTime, default=datetime.utcnow)
    last_yield_claimed_at = Column(DateTime, nullable=True)
    user = relationship("User", back_populates="products")

class Deposit(Base):
    __tablename__ = "deposits"
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), nullable=False)
    transaction_id = Column(String, unique=True, index=True, nullable=False)
    amount_etb = Column(Float, nullable=False)
    status = Column(String, default="Pending")
    payment_method = Column(String, default="CBE")
    created_at = Column(DateTime, default=datetime.utcnow)
    assigned_account = Column(String, nullable=True)
    user = relationship("User", back_populates="deposits")

class Withdrawal(Base):
    __tablename__ = "withdrawals"
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, ForeignKey("users.telegram_id", ondelete="CASCADE"), nullable=False)
    amount = Column(Float, nullable=False)
    fee = Column(Float, default=0.0)
    payout_amount = Column(Float, default=0.0)
    method = Column(String, nullable=False)
    account_details = Column(String, nullable=False)
    status = Column(String, default="Pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    user = relationship("User", back_populates="withdrawals")

class Setting(Base):
    __tablename__ = "settings"
    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)

def init_db():
    db = SessionLocal()
    try:
        try: db.execute(text("ALTER TABLE deposits ADD COLUMN IF NOT EXISTS assigned_account VARCHAR;"))
        except: pass
        try: db.execute(text("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS fee FLOAT DEFAULT 0;"))
        except: pass
        try: db.execute(text("ALTER TABLE withdrawals ADD COLUMN IF NOT EXISTS payout_amount FLOAT DEFAULT 0;"))
        except: pass
        try: db.execute(text("ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS available_balance FLOAT DEFAULT 0;"))
        except: pass
        db.commit()

        # Initialize available_balance for existing users if it's 0
        db.execute(text("UPDATE user_balances SET available_balance = registration_bonus + daily_income_balance + invitation_income WHERE available_balance = 0;"))
        db.commit()

        if not db.query(Setting).filter(Setting.key == "deposit_account_pool").first():
            db.add(Setting(key="deposit_account_pool", value="CBE, Platform Admin, 1000123456789\nTelebirr, John Doe, 0911223344\nAbyssinia, Jane Doe, 1000987654321"))
            db.commit()

        if not db.query(Setting).filter(Setting.key == "registration_bonus").first():
            db.add(Setting(key="registration_bonus", value="300.0"))
            db.add(Setting(key="commission_rate", value="0.30"))
            db.commit()
    except Exception as e:
        print(f"DB Init Error: {e}")
        db.rollback()
    finally:
        db.close()

init_db()

# --- PYDANTIC SCHEMAS ---
class RegisterRequest(BaseModel):
    telegram_id: Optional[int] = None
    telegram_username: Optional[str] = None
    phone_number: str
    password: str
    inviter_code: Optional[str] = None

    @field_validator("phone_number")
    @classmethod
    def validate_phone_digits(cls, v: str) -> str:
        clean = v.strip()
        digits = clean.lstrip("+")
        if not digits.isdigit():
            raise ValueError("Phone number must contain numbers only.")
        return clean

class LoginRequest(BaseModel):
    phone_number: str
    password: str

class DailyCheckinRequest(BaseModel):
    telegram_id: int

class PaynowWebhookRequest(BaseModel):
    reference: str
    status: str
    amount: float
    telegram_id: int
    payment_method: str = "CBE"
    assigned_account: Optional[str] = None

class BuyProductRequest(BaseModel):
    telegram_id: int
    product_name: str
    product_price: float
    daily_income: float

class WithdrawalRequest(BaseModel):
    telegram_id: int
    amount: float
    method: str
    account_details: str

class SystemSettingsUpdate(BaseModel):
    registration_bonus: Optional[float] = None
    commission_rate: Optional[float] = None
    deposit_account_pool: Optional[str] = None

class Token(BaseModel):
    access_token: str
    token_type: str

# --- HELPERS ---
def hash_password(password: str) -> str:
    if bcrypt is None:
        return hashlib.sha256(password.encode('utf-8')).hexdigest()
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pwd_bytes, salt)
    return hashed.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    if bcrypt is not None and hashed_password.startswith('$2'):
        try: return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
        except Exception: return False
    else:
        return hashlib.sha256(plain_password.encode()).hexdigest() == hashed_password

def init_admin_hash():
    global ADMIN_PASSWORD_HASH
    ADMIN_PASSWORD_HASH = hash_password(ADMIN_PASSWORD)

init_admin_hash()

def generate_invitation_code() -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=6))

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# --- BACKGROUND JOB ---
async def run_daily_yield_job():
    while True:
        await asyncio.sleep(60)
        db = SessionLocal()
        try:
            now = datetime.utcnow()
            ready_products = db.query(UserProduct).filter(
                (UserProduct.last_yield_claimed_at == None) |
                (UserProduct.last_yield_claimed_at <= now - timedelta(hours=24))
            ).all()
            for product in ready_products:
                balance = db.query(UserBalance).filter(UserBalance.telegram_id == product.telegram_id).first()
                if balance:
                    balance.daily_income_balance += product.daily_income
                    balance.total_balance += product.daily_income
                    balance.available_balance += product.daily_income # NEW: Add yield to available
                    product.last_yield_claimed_at = now
                    db.commit()
                    notification = {
                        "type": "yield_credited",
                        "amount": product.daily_income,
                        "new_balance": balance.total_balance,
                        "product": product.product_name
                    }
                    await manager.send_personal_message(json.dumps(notification), str(product.telegram_id))
        except Exception:
            pass
        finally:
            db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield_task = asyncio.create_task(run_daily_yield_job())
    yield
    yield_task.cancel()

# --- APP ---
app = FastAPI(title="AssetCore Backend", version="1.0", lifespan=lifespan, redirect_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "AssetCore Backend",
        "frontend": FRONTEND_BASE_URL,
        "docs": "/docs",
        "endpoints": ["/api/status", "/api/health", "/api/register", "/api/login", "/api/leaderboard", "/ws/{user_id}"]
    }

@app.get("/api/status")
def api_status():
    return {"status": "ok", "message": "AssetCore Backend is running"}

@app.get("/api/health")
def health_check():
    return {
        "status": "ok",
        "websocket": "enabled",
        "websocket_routes": ["/ws/{user_id}", "/ws/{user_id}/"]
    }

@app.get("/api/leaderboard")
def get_leaderboard(db: Session = Depends(get_db)):
    top_users = db.query(User, UserBalance).join(
        UserBalance, User.telegram_id == UserBalance.telegram_id
    ).order_by(desc(UserBalance.total_balance)).limit(10).all()
    
    results = []
    for u, b in top_users:
        display_name = u.telegram_username if u.telegram_username else f"User {str(u.telegram_id)[-4:]}"
        results.append({
            "name": display_name,
            "amount": b.total_balance
        })
    return results

# --- ADMIN AUTH ---
async def get_current_admin(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None or username != ADMIN_USERNAME:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username

@app.post("/api/admin/login", response_model=Token)
def admin_login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username != ADMIN_USERNAME or not verify_password(form_data.password, ADMIN_PASSWORD_HASH):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": ADMIN_USERNAME})
    return {"access_token": access_token, "token_type": "bearer"}

# --- WEBSOCKET ---
def _normalize_user_id(raw: str) -> Optional[str]:
    if raw is None: return None
    cleaned = raw.strip().rstrip("/")
    if not re.match(r"^\d+$", cleaned): return None
    return cleaned

async def _websocket_handler(websocket: WebSocket, user_id: str):
    try: await manager.connect(websocket, user_id)
    except Exception:
        try: await websocket.close(code=1011, reason="Internal server error")
        except Exception: pass
        return
    try:
        await websocket.send_text(json.dumps({"type": "connected", "user_id": int(user_id)}))
        while True:
            try: await websocket.receive_text()
            except WebSocketDisconnect: break
            except Exception: break
    except WebSocketDisconnect: pass
    except Exception: pass
    finally: manager.disconnect(user_id)

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    clean_id = _normalize_user_id(user_id)
    if clean_id is None:
        await websocket.close(code=1008, reason="Invalid user_id format")
        return
    await _websocket_handler(websocket, clean_id)

@app.websocket("/ws/{user_id}/")
async def websocket_endpoint_trailing_slash(websocket: WebSocket, user_id: str):
    clean_id = _normalize_user_id(user_id)
    if clean_id is None:
        await websocket.close(code=1008, reason="Invalid user_id format")
        return
    await _websocket_handler(websocket, clean_id)

# --- CORE ENDPOINTS ---
@app.post("/api/register", status_code=status.HTTP_201_CREATED)
def register_user(payload: RegisterRequest, db: Session = Depends(get_db)):
    clean_phone = payload.phone_number.strip()
    if not clean_phone.lstrip("+").isdigit():
        raise HTTPException(status_code=400, detail="Phone number must contain numbers only.")

    if db.query(User).filter(User.phone_number == clean_phone).first():
        raise HTTPException(status_code=400, detail="An account with this phone number is already registered.")

    target_tg_id = payload.telegram_id
    if target_tg_id:
        if db.query(User).filter(User.telegram_id == target_tg_id).first():
            raise HTTPException(status_code=400, detail="This Telegram ID is already registered.")
    else:
        target_tg_id = random.randint(100000000, 999999999)
        while db.query(User).filter(User.telegram_id == target_tg_id).first():
            target_tg_id = random.randint(100000000, 999999999)

    new_code = generate_invitation_code()
    while db.query(User).filter(User.invitation_code == new_code).first():
        new_code = generate_invitation_code()

    raw_user = payload.telegram_username.strip() if payload.telegram_username else ""
    formatted_username = f"@{raw_user.lstrip('@')}" if raw_user else None

    invitation_link = f"{FRONTEND_BASE_URL}/register?invite={new_code}"

    new_user = User(
        telegram_id=target_tg_id, telegram_username=formatted_username, phone_number=clean_phone,
        password_hash=hash_password(payload.password), invitation_code=new_code,
        invitation_link=invitation_link
    )
    db.add(new_user)
    db.flush()

    bonus_setting = db.query(Setting).filter(Setting.key == "registration_bonus").first()
    bonus = float(bonus_setting.value) if bonus_setting else 300.0
    db.add(UserBalance(telegram_id=new_user.telegram_id, registration_bonus=bonus, total_balance=bonus, available_balance=bonus))

    if payload.inviter_code:
        inviter = db.query(User).filter(User.invitation_code == payload.inviter_code).first()
        if inviter and inviter.telegram_id != new_user.telegram_id:
            db.add(TeamReferral(telegram_id=new_user.telegram_id, inviter_id=inviter.telegram_id, is_valid_depositor=False))

    db.commit()
    return {
        "message": "User registered successfully",
        "telegram_id": new_user.telegram_id,
        "phone_number": new_user.phone_number,
        "invitation_code": new_user.invitation_code,
        "invitation_link": new_user.invitation_link,
        "registration_bonus": bonus,
    }

@app.post("/api/login")
def login_user(payload: LoginRequest, db: Session = Depends(get_db)):
    identifier = payload.phone_number.strip()
    formatted_username = f"@{identifier.lstrip('@')}"
    user = db.query(User).filter(
        or_(User.phone_number == identifier, User.telegram_username == identifier, User.telegram_username == formatted_username)
    ).first()
    
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid phone number/username or password.")
    
    balance = db.query(UserBalance).filter(UserBalance.telegram_id == user.telegram_id).first()
    return {
        "message": "Login successful",
        "telegram_id": user.telegram_id,
        "phone_number": user.phone_number,
        "telegram_username": user.telegram_username,
        "invitation_code": user.invitation_code,
        "total_balance": balance.total_balance if balance else 0.0,
        "daily_income_balance": balance.daily_income_balance if balance else 0.0,
        "invite_income": balance.invitation_income if balance else 0.0
    }

@app.post("/api/daily-checkin")
def daily_checkin(payload: DailyCheckinRequest, db: Session = Depends(get_db)):
    balance = db.query(UserBalance).filter(UserBalance.telegram_id == payload.telegram_id).first()
    if not balance:
        raise HTTPException(status_code=404, detail="User balance not found")
    now = datetime.utcnow()
    if balance.last_checkin_at:
        delta = now - balance.last_checkin_at
        if delta < timedelta(hours=24):
            remaining = timedelta(hours=24) - delta
            raise HTTPException(status_code=400, detail=f"Daily check-in not available yet. Please wait {str(remaining).split('.')[0]}.")
    balance.daily_income_balance += 20.0
    balance.total_balance += 20.0
    balance.available_balance += 20.0 # NEW
    balance.last_checkin_at = now
    db.commit()
    return {"message": "Daily check-in successful! 20 ETB added to your balance.",
            "daily_income_balance": balance.daily_income_balance,
            "total_balance": balance.total_balance}

@app.post("/api/buy-product")
async def buy_product(payload: BuyProductRequest, db: Session = Depends(get_db)):
    balance = db.query(UserBalance).filter(UserBalance.telegram_id == payload.telegram_id).first()
    if not balance:
        raise HTTPException(status_code=404, detail="User not found. Please log in again.")
    if balance.total_balance < payload.product_price:
        raise HTTPException(status_code=400, detail="Insufficient balance. Please deposit first.")
    
    balance.total_balance -= payload.product_price
    # available_balance is NOT reduced, because the user is converting their capital into a product.
    # The available_balance only goes up when yields are credited.
    
    db.add(UserProduct(telegram_id=payload.telegram_id, product_name=payload.product_name,
                       product_price=payload.product_price, daily_income=payload.daily_income,
                       purchased_at=datetime.utcnow(), last_yield_claimed_at=datetime.utcnow()))
    db.commit()
    notification = {"type": "product_purchased", "name": payload.product_name, "new_balance": balance.total_balance}
    await manager.send_personal_message(json.dumps(notification), str(payload.telegram_id))
    return {"message": f"{payload.product_name} activated successfully!",
            "new_balance": balance.total_balance, "daily_income": payload.daily_income}

# --- GET DEPOSIT ACCOUNT ---
@app.get("/api/get-deposit-account")
def get_deposit_account(telegram_id: int, amount: float, method: str, db: Session = Depends(get_db)):
    if amount < 650 or amount > 100000:
        raise HTTPException(status_code=400, detail="Amount must be between 650 and 100,000 ETB.")
    
    method_lower = method.lower()
    if method_lower == "telebirr":
        if not (650 <= amount <= 1000):
            raise HTTPException(status_code=400, detail="Telebirr can only be used for amounts between 650 and 1000 ETB.")
    elif method_lower in ["cbe", "abyssinia", "bank of abysinia"]:
        if amount < 1600:
            raise HTTPException(status_code=400, detail="CBE/Abyssinia requires a minimum amount of 1600 ETB.")
    else:
        raise HTTPException(status_code=400, detail="Invalid payment method.")

    setting = db.query(Setting).filter(Setting.key == "deposit_account_pool").first()
    if not setting or not setting.value:
        raise HTTPException(status_code=404, detail="No accounts configured")
    
    accounts = []
    for line in setting.value.strip().split('\n'):
        parts = [p.strip() for p in line.split(',')]
        if len(parts) == 3:
            acc_method = parts[0]
            if acc_method.lower() == method_lower or \
               (method_lower == "abyssinia" and acc_method.lower() in ["abyssinia", "bank of abysinia"]) or \
               (method_lower == "cbe" and acc_method.lower() == "cbe"):
                
                accounts.append({"method": acc_method, "name": parts[1], "number": parts[2]})
    
    if not accounts:
        raise HTTPException(status_code=404, detail=f"No {method} accounts configured for this amount")
    
    three_hours_ago = datetime.utcnow() - timedelta(hours=3)
    recent_deposits = db.query(Deposit).filter(
        Deposit.created_at >= three_hours_ago,
        Deposit.assigned_account.isnot(None)
    ).all()

    usage_counts = {}
    for d in recent_deposits:
        usage_counts[d.assigned_account] = usage_counts.get(d.assigned_account, 0) + 1

    random.shuffle(accounts)

    available_accounts = []
    for acc in accounts:
        acc_str = f"{acc['method']}, {acc['name']}, {acc['number']}"
        count = usage_counts.get(acc_str, 0)
        
        limit = 2 
        if acc['method'].lower() in ["abyssinia", "bank of abysinia"]:
            limit = 4
            
        if count < limit:
            available_accounts.append(acc)

    if available_accounts:
        chosen_acc = random.choice(available_accounts)
        return {"account_string": f"{chosen_acc['method']}, {chosen_acc['name']}, {chosen_acc['number']}", **chosen_acc}

    raise HTTPException(status_code=429, detail="Currently payment method unavailable, please wait while we update our accounts.")

@app.post("/api/paynow-webhook")
def paynow_webhook(payload: PaynowWebhookRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.telegram_id == payload.telegram_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    deposit = db.query(Deposit).filter(Deposit.transaction_id == payload.reference).first()
    if not deposit:
        deposit = Deposit(
            telegram_id=payload.telegram_id, transaction_id=payload.reference,
            amount_etb=payload.amount, status="Pending", payment_method=payload.payment_method,
            assigned_account=payload.assigned_account
        )
        db.add(deposit)
        db.flush()
    if payload.status.lower() == "paid":
        if deposit.status.lower() != "paid":
            deposit.status = "Paid"
            db.commit()
            return {"message": "Deposit marked as Paid. Awaiting admin approval."}
        return {"message": "Deposit already processed."}
    deposit.status = payload.status
    db.commit()
    return {"message": f"Deposit status updated to {payload.status}."}

@app.post("/api/request-withdrawal")
def request_withdrawal(payload: WithdrawalRequest, db: Session = Depends(get_db)):
    if payload.amount < 300:
        raise HTTPException(status_code=400, detail="Minimum withdrawal is 300 ETB.")
    
    balance = db.query(UserBalance).filter(UserBalance.telegram_id == payload.telegram_id).first()
    if not balance:
        raise HTTPException(status_code=400, detail="User balance not found.")
    
    # FIX: Check against available_balance, not total_balance
    if payload.amount > balance.available_balance:
        raise HTTPException(status_code=400, detail="Withdrawal failed. You can only withdraw your registration bonus and earned income. Please purchase a product to activate daily income.")
    
    fee = payload.amount * 0.20
    payout = payload.amount - fee

    balance.total_balance -= payload.amount
    balance.available_balance -= payload.amount # NEW: Deduct from available
    
    db.add(Withdrawal(
        telegram_id=payload.telegram_id, amount=payload.amount, 
        fee=fee, payout_amount=payout,
        method=payload.method, account_details=payload.account_details, status="Pending"
    ))
    db.commit()
    return {"message": "Withdrawal request submitted. Waiting for admin approval."}

# --- ADMIN ENDPOINTS ---
@app.get("/api/admin/pending-deposits")
def get_pending_deposits(admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    deposits = db.query(Deposit).filter(Deposit.status.in_(["Pending", "Paid"])).all()
    return [
        {
            "id": d.id,
            "telegram_id": d.telegram_id,
            "amount_etb": d.amount_etb,
            "transaction_id": d.transaction_id,
            "payment_method": d.payment_method,
            "status": d.status,
            "assigned_account": d.assigned_account or "N/A"
        } for d in deposits
    ]

@app.get("/api/admin/pending-withdrawals")
def get_pending_withdrawals(admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    return db.query(Withdrawal).filter(Withdrawal.status == "Pending").all()

@app.get("/api/admin/transactions")
def get_all_transactions(admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    deposits = db.query(Deposit).all()
    withdrawals = db.query(Withdrawal).all()
    history = []
    for d in deposits:
        history.append({"id": d.id, "type": "Deposit", "user": d.telegram_id, "amount": d.amount_etb,
                        "status": d.status, "date": d.created_at, "method": d.payment_method})
    for w in withdrawals:
        history.append({"id": w.id, "type": "Withdrawal", "user": w.telegram_id, "amount": w.amount,
                        "status": w.status, "date": w.created_at, "method": w.method})
    return history

@app.post("/api/admin/approve-deposit/{deposit_id}")
async def approve_deposit(deposit_id: int, admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    deposit = db.query(Deposit).filter(Deposit.id == deposit_id).first()
    if not deposit: raise HTTPException(status_code=404, detail="Deposit not found.")
    if deposit.status == "Approved": raise HTTPException(status_code=400, detail="Deposit is already approved.")
    deposit.status = "Approved"
    user_balance = db.query(UserBalance).filter(UserBalance.telegram_id == deposit.telegram_id).first()
    if not user_balance: raise HTTPException(status_code=404, detail="User balance record not found.")
    
    user_balance.total_balance += deposit.amount_etb
    # available_balance is NOT increased here, because deposited capital must be used to buy a product first.
    
    comm_setting = db.query(Setting).filter(Setting.key == "commission_rate").first()
    comm_rate = float(comm_setting.value) if comm_setting else 0.30
    ref = db.query(TeamReferral).filter(TeamReferral.telegram_id == deposit.telegram_id).first()
    if ref and not ref.is_valid_depositor:
        commission = deposit.amount_etb * comm_rate
        ref.is_valid_depositor = True
        ref.commission_earned += commission
        inv_bal = db.query(UserBalance).filter(UserBalance.telegram_id == ref.inviter_id).first()
        if inv_bal:
            inv_bal.invitation_income += commission
            inv_bal.total_balance += commission
            inv_bal.available_balance += commission # NEW: Referral commission is withdrawable
    db.commit()
    notification = {"type": "deposit_approved", "amount": deposit.amount_etb, "new_balance": user_balance.total_balance}
    await manager.send_personal_message(json.dumps(notification), str(deposit.telegram_id))
    return {"message": "Deposit approved successfully."}

@app.post("/api/admin/reject-deposit/{deposit_id}")
async def reject_deposit(deposit_id: int, admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    deposit = db.query(Deposit).filter(Deposit.id == deposit_id).first()
    if not deposit: raise HTTPException(status_code=404, detail="Deposit not found.")
    deposit.status = "Rejected"
    db.commit()
    notification = {"type": "deposit_rejected", "amount": deposit.amount_etb}
    await manager.send_personal_message(json.dumps(notification), str(deposit.telegram_id))
    return {"message": "Deposit rejected successfully."}

@app.post("/api/admin/approve-withdrawal/{withdrawal_id}")
async def approve_withdrawal(withdrawal_id: int, admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    w = db.query(Withdrawal).filter(Withdrawal.id == withdrawal_id).first()
    if not w: raise HTTPException(status_code=404, detail="Withdrawal not found.")
    if w.status != "Pending": raise HTTPException(status_code=400, detail="Withdrawal already processed.")
    w.status = "Approved"
    db.commit()
    notification = {"type": "withdrawal_approved", "amount": w.amount}
    await manager.send_personal_message(json.dumps(notification), str(w.telegram_id))
    return {"message": "Withdrawal approved successfully."}

@app.post("/api/admin/reject-withdrawal/{withdrawal_id}")
async def reject_withdrawal(withdrawal_id: int, admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    w = db.query(Withdrawal).filter(Withdrawal.id == withdrawal_id).first()
    if not w: raise HTTPException(status_code=404, detail="Withdrawal not found.")
    w.status = "Rejected"
    bal = db.query(UserBalance).filter(UserBalance.telegram_id == w.telegram_id).first()
    if bal: 
        bal.total_balance += w.amount
        bal.available_balance += w.amount # NEW: Refund to available
    db.commit()
    notification = {"type": "withdrawal_rejected", "amount": w.amount}
    await manager.send_personal_message(json.dumps(notification), str(w.telegram_id))
    return {"message": "Withdrawal rejected and user refunded."}

@app.get("/api/admin/settings")
def get_settings(admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    return {s.key: s.value for s in db.query(Setting).all()}

@app.post("/api/admin/settings")
def update_settings(payload: SystemSettingsUpdate, admin: str = Depends(get_current_admin), db: Session = Depends(get_db)):
    data = payload.dict(exclude_none=True)
    for key, value in data.items():
        setting = db.query(Setting).filter(Setting.key == key).first()
        if setting: setting.value = str(value)
        else: db.add(Setting(key=key, value=str(value)))
    db.commit()
    return {"message": "Settings updated successfully"}