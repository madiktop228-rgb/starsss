from __future__ import annotations

from environs import Env
from dataclasses import dataclass
import time
from sqlalchemy import event
from sqlalchemy.engine import Engine
# from utils.logging import logger

@dataclass
class DbConfig:
    host: str
    password: str
    user: str
    database: str

@dataclass
class Config:
    bot_token: str
    logs_id: int
    withdraw_id: int
    admin_ids: list[int]
    db: DbConfig

    @property
    def database_url(self) -> str:
        if self.db.password:
            return f"postgresql+asyncpg://{self.db.user}:{self.db.password}@{self.db.host}/{self.db.database}"
        return f"postgresql+asyncpg://{self.db.user}@{self.db.host}/{self.db.database}"

def load_config():
    env = Env()
    env.read_env()
    
    admin_ids_raw = env.str("ADMIN_IDSS", "0")
    admin_ids = [int(x.strip()) for x in admin_ids_raw.split(",") if x.strip().isdigit()]
    
    return Config(
        bot_token=env.str("BOT_TOKENS"),
        logs_id=env.int("LOGS_IDSS", 0),
        withdraw_id=env.int("WITHDRAW_ID", 0),
        admin_ids=admin_ids,
        db=DbConfig(
            host=env.str('DB_HOST', 'localhost'),
            password=env.str('DB_PASS', ''),
            user=env.str('DB_USER', 'postgres'),
            database=env.str('DB_NAME', 'startovs_bot')
        )
    )

config = load_config()

# OPTIMIZE_DB = True
# if OPTIMIZE_DB:
#     @event.listens_for(Engine, "before_cursor_execute")
#     def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
#         conn.info.setdefault('query_start_time', []).append(time.time())
        
#     @event.listens_for(Engine, "after_cursor_execute")
#     def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
#         total = time.time() - conn.info['query_start_time'].pop()
#         if total > 0.2:  # Логируем медленные запросы
#             logger.warning(f"Long running query ({total:.2f}s): {statement}")