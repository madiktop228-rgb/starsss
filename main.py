from __future__ import annotations

import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from aiogram.client.default import DefaultBotProperties
from aiohttp import web
import ssl

from bot.core.config import config
from bot.handlers import admin, user
from bot.handlers.webhooks import setup_webhook_routes
from bot.database.models import Base
from bot.database import requests as db
from bot.middlewares.subscription_checker import BanCheckMiddleware, RateLimitMiddleware
from bot.tasks.bio_checker import check_users_bio
from bot.tasks.task_extender import extend_overdue_time_distributed_tasks

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from bot.core.utils.logging import setup_logging
from bot.core.config import load_config

try:
    from gift_sender_bot import create_app, GiftSender, GiftWithdrawProcessor
    GIFT_SENDER_AVAILABLE = True
except ImportError:
    GIFT_SENDER_AVAILABLE = False

# Настраиваем логирование ДО всего остального
setup_logging()

# Можно использовать логгер и здесь (опционально)
logger = logging.getLogger(__name__)

config = load_config()
# --- Временный вывод для проверки --- 
print(f"Loaded admin IDs: {config.admin_ids}")
logger.info(f"Loaded admin IDs: {config.admin_ids}") # И в логгер
# -----------------------------------

# OPTIMIZE_DB = True

async def main():
    engine = create_async_engine(
        config.database_url,
        echo=False
    )
    
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, checkfirst=True)
            logger.info("Database tables checked/created.")
    except Exception as e:
        logger.critical(f"Failed to connect to database or create tables: {e}", exc_info=True)
        return
    
    async_session_factory = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    storage = MemoryStorage()
    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    # Настройка бота для работы в inline режиме 
    # (это нужно сделать и в BotFather, отправив команду /setinline)
    await bot.set_my_commands([])
    
    gift_client = None
    gift_processor = None
    if GIFT_SENDER_AVAILABLE:
        try:
            gift_client = create_app()
            await gift_client.start()
            logger.info("Pyrogram client started successfully")
            
            gift_sender = GiftSender(gift_client)
            gift_processor = GiftWithdrawProcessor(gift_sender)
            logger.info("Gift processor initialized")
        except Exception as e:
            logger.warning(f"Gift sender not initialized (optional): {e}")
    else:
        logger.info("Gift sender module not available, skipping")
    
    dp = Dispatcher(storage=storage)
    
    # ОП (обязательная подписка) отключена
    dp.message.middleware(BanCheckMiddleware())
    dp.callback_query.middleware(BanCheckMiddleware())
    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())

    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')

    # --- Новые асинхронные функции-обертки ---
    async def run_reset_referrals():
        logger.info("Running scheduled job: reset_referrals_24h")
        try:
            async with async_session_factory() as session:
                await db.reset_referrals_24h(session)
            logger.info("Scheduled job 'reset_referrals_24h' completed successfully.")
        except Exception as e:
            logger.error(f"Error in scheduled job 'reset_referrals_24h': {e}", exc_info=True)

    async def run_reset_daily_bonus():
        logger.info("Running scheduled job: reset_daily_bonus")
        try:
            async with async_session_factory() as session:
                await db.reset_daily_bonus(session)
            logger.info("Scheduled job 'reset_daily_bonus' completed successfully.")
        except Exception as e:
            logger.error(f"Error in scheduled job 'reset_daily_bonus': {e}", exc_info=True)

    async def run_bio_check():
        logger.info("Running scheduled job: bio_check")
        try:
            await check_users_bio(bot, config, async_session_factory)
            logger.info("Scheduled job 'bio_check' completed successfully.")
        except Exception as e:
            logger.error(f"Error in scheduled job 'bio_check': {e}", exc_info=True)
    # -----------------------------------------

    scheduler.add_job(run_reset_referrals, CronTrigger(hour=0, minute=0, timezone='Europe/Moscow'))
    scheduler.add_job(run_reset_daily_bonus, CronTrigger(hour=0, minute=0, timezone='Europe/Moscow'))
    # Проверка био каждую минуту
    # scheduler.add_job(run_bio_check, CronTrigger(minute=0, timezone='Europe/Moscow'))
    # scheduler.add_job(run_bio_check, CronTrigger(hour=21, timezone='Europe/Moscow'))

    # --- Планировщик продления заданий: каждый час ---
    async def run_task_extension():
        logger.info("Running scheduled job: extend_time_distributed_tasks")
        try:
            async with async_session_factory() as session:
                stats = await extend_overdue_time_distributed_tasks(session, append_hours=4)
                try:
                    await session.commit()
                    # Отправляем статистику администратору
                    if stats and stats.get("extended_count", 0) > 0:
                        from bot.tasks.task_extender import send_extension_stats_to_admin
                        await send_extension_stats_to_admin(bot, stats)
                except Exception as e:
                    logger.error(f"Commit error in task extender job: {e}")
                    await session.rollback()
            logger.info("Scheduled job 'extend_time_distributed_tasks' completed successfully.")
        except Exception as e:
            logger.error(f"Error in scheduled job 'extend_time_distributed_tasks': {e}", exc_info=True)

    scheduler.add_job(run_task_extension, CronTrigger(minute=16, timezone='Europe/Moscow'))

    scheduler.start()

    dp.include_router(admin.router)
    dp.include_router(user.router)
    
    @dp.update.middleware()
    async def db_session_middleware(handler, event, data):
        # Создаем новую сессию для каждого запроса
        async with async_session_factory() as session:
            try:
                # Помещаем сессию и другие данные в контекст
                data["session"] = session
                data["config"] = config
                data["session_factory"] = async_session_factory
                data["gift_processor"] = gift_processor  # Добавляем gift_processor в контекст
                
                # Выполняем обработчик и получаем результат
                result = await handler(event, data)
                
                # Если все хорошо, фиксируем изменения
                try:
                    await session.commit()
                except Exception as e:
                    logger.error(f"Error committing session: {e}")
                    await session.rollback()
                    raise
                
                return result
            except Exception as e:
                # В случае ошибки откатываем изменения
                try:
                    await session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during session rollback: {rollback_error}")
                
                # Передаем исключение дальше
                raise
            # Сессия автоматически закроется благодаря async with
    
    # Настройка веб-сервера для webhook'ов
    app = web.Application()
    setup_webhook_routes(app, config, async_session_factory, bot)
    
    # Запуск веб-сервера и бота параллельно
    async def start_webhook_server():
        runner = web.AppRunner(app)
        await runner.setup()
        
        # Слушаем на всех интерфейсах для доступа извне
        site = web.TCPSite(runner, '0.0.0.0', 8081)
        await site.start()
        logger.info("Webhook server started on 0.0.0.0:8081 (accessible from outside)")
        return runner
    
    try:
        logger.info("Starting webhook server and bot...")
        
        # Запускаем веб-сервер
        runner = await start_webhook_server()
        
        # Запускаем бота
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
        
    finally:
        logger.info("Stopping services...")
        scheduler.shutdown()
        if 'runner' in locals():
            await runner.cleanup()
        if gift_client:
            try:
                await gift_client.stop()
                logger.info("Pyrogram client stopped")
            except Exception as e:
                logger.error(f"Error stopping gift client: {e}")
        await bot.session.close()
        await engine.dispose()
        logger.info("All services stopped.")

if __name__ == "__main__":
    asyncio.run(main())