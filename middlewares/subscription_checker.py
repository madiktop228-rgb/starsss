from __future__ import annotations

from typing import Any, Callable, Dict, Awaitable, Optional, List, Union
from aiogram import BaseMiddleware, Bot
from aiogram.types import Message, CallbackQuery, TelegramObject, FSInputFile, User
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
import bot.database.requests as db
import logging as logger
import time
from collections import defaultdict
# --- Убедитесь, что импортированы нужные функции ---
try:
    from bot.handlers.user import check_member_with_delay, show_subscription_channels, request_subgram_sponsors
except ImportError as e:
    logger.critical(f"Failed to import helpers from user.py or elsewhere: {e}")
    async def check_member_with_delay(*args, **kwargs): return False
    async def show_subscription_channels(*args, **kwargs): pass
    async def request_subgram_sponsors(*args, **kwargs): return 'ok', []
# ---------------------------------------------------------
import bot.keyboards.keyboards as kb
from bot.database.requests import get_user
from bot.core.utils.logging import logger
from bot.core.config import config
from bot.database.models import Channel
# --- Импортируем состояния ---
from bot.core.utils.state import SubscriptionCheckStates # Убедитесь, что путь правильный

# --- Добавлено: Префиксы для админ-команд/коллбэков ---
ADMIN_COMMAND_PREFIX = '/admin'
ADMIN_CALLBACK_PREFIX = 'admin_'
ADMIN_ROUTES = ['admin.', '/admin'] # Список префиксов для админских путей
# Определим префиксы для пропуска вывода средств
WITHDRAW_CONFIRM_PREFIX = 'withdraw_confirm_'
WITHDRAW_REJECT_PREFIX = 'withdraw_reject_'
# --- Конец добавления ---

# --- Обновлено: Callback data для пропуска в middleware ---
# Теперь включает этап: recheck_sub_start_stage_1, recheck_sub_start_stage_2, etc.
RECHECK_CALLBACK_PREFIX = "recheck_sub_"
# --- Конец обновления ---

class SubscriptionCheckerMiddleware(BaseMiddleware):
    # Добавляем словарь для хранения времени последнего callback
    last_callback_time = {}

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user = event.from_user
        user_language_code = None

        # --- Ограничение нажатия на callback-кнопки ---
        if isinstance(event, CallbackQuery):
            user_id = event.from_user.id
            now = time.time()
            last_time = self.last_callback_time.get(user_id, 0)
            if now - last_time < 0.5:
                try:
                    await event.answer("⏳ Подождите чуть-чуть...", show_alert=True)
                except Exception:
                    pass
                return None
            self.last_callback_time[user_id] = now
        # --- Конец ограничения ---

        # --- Проверка на администратора: теперь это единственная проверка в начале ---
        user = event.from_user
        
        # Принудительно получаем список админов напрямую из конфига для отладки
        admin_ids = config.admin_ids
        logger.debug(f"Admin IDs from config: {admin_ids}, User ID: {user.id}")
        
        # Проверяем, является ли пользователь администратором
        if user.id in admin_ids:
            logger.info(f"Middleware: Admin detected ({user.id}). Skipping ALL subscription checks.")
            return await handler(event, data)
        
        # Все остальные проверки только для НЕ-администраторов
        session: AsyncSession = data["session"]
        bot: Bot = data["bot"]
        state: FSMContext = data["state"]

        # --- Проверка на язык/гео ---
        user_language_code = user.language_code
        allowed_languages = ['ru', 'by', 'ua', 'uk']
        if user_language_code and user_language_code.lower() not in allowed_languages:
            logger.warning(f"User {user.id} with language_code '{user_language_code}' not in allowed list {allowed_languages}. Attempting to ban.")
            
            # Уведомляем админа
            if config.admin_ids:
                try:
                    admin_notification = f"User {user.id} (@{user.username}) was banned due to language_code: '{user_language_code}'."
                    await bot.send_message(config.admin_ids[0], admin_notification)
                except Exception as e:
                    logger.error(f"Failed to send geo-ban notification to admin for user {user.id}: {e}")

            # Уведомляем пользователя
            ban_message = "Извините, использование бота ограничено для вашего региона/языка согласно правилам."
            if isinstance(event, Message):
                try:
                    await event.answer(ban_message)
                except Exception as e:
                    logger.error(f"Failed to send region restriction message to user {user.id}: {e}")
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer(ban_message, show_alert=True)
                except Exception as e:
                    logger.error(f"Failed to answer/delete region restriction callback to user {user.id}: {e}")
            
            # Баним в БД
            try:
                await db.set_user_ban_status(session, user.id, True)
                await session.commit()
                logger.info(f"User {user.id} was banned in DB due to language_code '{user_language_code}'.")
            except Exception as e:
                logger.error(f"Failed to ban user {user.id} in DB (language_code '{user_language_code}'). Error: {e}")
                await session.rollback()
            
            return None # Прерываем дальнейшую обработку

        # --- Получаем путь текущего обработчика и проверяем, относится ли он к админским маршрутам ---
        try:
            from_admin_router = False
            if 'raw_state' in data and data['raw_state']:
                state_path = str(data['raw_state'])
                if 'admin' in state_path.lower():
                    from_admin_router = True
                    
            if from_admin_router:
                logger.info(f"Middleware: Skipping subscription check for admin route. State: {data.get('raw_state')}")
                return await handler(event, data)
        except Exception as e:
            logger.error(f"Error while trying to determine if handler is from admin router: {e}")
            
        # --- Логируем начало проверки ---
        logger.debug(f"Middleware: Checking user {user.id}. Event type: {type(event).__name__}")

        # --- ЯВНЫЙ ПРОПУСК ДЛЯ /start ---
        if isinstance(event, Message) and event.text and event.text.startswith('/start'):
            logger.info(f"Middleware: Skipping subscription check for /start command from user {user.id}.")
            return await handler(event, data) # Сразу передаем управление дальше
        # --- КОНЕЦ ПРОПУСКА ДЛЯ /start ---

        # --- Проверка состояния FSM ---
        current_state = await state.get_state()
        # --- Пропускаем обработку, если пользователь уже в процессе проверки ---
        if current_state in [SubscriptionCheckStates.waiting_primary_check, SubscriptionCheckStates.waiting_secondary_check]:
            # Разрешаем только коллбэки перепроверки подписки
            if isinstance(event, CallbackQuery) and event.data and event.data.startswith(RECHECK_CALLBACK_PREFIX):
                 logger.debug(f"Middleware: User {user.id} in state {current_state}, allowing recheck callback: {event.data}")
                 return await handler(event, data) # Пропускаем хендлер перепроверки

            logger.debug(f"Middleware: User {user.id} tried action while in state {current_state}. Reminding to subscribe.")
            stage = 1 if current_state == SubscriptionCheckStates.waiting_primary_check else 2
            state_data = await state.get_data()
            sg_sponsors = state_data.get('subgram_sponsors', []) if stage == 2 else []
            await show_subscription_channels(event, state, bot, session, stage, subgram_sponsors=sg_sponsors)
            if isinstance(event, CallbackQuery):
                try: await event.answer("Сначала завершите проверку подписки", show_alert=True)
                except Exception: pass
            return None

        # --- Пропускаем остальные админские команды и специфичные коллбэки ---
        if isinstance(event, Message) and event.text:
            # Пропускаем админ-команды (кроме /start, который обработан выше)
            if event.text.startswith(ADMIN_COMMAND_PREFIX) or '/admin' in event.text:
                logger.debug(f"Middleware: Skipping check for ADMIN command: {event.text}")
                return await handler(event, data)
        elif isinstance(event, CallbackQuery) and event.data:
            # Пропускаем админские, служебные и речек-коллбэки
            if event.data.startswith(ADMIN_CALLBACK_PREFIX) or 'admin' in event.data:
                logger.debug(f"Middleware: Skipping check for ADMIN callback: {event.data}")
                return await handler(event, data)
            elif (event.data.startswith(WITHDRAW_CONFIRM_PREFIX) or
                    event.data.startswith(WITHDRAW_REJECT_PREFIX) or
                    event.data == "start_withdraw" or
                    event.data == "withdraw_again"):
                logger.debug(f"Middleware: Skipping check for specific callback: {event.data}")
                return await handler(event, data)
            elif event.data.startswith(RECHECK_CALLBACK_PREFIX):
                 logger.debug(f"Middleware: Skipping check for recheck callback: {event.data}")
                 return await handler(event, data)

        # --- Логируем, если проверка НЕ пропущена ---
        logger.debug(f"Middleware: Proceeding with subscription check for user {user.id}. Event data/text: {event.data if isinstance(event, CallbackQuery) else event.text}")

        user_id = user.id
        if not user_id or not bot or not session:
            logger.warning("Middleware: Couldn't get user_id, bot, or session. Skipping check.")
            return await handler(event, data)

        # --- Проверка бана ---
        user_data = await db.get_user(session, user_id)
        if user_data and user_data.banned:
            # ... (логика бана остается прежней) ...
            return

        # --- Проверка Premium ---
        is_premium = user.is_premium
        logger.debug(f"Middleware: User {user_id} Premium status: {is_premium}")

        # --- ЭТАП 1: Проверка подписки на каналы первого этапа ---
        logger.debug(f"Middleware: Fetching STAGE 1 channels for user {user_id}.")
        channels_stage1_check = await db.get_filtered_start_channels(session, is_premium=is_premium)
        logger.debug(f"Middleware: Found {len(channels_stage1_check)} channels to check for STAGE 1.")
        channels_stage1_show = await db.get_filtered_start_channels_all(session, is_premium=is_premium)

        # --- ЭТАП 1: только локальные каналы ---
        all_subscribed_stage1 = True
        failed_channels_stage1 = []

        if channels_stage1_check:
            logger.debug(f"Middleware: Entering STAGE 1 check loop for user {user.id}.")
            for channel in channels_stage1_check:
                try:
                    is_subscribed = await check_member_with_delay(bot, channel.channel_id, user_id)
                    if not is_subscribed:
                        all_subscribed_stage1 = False
                        failed_channels_stage1.append(channel)
                        logger.info(f"Middleware: User {user_id} FAILED check for STAGE 1 channel {channel.channel_id}.")
                except Exception as e:
                    logger.error(f"Error checking STAGE 1 subscription for channel {channel.channel_id} user {user_id}: {e}")
                    all_subscribed_stage1 = False
                    if channel not in failed_channels_stage1:
                        failed_channels_stage1.append(channel)
        else:
            logger.debug(f"Middleware: No channels to check for STAGE 1 for user {user.id}. Skipping loop.")

        if not all_subscribed_stage1:
            logger.info(f"Middleware: User {user_id} failed Stage 1 (local).")
            await state.set_state(SubscriptionCheckStates.waiting_primary_check)
            await show_subscription_channels(event, state, bot, session, stage=1, failed_channels=failed_channels_stage1)
            if isinstance(event, CallbackQuery):
                try: await event.answer("Необходимо подписаться на каналы", show_alert=True)
                except Exception: pass
            return None

        logger.info(f"Middleware: User {user_id} PASSED STAGE 1. Proceeding.")

        # --- ЭТАП 2: локальные каналы + SubGram ---
        logger.debug(f"Middleware: Fetching STAGE 2 channels for user {user_id}.")
        channels_stage2_check = await db.get_filtered_second_stage_channels(session, is_premium=is_premium)
        logger.debug(f"Middleware: Found {len(channels_stage2_check)} channels to check for STAGE 2.")
        channels_stage2_show = await db.get_filtered_second_stage_channels_all(session, is_premium=is_premium)

        all_subscribed_stage2 = True
        failed_channels_stage2 = []
        sg_sponsors_stage2 = []

        if channels_stage2_check:
            logger.debug(f"Middleware: Entering STAGE 2 check loop for user {user_id}.")
            for channel in channels_stage2_check:
                try:
                    is_subscribed = await check_member_with_delay(bot, channel.channel_id, user_id)
                    if not is_subscribed:
                        all_subscribed_stage2 = False
                        failed_channels_stage2.append(channel)
                        logger.info(f"Middleware: User {user_id} FAILED check for STAGE 2 channel {channel.channel_id}.")
                except Exception as e:
                    logger.error(f"Error checking STAGE 2 subscription for channel {channel.channel_id} user {user_id}: {e}")
                    all_subscribed_stage2 = False
                    if channel not in failed_channels_stage2:
                        failed_channels_stage2.append(channel)

        try:
            sg_status, sg_sponsors = await request_subgram_sponsors(
                user_id=user_id,
                chat_id=user_id,
                first_name=user.first_name if user else None,
                username=user.username if user else None,
                language_code=user.language_code if user else None,
                is_premium=is_premium,
                action="subscribe"
            )
            if sg_status == 'warning' and sg_sponsors:
                sg_sponsors_stage2 = sg_sponsors
                logger.info(f"Middleware: User {user_id} has {len(sg_sponsors_stage2)} SubGram sponsors in Stage 2.")
        except Exception as e:
            logger.error(f"Middleware: SubGram check error for user {user_id}: {e}", exc_info=True)

        if not all_subscribed_stage2 or sg_sponsors_stage2:
            logger.info(f"Middleware: User {user_id} failed Stage 2 (local={not all_subscribed_stage2}, subgram={len(sg_sponsors_stage2)}).")
            await state.set_state(SubscriptionCheckStates.waiting_secondary_check)
            await show_subscription_channels(
                event, state, bot, session, stage=2,
                failed_channels=failed_channels_stage2,
                subgram_sponsors=sg_sponsors_stage2
            )
            if isinstance(event, CallbackQuery):
                try: await event.answer("Необходимо подписаться на каналы", show_alert=True)
                except Exception: pass
            return None

        logger.info(f"Middleware: User {user_id} PASSED STAGE 2 (local + SubGram). PASSED ALL checks.")

        # --- Если все проверки пройдены ---
        logger.debug(f"Middleware: Clearing state (if any) for user {user.id} and passing to handler.")
        # Сбрасываем состояние, если оно было установлено ранее (на всякий случай)
        current_state_final = await state.get_state() # Проверяем состояние еще раз
        if current_state_final in [SubscriptionCheckStates.waiting_primary_check, SubscriptionCheckStates.waiting_secondary_check]:
             await state.clear()
        # Пропускаем дальше к хендлеру
        return await handler(event, data)

class BanCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Проверяем только для Message и CallbackQuery, где есть from_user
        if not hasattr(event, 'from_user') or not event.from_user:
             return await handler(event, data) # Пропускаем для других типов событий

        user_id = event.from_user.id

        # Извлекаем и логируем список админов для отладки
        admin_ids = config.admin_ids
        logger.debug(f"BanCheck: Admin IDs: {admin_ids}, checking user {user_id}")

        # Пропускаем проверку для админов с подробным логированием
        if user_id in admin_ids:
            logger.info(f"BanCheck: Admin user {user_id} detected, skipping ban check completely")
            return await handler(event, data)
        
        session: AsyncSession = data.get('session') # Получаем сессию из контекста

        # Если сессия не передана (например, DbSessionMiddleware не отработал), пропускаем проверку
        if not session:
            logger.warning(f"Session not found in data for user {user_id}. Skipping ban check in BanCheckMiddleware.")
            return await handler(event, data)

        user = await get_user(session, user_id)

        # Если пользователь найден и забанен
        if user and user.banned:
            logger.warning(f"Access denied for banned user {user_id} (State: {data.get('raw_state')}). Event type: {type(event).__name__}")
            # Отправляем сообщение пользователю о бане (только для Message, чтобы не спамить на callback)
            if isinstance(event, Message):
                try:
                    # Проверяем, не команда /start ли это, чтобы избежать зацикливания бана при старте
                    if not event.text or not event.text.startswith('/start'):
                        await event.answer("❌ Вы были заблокированы в этом боте.")
                except Exception as e:
                    logger.debug(f"Could not notify banned user {user_id} about ban: {e}")
            elif isinstance(event, CallbackQuery):
                try:
                    # Отвечаем на колбэк, если он от забаненного пользователя
                    await event.answer("❌ Доступ запрещен.", show_alert=True)
                except Exception as e:
                    logger.debug(f"Could not answer callback from banned user {user_id}: {e}")
            # Прерываем дальнейшую обработку для этого пользователя
            return None # В aiogram 3 это прервет обработку

        # Если пользователь не забанен или не найден (что маловероятно для существующих пользователей),
        # продолжаем выполнение следующих middleware и хэндлеров
        return await handler(event, data)

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, rate_limit: int = 6, cooldown: int = 5):
        self.rate_limit = rate_limit  # Количество разрешенных запросов
        self.cooldown = cooldown  # Время кулдауна в секундах
        self.user_requests = defaultdict(list)  # Словарь для хранения времени запросов пользователей
        super().__init__()

    async def __call__(
        self,
        handler: Callable[[Message | CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user_id = event.from_user.id
        
        # Пропускаем проверку для администраторов
        if user_id in config.admin_ids:
            logger.debug(f"RateLimit: Admin user {user_id} detected, skipping rate limiting")
            return await handler(event, data)
            
        current_time = time.time()

        # Очищаем старые запросы
        self.user_requests[user_id] = [t for t in self.user_requests[user_id] 
                                     if current_time - t < self.cooldown]

        # Проверяем количество запросов
        if len(self.user_requests[user_id]) >= self.rate_limit:
            # Если превышен лимит, отправляем сообщение и блокируем
            bot: Bot = data["bot"]
            remaining_time = int(self.cooldown - (current_time - self.user_requests[user_id][0]))
            
            if isinstance(event, CallbackQuery):
                await event.answer(
                    f"Вы слишком много запросов подаете, попробуйте через {remaining_time} секунд",
                    show_alert=True
                )
            else:
                await event.answer(
                    f"Вы слишком много запросов подаете, попробуйте через {remaining_time} секунд"
                )
            return None

        # Добавляем текущий запрос
        self.user_requests[user_id].append(current_time)
        
        # Продолжаем обработку
        return await handler(event, data)