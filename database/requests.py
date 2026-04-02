from __future__ import annotations

from typing import Optional, List, Tuple, Dict, Any
from sqlalchemy import select, delete, update, func, not_, and_, String, cast, or_, text, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, aliased, joinedload
from .models import User, Channel, Settings, DailyBonus, LocalCompletedTask, DailyTask, Withdraws, Task, user_completed_tasks_table, IndividualLink, PromoCode, BroadcastTemplate, user_used_promocodes_table, SubGramWebhook, SubGramCompletedTask, TraffyCompletedTask, GiftWithdrawSettings, Show
from datetime import datetime, timedelta, date
from bot.core.utils.generate_random_id import generate_random_id
from bot.core.utils.logging import logger
from sqlalchemy.exc import SQLAlchemyError
from bot.database.models import Task, Channel
from aiogram.types import InlineKeyboardMarkup
import logging
import asyncio
from bot.gift_sender_bot import GiftWithdrawProcessor, create_app
from bot.core.config import Config
from aiogram import Bot
import json
import random

logger = logging.getLogger(__name__)

async def get_user(session: AsyncSession, user_id: int) -> Optional[User]:
    """Получить пользователя по ID"""
    query = select(User).where(User.user_id == user_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

# --- Добавляем новую функцию ---
async def get_user_by_username(session: AsyncSession, username: str) -> Optional[User]:
    """Получить пользователя по юзернейму (без @)."""
    # Убираем @ если он есть в начале
    clean_username = username.lstrip('@')
    query = select(User).where(User.username == clean_username)
    result = await session.execute(query)
    return result.scalar_one_or_none()
# -----------------------------

async def add_user(
    session: AsyncSession,
    user_id: int,
    username: str,
    refferal_id: Optional[int] = None,
    individual_link_id: Optional[int] = None
) -> User:
    """Добавить нового пользователя с возможной привязкой к рефералу или инд. ссылке."""
    if refferal_id and individual_link_id:
        individual_link_id = None
        print(f"[Warning] User {user_id} tried to register with both referral {refferal_id} and individual link {individual_link_id}. Prioritizing referral.")

    user = User(
        user_id=user_id,
        username=username,
        refferal_id=refferal_id,
        individual_link_id=individual_link_id
    )
    session.add(user)
    await session.commit()
    print(f'Добавлен пользователь: [{user_id}] - @{username}')
    return user

async def get_all_channels(session: AsyncSession) -> List[Channel]:
    """Получить все каналы"""
    # --- Сортируем сначала по этапу, потом по типу и ID ---
    query = select(Channel).order_by(Channel.check_stage, Channel.check_type, Channel.id)
    result = await session.execute(query)
    return result.scalars().all()

async def get_public_channels(session: AsyncSession) -> List[Channel]:
    """Получить все публичные каналы (устарело, используйте get_public_start_channels или get_public_withdraw_channels)"""
    logger.warning("Вызвана устаревшая функция get_public_channels. Используйте get_public_start_channels или get_public_withdraw_channels.")
    query = select(Channel).where(Channel.channel_status == 'Публичный')
    result = await session.execute(query)
    return result.scalars().all()

async def get_channel(session: AsyncSession, channel_id: int) -> Optional[Channel]:
    """Получить канал по его Telegram ID"""
    query = select(Channel).where(Channel.channel_id == channel_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_reward(session: AsyncSession) -> Optional[int]:
    """Получить награду за реферала"""
    query = select(Settings).where(Settings.id == 1)
    result = await session.execute(query)
    settings = result.scalar_one_or_none()
    return settings.refferal_reward if settings else None

async def add_balance(session: AsyncSession, user_id: int, balance: int) -> None:
    """Добавить баланс пользователю"""
    query = update(User).where(User.user_id == user_id).values(balance=User.balance + balance)
    await session.execute(query)

async def set_user_ref_bonus_passed(session: AsyncSession, user_id: int) -> bool:
    """
    Устанавливает флаг ref_bonus = True для пользователя, ТОЛЬКО ЕСЛИ он был False.
    Возвращает True, ТОЛЬКО ЕСЛИ флаг был успешно изменен с False на True в этой транзакции.
    Возвращает False во всех остальных случаях (ошибка, пользователь не найден, флаг уже был True).
    ВАЖНО: Эта функция НЕ делает commit.
    """
    try:
        # Запрос обновляет строку только если user_id совпадает И ref_bonus равен False
        query = update(User).where(
            User.user_id == user_id,
            User.ref_bonus == False  # Строго проверяем, что флаг был False
        ).values(ref_bonus=True)

        result = await session.execute(query)

        # Проверяем, была ли ФАКТИЧЕСКИ обновлена строка в результате этого запроса
        if result.rowcount > 0:
            # rowcount > 0 означает, что условие where (user_id = ? AND ref_bonus = False) было выполнено,
            # и значение ref_bonus было изменено на True именно этим вызовом.
            logger.info(f"[DB Request] Prepared update ref_bonus=True for user {user_id}. Flag was changed from False to True.")
            return True # Успешно изменили флаг
        else:
            # Если rowcount == 0, значит либо пользователь не найден, либо флаг УЖЕ был True
            # (или стал True в параллельной транзакции до выполнения этого update).
            # В любом случае, повторно награду начислять не нужно.
            # Можно добавить лог для ясности, но главное - вернуть False.
            existing_user = await get_user(session, user_id) # Проверим причину для лога
            if existing_user and existing_user.ref_bonus is True:
                logger.info(f"[DB Request] ref_bonus was already True for user {user_id}. No update prepared by this call.")
            elif not existing_user:
                logger.warning(f"[DB Request] Failed to set ref_bonus=True for user {user_id}. User not found.")
            else: # Неожиданно, пользователь есть, но флаг не False и не True?
                 logger.warning(f"[DB Request] Failed to set ref_bonus=True for user {user_id}. Unexpected state (ref_bonus is not False).")
            return False # Возвращаем False, т.к. флаг не был изменен с False на True ЭТИМ вызовом

    except Exception as e:
        logger.error(f"[DB Request Error] Failed to prepare update ref_bonus for user {user_id}: {e}", exc_info=True)
        # В случае любой ошибки СУБД также считаем операцию неуспешной
        return False

async def get_daily_bonus(session: AsyncSession, user_id: int) -> Optional[DailyBonus]:
    """Получить ежедневный бонус пользователя"""
    query = select(DailyBonus).where(DailyBonus.user_id == user_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def add_daily_bonus(session: AsyncSession, user_id: int, daily_bonus: int) -> None:
    """Добавить ежедневный бонус пользователя"""
    bonus = DailyBonus(user_id=user_id, daily_bonus=daily_bonus)
    await add_balance(session, user_id, daily_bonus)
    session.add(bonus)
    await session.commit()

async def delete_daily_bonus(session: AsyncSession, user_id: int) -> None:
    """Удалить ежедневный бонус пользователя"""
    query = delete(DailyBonus).where(DailyBonus.user_id == user_id)
    await session.execute(query)
    await session.commit()

async def reset_daily_bonus(session: AsyncSession) -> None:
    """Сбросить ежедневный бонус пользователей"""
    query = delete(DailyBonus)
    await session.execute(query)
    await session.commit()

async def reset_referrals_24h(session: AsyncSession) -> None:
    """Сбросить счетчик рефералов за 24 часа у всех пользователей"""
    query = select(User)
    result = await session.execute(query)
    users = result.scalars().all()
    
    for user in users:
        user.refferals_24h_count = 0
    
    await session.commit()

async def get_settings(session: AsyncSession) -> Settings:
    """
    Получает объект Settings из БД.
    Если настроек нет, создает запись с значениями по умолчанию.
    """
    stmt = select(Settings).limit(1)
    result = await session.execute(stmt)
    settings = result.scalar_one_or_none()

    if not settings:
        logger.info("No settings found in DB, creating default settings.")
        settings = Settings(refferal_reward=1.25) # Установите ваше значение по умолчанию
        session.add(settings)
        try:
            # Используем flush для получения ID до коммита, если нужно, но здесь просто добавляем
            # await session.flush([settings])
            # Коммитим сразу, чтобы настройка была доступна
            await session.commit()
            logger.info(f"Default settings created with refferal_reward={settings.refferal_reward}.")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create default settings: {e}")
            await session.rollback()
            # В случае ошибки возвращаем временный объект или вызываем исключение
            # Возвращаем объект с дефолтным значением, но он не будет в БД
            return Settings(refferal_reward=1.25) # Или raise e

    return settings

async def get_refferal_reward(session: AsyncSession) -> float:
    """Получает текущую реферальную награду из настроек."""
    settings = await get_settings(session)
    # Убедимся, что возвращаем float
    reward = float(settings.refferal_reward) if settings.refferal_reward is not None else 0.0
    logger.debug(f"Retrieved referral reward: {reward}")
    return reward

async def update_refferal_reward(session: AsyncSession, new_reward: float) -> bool:
    """Обновляет реферальную награду в настройках."""
    try:
        # Сначала получаем настройки (это создаст их, если нет)
        settings = await get_settings(session)
        if not settings:
             # Если get_settings не смог создать настройки, выходим
             logger.error("Cannot update referral reward: failed to get or create settings.")
             return False

        # Обновляем значение
        stmt = (
            update(Settings)
            .where(Settings.id == settings.id) # Обновляем по ID полученной/созданной строки
            .values(refferal_reward=new_reward)
        )
        await session.execute(stmt)
        await session.commit()
        logger.info(f"Referral reward updated to {new_reward}")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Failed to update referral reward to {new_reward}: {e}")
        await session.rollback()
        return False

async def get_daily_bonus_status(session: AsyncSession, user_id: int) -> Optional[int]:
    """Получить статус ежедневного бонуса пользователя"""
    query = select(DailyBonus).where(DailyBonus.user_id == user_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_top_users(session: AsyncSession, limit: int = 30) -> List[User]:
    """Получить топ пользователей (рефереров) за последние 24 часа"""
    query = select(User).order_by(User.refferals_24h_count.desc()).limit(limit)
    logger.debug(f"Executing get_top_users (top referrers 24h) with limit={limit}")
    result = await session.execute(query)
    # Важно: .all() нужно вызывать один раз, чтобы не исчерпать итератор
    users = result.scalars().all()
    logger.debug(f"get_top_users (top referrers 24h) returned {len(users)} users.")
    return users

# 2. Топ рефереров за все время:
#    Функция `get_top_users_all_time` сортирует пользователей по полю `refferals_count`
#    в порядке убывания и возвращает указанное количество (`limit`).
#    Это и есть топ рефереров за все время.
async def get_top_users_all_time(session: AsyncSession, limit: int = 30) -> List[User]:
    """Получить топ пользователей (рефереров) за все время"""
    query = select(User).order_by(User.refferals_count.desc()).limit(limit)
    logger.debug(f"Executing get_top_users_all_time (top referrers all time) with limit={limit}")
    result = await session.execute(query)
    # Важно: .all() нужно вызывать один раз
    users = result.scalars().all()
    logger.debug(f"get_top_users_all_time (top referrers all time) returned {len(users)} users.")
    return users

async def get_total_users(session: AsyncSession) -> int:
    """Получить общее количество пользователей"""
    query = select(User)
    result = await session.execute(query)
    return len(result.scalars().all())  

async def total_withdraws(session: AsyncSession) -> int:
    query = select(Withdraws)
    result = await session.execute(query)
    return len(result.scalars().all())

async def minus_balance(session: AsyncSession, user_id: int, sum: float) -> bool:
    """Вычитает сумму из баланса пользователя, если баланс достаточен.

    Возвращает True, если обновление подготовлено (баланс достаточен),
    False в противном случае.
    ВАЖНО: Эта функция НЕ делает commit.
    """
    try:
        # 1. Получаем текущего пользователя и его баланс
        user_query = select(User).where(User.user_id == user_id)
        user_result = await session.execute(user_query)
        user = user_result.scalar_one_or_none()

        if user is None:
            logger.warning(f"[Minus Balance] User {user_id} not found.")
            return False

        # 2. Проверяем, достаточно ли баланса
        if user.balance >= sum:
            # 3. Готовим обновление
            query = update(User).where(User.user_id == user_id).values(balance=User.balance - sum)
            await session.execute(query)
            logger.info(f"[Minus Balance] Prepared balance subtraction for user {user_id}. Amount: {sum}. New potential balance: {user.balance - sum}")
            return True
        else:
            logger.warning(f"[Minus Balance] Insufficient balance for user {user_id}. Current: {user.balance}, Tried to subtract: {sum}")
            return False
    except SQLAlchemyError as e:
        logger.error(f"[Minus Balance Error] SQLAlchemyError for user {user_id}: {e}", exc_info=True)
        # Не откатываем здесь, т.к. коммита не было
        return False # Возвращаем False при ошибке
    except Exception as e:
        logger.error(f"[Minus Balance Error] Unexpected error for user {user_id}: {e}", exc_info=True)
        return False
    
async def increment_referral_counts(session: AsyncSession, user_id: int) -> bool:
    """Подготавливает инкремент счетчиков рефералов (общего и за 24ч) для пользователя.
    ВАЖНО: Эта функция НЕ делает commit.
    Возвращает True, если подготовка прошла успешно, False в случае ошибки или если пользователь не найден.
    """
    try:
        query = (
            update(User)
            .where(User.user_id == user_id)
            .values(
                refferals_count=User.refferals_count + 1,
                refferals_24h_count=User.refferals_24h_count + 1 # Обновляем и счетчик за 24 часа
            )
        )
        result = await session.execute(query)
        if result.rowcount > 0:
            logger.info(f"[DB Request] Prepared increment referral counts for user {user_id}")
            return True
        else:
            # Пользователь не найден, хотя это маловероятно, если мы только что начисляли ему баланс
            logger.warning(f"[DB Request] Failed to prepare increment referral counts for user {user_id}. User not found.")
            return False
    except Exception as e:
        logger.error(f"[DB Request Error] Failed to prepare increment referral counts for user {user_id}: {e}", exc_info=True)
        return False

async def create_withdraw(session: AsyncSession, user_id: int, sum: int, username: str, 
                         bot=None, config=None, gift_processor=None, gift_data=None) -> None:
    """Создать заявку на вывод с возможностью автоматической обработки подарками"""
    
    # Определяем тип обработки и детали подарка
    processing_type = 'manual'
    gift_details = None
    
    if gift_data:
        # Если переданы данные подарка, сохраняем их
        gift_details = f"Gift ID: {gift_data.get('gift_id')}, Emoji: {gift_data.get('gift_emoji')}, Name: {gift_data.get('gift_name')}"
        processing_type = 'gift_processing' if (
            bot is not None and config is not None and gift_processor is not None
        ) else 'manual'
    
    # Создаем заявку на вывод
    withdraw = Withdraws(
        user_id=user_id, 
        withdraw_amount=sum, 
        withdraw_username=username, 
        withdraw_id=generate_random_id(),
        processing_type=processing_type,
        gift_details=gift_details
    )
    
    # Списываем баланс и сохраняем заявку
    await minus_balance(session, user_id, sum)
    session.add(withdraw)
    await session.commit()
    await session.refresh(withdraw)  # Получаем ID созданной заявки
    
    # Проверяем настройки автоматических выплат
    settings = await get_gift_withdraw_settings(session)
    
    # Если автообработка включена, сумма подходит для подарков и переданы все необходимые параметры
    if (settings and settings.enabled and 
        sum >= settings.min_amount_for_gifts and
        bot is not None and config is not None and
        gift_processor is not None):
        
        logger.info(f"Запуск автоматической обработки подарками для заявки {withdraw.withdraw_id}")
        
        update_stmt = update(Withdraws).where(
            Withdraws.id == withdraw.id
        ).values(processing_type='gift_processing')
        
        await session.execute(update_stmt)
        await session.commit()
        
        # Формируем текст сообщения в зависимости от типа заявки
        if gift_data:
            status_text = f'''✅ Запрос на отправку подарка №{withdraw.id}

👤 Пользователь: {username} | ID: {user_id}
🎁 Подарок: {gift_data.get('gift_emoji')} {gift_data.get('gift_name')} ({sum}⭐️)

🔧 Статус: Отправляем подарок ⚙️'''
        else:
            status_text = f'''✅ Запрос на вывод №{withdraw.id}

👤 Пользователь: {username} | ID: {user_id}
🔑 Количество: {sum}⭐️

🔧 Статус: Отправляем подарок ⚙️'''
        
        # Отправляем сообщение в канал администраторов с статусом "Отправляем подарок"
        from bot.keyboards import keyboards as kb
        message = await bot.send_message(
            chat_id=config.withdraw_id, 
            text=status_text, 
            reply_markup=kb.withdraw_admin_keyboard(withdraw.withdraw_id, sum, username, user_id, withdraw.id)
        )
        
        # Сохраняем ID сообщения для последующего обновления
        update_msg_stmt = update(Withdraws).where(
            Withdraws.id == withdraw.id
        ).values(admin_message_id=message.message_id)
        
        await session.execute(update_msg_stmt)
        await session.commit()
        
        # Запускаем фоновую обработку с уже запущенным gift_processor
        try:
            # Запускаем обработку в фоне с правильными аргументами
            asyncio.create_task(gift_processor.process_withdraw(
                withdraw_id=withdraw.withdraw_id,
                visual_id = withdraw.id,  # Передаем строковый ID заявки
                user_id=user_id,
                amount=sum,
                session=session,
                bot=bot,
                config=config,
                max_remainder=settings.max_remainder
            ))
            
            logger.info(f"Фоновая обработка подарками запущена для заявки {withdraw.withdraw_id}")
            
        except Exception as e:
            logger.error(f"Ошибка запуска автообработки для заявки {withdraw.withdraw_id}: {e}")
            
            # Возвращаем статус на ручную обработку при ошибке
            rollback_stmt = update(Withdraws).where(
                Withdraws.id == withdraw.id
            ).values(
                processing_type='manual',
                processing_error=f"Ошибка запуска автообработки: {str(e)}"
            )
            await session.execute(rollback_stmt)
            await session.commit()
    
    else:
        reason = []
        if not settings or not settings.enabled:
            reason.append("автообработка отключена")
        if settings and sum < settings.min_amount_for_gifts:
            reason.append(f"сумма {sum} меньше минимальной {settings.min_amount_for_gifts}")
        if bot is None or config is None:
            reason.append("не переданы bot/config")
        if gift_processor is None:
            reason.append("gift_processor недоступен")
        
        logger.info(f"Заявка {withdraw.withdraw_id} будет обработана вручную ({', '.join(reason)})")
        
        # Формируем текст сообщения в зависимости от типа заявки
        if gift_data:
            status_text = f'''✅ Запрос на отправку подарка №{withdraw.id}

👤 Пользователь: {username} | ID: {user_id}
🎁 Подарок: {gift_data.get('gift_emoji')} {gift_data.get('gift_name')} ({sum}⭐️)

🔧 Статус: Ожидает обработки ⚙️'''
        else:
            status_text = f'''✅ Запрос на вывод №{withdraw.id}

👤 Пользователь: {username} | ID: {user_id}
🔑 Количество: {sum}⭐️

🔧 Статус: Ожидает обработки ⚙️'''
        
        # Отправляем обычное сообщение для ручной обработки
        from bot.keyboards import keyboards as kb
        await bot.send_message(
            chat_id=config.withdraw_id, 
            text=status_text, 
            reply_markup=kb.withdraw_admin_keyboard(withdraw.withdraw_id, sum, username, user_id, withdraw.id)
        )

async def get_withdraw(session: AsyncSession, withdraw_id: str) -> Optional[Withdraws]:
    query = select(Withdraws).where(Withdraws.withdraw_id == withdraw_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_withdraw_id(session: AsyncSession, user_id: int) -> Optional[int]: # Изменяем тип возвращаемого значения
    """Получить ID последней заявки на вывод"""
    query = (
        select(Withdraws)
        .where(Withdraws.user_id == user_id)
        .order_by(Withdraws.withdraw_date.desc())  # сортируем по дате, самая новая первая
        .limit(1)  # берем только одну запись
    )
    result = await session.execute(query)
    withdraw = result.scalar_one_or_none()
    return withdraw.id if withdraw else None

async def get_withdraw_id_two(session: AsyncSession, user_id: int) -> Optional[int]: # Изменяем тип возвращаемого значения
    """Получить ID последней заявки на вывод"""
    query = (
        select(Withdraws)
        .where(Withdraws.user_id == user_id)
        .order_by(Withdraws.withdraw_date.desc())  # сортируем по дате, самая новая первая
        .limit(1)  # берем только одну запись
    )
    result = await session.execute(query)
    withdraw = result.scalar_one_or_none()
    return withdraw.withdraw_id if withdraw else None

async def confirm_withdraw(session: AsyncSession, withdraw_id: str) -> None:
    query = update(Withdraws).where(Withdraws.withdraw_id == withdraw_id).values(withdraw_status = True)
    await session.execute(query)
    await session.commit()

async def reject_withdraw(session: AsyncSession, withdraw_id: str) -> None:
    """Отклонить заявку на вывод и вернуть звезды пользователю"""
    logger.info(f"Attempting to reject withdraw request ID: {withdraw_id}")
    # Ищем по строковому полю Withdraws.withdraw_id
    query = select(Withdraws).where(Withdraws.withdraw_id == withdraw_id)
    result = await session.execute(query)
    withdraw = result.scalar_one_or_none()

    # Добавляем лог, чтобы увидеть результат
    logger.info(f"Result of query for withdraw_id {withdraw_id}: {withdraw}")

    if withdraw:
        logger.info(f"Withdraw request found: User ID {withdraw.user_id}, Amount {withdraw.withdraw_amount}")
        try:
            # Возвращаем звезды пользователю
            await add_balance(session, withdraw.user_id, withdraw.withdraw_amount)
            logger.info(f"Balance added back to user {withdraw.user_id}. Amount: {withdraw.withdraw_amount}")

            # Удаляем найденную заявку по ее первичному ключу (withdraw.id)
            delete_query = delete(Withdraws).where(Withdraws.withdraw_id == withdraw_id)
            await session.execute(delete_query)
            logger.info(f"Withdraw request {withdraw_id} (DB ID: {withdraw.id}) deleted after rejection.")

            await session.commit()
            logger.info(f"Transaction committed for rejection of {withdraw_id}.")
        except Exception as e:
            logger.error(f"Error during withdraw rejection for {withdraw_id}: {e}")
            await session.rollback() # Откатываем изменения в случае ошибки
    else:
        logger.warning(f"Withdraw request with string ID {withdraw_id} not found for rejection.")

async def get_refferals_count(session: AsyncSession, user_id: int) -> int:
    """Получить количество рефералов пользователя из поля refferals_count"""
    try:
        # Получаем пользователя по user_id
        query = select(User).where(User.user_id == user_id)
        result = await session.execute(query)
        user = result.scalar_one_or_none()
        
        if user and user.refferals_count is not None:
            return user.refferals_count
        else:
            return 0
    except Exception as e:
        logger.error(f"Ошибка при получении количества рефералов для пользователя {user_id}: {e}")
        return 0

async def get_task_by_id(session: AsyncSession, task_id: int, for_update: bool = False) -> Optional[Task]:
    """
    Получает задание по его ID.
    
    :param session: Сессия базы данных.
    :param task_id: ID задания.
    :param for_update: Если True, блокирует строку задания для обновления (SELECT FOR UPDATE).
    :return: Объект Task или None.
    """
    query = select(Task).where(Task.id == task_id)
    if for_update:
        query = query.with_for_update()
        
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_next_available_task(session: AsyncSession, user_id: int, is_premium: bool, previous_task_id: Optional[int] = None) -> Optional[Task]:
    """
    Находит следующее доступное для выполнения задание для пользователя.
    Логика изменена, чтобы избежать повторной выдачи только что пропущенного задания.
    """
    
    # 1. Получаем ID заданий, уже выполненных пользователем.
    # ИСПРАВЛЕНО: Добавлен join с таблицей User для корректной фильтрации по telegram user_id
    completed_tasks_subquery = (
        select(user_completed_tasks_table.c.task_id)
        .join(User, user_completed_tasks_table.c.user_id == User.id)
        .where(User.user_id == user_id)
    )
    
    # Дополнительное логирование для отладки
    logger.debug(f"get_next_available_task: user_id={user_id}, is_premium={is_premium}, previous_task_id={previous_task_id}")
    
    # Проверяем выполненные задания напрямую
    try:
        completed_result = await session.execute(completed_tasks_subquery)
        completed_task_ids = completed_result.scalars().all()
        logger.debug(f"Completed task IDs for user {user_id}: {completed_task_ids}")
    except Exception as e:
        logger.error(f"Error getting completed tasks for user {user_id}: {e}")
        completed_task_ids = []

    # 2. Строим базовый запрос для ВСЕХ потенциально доступных заданий.
    # Убираем проверку на current_completions, т.к. check_task_limits будет единым источником правды.
    query = select(Task).where(
        Task.is_active == True,
        Task.id.notin_(completed_tasks_subquery)
    )
    
    # Фильтр по премиум-статусу
    if is_premium:
        query = query.where(Task.premium_requirement.in_(['all', 'premium_only']))
    else:
        query = query.where(Task.premium_requirement.in_(['all', 'non_premium_only']))

    # 3. Получаем ВСЕ подходящие задания из БД
    result = await session.execute(query)
    all_available_tasks = result.scalars().all()

    # 4. Проверяем задания с временным распределением и лимитами
    valid_tasks = []
    for task in all_available_tasks:
        try:
            can_complete, reason = await check_task_limits(session, task)
            if can_complete:
                valid_tasks.append(task)
            else:
                logger.debug(f"Task {task.id} filtered out: {reason}")
        except Exception as e:
            logger.error(f"Error checking limits for task {task.id}: {e}")
            continue
            
    if not valid_tasks:
        logger.debug(f"No valid tasks found for user {user_id} after limits check")
        return None

    # 5. Если есть предыдущее задание (т.е. юзер пропустил), пытаемся не выдать его снова
    if previous_task_id:
        # Отфильтровываем только что пропущенное задание
        candidates = [task for task in valid_tasks if task.id != previous_task_id]
        
        # Если после фильтрации остались другие кандидаты, выбираем случайного из них
        if candidates:
            return random.choice(candidates)
        
        # Если других заданий нет, а пропущенное все еще доступно, то вернем его.
        # Это предотвращает "застревание", если доступно только одно задание.
        still_available = [task for task in valid_tasks if task.id == previous_task_id]
        if still_available:
            return still_available[0]
        
        # Если пропущенное задание уже недоступно (например, лимит исчерпан), вернем None
        return None

    # 6. Если предыдущего задания не было, просто выбираем случайное из всех доступных
    return random.choice(valid_tasks)

async def get_first_available_task(session: AsyncSession, is_premium: bool) -> Optional[Task]:
    """Получить первое доступное активное задание, учитывая премиум-статус и временное распределение."""
    query = select(Task).where(
        Task.is_active == True
        # УБИРАЕМ проверку Task.current_completions < Task.max_completions для заданий с временным распределением
    )

    # --- Фильтрация по премиум-статусу ---
    if is_premium:
        query = query.where(
            (Task.premium_requirement == 'all') | (Task.premium_requirement == 'premium_only')
        )
    else:
        query = query.where(
            (Task.premium_requirement == 'all') | (Task.premium_requirement == 'non_premium_only')
        )
    # ------------------------------------

    # Добавляем сортировку по ID
    query = query.order_by(Task.id.asc())
    
    result = await session.execute(query)
    tasks = result.scalars().all()
    
    # Проверяем каждое задание на доступность
    for task in tasks:
        try:
            can_complete, reason = await check_task_limits(session, task)
            if can_complete:
                logger.debug(f"Task {task.id} available: {reason}")
                return task
            else:
                logger.debug(f"Task {task.id} not available: {reason}")
                continue
        except Exception as e:
            logger.error(f"Error checking limits for task {task.id}: {e}")
            continue
    
    # Если ни одно задание не доступно
    return None

async def update_user_current_task(session: AsyncSession, user_id: int, next_task_id: Optional[int]):
    """Обновить текущее задание пользователя"""
    query = update(User).where(User.user_id == user_id).values(current_task_id=next_task_id)
    await session.execute(query)

async def mark_task_as_completed(session: AsyncSession, user_id: int, task_id: int) -> None:
    """Отметить задание как выполненное для пользователя."""
    try:
        logger.debug(f"Attempting to mark task {task_id} as completed for user {user_id}")
        
        # Используем session.get для получения пользователя
        user = await get_user(session, user_id)
        if user is None:
            logger.error(f"User {user_id} not found in the database")
            return
        logger.debug(f"User {user_id} found: {user}")

        # Получаем задание
        task = await session.get(Task, task_id)
        if task is None:
            logger.error(f"Task {task_id} not found in the database")
            return
        logger.debug(f"Task {task_id} found: {task}")

        # Проверяем и добавляем задание в выполненные
        if task not in user.completed_tasks:
            # Проверяем лимиты перед выполнением
            can_complete, reason = await check_task_limits(session, task)
            if not can_complete:
                logger.warning(f"Task {task_id} limits exceeded for user {user_id}: {reason}")
                return
            
            user.completed_tasks.append(task)
            logger.info(f"Task {task_id} added to completed_tasks for user {user_id}. Total completed: {len(user.completed_tasks)}")
            
            # Дополнительная проверка: убеждаемся, что задание действительно добавлено
            logger.debug(f"Verifying task {task_id} is in completed_tasks: {task in user.completed_tasks}")
            
            # Проверяем таблицу связей
            try:
                link_check = select(user_completed_tasks_table).where(
                    user_completed_tasks_table.c.user_id == user.id,
                    user_completed_tasks_table.c.task_id == task_id
                )
                link_result = await session.execute(link_check)
                link_exists = link_result.fetchone()
                logger.debug(f"Link in user_completed_tasks table: {link_exists is not None}")
            except Exception as e:
                logger.error(f"Error checking link table: {e}")
            
            # Увеличиваем счетчик выполнений задания
            task.current_completions = getattr(task, 'current_completions', 0) + 1
            logger.debug(f"Task {task_id} completion count increased to {task.current_completions}")
            
            # Проверяем, достиг ли счетчик максимального значения
            max_completions = getattr(task, 'max_completions', 1000000)
            if task.current_completions >= max_completions:
                task.is_active = False
                logger.info(f"Task {task_id} automatically deactivated: reached max completions ({task.current_completions}/{max_completions})")
            
            # Проверяем почасовые лимиты для заданий с временным распределением
            if task.is_time_distributed:
                current_hour_limit = await get_current_hour_limit(session, task.id)
                current_hour_completions = await get_current_hour_completions(session, task.id)
                if current_hour_completions >= current_hour_limit:
                    logger.info(f"Task {task_id} hourly limit reached: {current_hour_completions}/{current_hour_limit}")
            
            logger.debug(f"Task {task_id} marked as completed for user {user_id}")
        else:
            logger.debug(f"Task {task_id} already completed for user {user_id}")
    except Exception as e:
        logger.error(f"Error marking task {task_id} as completed for user {user_id}: {e}", exc_info=True)

# ... (остальные функции) ...

async def add_individual_link(session: AsyncSession, identifier: str, description: Optional[str] = None) -> Optional[IndividualLink]:
    """Добавить новую индивидуальную ссылку."""
    # Проверка на существование
    existing_link = await get_individual_link_by_identifier(session, identifier)
    if existing_link:
        return None # Или можно выбросить исключение

    new_link = IndividualLink(identifier=identifier, description=description)
    session.add(new_link)
    await session.commit()
    return new_link

async def get_individual_link_by_identifier(session: AsyncSession, identifier: str) -> Optional[IndividualLink]:
    """Получить индивидуальную ссылку по ее идентификатору."""
    query = select(IndividualLink).where(IndividualLink.identifier == identifier)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_individual_link_by_id(session: AsyncSession, link_id: int) -> Optional[IndividualLink]:
    """Получить индивидуальную ссылку по ее ID."""
    query = select(IndividualLink).where(IndividualLink.id == link_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_all_individual_links(session: AsyncSession) -> List[IndividualLink]:
    """Получить все индивидуальные ссылки."""
    query = select(IndividualLink).order_by(IndividualLink.identifier)
    result = await session.execute(query)
    return result.scalars().all()

async def delete_individual_link(session: AsyncSession, link_id: int) -> bool:
    """Удалить индивидуальную ссылку по ID."""
    # Опционально: перед удалением ссылки сбросить individual_link_id у связанных пользователей?
    # reset_users_query = update(User).where(User.individual_link_id == link_id).values(individual_link_id=None)
    # await session.execute(reset_users_query)

    query = delete(IndividualLink).where(IndividualLink.id == link_id)
    result = await session.execute(query)
    await session.commit()
    return result.rowcount > 0

async def get_individual_link_stats(session: AsyncSession, link_id: int) -> tuple[int, int]:
    """Получить статистику по индивидуальной ссылке: (Всего зарег., Прошли ОП)."""
    # Всего зарегистрировано по этой ссылке
    total_registered_query = (
        select(func.count(User.id))
        .where(User.individual_link_id == link_id)
    )
    total_registered_result = await session.execute(total_registered_query)
    total_registered = total_registered_result.scalar_one_or_none() or 0

    # Прошли ОП (ref_bonus = True) по этой ссылке
    passed_op_query = (
        select(func.count(User.id))
        .where(User.individual_link_id == link_id, User.ref_bonus == True)
    )
    passed_op_result = await session.execute(passed_op_query)
    passed_op = passed_op_result.scalar_one_or_none() or 0

    return total_registered, passed_op

# --- Функции для администрирования Заданий ---

async def add_task(
    session: AsyncSession, 
    description: str, 
    reward: float, 
    instruction_link: Optional[str], 
    action_link: Optional[str], 
    channel_id_to_check: Optional[int], 
    check_subscription: bool,
    premium_requirement: str,
    max_completions: int = 1000000
) -> Task:
    """Добавить новое задание, включая требование по премиум-статусу и лимит выполнений."""
    new_task = Task(
        description=description,
        reward=reward,
        instruction_link=instruction_link,
        action_link=action_link,
        channel_id_to_check=channel_id_to_check,
        check_subscription=check_subscription,
        is_active=True, # Новые задания по умолчанию активны
        premium_requirement=premium_requirement, # Сохраняем требование
        max_completions=max_completions,
        current_completions=0
    )
    session.add(new_task)
    # Убираем commit, чтобы им управлял middleware
    await session.flush()
    await session.refresh(new_task)
    logger.info(f"Prepared new task for addition: Reward={reward}, PremReq='{premium_requirement}', MaxCompletions={max_completions}")
    return new_task

async def get_all_tasks_admin(session: AsyncSession) -> List[Task]:
    """Получить ВСЕ задания (активные и неактивные) для админ-панели (СТАРАЯ ВЕРСИЯ)"""
    query = select(Task).order_by(Task.id.asc())
    result = await session.execute(query)
    return result.scalars().all()

async def set_task_active_status(session: AsyncSession, task_id: int, is_active: bool) -> bool:
    """Изменить статус активности задания"""
    query = update(Task).where(Task.id == task_id).values(is_active=is_active)
    result = await session.execute(query)
    await session.commit()
    return result.rowcount > 0 # Возвращает True, если строка была обновлена

async def delete_task_by_id(session: AsyncSession, task_id: int) -> bool:
    """Удалить задание по ID"""
    # Сначала проверим, что такое задание существует
    task = await get_task_by_id(session, task_id)
    if not task:
        return False

    # Сбросим current_task_id у пользователей, если они выполняли это задание
    reset_users_query = update(User).where(User.current_task_id == task_id).values(current_task_id=None)
    await session.execute(reset_users_query)

    # Удаляем само задание
    query = delete(Task).where(Task.id == task_id)
    result = await session.execute(query)
    await session.commit()
    return result.rowcount > 0 # Возвращает True, если строка была удалена

async def add_channel(session: AsyncSession, channel_id: int, channel_link: str, channel_name: str, channel_status: str, check_type: str, premium_requirement: str, stage: int = 1) -> Channel:
    """Добавляет новый канал для проверки подписки."""
    # --- Добавляем параметр stage ---
    new_channel = Channel(channel_id=channel_id, channel_name=channel_name, channel_link=channel_link, channel_status=channel_status, check_type=check_type, premium_requirement=premium_requirement, check_stage=stage)
    session.add(new_channel)
    await session.flush()
    await session.refresh(new_channel)
    return new_channel

async def delete_channel(session: AsyncSession, channel_db_id: int) -> bool:
    """Удалить канал по его ID в базе данных (не Telegram ID)."""
    logger.debug(f"[DB Request] Начало удаления канала с DB ID: {channel_db_id}")
    try:
        # --- Используем новую функцию delete_channel_by_db_id ---
        deleted = await delete_channel_by_db_id(session, channel_db_id) # Используем новую функцию
        if deleted:
            logger.info(f"[DB Request] Удален канал с DB ID: {channel_db_id}")
        else:
            logger.warning(f"[DB Request] Не найден канал для удаления с DB ID: {channel_db_id}")
        return deleted
    except Exception as e:
        logger.error(f"[DB Request] Ошибка при удалении канала с DB ID: {channel_db_id}: {e}", exc_info=True)
        return False

# --- Функции для Промокодов ---

async def get_promocode_by_code(session: AsyncSession, code: str) -> Optional[PromoCode]:
    """Найти активный промокод по его коду."""
    query = select(PromoCode).where(PromoCode.code == code, PromoCode.is_active == True)
    result = await session.execute(query)
    return result.scalar_one_or_none()

# --- Вспомогательная функция для склонения слова "реферал" ---
def get_referral_word(count: int) -> str:
    """
    Возвращает правильную форму слова 'реферал' для фразы 'пригласить X ...'.
    Например: 1 реферала, 2 реферала, 5 рефералов.
    """
    count = abs(count) # Работаем с положительным числом
    last_digit = count % 10
    last_two_digits = count % 100

    if 11 <= last_two_digits <= 14:
        return "рефералов"
    if last_digit == 1:
        return "реферала"
    if 2 <= last_digit <= 4:
        return "реферала" # Для 2, 3, 4 используется форма род. падежа ед. числа
    return "рефералов"
# --- Конец вспомогательной функции ---

async def activate_promocode(session: AsyncSession, user_id: int, promocode: PromoCode) -> tuple[bool, str]:
    """Активирует промокод для пользователя.
    
    Проверяет все условия:
    - Не превышен ли лимит использований промокода
    - Не использовал ли пользователь этот промокод ранее
    - Выполнены ли условия по рефералам (как за все время, так и за 24 часа) 
    
    При успешной активации:
    - Начисляет награду
    - Увеличивает счетчик использований промокода
    - Помечает код как использованный пользователем
    - Деактивирует код, если превышен лимит использований
    
    Возвращает кортеж (успех, сообщение).
    """
    # Получаем объект пользователя
    user = await get_user(session, user_id)
    if not user:
        return False, "Ошибка: Пользователь не найден."
    
    # 1. Проверка, не использовал ли пользователь этот код ранее
    if promocode in user.used_promocodes:
        logger.info(f"User {user_id} already used promocode {promocode.code} (ID: {promocode.id}). Activation failed.")
        return False, "❌ Вы уже использовали этот промокод."

    # 2. Проверка лимита использований промокода
    if promocode.max_uses is not None and promocode.uses_count >= promocode.max_uses:
        logger.warning(f"Promocode {promocode.code} (ID: {promocode.id}) reached max uses ({promocode.max_uses}). Activation failed for user {user_id}.")
        return False, "❌ Этот промокод больше не действителен."
        
    # --- Проверка условия по рефералам за все время --- 
    if promocode.required_referrals_all_time is not None:
        if user.refferals_count < promocode.required_referrals_all_time:
            remaining_referrals = promocode.required_referrals_all_time - user.refferals_count
            referral_word = get_referral_word(remaining_referrals)
            logger.info(f"User {user_id} (refs all-time: {user.refferals_count}) tried to activate promocode {promocode.code} which requires {promocode.required_referrals_all_time} refs all-time. Activation failed.")
            return False, f"❌ Для активации этого промокода нужно пригласить еще {remaining_referrals} {referral_word} (всего)"

    # --- Проверка условия по рефералам за 24 часа --- 
    if promocode.required_referrals_24h is not None:
        if user.refferals_24h_count < promocode.required_referrals_24h:
            remaining_referrals = promocode.required_referrals_24h - user.refferals_24h_count
            referral_word = get_referral_word(remaining_referrals)
            logger.info(f"User {user_id} (refs 24h: {user.refferals_24h_count}) tried to activate promocode {promocode.code} which requires {promocode.required_referrals_24h} refs in 24h. Activation failed.")
            return False, f"❌ Для активации этого промокода нужно пригласить еще {remaining_referrals} {referral_word} за 24 часа"
    # -------------------------------------------------

    # 3. Все проверки пройдены, активируем
    try:
        # Начисляем награду
        await add_balance(session, user_id, promocode.reward)
        logger.info(f"Added {promocode.reward} stars to user {user_id} for promocode {promocode.code}.")
        
        # Увеличиваем счетчик использований промокода
        promocode.uses_count += 1
        logger.info(f"Incremented uses_count for promocode {promocode.code} to {promocode.uses_count}.")
        
        # Добавляем промокод в список использованных пользователем
        user.used_promocodes.append(promocode)
        logger.info(f"Marked promocode {promocode.code} as used for user {user_id}.")
        
        # Опционально: Деактивировать код, если он достиг лимита после этого использования
        if promocode.max_uses is not None and promocode.uses_count >= promocode.max_uses:
            promocode.is_active = False
            logger.info(f"Deactivated promocode {promocode.code} as it reached max uses.")
        
        # Сохраняем все изменения
        await session.commit()
        logger.info(f"Successfully activated promocode {promocode.code} for user {user_id}.")
        return True, f"✅ Промокод успешно активирован! Вам начислено {promocode.reward}⭐️."

    except Exception as e:
        logger.error(f"Error activating promocode {promocode.code} for user {user_id}: {e}", exc_info=True)
        await session.rollback()
        return False, "❌ Произошла ошибка при активации промокода. Попробуйте позже."

async def get_all_promocodes(session: AsyncSession) -> List[PromoCode]:
    """Получить все промокоды из базы данных.
    
    Сортирует по статусу (активные сначала), затем по ID.
    """
    query = select(PromoCode).order_by(PromoCode.is_active.desc(), PromoCode.id.asc())
    result = await session.execute(query)
    return result.scalars().all()

async def get_promocode_by_id(session: AsyncSession, promo_id: int) -> Optional[PromoCode]:
    """Получить промокод по его ID.
    
    Необходим для просмотра/управления конкретным промокодом в админке.
    """
    query = select(PromoCode).where(PromoCode.id == promo_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def add_promocode(
    session: AsyncSession, 
    code: str, 
    reward: float, 
    max_uses: Optional[int], 
    required_referrals_all_time: Optional[int] = None,
    required_referrals_24h: Optional[int] = None
) -> Optional[PromoCode]:
    """Добавить новый промокод в базу данных.
    
    Возвращает созданный объект промокода или None, если код уже существует.
    Поддерживает условия по рефералам за все время или за 24 часа.
    """
    # Дополнительная проверка на уникальность перед добавлением
    existing = await session.execute(select(PromoCode).where(PromoCode.code == code))
    if existing.scalar_one_or_none():
        return None 

    new_promo = PromoCode(
        code=code,
        reward=reward,
        max_uses=max_uses,
        required_referrals_all_time=required_referrals_all_time,
        required_referrals_24h=required_referrals_24h,
        is_active=True # По умолчанию активен
    )
    session.add(new_promo)
    await session.commit()
    return new_promo

async def set_promocode_active_status(session: AsyncSession, promo_id: int) -> Optional[PromoCode]:
    """Изменить статус активности промокода (активировать/деактивировать).
    
    Возвращает обновленный объект промокода или None, если промокод не найден.
    """
    promo = await get_promocode_by_id(session, promo_id)
    if not promo:
        return None
    
    promo.is_active = not promo.is_active
    await session.commit()
    return promo

async def delete_promo_code_by_id(session: AsyncSession, promo_id: int) -> bool:
    """Удаляет промокод по ID."""
    logger.info(f"Attempting to delete promo code with ID: {promo_id}") # Лог начала операции
    try:
        # Находим промокод по ID
        promo_code = await session.get(PromoCode, promo_id)

        if promo_code:
            logger.info(f"Promo code found: {promo_code.code}. Proceeding with deletion.")
            # Опционально: Обработка связей (если необходимо)
            # logger.debug(f"Handling relationships for promo code {promo_id} before deletion...")
            # ... (код для удаления связей, если требуется) ...
            # await session.flush()

            await session.delete(promo_code)
            logger.info(f"Promo code {promo_id} marked for deletion.")

            await session.commit() # <-- Сохраняем изменения
            logger.info(f"Commit successful. Promo code with ID {promo_id} should be deleted.")
            return True
        else:
            logger.warning(f"Promo code with ID {promo_id} not found for deletion.")
            # Коммит не нужен, т.к. ничего не меняли
            return False # Промокод не найден
    except Exception as e:
        logger.error(f"Exception during deletion of promo code {promo_id}: {e}", exc_info=True) # Логируем полную ошибку
        try:
            await session.rollback() # <-- Откатываем изменения в случае ошибки
            logger.info(f"Session rolled back after error deleting promo code {promo_id}.")
        except Exception as rollback_err:
            logger.error(f"Error during rollback after failed deletion of promo code {promo_id}: {rollback_err}", exc_info=True)
        return False # Ошибка при удалении

# --- Конец функций для Промокодов ---

# --- Новые функции для получения каналов по типу --- 
async def get_start_check_channels(session: AsyncSession) -> List[Channel]:
    """Получить все каналы для проверки при старте (для админки).
    Сортирует по ID.
    """
    query = select(Channel).where(Channel.check_type == 'start').order_by(Channel.id)
    result = await session.execute(query)
    return result.scalars().all()

async def get_withdraw_check_channels(session: AsyncSession) -> List[Channel]:
    """Получить все каналы для проверки при выводе (для админки).
    Сортирует по ID.
    """
    query = select(Channel).where(Channel.check_type == 'withdraw').order_by(Channel.id)
    result = await session.execute(query)
    return result.scalars().all()

async def get_filtered_start_channels(session: AsyncSession, is_premium: bool) -> List[Channel]:
    """Получить ПУБЛИЧНЫЕ каналы для ПЕРВОГО ЭТАПА проверки при старте,
    отфильтрованные по премиум-статусу пользователя.
    Сортирует по ID.
    """
    # --- Эта функция теперь возвращает только ПЕРВЫЙ этап ---
    base_query = select(Channel).where(
        Channel.channel_status == 'Публичный', 
        Channel.check_type == 'start',
        Channel.check_stage == 1 # <-- Добавляем фильтр по этапу 1
    )
    
    if is_premium:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'premium_only')
        )
    else:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'non_premium_only')
        )
        
    query = filtered_query.order_by(Channel.id)
    result = await session.execute(query)
    return result.scalars().all()

async def get_filtered_start_channels_all(session: AsyncSession, is_premium: bool) -> List[Channel]:
    """Получить ВСЕ каналы для ПЕРВОГО ЭТАПА проверки при старте,
    отфильтрованные по премиум-статусу пользователя (для показа).
    Сортирует по ID.
    """
    # --- Эта функция теперь возвращает только ПЕРВЫЙ этап ---
    base_query = select(Channel).where(
        Channel.check_type == 'start',
        Channel.check_stage == 1 # <-- Добавляем фильтр по этапу 1
    )
    
    if is_premium:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'premium_only')
        )
    else:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'non_premium_only')
        )
        
    query = filtered_query.order_by(Channel.id)
    result = await session.execute(query)
    return result.scalars().all()
    
# --- НОВЫЕ ФУНКЦИИ ДЛЯ ВТОРОГО ЭТАПА ---

async def get_filtered_second_stage_channels(session: AsyncSession, is_premium: bool) -> List[Channel]:
    """Получить ПУБЛИЧНЫЕ каналы для ВТОРОГО ЭТАПА проверки,
    отфильтрованные по премиум-статусу пользователя.
    Сортирует по ID.
    """
    base_query = select(Channel).where(
        Channel.channel_status == 'Публичный', 
        # Убедитесь, что тип проверки не мешает (если он важен для 2 этапа)
        # Channel.check_type == 'start', # Или другой нужный тип
        Channel.check_stage == 2 # <--- КЛЮЧЕВОЙ ФИЛЬТР
    )
    
    if is_premium:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'premium_only')
        )
    else:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'non_premium_only')
        )
        
    query = filtered_query.order_by(Channel.id)
    result = await session.execute(query)
    return result.scalars().all()

async def get_filtered_second_stage_channels_all(session: AsyncSession, is_premium: bool) -> List[Channel]:
    """Получить ВСЕ каналы для ВТОРОГО ЭТАПА проверки,
    отфильтрованные по премиум-статусу пользователя (для показа).
    Сортирует по ID.
    """
    base_query = select(Channel).where(
        # Убедитесь, что тип проверки не мешает (если он важен для 2 этапа)
        # Channel.check_type == 'start', # Или другой нужный тип
        Channel.check_stage == 2 # <--- КЛЮЧЕВОЙ ФИЛЬТР
    )
    
    if is_premium:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'premium_only')
        )
    else:
        filtered_query = base_query.where(
            (Channel.premium_requirement == 'all') | (Channel.premium_requirement == 'non_premium_only')
        )
        
    query = filtered_query.order_by(Channel.id)
    result = await session.execute(query)
    return result.scalars().all()

# --- КОНЕЦ НОВЫХ ФУНКЦИЙ ДЛЯ ВТОРОГО ЭТАПА ---


# --- НУЖНО ДОБАВИТЬ ФУНКЦИИ ДЛЯ АДМИНКИ ---
async def get_all_channels_with_stage(session: AsyncSession) -> List[Channel]:
    """Возвращает все каналы с указанием их этапа."""
    # --- Сортируем по этапу, затем по ID ---
    stmt = select(Channel).order_by(Channel.check_stage, Channel.id)
    result = await session.execute(stmt)
    return result.scalars().all()

async def delete_channel_by_db_id(session: AsyncSession, db_id: int) -> bool:
    """Удаляет канал по его ID в базе данных."""
    logger.debug(f"[DB Request] Начало удаления канала с DB ID: {db_id}")
    try:
        # Проверяем, существует ли канал перед удалением
        channel = await session.get(Channel, db_id)
        if not channel:
            logger.warning(f"[DB Request] Канал с DB ID: {db_id} не найден перед удалением.")
            return False

        logger.debug(f"[DB Request] Канал найден: {channel}. Готовимся к удалению.")

        stmt = delete(Channel).where(Channel.id == db_id)
        result = await session.execute(stmt)
        logger.debug(f"[DB Request] Выполнен запрос на удаление канала с DB ID: {db_id}. Результат: {result.rowcount} строк удалено.")

        # Проверяем, действительно ли строка была удалена
        if result.rowcount > 0:
            await session.commit()  # Убедитесь, что коммит выполняется
            logger.info(f"[DB Request] Успешно удален канал с DB ID: {db_id} Строк: {result.rowcount}")
            return True
        else:
            logger.warning(f"[DB Request] Канал с DB ID: {db_id} не найден для удаления.")
            return False
    except Exception as e:
        logger.error(f"[DB Request] Ошибка при удалении канала с DB ID: {db_id}: {e}", exc_info=True)
        return False

async def get_channel_by_db_id(session: AsyncSession, db_id: int) -> Optional[Channel]:
    """Получает канал по его ID в базе данных."""
    # --- Используем session.get для получения по первичному ключу ---
    return await session.get(Channel, db_id)

async def update_channel_stage(session: AsyncSession, db_id: int, new_stage: int) -> bool:
    """Обновляет этап проверки для канала."""
    stmt = update(Channel).where(Channel.id == db_id).values(check_stage=new_stage)
    result = await session.execute(stmt)
    # await session.commit() # Коммит в хендлере
    # --- Возвращаем True если обновлена хотя бы одна строка ---
    return result.rowcount > 0

# --- УДАЛЯЕМ СТАРЫЕ ФУНКЦИИ, ЕСЛИ ОНИ БОЛЬШЕ НЕ НУЖНЫ ---
# async def get_second_stage_channels(session: AsyncSession) -> List[Channel]:
#     """Получить все каналы для второго этапа ОП."""
#     query = select(Channel).where(Channel.check_stage == 2).order_by(Channel.id)
#     result = await session.execute(query)
#     return result.scalars().all()

# async def add_second_stage_channel(session: AsyncSession, channel_id: int, channel_link: str, channel_name: str) -> Channel:
#     """Добавляет новый канал для второго этапа ОП."""
#     new_channel = Channel(channel_id=channel_id, channel_name=channel_name, channel_link=channel_link, check_stage=2)
#     session.add(new_channel)
#     await session.flush()
#     await session.refresh(new_channel)
#     return new_channel
# --- КОНЕЦ УДАЛЕНИЯ ---

async def get_all_tasks_with_completion_count(session: AsyncSession) -> List[tuple[Task, int]]:
    """Получить ВСЕ задания с количеством выполнений для админ-панели."""
    try:
        # Подзапрос для подсчета выполнений из LocalCompletedTask (только не оштрафованные)
        local_completions_subq = (
            select(
                LocalCompletedTask.task_id,
                func.count(LocalCompletedTask.id).label("completion_count")
            )
            .where(LocalCompletedTask.penalty_applied == False)
            .group_by(LocalCompletedTask.task_id)
            .subquery()
        )

        # Основной запрос к Task, присоединяем подзапрос
        query = (
            select(
                Task, 
                func.coalesce(local_completions_subq.c.completion_count, 0)
            )
            .outerjoin(local_completions_subq, Task.id == local_completions_subq.c.task_id)
            .order_by(Task.id.asc())
        )

        result = await session.execute(query)
        # Возвращаем список кортежей (Task, count)
        return result.all()
        
    except Exception as e:
        logger.error(f"Error getting tasks with completion count: {e}", exc_info=True)
        return []

async def get_all_users(session: AsyncSession) -> List[int]:
    """Возвращает список ID всех пользователей."""
    stmt = select(User.user_id)
    result = await session.execute(stmt)
    user_ids = result.scalars().all()
    logger.info(f"Retrieved {len(user_ids)} user IDs for newsletter.")
    return user_ids

async def set_user_ban_status(session: AsyncSession, user_id: int, banned: bool) -> bool:
    """Установить статус бана для пользователя."""
    query = update(User).where(User.user_id == user_id).values(banned=banned)
    result = await session.execute(query)
    await session.commit()
    return result.rowcount > 0

async def get_all_user_ids(session: AsyncSession) -> List[int]:
    """Возвращает список ID всех пользователей."""
    stmt = select(User.user_id)
    result = await session.execute(stmt)
    user_ids = result.scalars().all()
    return user_ids

# --- Функции для работы с Шаблонами Рассылок ---

async def create_broadcast_template(
    session: AsyncSession,
    name: str,
    text: str | None,
    photo_file_id: str | None,
    keyboard_json: str | None
) -> BroadcastTemplate:
    """Создает новый шаблон рассылки."""
    new_template = BroadcastTemplate(
        name=name,
        text=text,
        photo_file_id=photo_file_id,
        keyboard_json=keyboard_json
    )
    session.add(new_template)
    await session.flush()
    logger.info(f"Prepared new broadcast template object: {name}")
    return new_template

async def get_broadcast_template_by_id(session: AsyncSession, template_id: int) -> BroadcastTemplate | None:
    """Получает шаблон рассылки по его ID."""
    result = await session.execute(
        select(BroadcastTemplate).where(BroadcastTemplate.id == template_id)
    )
    return result.scalar_one_or_none()

async def get_broadcast_template_by_name(session: AsyncSession, name: str) -> BroadcastTemplate | None:
    """Получает шаблон рассылки по его уникальному имени."""
    result = await session.execute(
        select(BroadcastTemplate).where(BroadcastTemplate.name == name)
    )
    return result.scalar_one_or_none()

async def get_all_broadcast_templates(session: AsyncSession) -> list[BroadcastTemplate]:
    """Получает список всех шаблонов рассылки."""
    result = await session.execute(select(BroadcastTemplate).order_by(BroadcastTemplate.name))
    return list(result.scalars().all())

async def update_broadcast_template(
    session: AsyncSession,
    template_id: int,
    name: str | None = None,
    text: str | None = None,
    photo_file_id: str | None = None,
    keyboard: InlineKeyboardMarkup | None = None,
    set_photo_null: bool = False, # Флаг для удаления фото
    set_keyboard_null: bool = False # Флаг для удаления клавиатуры
) -> BroadcastTemplate | None:
    """Обновляет существующий шаблон рассылки."""
    template = await get_broadcast_template_by_id(session, template_id)
    if not template:
        return None

    update_values = {}
    if name is not None:
        update_values['name'] = name
    if text is not None:
        update_values['text'] = text
    if photo_file_id is not None:
        update_values['photo_file_id'] = photo_file_id
    elif set_photo_null: # Если передан флаг удаления фото
        update_values['photo_file_id'] = None

    # Обновляем клавиатуру через метод модели перед commit
    keyboard_changed = False
    if keyboard is not None:
        template.set_keyboard(keyboard)
        update_values['keyboard_json'] = template.keyboard_json # Сохраняем результат set_keyboard
        keyboard_changed = True
    elif set_keyboard_null: # Если передан флаг удаления клавиатуры
        template.set_keyboard(None)
        update_values['keyboard_json'] = None
        keyboard_changed = True

    if update_values:
        await session.execute(
            update(BroadcastTemplate)
            .where(BroadcastTemplate.id == template_id)
            .values(**update_values)
        )
        await session.commit()
        await session.refresh(template) # Обновляем объект после commit
        logger.info(f"Updated broadcast template ID: {template_id}. Changes: {list(update_values.keys())}")
        return template
    elif keyboard_changed: # Если изменилась только клавиатура (через set_keyboard)
        await session.commit() # Все равно нужно сохранить изменения keyboard_json
        await session.refresh(template)
        logger.info(f"Updated broadcast template ID: {template_id}. Keyboard updated/removed.")
        return template
    else:
        logger.info(f"No changes detected for broadcast template ID: {template_id}")
        return template # Возвращаем без изменений, если нечего было обновлять

async def delete_broadcast_template(session: AsyncSession, template_id: int) -> bool:
    """Удаляет шаблон рассылки по его ID."""
    result = await session.execute(
        delete(BroadcastTemplate).where(BroadcastTemplate.id == template_id)
    )
    await session.commit()
    deleted_count = result.rowcount
    if deleted_count > 0:
        logger.info(f"Deleted broadcast template ID: {template_id}")
        return True
    else:
        logger.warning(f"Attempted to delete non-existent broadcast template ID: {template_id}")
        return False

# --- Конец функций для Шаблонов Рассылок ---

# --- НОВАЯ ФУНКЦИЯ для списания баланса ---
async def subtract_balance(session: AsyncSession, user_id: int, amount: float) -> tuple[bool, float | None]:
    """
    Списывает указанную сумму с баланса пользователя.
    Проверяет, чтобы баланс не стал отрицательным.
    Возвращает кортеж: (успех: bool, новый_баланс: float | None).
    НЕ ДЕЛАЕТ COMMIT! Commit должен быть вызван в хендлере.
    """
    if amount <= 0:
        logger.warning(f"Attempted to subtract non-positive amount {amount} from user {user_id}")
        return False, None # Нельзя списать неположительную сумму

    try:
        # Получаем пользователя с блокировкой для обновления (пессимистичная блокировка)
        # Это помогает предотвратить race conditions, если несколько запросов пытаются изменить баланс одновременно
        result = await session.execute(
            select(User).where(User.user_id == user_id).with_for_update()
        )
        user = result.scalar_one_or_none()

        if not user:
            logger.warning(f"User {user_id} not found for balance subtraction.")
            return False, None

        if user.balance < amount:
            logger.warning(f"Insufficient balance for user {user_id}. Current: {user.balance}, Tried to subtract: {amount}")
            return False, user.balance # Возвращаем текущий баланс

        # Выполняем списание
        user.balance -= amount
        new_balance = user.balance

        # Добавляем пользователя в сессию для сохранения изменений (если он еще не там)
        session.add(user)
        await session.flush() # Применяем изменения к сессии, но не коммитим

        logger.info(f"Prepared subtraction of {amount} from user {user_id}. New balance will be {new_balance}")
        return True, new_balance

    except Exception as e:
        logger.error(f"Database error during balance subtraction for user {user_id}: {e}", exc_info=True)
        # Откат будет сделан в хендлере, здесь просто возвращаем ошибку
        return False, None

async def get_channels_to_subscribe(session: AsyncSession, stage: int) -> List[Channel]:
    """Возвращает список каналов для подписки для указанного этапа."""
    stmt = select(Channel).where(Channel.check_stage == stage).order_by(Channel.id) # Фильтруем по stage
    result = await session.execute(stmt)
    return result.scalars().all()

async def get_promo_code_name(session: AsyncSession) -> str:
    query = select(Settings.promo_code_name)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def set_promo_code_name(session: AsyncSession, promo_code_name: str) -> bool:
    query = update(Settings).where(Settings.id == 1).values(promo_code_name=promo_code_name)
    result = await session.execute(query)
    await session.commit()
    return result.rowcount > 0

async def delete_users_by_ids(session: AsyncSession, user_ids: List[int]) -> int:
    """Удалить пользователей по списку ID и вернуть количество удаленных пользователей"""
    if not user_ids:
        logger.debug("delete_users_by_ids: Empty user_ids list provided")
        return 0
    
    # Фильтруем валидные ID
    valid_user_ids = [uid for uid in user_ids if isinstance(uid, int) and uid > 0]
    if len(valid_user_ids) != len(user_ids):
        logger.warning(f"delete_users_by_ids: Filtered {len(user_ids) - len(valid_user_ids)} invalid user IDs")
    
    if not valid_user_ids:
        logger.warning("delete_users_by_ids: No valid user IDs to delete")
        return 0
    
    logger.info(f"delete_users_by_ids: Starting deletion of {len(valid_user_ids)} users")
    
    try:
        # Проверяем, сколько пользователей действительно существует
        existing_count_result = await session.execute(
            select(func.count(User.id)).where(User.user_id.in_(valid_user_ids))
        )
        existing_count = existing_count_result.scalar_one()
        logger.info(f"delete_users_by_ids: Found {existing_count} existing users out of {len(valid_user_ids)} requested")
        
        if existing_count == 0:
            logger.info("delete_users_by_ids: No users found to delete")
            return 0
        
        # Получаем список ID пользователей, которые реально существуют в БД
        existing_users_result = await session.execute(
            select(User.user_id).where(User.user_id.in_(valid_user_ids))
        )
        existing_user_ids = [row[0] for row in existing_users_result.fetchall()]
        logger.info(f"delete_users_by_ids: Users to delete: {existing_user_ids[:10]}..." if len(existing_user_ids) > 10 else f"delete_users_by_ids: Users to delete: {existing_user_ids}")
        
        # Удаляем связанные записи из других таблиц в правильном порядке
        logger.debug("delete_users_by_ids: Deleting daily bonuses...")
        daily_bonus_result = await session.execute(
            delete(DailyBonus).where(DailyBonus.user_id.in_(existing_user_ids))
        )
        logger.debug(f"delete_users_by_ids: Deleted {daily_bonus_result.rowcount} daily bonuses")
        
        logger.debug("delete_users_by_ids: Deleting withdraws...")
        withdraws_result = await session.execute(
            delete(Withdraws).where(Withdraws.user_id.in_(existing_user_ids))
        )
        logger.debug(f"delete_users_by_ids: Deleted {withdraws_result.rowcount} withdraws")
        
        logger.debug("delete_users_by_ids: Deleting user completed tasks...")
        tasks_result = await session.execute(
            delete(user_completed_tasks_table).where(user_completed_tasks_table.c.user_id.in_(existing_user_ids))
        )
        logger.debug(f"delete_users_by_ids: Deleted {tasks_result.rowcount} completed tasks")
        
        logger.debug("delete_users_by_ids: Deleting user used promocodes...")
        promo_result = await session.execute(
            delete(user_used_promocodes_table).where(user_used_promocodes_table.c.user_id.in_(existing_user_ids))
        )
        logger.debug(f"delete_users_by_ids: Deleted {promo_result.rowcount} used promocodes")
        
        # Добавляем удаление связанных записей из других таблиц
        logger.debug("delete_users_by_ids: Deleting daily tasks...")
        daily_tasks_result = await session.execute(
            delete(DailyTask).where(DailyTask.user_id.in_(existing_user_ids))
        )
        logger.debug(f"delete_users_by_ids: Deleted {daily_tasks_result.rowcount} daily tasks")
        
        # Удаляем самих пользователей
        logger.debug("delete_users_by_ids: Deleting users...")
        user_result = await session.execute(
            delete(User).where(User.user_id.in_(existing_user_ids))
        )
        
        deleted_count = user_result.rowcount
        logger.info(f"delete_users_by_ids: Successfully deleted {deleted_count} users and their related records")
        
        # Проверяем соответствие ожидаемому количеству
        if deleted_count != existing_count:
            logger.warning(f"delete_users_by_ids: Expected to delete {existing_count} users, but deleted {deleted_count}")
            
            # Дополнительная диагностика - проверяем, остались ли пользователи
            remaining_users_result = await session.execute(
                select(User.user_id).where(User.user_id.in_(existing_user_ids))
            )
            remaining_users = [row[0] for row in remaining_users_result.fetchall()]
            if remaining_users:
                logger.error(f"delete_users_by_ids: Users still remaining in DB: {remaining_users[:5]}...")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"delete_users_by_ids: Error deleting users: {e}", exc_info=True)
        # НЕ делаем rollback здесь - это должно быть сделано на уровне вызывающего кода
        return 0

async def delete_inactive_users_from_list(session: AsyncSession, failed_user_ids: List[int]) -> tuple[int, int]:
    """Удалить пользователей из списка неудачных отправок и вернуть (количество удаленных, общее количество)"""
    total_count = len(failed_user_ids)
    if total_count == 0:
        return 0, 0
    
    deleted_count = await delete_users_by_ids(session, failed_user_ids)
    return deleted_count, total_count

async def get_users_count_before_cleanup(session: AsyncSession) -> int:
    """Получить общее количество пользователей перед очисткой"""
    result = await session.execute(select(func.count(User.id)))
    return result.scalar_one()

# --- Функции для работы с ежедневными заданиями ---

async def can_complete_daily_task(session: AsyncSession, user_id: int, task_type: str) -> bool:
    """Проверяет, может ли пользователь выполнить ежедневное задание (прошло ли 24 часа)"""
    from datetime import datetime, timedelta
    
    yesterday = datetime.utcnow() - timedelta(hours=24)
    
    # Ищем последнее выполнение этого типа задания пользователем
    last_completion = await session.execute(
        select(DailyTask).where(
            DailyTask.user_id == user_id,
            DailyTask.task_type == task_type,
            DailyTask.completed_at > yesterday
        ).order_by(DailyTask.completed_at.desc()).limit(1)
    )
    
    result = last_completion.scalar_one_or_none()
    return result is None  # Можно выполнить, если нет записи за последние 24 часа


async def complete_daily_task(session: AsyncSession, user_id: int, task_type: str, reward: float) -> bool:
    """Отмечает ежедневное задание как выполненное и начисляет награду"""
    from datetime import datetime
    
    # Проверяем, можно ли выполнить задание
    if not await can_complete_daily_task(session, user_id, task_type):
        return False
    
    # Создаем запись о выполнении
    daily_task = DailyTask(
        user_id=user_id,
        task_type=task_type,
        completed_at=datetime.utcnow(),
        reward=reward
    )
    session.add(daily_task)
    
    # Начисляем награду
    await add_balance(session, user_id, reward)
    
    return True


async def get_daily_task_last_completion(session: AsyncSession, user_id: int, task_type: str) -> Optional[datetime]:
    """Возвращает время последнего выполнения ежедневного задания"""
    from datetime import datetime
    
    last_completion = await session.execute(
        select(DailyTask.completed_at).where(
            DailyTask.user_id == user_id,
            DailyTask.task_type == task_type
        ).order_by(DailyTask.completed_at.desc()).limit(1)
    )
    
    result = last_completion.scalar_one_or_none()
    return result


async def get_daily_task_stats(session: AsyncSession, task_type: str) -> tuple[int, float]:
    """Возвращает статистику по ежедневному заданию: количество выполнений и общая сумма наград"""
    from datetime import datetime, timedelta
    
    today = datetime.utcnow() - timedelta(hours=24)
    
    stats = await session.execute(
        select(
            func.count(DailyTask.id),
            func.coalesce(func.sum(DailyTask.reward), 0.0)
        ).where(
            DailyTask.task_type == task_type,
            DailyTask.completed_at > today
        )
    )
    
    result = stats.first()
    return result[0], result[1] if result else (0, 0.0)


async def check_referral_link_in_bio(bot, user_id: int, referral_link: str) -> bool:
    """Проверяет наличие реферальной ссылки в био пользователя"""
    try:
        # Получаем информацию о пользователе
        user_info = await bot.get_chat(user_id)
        
        # Проверяем есть ли био и содержит ли оно реферральную ссылку
        if user_info.bio and referral_link in user_info.bio:
            logger.info(f"Referral link found in bio for user {user_id}")
            return True
        else:
            logger.info(f"Referral link not found in bio for user {user_id}. Bio: {user_info.bio}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking bio for user {user_id}: {e}")
        return False

# SubGram Webhook functions
async def save_subgram_webhook(
    session: AsyncSession,
    webhook_id: int,
    link: str,
    user_id: int,
    bot_id: int,
    status: str,
    subscribe_date: str
) -> bool:
    """
    Сохраняет webhook SubGram в базу данных
    """
    try:
        from datetime import datetime
        from bot.database.models import SubGramWebhook
        
        logger.info(f"Попытка сохранить webhook {webhook_id} для пользователя {user_id}")
        
        # Проверяем, не существует ли уже такой webhook
        existing = await session.execute(
            select(SubGramWebhook).where(SubGramWebhook.webhook_id == webhook_id)
        )
        if existing.scalar_one_or_none():
            logger.warning(f"Webhook {webhook_id} уже существует в базе")
            return False
        
        # Парсим дату - пробуем разные форматы
        subscribe_date_obj = None
        date_formats = [
            "%Y-%m-%d",
            "%d.%m.%Y", 
            "%d/%m/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S"
        ]
        
        for fmt in date_formats:
            try:
                if ' ' in subscribe_date and ' ' not in fmt:
                    # Если в дате есть время, но формат без времени, берем только дату
                    date_part = subscribe_date.split(' ')[0]
                    subscribe_date_obj = datetime.strptime(date_part, fmt).date()
                else:
                    subscribe_date_obj = datetime.strptime(subscribe_date, fmt).date()
                break
            except ValueError:
                continue
        
        if subscribe_date_obj is None:
            logger.error(f"Не удалось распарсить дату: {subscribe_date}")
            # Используем текущую дату как fallback
            subscribe_date_obj = datetime.now().date()
        
        # Создаем новый webhook
        webhook = SubGramWebhook(
            webhook_id=webhook_id,
            link=link,
            user_id=user_id,
            bot_id=bot_id,
            status=status,
            subscribe_date=subscribe_date_obj,
            processed=False
        )
        
        session.add(webhook)
        await session.commit()
        logger.info(f"Сохранен webhook {webhook_id} для пользователя {user_id} со статусом {status}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении webhook: {e}", exc_info=True)
        await session.rollback()
        return False

async def get_unprocessed_subgram_webhooks(session: AsyncSession, limit: int = 100) -> List:
    """
    Получает необработанные webhook'и SubGram
    """
    try:
        from bot.database.models import SubGramWebhook
        
        result = await session.execute(
            select(SubGramWebhook)
            .where(SubGramWebhook.processed == False)
            .order_by(SubGramWebhook.webhook_id.asc())
            .limit(limit)
        )
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Ошибка при получении необработанных webhook'ов: {e}")
        return []

async def mark_webhook_as_processed(session: AsyncSession, webhook_id: int) -> bool:
    """
    Отмечает webhook как обработанный
    """
    try:
        from bot.database.models import SubGramWebhook
        
        result = await session.execute(
            select(SubGramWebhook).where(SubGramWebhook.webhook_id == webhook_id)
        )
        webhook = result.scalar_one_or_none()
        
        if webhook:
            webhook.processed = True
            await session.commit()
            return True
        return False
        
    except Exception as e:
        logger.error(f"Ошибка при отметке webhook {webhook_id} как обработанного: {e}")
        await session.rollback()
        return False

async def get_subgram_webhooks_stats(session: AsyncSession) -> dict:
    """
    Получает статистику по всем SubGram вебхукам
    """
    # Общее количество записей
    total_query = select(func.count(SubGramWebhook.id))
    total_result = await session.execute(total_query)
    total_count = total_result.scalar()

    # Количество по статусам
    status_query = select(
        SubGramWebhook.status,
        func.count(SubGramWebhook.id)
    ).group_by(SubGramWebhook.status)
    
    status_result = await session.execute(status_query)
    status_stats = dict(status_result.all())

    # Количество обработанных
    processed_query = select(func.count(SubGramWebhook.id)).where(SubGramWebhook.processed == True)
    processed_result = await session.execute(processed_query)
    processed_count = processed_result.scalar()

    # Количество за последние 24 часа
    yesterday = datetime.utcnow() - timedelta(days=1)
    recent_query = select(func.count(SubGramWebhook.id)).where(SubGramWebhook.received_at >= yesterday)
    recent_result = await session.execute(recent_query)
    recent_count = recent_result.scalar()

    return {
        'total': total_count or 0,
        'status_stats': status_stats,
        'processed': processed_count or 0,
        'unprocessed': (total_count or 0) - (processed_count or 0),
        'last_24h': recent_count or 0
    }


# ================ НОВЫЕ ФУНКЦИИ ДЛЯ SUBGRAM ЗАДАНИЙ ================

async def save_subgram_completed_task(
    session: AsyncSession,
    user_id: int,
    subgram_task_id: int,
    channel_link: str,
    channel_name: str,
    reward_given: float
) -> bool:
    """
    Сохраняет информацию о выполненном задании SubGram
    """
    try:
        from .models import SubGramCompletedTask
        
        # Проверяем, не существует ли уже такое задание для предотвращения дублирования
        existing_task = await session.execute(
            select(SubGramCompletedTask).where(
                SubGramCompletedTask.user_id == user_id,
                SubGramCompletedTask.subgram_task_id == subgram_task_id,
                SubGramCompletedTask.channel_link == channel_link
            )
        )
        
        if existing_task.scalar_one_or_none():
            logger.warning(f"⚠️ SubGram task already exists: user_id={user_id}, task_id={subgram_task_id}, channel={channel_link}")
            return True  # Считаем успешным, так как задание уже сохранено
        
        completed_task = SubGramCompletedTask(
            user_id=user_id,
            subgram_task_id=subgram_task_id,
            channel_link=channel_link,
            channel_name=channel_name,
            reward_given=reward_given
        )
        
        session.add(completed_task)
        # Не делаем commit здесь - это будет сделано в вызывающей функции
        
        logger.info(f"📝 Added SubGram completed task to session: user_id={user_id}, task_id={subgram_task_id}, reward={reward_given}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving SubGram completed task: {e}", exc_info=True)
        return False


async def get_user_subgram_tasks_for_unsubscribe(
    session: AsyncSession,
    user_id: int,
    channel_link: str
) -> List:
    """
    Получает выполненные задания SubGram пользователя для определенного канала,
    по которым еще не был применен штраф
    """
    try:
        from .models import SubGramCompletedTask
        
        logger.info(f"🔍 Ищем задания SubGram для пользователя {user_id}, канал: {channel_link}")
        
        query = select(SubGramCompletedTask).where(
            SubGramCompletedTask.user_id == user_id,
            SubGramCompletedTask.channel_link == channel_link,
            SubGramCompletedTask.penalty_applied == False
        )
        
        result = await session.execute(query)
        tasks = result.scalars().all()
        
        logger.info(f"📋 Найдено заданий: {len(tasks)}")
        
        # Логируем все задания пользователя для отладки
        all_tasks_query = select(SubGramCompletedTask).where(
            SubGramCompletedTask.user_id == user_id
        )
        all_tasks_result = await session.execute(all_tasks_query)
        all_tasks = all_tasks_result.scalars().all()
        
        logger.info(f"🗂️ Всего заданий пользователя {user_id}: {len(all_tasks)}")
        for task in all_tasks:
            logger.info(f"📄 Задание ID={task.id}, канал='{task.channel_link}', штраф_применен={task.penalty_applied}")
        
        return tasks
        
    except Exception as e:
        logger.error(f"❌ Ошибка при поиске заданий SubGram: {e}")
        return []


async def apply_unsubscribe_penalty(
    session: AsyncSession,
    completed_task_id: int,
    penalty_amount: float,
    webhook_id: int
) -> bool:
    """
    Применяет штраф за отписку от канала SubGram
    """
    try:
        from .models import SubGramCompletedTask
        
        # Обновляем запись о выполненном задании
        query = update(SubGramCompletedTask).where(
            SubGramCompletedTask.id == completed_task_id
        ).values(
            penalty_applied=True,
            penalty_amount=penalty_amount,
            penalty_applied_at=datetime.utcnow(),
            webhook_id=webhook_id
        )
        
        result = await session.execute(query)
        
        if result.rowcount > 0:
            await session.commit()
            logger.info(f"Applied unsubscribe penalty: task_id={completed_task_id}, penalty={penalty_amount}")
            return True
        else:
            logger.warning(f"No SubGram completed task found with id={completed_task_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error applying unsubscribe penalty: {e}")
        await session.rollback()
        return False


async def get_user_subgram_penalties_stats(session: AsyncSession, user_id: int) -> dict:
    """Получить статистику штрафов пользователя по SubGram заданиям"""
    try:
        # Получаем все выполненные задания пользователя
        result = await session.execute(
            select(SubGramCompletedTask)
            .filter(SubGramCompletedTask.user_id == user_id)
        )
        completed_tasks = result.scalars().all()
        
        # Считаем статистику
        total_completed = len(completed_tasks)
        penalties_count = len([t for t in completed_tasks if t.penalty_applied])
        total_rewards = sum(t.reward_given for t in completed_tasks)
        total_penalty_amount = sum(t.penalty_amount for t in completed_tasks if t.penalty_amount)
        net_earnings = total_rewards - total_penalty_amount
        
        return {
            'total_completed': total_completed,
            'penalties_count': penalties_count,
            'total_rewards': total_rewards,
            'total_penalty_amount': total_penalty_amount,
            'net_earnings': net_earnings
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики штрафов пользователя {user_id}: {e}")
        return {
            'total_completed': 0,
            'penalties_count': 0,
            'total_rewards': 0.0,
            'total_penalty_amount': 0.0,
            'net_earnings': 0.0
        }


async def get_subgram_tasks_statistics(session: AsyncSession) -> dict:
    """
    Получает общую статистику по заданиям SubGram
    """
    try:
        from .models import SubGramCompletedTask
        
        # Общее количество выполненных заданий
        total_query = select(func.count(SubGramCompletedTask.id))
        total_result = await session.execute(total_query)
        total_completed = total_result.scalar() or 0
        
        # Количество уникальных пользователей
        users_query = select(func.count(func.distinct(SubGramCompletedTask.user_id)))
        users_result = await session.execute(users_query)
        unique_users = users_result.scalar() or 0
        
        # Статистика по штрафам
        penalties_query = select(
            func.count(SubGramCompletedTask.id),
            func.sum(SubGramCompletedTask.penalty_amount),
            func.sum(SubGramCompletedTask.reward_given)
        ).where(SubGramCompletedTask.penalty_applied == True)
        penalties_result = await session.execute(penalties_query)
        penalties_count, total_penalties, total_rewards_with_penalties = penalties_result.first()
        
        # Общая сумма выданных наград
        total_rewards_query = select(func.sum(SubGramCompletedTask.reward_given))
        total_rewards_result = await session.execute(total_rewards_query)
        total_rewards = total_rewards_result.scalar() or 0
        
        return {
            'total_completed_tasks': total_completed,
            'unique_users': unique_users,
            'penalties_applied': penalties_count or 0,
            'total_penalty_amount': float(total_penalties or 0),
            'total_rewards_given': float(total_rewards),
            'penalty_rate': (penalties_count or 0) / total_completed * 100 if total_completed > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting SubGram tasks statistics: {e}")
        return {
            'total_completed_tasks': 0,
            'unique_users': 0,
            'penalties_applied': 0,
            'total_penalty_amount': 0.0,
            'total_rewards_given': 0.0,
            'penalty_rate': 0.0
        }


async def update_user_bio_reward_date(session: AsyncSession, user_id: int, reward_date: datetime) -> bool:
    """
    Обновляет время последней награды за реферальную ссылку в био
    """
    try:
        from .models import User
        
        query = update(User).where(
            User.user_id == user_id
        ).values(
            last_bio_reward_date=reward_date
        )
        
        result = await session.execute(query)
        
        if result.rowcount > 0:
            await session.commit()
            logger.info(f"Updated bio reward date for user {user_id}")
            return True
        else:
            logger.warning(f"No user found with id={user_id} to update bio reward date")
            return False
            
    except Exception as e:
        logger.error(f"Error updating bio reward date for user {user_id}: {e}")
        await session.rollback()
        return False

async def get_comprehensive_daily_tasks_stats(session: AsyncSession) -> dict:
    """Получает статистику по ежедневным заданиям на основе last_bio_reward_date"""
    try:
        # Общая статистика пользователей с наградами за био
        total_completions_result = await session.execute(
            select(func.count(User.id)).where(User.last_bio_reward_date.isnot(None))
        )
        total_completions = total_completions_result.scalar() or 0
        
        # Считаем общую сумму наград (каждая награда = 0.2 звезды)
        reward_per_task = 0.5
        total_rewards = total_completions * reward_per_task
        
        # Количество уникальных пользователей = total_completions (каждый пользователь уникален)
        unique_users = total_completions
        
        # Статистика за последние 24 часа
        yesterday = datetime.now() - timedelta(hours=24)
        recent_completions_result = await session.execute(
            select(func.count(User.id)).where(
                User.last_bio_reward_date.isnot(None),
                User.last_bio_reward_date > yesterday
            )
        )
        recent_completions = recent_completions_result.scalar() or 0
        
        # Статистика за последние 7 дней
        seven_days_ago = datetime.now() - timedelta(days=7)
        weekly_completions_result = await session.execute(
            select(func.count(User.id)).where(
                User.last_bio_reward_date.isnot(None),
                User.last_bio_reward_date > seven_days_ago
            )
        )
        weekly_completions = weekly_completions_result.scalar() or 0
        
        # Статистика за сегодня
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_completions_result = await session.execute(
            select(func.count(User.id)).where(
                User.last_bio_reward_date.isnot(None),
                User.last_bio_reward_date >= today_start
            )
        )
        today_completions = today_completions_result.scalar() or 0
        
        # Топ пользователей (последние получившие награду)
        top_users_result = await session.execute(
            select(
                User.user_id,
                User.username,
                User.last_bio_reward_date
            ).where(
                User.last_bio_reward_date.isnot(None)
            ).order_by(
                User.last_bio_reward_date.desc()
            ).limit(10)
        )
        top_users = top_users_result.fetchall()
        
        # Средняя награда за задание
        avg_reward = reward_per_task
        
        return {
            'total_completions': total_completions,
            'total_rewards': round(total_rewards, 2),
            'unique_users': unique_users,
            'avg_reward': round(avg_reward, 2),
            'task_types_stats': [
                {
                    'task_type': 'bio_link',
                    'completions': total_completions,
                    'total_reward': round(total_rewards, 2),
                    'unique_users': unique_users
                }
            ],
            'weekly_stats': [
                {
                    'date': 'За 7 дней',
                    'completions': weekly_completions,
                    'rewards': round(weekly_completions * reward_per_task, 2)
                }
            ],
            'today_stats': [
                {
                    'task_type': 'bio_link',
                    'completions': today_completions,
                    'rewards': round(today_completions * reward_per_task, 2)
                }
            ],
            'top_users': [
                {
                    'user_id': row.user_id,
                    'username': row.username or 'Без username',
                    'completions': 1,  # Каждый пользователь выполнил минимум 1 задание
                    'total_earned': round(reward_per_task, 2),
                    'last_reward': row.last_bio_reward_date.strftime('%d.%m.%Y %H:%M') if row.last_bio_reward_date else 'Никогда'
                }
                for row in top_users
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting comprehensive daily tasks stats: {e}")
        return {
            'total_completions': 0,
            'total_rewards': 0.0,
            'unique_users': 0,
            'avg_reward': 0.4,
            'task_types_stats': [],
            'weekly_stats': [],
            'today_stats': [],
            'top_users': []
        }


async def get_daily_tasks_monthly_stats(session: AsyncSession, months: int = 3) -> dict:
    """Получает статистику по ежедневным заданиям за последние N месяцев"""
    try:
        months_ago = datetime.now() - timedelta(days=30 * months)
        
        monthly_stats_result = await session.execute(
            select(
                func.date_trunc('month', DailyTask.completed_at).label('month'),
                func.count(DailyTask.id).label('completions'),
                func.sum(DailyTask.reward).label('rewards'),
                func.count(func.distinct(DailyTask.user_id)).label('unique_users')
            ).where(
                DailyTask.completed_at >= months_ago
            ).group_by(
                func.date_trunc('month', DailyTask.completed_at)
            ).order_by(
                func.date_trunc('month', DailyTask.completed_at).desc()
            )
        )
        monthly_stats = monthly_stats_result.fetchall()
        
        return {
            'monthly_stats': [
                {
                    'month': row.month.strftime('%m.%Y'),
                    'completions': row.completions,
                    'rewards': round(row.rewards, 2),
                    'unique_users': row.unique_users
                }
                for row in monthly_stats
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting monthly daily tasks stats: {e}")
        return {'monthly_stats': []}


async def get_user_daily_tasks_history(session: AsyncSession, user_id: int, limit: int = 50) -> dict:
    """Получает историю выполнения ежедневных заданий конкретного пользователя на основе last_bio_reward_date"""
    try:
        # Получаем пользователя
        user_result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = user_result.scalar_one_or_none()
        
        if not user:
            return {
                'user_id': user_id,
                'total_completions': 0,
                'total_earned': 0.0,
                'history': []
            }
        
        # Если у пользователя есть last_bio_reward_date, значит он выполнил задание
        if user.last_bio_reward_date:
            reward_per_task = 0.4
            total_completions = 1
            total_earned = reward_per_task
            
            history = [
                {
                    'task_type': 'bio_link',
                    'completed_at': user.last_bio_reward_date.strftime('%d.%m.%Y %H:%M'),
                    'reward': reward_per_task
                }
            ]
        else:
            total_completions = 0
            total_earned = 0.0
            history = []
        
        return {
            'user_id': user_id,
            'total_completions': total_completions,
            'total_earned': round(total_earned, 2),
            'history': history
        }
        
    except Exception as e:
        logger.error(f"Error getting user daily tasks history for user {user_id}: {e}")
        return {
            'user_id': user_id,
            'total_completions': 0,
            'total_earned': 0.0,
            'history': []
        }

async def get_user_by_id(session: AsyncSession, user_id: int) -> Optional[User]:
    """Получить пользователя по ID."""
    return await get_user(session, user_id)


# === Функции для работы с локальными выполненными заданиями ===

async def save_local_completed_task(
    session: AsyncSession,
    user_id: int,
    task_id: int,
    channel_id: Optional[int],
    reward_given: float
) -> bool:
    """Сохранить информацию о выполненном локальном задании."""
    try:
        from bot.database.models import LocalCompletedTask
        
        # Создаем новую запись (убираем проверку на существующее задание)
        # Это позволяет создавать новые записи при повторном выполнении заданий
        completed_task = LocalCompletedTask(
            user_id=user_id,
            task_id=task_id,
            channel_id=channel_id,
            reward_given=reward_given
        )
        
        session.add(completed_task)
        await session.commit()
        logger.info(f"Saved local completed task: user {user_id}, task {task_id}, reward {reward_given}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving local completed task for user {user_id}, task {task_id}: {e}", exc_info=True)
        await session.rollback()
        return False


async def get_user_local_tasks_for_unsubscribe(
    session: AsyncSession,
    user_id: int,
    channel_id: int
) -> List:
    """Получить локальные задания пользователя, которые требуют проверки подписки на указанный канал."""
    try:
        from bot.database.models import LocalCompletedTask
        
        query = select(LocalCompletedTask).where(
            LocalCompletedTask.user_id == user_id,
            LocalCompletedTask.channel_id == channel_id,
            LocalCompletedTask.penalty_applied == False
        ).order_by(LocalCompletedTask.completed_at.desc())
        
        result = await session.execute(query)
        return result.scalars().all()
        
    except Exception as e:
        logger.error(f"Error getting local tasks for unsubscribe for user {user_id}, channel {channel_id}: {e}", exc_info=True)
        return []


async def apply_local_unsubscribe_penalty(
    session: AsyncSession,
    completed_task_id: int,
    penalty_amount: Optional[float] = None
) -> bool:
    """Применить штраф за отписку от канала для локального задания."""
    try:
        from bot.database.models import LocalCompletedTask
        from datetime import datetime
        
        # Найти выполненное задание
        completed_task = await session.get(LocalCompletedTask, completed_task_id)
        if not completed_task:
            logger.error(f"Local completed task {completed_task_id} not found")
            return False
        
        if completed_task.penalty_applied:
            logger.warning(f"Penalty already applied for local completed task {completed_task_id}")
            return False
        
        # Если штраф не указан, используем размер награды
        if penalty_amount is None:
            penalty_amount = completed_task.reward_given
        
        # Снять звезды с пользователя
        user = await get_user(session, completed_task.user_id)
        if not user:
            logger.error(f"User {completed_task.user_id} not found for penalty")
            return False
        
        # Проверяем, хватает ли баланса
        if user.balance < penalty_amount:
            penalty_amount = user.balance  # Снимаем сколько есть
        
        user.balance -= penalty_amount
        
        # Отметить штраф как примененный
        completed_task.penalty_applied = True
        completed_task.penalty_amount = penalty_amount
        completed_task.penalty_applied_at = datetime.utcnow()
        
        # Удаляем запись из user_completed_tasks чтобы пользователь мог снова выполнить это задание
        try:
            delete_completed_stmt = delete(user_completed_tasks_table).where(
                and_(
                    user_completed_tasks_table.c.user_id == completed_task.user_id,
                    user_completed_tasks_table.c.task_id == completed_task.task_id
                )
            )
            await session.execute(delete_completed_stmt)
            logger.info(f"Removed user {completed_task.user_id} task {completed_task.task_id} from user_completed_tasks due to penalty")
        except Exception as delete_error:
            logger.error(f"Error removing from user_completed_tasks: {delete_error}")
            # Продолжаем выполнение, даже если удаление не удалось
        
        await session.commit()
        logger.info(f"Applied local unsubscribe penalty: user {completed_task.user_id}, task {completed_task.task_id}, penalty {penalty_amount}")
        return True
        
    except Exception as e:
        logger.error(f"Error applying local penalty for completed task {completed_task_id}: {e}", exc_info=True)
        await session.rollback()
        return False


async def get_user_local_penalties_stats(session: AsyncSession, user_id: int) -> dict:
    """Получить статистику штрафов пользователя по локальным заданиям."""
    try:
        from bot.database.models import LocalCompletedTask
        
        # Общее количество штрафов
        total_penalties_query = select(func.count(LocalCompletedTask.id)).where(
            LocalCompletedTask.user_id == user_id,
            LocalCompletedTask.penalty_applied == True
        )
        total_penalties_result = await session.execute(total_penalties_query)
        total_penalties = total_penalties_result.scalar_one_or_none() or 0
        
        # Общая сумма штрафов
        total_penalty_amount_query = select(func.sum(LocalCompletedTask.penalty_amount)).where(
            LocalCompletedTask.user_id == user_id,
            LocalCompletedTask.penalty_applied == True
        )
        total_penalty_amount_result = await session.execute(total_penalty_amount_query)
        total_penalty_amount = total_penalty_amount_result.scalar_one_or_none() or 0.0
        
        # Последние штрафы
        recent_penalties_query = select(LocalCompletedTask).where(
            LocalCompletedTask.user_id == user_id,
            LocalCompletedTask.penalty_applied == True
        ).order_by(LocalCompletedTask.penalty_applied_at.desc()).limit(10)
        
        recent_penalties_result = await session.execute(recent_penalties_query)
        recent_penalties = recent_penalties_result.scalars().all()
        
        return {
            'total_penalties': total_penalties,
            'total_penalty_amount': float(total_penalty_amount),
            'recent_penalties': [
                {
                    'task_id': penalty.task_id,
                    'penalty_amount': penalty.penalty_amount,
                    'penalty_applied_at': penalty.penalty_applied_at,
                    'reward_given': penalty.reward_given
                }
                for penalty in recent_penalties
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting local penalties stats for user {user_id}: {e}", exc_info=True)
        return {
            'total_penalties': 0,
            'total_penalty_amount': 0.0,
            'recent_penalties': []
        }


async def get_local_tasks_statistics(session: AsyncSession) -> dict:
    """Получить общую статистику по локальным заданиям."""
    try:
        from bot.database.models import LocalCompletedTask
        
        # Общее количество выполненных заданий
        total_completed_query = select(func.count(LocalCompletedTask.id))
        total_completed_result = await session.execute(total_completed_query)
        total_completed = total_completed_result.scalar_one_or_none() or 0
        
        # Общая сумма выданных наград
        total_rewards_query = select(func.sum(LocalCompletedTask.reward_given))
        total_rewards_result = await session.execute(total_rewards_query)
        total_rewards = total_rewards_result.scalar_one_or_none() or 0.0
        
        # Количество применённых штрафов
        total_penalties_query = select(func.count(LocalCompletedTask.id)).where(
            LocalCompletedTask.penalty_applied == True
        )
        total_penalties_result = await session.execute(total_penalties_query)
        total_penalties = total_penalties_result.scalar_one_or_none() or 0
        
        # Общая сумма штрафов
        total_penalty_amount_query = select(func.sum(LocalCompletedTask.penalty_amount)).where(
            LocalCompletedTask.penalty_applied == True
        )
        total_penalty_amount_result = await session.execute(total_penalty_amount_query)
        total_penalty_amount = total_penalty_amount_result.scalar_one_or_none() or 0.0
        
        # Уникальные пользователи
        unique_users_query = select(func.count(func.distinct(LocalCompletedTask.user_id)))
        unique_users_result = await session.execute(unique_users_query)
        unique_users = unique_users_result.scalar_one_or_none() or 0
        
        return {
            'total_completed': total_completed,
            'total_rewards': float(total_rewards),
            'total_penalties': total_penalties,
            'total_penalty_amount': float(total_penalty_amount),
            'unique_users': unique_users,
            'net_rewards': float(total_rewards - total_penalty_amount)
        }
        
    except Exception as e:
        logger.error(f"Error getting local tasks statistics: {e}", exc_info=True)
        return {
            'total_completed': 0,
            'total_rewards': 0.0,
            'total_penalties': 0,
            'total_penalty_amount': 0.0,
            'unique_users': 0,
            'net_rewards': 0.0
        }


async def apply_local_task_unsubscribe_penalties(
    session: AsyncSession,
    user_id: int,
    channel_id: int
) -> tuple[int, float]:
    """
    Применить штрафы за отписку от канала для всех локальных заданий пользователя.
    
    Returns:
        tuple[int, float]: (количество примененных штрафов, общая сумма штрафов)
    """
    try:
        # Получаем все выполненные локальные задания пользователя для данного канала
        completed_tasks = await get_user_local_tasks_for_unsubscribe(session, user_id, channel_id)
        
        penalties_applied = 0
        total_penalty_amount = 0.0
        
        for completed_task in completed_tasks:
            success = await apply_local_unsubscribe_penalty(session, completed_task.id)
            if success:
                penalties_applied += 1
                total_penalty_amount += completed_task.penalty_amount or completed_task.reward_given
                logger.info(f"Applied local penalty for user {user_id}, task {completed_task.task_id}, amount {completed_task.penalty_amount or completed_task.reward_given}")
        
        if penalties_applied > 0:
            logger.info(f"Applied {penalties_applied} local penalties for user {user_id}, channel {channel_id}, total penalty: {total_penalty_amount}")
        
        return penalties_applied, total_penalty_amount
        
    except Exception as e:
        logger.error(f"Error applying local unsubscribe penalties for user {user_id}, channel {channel_id}: {e}", exc_info=True)
        return 0, 0.0

# ... existing code ...


# === ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ ФУНКЦИЙ ДЛЯ ЛОКАЛЬНЫХ ЗАДАНИЙ ===
"""
Пример 1: Проверка отписки пользователя от канала и применение штрафов

async def handle_user_unsubscribed_from_channel(session: AsyncSession, user_id: int, channel_id: int):
    # Применяем штрафы за все выполненные локальные задания для этого канала
    penalties_count, total_penalty = await apply_local_task_unsubscribe_penalties(session, user_id, channel_id)
    
    if penalties_count > 0:
        print(f"Пользователь {user_id} отписался от канала {channel_id}")
        print(f"Применено штрафов: {penalties_count}")
        print(f"Общая сумма штрафа: {total_penalty} звезд")
    else:
        print(f"Штрафы не применены для пользователя {user_id} и канала {channel_id}")

Пример 2: Применение штрафа за конкретное выполненное задание

async def handle_specific_task_penalty(session: AsyncSession, completed_task_id: int):
    # Применяем штраф за конкретное выполненное задание
    success = await apply_local_unsubscribe_penalty(session, completed_task_id)
    
    if success:
        print(f"Штраф за задание {completed_task_id} успешно применен")
    else:
        print(f"Не удалось применить штраф за задание {completed_task_id}")

Пример 3: Применение штрафа с кастомной суммой

async def handle_custom_penalty(session: AsyncSession, completed_task_id: int, penalty_amount: float):
    # Применяем штраф с указанной суммой (вместо размера награды)
    success = await apply_local_unsubscribe_penalty(session, completed_task_id, penalty_amount)
    
    if success:
        print(f"Кастомный штраф {penalty_amount} звезд применен за задание {completed_task_id}")
"""
# ================================================================

async def send_local_penalty_notification(
    bot, 
    user_id: int, 
    channel_id: int, 
    penalties_count: int, 
    total_penalty: float
) -> bool:
    """
    Отправляет уведомление пользователю о применении штрафов за отписку от локальных заданий.
    
    Args:
        bot: Экземпляр бота для отправки сообщений
        user_id: ID пользователя для отправки уведомления
        channel_id: ID канала, от которого отписался пользователь
        penalties_count: Количество примененных штрафов
        total_penalty: Общая сумма штрафа в звездах
        
    Returns:
        bool: True если уведомление отправлено успешно, False в случае ошибки
    """
    try:
        # Формируем сообщение о штрафах
        penalty_message = (
            f"⚠️ <b>Штраф за отписку!</b>\n\n"
            f"Вы отписались от канала, за который получили награду.\n\n"
            f"📉 <b>Снято звезд:</b> {total_penalty:.2f}⭐️\n"
            f"💡 <i>Не отписывайтесь от каналов в течение 7 дней после получения награды.</i>"
        )
        # Отправляем уведомление пользователю
        await bot.send_message(
            chat_id=user_id,
            text=penalty_message,
            parse_mode="HTML"
        )
        
        logger.info(f"Local penalty notification sent to user {user_id} for channel {channel_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send local penalty notification to user {user_id}: {e}")
        return False


async def get_user_completed_task_channels(
    session: AsyncSession,
    user_id: int
) -> List[int]:
    """
    Получает список уникальных каналов, где пользователь выполнял локальные задания.
    
    Args:
        session: Асинхронная сессия базы данных
        user_id: ID пользователя
        
    Returns:
        List[int]: Список уникальных channel_id где пользователь выполнял задания
    """
    try:
        result = await session.execute(
            select(LocalCompletedTask.channel_id)
            .where(
                and_(
                    LocalCompletedTask.user_id == user_id,
                    LocalCompletedTask.channel_id.isnot(None),  # Исключаем задания без канала
                    LocalCompletedTask.penalty_applied == False  # Только задания без примененных штрафов
                )
            )
            .distinct()
        )
        
        channels = [row[0] for row in result.fetchall()]
        logger.debug(f"Found {len(channels)} unique channels with completed tasks for user {user_id}")
        return channels
        
    except Exception as e:
        logger.error(f"Error getting user completed task channels for user {user_id}: {e}")
        return []


async def check_and_apply_penalties_for_all_channels(
    session: AsyncSession,
    bot,
    user_id: int
) -> tuple[int, float]:
    """
    Проверяет подписку пользователя на все каналы где он выполнял задания и применяет штрафы при отписке.
    
    Args:
        session: Асинхронная сессия базы данных
        bot: Экземпляр бота для проверки подписки и отправки уведомлений
        user_id: ID пользователя
        
    Returns:
        tuple[int, float]: (общее количество штрафов, общая сумма штрафа)
    """
    total_penalties = 0
    total_penalty_amount = 0.0
    
    try:
        # Получаем все каналы где пользователь выполнял задания
        channels = await get_user_completed_task_channels(session, user_id)
        
        for channel_id in channels:
            try:
                # Проверяем подписку на канал
                from bot.handlers.user import check_member_with_delay
                is_subscribed = await check_member_with_delay(bot, channel_id, user_id)
                
                if not is_subscribed:
                    # Применяем штрафы за отписку от этого канала
                    penalties_count, total_penalty = await apply_local_task_unsubscribe_penalties(
                        session, user_id, channel_id
                    )
                    
                    if penalties_count > 0:
                        total_penalties += penalties_count
                        total_penalty_amount += total_penalty
                        
                        logger.info(f"Applied {penalties_count} penalties (total: {total_penalty} stars) for user {user_id}, channel {channel_id}")
                        
                        # Отправляем уведомление пользователю
                        await send_local_penalty_notification(
                            bot, user_id, channel_id, penalties_count, total_penalty
                        )
                    
            except Exception as channel_error:
                logger.error(f"Error checking/applying penalties for channel {channel_id}, user {user_id}: {channel_error}")
                continue
        
        if total_penalties > 0:
            logger.info(f"Total penalties applied for user {user_id}: {total_penalties} penalties, {total_penalty_amount} stars")
        
        return total_penalties, total_penalty_amount
        
    except Exception as e:
        logger.error(f"Error in check_and_apply_penalties_for_all_channels for user {user_id}: {e}")
        return 0, 0.0

async def add_completed_traffy_task(session: AsyncSession, user_id: int, task_id: str) -> bool:
    """
    Сохраняет ID выполненного Traffy задания для пользователя в базе данных.
    
    Args:
        session: Асинхронная сессия базы данных
        user_id: ID пользователя Telegram
        task_id: ID задания Traffy
        
    Returns:
        bool: True если задание успешно сохранено, False в случае ошибки
    """
    try:
        # Создаем модель для сохранения выполненного задания Traffy
        # Используем новую модель TraffyCompletedTask для сохранения информации
        from .models import TraffyCompletedTask
        
        # Проверяем, не выполнил ли пользователь это задание ранее
        user = await get_user(session, user_id)
        if not user:
            logger.error(f"User {user_id} not found when saving completed Traffy task")
            return False
            
        # Проверяем, существует ли уже запись о выполнении этого задания пользователем
        existing_task_query = select(TraffyCompletedTask).where(
            TraffyCompletedTask.user_id == user_id,
            TraffyCompletedTask.traffy_task_id == task_id
        )
        existing_task_result = await session.execute(existing_task_query)
        existing_task = existing_task_result.scalar_one_or_none()
        
        if existing_task:
            logger.warning(f"User {user_id} already completed Traffy task {task_id}")
            return False
        
        # Получаем данные о задании из state (должны быть сохранены ранее)
        # Если данных нет, создаем запись только с ID
        # В идеале title и link должны быть получены из state
        
        # Создаем новую запись о выполненном задании
        traffy_task = TraffyCompletedTask(
            user_id=user_id,
            traffy_task_id=task_id,
            # task_title будет заполнен, если он передан в функцию
            # task_link будет заполнен, если он передан в функцию
            reward_given=0.25  # Фиксированная награда за Traffy задания
        )
        
        # Также добавляем задание в список выполненных для пользователя через стандартный механизм
        # Создаем или получаем Task для добавления в user.completed_tasks
        task_query = select(Task).where(Task.id == int(task_id))
        result = await session.execute(task_query)
        task = result.scalar_one_or_none()
        
        if not task:
            # Создаем новую запись задания
            task = Task(
                id=int(task_id),
                description=f"Traffy Task ID: {task_id}",
                reward=0.25,
                is_active=True,
                premium_requirement="all"
            )
            session.add(task)
            logger.info(f"Created new Traffy task record with ID {task_id}")
        
        # Добавляем задание в список выполненных для пользователя
        if task not in user.completed_tasks:
            user.completed_tasks.append(task)
        
        # Добавляем запись в TraffyCompletedTask
        session.add(traffy_task)
        
        # Здесь не делаем commit, это будет сделано в вызывающей функции
        logger.info(f"Marked Traffy task {task_id} as completed for user {user_id}")
        return True
            
    except Exception as e:
        logger.error(f"Error saving completed Traffy task for user {user_id}, task {task_id}: {e}", exc_info=True)
        return False

async def check_traffy_task_availability(session: AsyncSession, user_id: int, task_id: str) -> bool:
    """
    Проверяет, доступно ли Traffy задание для пользователя.
    
    Задание недоступно, если пользователь уже выполнил его ранее.
    
    Args:
        session: Асинхронная сессия базы данных
        user_id: ID пользователя Telegram
        task_id: ID задания Traffy
        
    Returns:
        bool: True если задание доступно, False если уже выполнено или произошла ошибка
    """
    try:
        from .models import TraffyCompletedTask
        
        # Проверяем, не выполнил ли пользователь это задание ранее
        existing_task_query = select(TraffyCompletedTask).where(
            TraffyCompletedTask.user_id == user_id,
            TraffyCompletedTask.traffy_task_id == task_id
        )
        existing_task_result = await session.execute(existing_task_query)
        existing_task = existing_task_result.scalar_one_or_none()
        
        if existing_task:
            # Задание уже выполнено
            logger.info(f"User {user_id} already completed Traffy task {task_id} (found in TraffyCompletedTask)")
            return False
        
        # Задание доступно
        return True
        
    except Exception as e:
        logger.error(f"Error checking Traffy task availability for user {user_id}, task {task_id}: {e}", exc_info=True)
        # В случае ошибки считаем задание недоступным для безопасности
        return False

async def get_user_traffy_stats(session: AsyncSession, user_id: int) -> dict:
    """
    Получает статистику пользователя по выполненным Traffy заданиям.
    
    Args:
        session: Асинхронная сессия базы данных
        user_id: ID пользователя Telegram
        
    Returns:
        dict: Словарь со статистикой
    """
    try:
        from .models import TraffyCompletedTask
        
        # Получаем количество выполненных заданий
        count_query = select(func.count(TraffyCompletedTask.id)).where(
            TraffyCompletedTask.user_id == user_id
        )
        count_result = await session.execute(count_query)
        tasks_count = count_result.scalar_one_or_none() or 0
        
        # Получаем общую сумму наград
        reward_query = select(func.sum(TraffyCompletedTask.reward_given)).where(
            TraffyCompletedTask.user_id == user_id
        )
        reward_result = await session.execute(reward_query)
        total_reward = reward_result.scalar_one_or_none() or 0.0
        
        # Получаем последние выполненные задания
        recent_tasks_query = select(TraffyCompletedTask).where(
            TraffyCompletedTask.user_id == user_id
        ).order_by(TraffyCompletedTask.completed_at.desc()).limit(5)
        
        recent_tasks_result = await session.execute(recent_tasks_query)
        recent_tasks = recent_tasks_result.scalars().all()
        
        # Формируем и возвращаем статистику
        return {
            "total_tasks": tasks_count,
            "total_reward": total_reward,
            "recent_tasks": [
                {
                    "task_id": task.traffy_task_id,
                    "title": task.task_title,
                    "reward": task.reward_given,
                    "completed_at": task.completed_at.strftime("%d.%m.%Y %H:%M")
                }
                for task in recent_tasks
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting Traffy stats for user {user_id}: {e}", exc_info=True)
        return {
            "total_tasks": 0,
            "total_reward": 0.0,
            "recent_tasks": []
        }


# === Функции для работы с автоматическими выплатами подарками ===

async def get_gift_withdraw_settings(session: AsyncSession) -> Optional[GiftWithdrawSettings]:
    """Получить настройки автоматических выплат подарками"""
    try:
        result = await session.execute(
            select(GiftWithdrawSettings).limit(1)
        )
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Ошибка получения настроек выплат подарками: {e}")
        return None

async def create_default_gift_settings(session: AsyncSession) -> GiftWithdrawSettings:
    """Создать настройки по умолчанию для выплат подарками"""
    try:
        settings = GiftWithdrawSettings(
            enabled=True,
            min_amount_for_gifts=15,
            max_remainder=10,
            preferred_gifts=None
        )
        
        session.add(settings)
        await session.commit()
        await session.refresh(settings)
        
        logger.info("Созданы настройки по умолчанию для выплат подарками")
        return settings
        
    except Exception as e:
        logger.error(f"Ошибка создания настроек выплат подарками: {e}")
        await session.rollback()
        raise

async def update_gift_withdraw_settings(
    session: AsyncSession,
    enabled: Optional[bool] = None,
    min_amount: Optional[int] = None,
    max_remainder: Optional[int] = None,
    preferred_gifts: Optional[str] = None
) -> bool:
    """Обновить настройки автоматических выплат подарками"""
    from datetime import datetime
    
    try:
        # Получаем или создаем настройки
        settings = await get_gift_withdraw_settings(session)
        if not settings:
            settings = await create_default_gift_settings(session)
        
        # Подготавливаем данные для обновления
        update_data = {'updated_at': datetime.utcnow()}
        
        if enabled is not None:
            update_data['enabled'] = enabled
        if min_amount is not None:
            update_data['min_amount_for_gifts'] = min_amount
        if max_remainder is not None:
            update_data['max_remainder'] = max_remainder
        if preferred_gifts is not None:
            update_data['preferred_gifts'] = preferred_gifts
        
        # Обновляем настройки
        stmt = update(GiftWithdrawSettings).where(
            GiftWithdrawSettings.id == settings.id
        ).values(**update_data)
        
        await session.execute(stmt)
        await session.commit()
        
        logger.info(f"Обновлены настройки выплат подарками: {update_data}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка обновления настроек выплат подарками: {e}")
        await session.rollback()
        return False

async def get_pending_gift_withdraws(session: AsyncSession, limit: int = 10) -> List[Withdraws]:
    """Получить заявки на вывод, ожидающие автоматической обработки подарками"""
    try:
        result = await session.execute(
            select(Withdraws)
            .where(
                and_(
                    Withdraws.withdraw_status == False,
                    Withdraws.processing_type == 'manual'
                )
            )
            .order_by(Withdraws.withdraw_date.asc())
            .limit(limit)
        )
        return result.scalars().all()
        
    except Exception as e:
        logger.error(f"Ошибка получения заявок для автообработки: {e}")
        return []

async def get_withdraw_stats_by_type(session: AsyncSession) -> dict:
    """Получить статистику выплат по типам обработки"""
    try:
        result = await session.execute(
            select(
                Withdraws.processing_type,
                func.count(Withdraws.id).label('count'),
                func.sum(Withdraws.withdraw_amount).label('total_amount'),
                func.avg(Withdraws.withdraw_amount).label('avg_amount')
            )
            .where(Withdraws.withdraw_status == True)
            .group_by(Withdraws.processing_type)
        )
        
        stats = {}
        for row in result:
            stats[row.processing_type or 'manual'] = {
                'count': row.count,
                'total_amount': float(row.total_amount or 0),
                'avg_amount': float(row.avg_amount or 0)
            }
        
        return stats
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики выплат: {e}")
        return {}

async def get_failed_gift_withdraws(session: AsyncSession, limit: int = 20) -> List[Withdraws]:
    """Получить заявки с ошибками автоматической обработки"""
    try:
        result = await session.execute(
            select(Withdraws)
            .where(
                Withdraws.processing_type == 'requires_manual_processing'
            )
            .order_by(Withdraws.auto_processed_at.desc())
            .limit(limit)
        )
        return result.scalars().all()
        
    except Exception as e:
        logger.error(f"Ошибка получения проблемных заявок: {e}")
        return []

async def debug_all_tasks(session: AsyncSession) -> dict:
    """Отладочная функция для проверки всех заданий в базе данных"""
    try:
        # Получаем все задания
        all_tasks_query = select(Task).order_by(Task.id.asc())
        all_tasks_result = await session.execute(all_tasks_query)
        all_tasks = all_tasks_result.scalars().all()
        
        # Получаем только активные задания
        active_tasks_query = select(Task).where(Task.is_active == True).order_by(Task.id.asc())
        active_tasks_result = await session.execute(active_tasks_query)
        active_tasks = active_tasks_result.scalars().all()
        
        # Группируем по premium_requirement
        premium_stats = {}
        for task in active_tasks:
            req = task.premium_requirement
            if req not in premium_stats:
                premium_stats[req] = []
            premium_stats[req].append({
                'id': task.id,
                'description': task.description[:50] + '...' if len(task.description) > 50 else task.description,
                'reward': task.reward,
                'is_active': task.is_active
            })
        
        return {
            'total_tasks': len(all_tasks),
            'active_tasks': len(active_tasks),
            'inactive_tasks': len(all_tasks) - len(active_tasks),
            'premium_stats': premium_stats,
            'all_tasks_info': [
                {
                    'id': t.id,
                    'description': t.description[:30] + '...' if len(t.description) > 30 else t.description,
                    'is_active': t.is_active,
                    'premium_requirement': t.premium_requirement,
                    'reward': t.reward
                } for t in all_tasks
            ]
        }
    except Exception as e:
        logger.error(f"Error in debug_all_tasks: {e}", exc_info=True)
        return {'error': str(e)}

# Простые функции для проверки био
async def get_users_for_bio_check(session: AsyncSession, hours_ago: int = 24) -> List[User]:
    """Получить пользователей для проверки био"""
    try:
        # Получаем только пользователей с заполненным last_bio_reward_date
        result = await session.execute(
            select(User).where(
                User.banned == False,  # Исключаем заблокированных пользователей
                User.last_bio_reward_date.isnot(None)  # Только пользователи с заполненной датой награды за био
            )
        )
        users = result.scalars().all()
        logger.info(f"Найдено {len(users)} пользователей для проверки био")
        return users
    except Exception as e:
        logger.error(f"Error getting users for bio check: {e}")
        return []

async def apply_bio_penalty(session: AsyncSession, user_id: int, penalty_amount: float = 0.5) -> bool:
    """Применить штраф за удаление реферальной ссылки"""
    try:
        from datetime import datetime
        logger.info(f"Начинаем применение штрафа для пользователя {user_id}, сумма: {penalty_amount}")
        
        # Ищем пользователя по user_id, а не по первичному ключу
        result = await session.execute(select(User).where(User.user_id == user_id))
        user = result.scalar_one_or_none()
        
        if not user:
            logger.warning(f"Пользователь {user_id} не найден в базе данных - пропускаем штраф")
            return False
            
        logger.info(f"Пользователь {user_id} найден, текущий баланс: {user.balance}")
        
        # Увеличиваем счетчик штрафов
        if user.bio_link_penalties is None:
            user.bio_link_penalties = 0
        old_penalties = user.bio_link_penalties
        user.bio_link_penalties += 1
        logger.info(f"Штрафы для {user_id}: {old_penalties} -> {user.bio_link_penalties}")
        
        # Снимаем баланс
        old_balance = user.balance
        user.balance = max(0, user.balance - penalty_amount)
        logger.info(f"Баланс для {user_id}: {old_balance} -> {user.balance}")
        
        # Записываем дату последнего штрафа
        user.last_bio_penalty_date = datetime.utcnow()
        
        logger.info(f"Штраф успешно применен к пользователю {user_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при применении штрафа к {user_id}: {e}", exc_info=True)
        return False

async def update_bio_check_date(session: AsyncSession, user_id: int) -> bool:
    """Обновить дату последней проверки био"""
    try:
        from datetime import datetime
        
        # Ищем пользователя по user_id, а не по первичному ключу
        result = await session.execute(select(User).where(User.user_id == user_id))
        user = result.scalar_one_or_none()
        
        if not user:
            logger.warning(f"Пользователь {user_id} не найден - не можем обновить дату проверки био")
            return False
            
        user.last_bio_check_date = datetime.utcnow()
        logger.debug(f"Обновлена дата проверки био для пользователя {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error updating bio check date for {user_id}: {e}")
        return False

async def get_user_successful_withdraws_count(session: AsyncSession, user_id: int) -> int:
    """Получить количество успешных выводов пользователя"""
    query = select(func.count(Withdraws.id)).where(
        Withdraws.user_id == user_id,
        Withdraws.withdraw_status == True
    )
    result = await session.execute(query)
    return result.scalar() or 0

async def get_refferals_count(session: AsyncSession, user_id: int) -> int:
    """Получить количество рефералов пользователя из поля refferals_count"""
    try:
        # Получаем пользователя по user_id
        query = select(User).where(User.user_id == user_id)
        result = await session.execute(query)
        user = result.scalar_one_or_none()
        
        if user and user.refferals_count is not None:
            return user.refferals_count
        else:
            return 0
    except Exception as e:
        logger.error(f"Ошибка при получении количества рефералов для пользователя {user_id}: {e}")
        return 0

def generate_uneven_distribution(total_completions: int, hours: int) -> List[int]:
    """
    Генерирует "неровное" распределение заданий по часам.
    - Гарантирует, что в первом часе всегда будет хотя бы одно выполнение (если total_completions > 0).
    - Допускает нулевые значения для остальных часов.
    """
    if hours <= 0:
        return []
    if total_completions <= 0:
        return [0] * hours

    # 1. Инициализируем распределение нулями.
    distribution = [0] * hours
    
    # 2. Гарантируем хотя бы одно выполнение в первом часе.
    distribution[0] = 1
    remaining_completions = total_completions - 1
    
    # 3. Распределяем оставшиеся выполнения случайно по всем часам.
    if remaining_completions > 0:
        for _ in range(remaining_completions):
            idx = random.randrange(hours)
            distribution[idx] += 1
            
    # Контрольная проверка
    assert sum(distribution) == total_completions, "Сумма распределения не сошлась!"
    assert distribution[0] > 0, "В первом часе не должно быть нуля!"

    return distribution

async def create_time_distributed_task(
    session: AsyncSession,
    description: str,
    reward: float,
    instruction_link: Optional[str],
    action_link: Optional[str],
    channel_id_to_check: Optional[int],
    check_subscription: bool,
    premium_requirement: str,
    max_completions: int,
    distribution_hours: int
) -> Task:
    """
    Создает задание с временным распределением
    """
    # Генерируем распределение
    hourly_dist = generate_uneven_distribution(max_completions, distribution_hours)
    
    task = Task(
        description=description,
        reward=reward,
        instruction_link=instruction_link,
        action_link=action_link,
        channel_id_to_check=channel_id_to_check,
        check_subscription=check_subscription,
        premium_requirement=premium_requirement,
        max_completions=max_completions,
        time_distribution_hours=distribution_hours,
        hourly_distribution=json.dumps(hourly_dist),
        start_time=datetime.utcnow(),
        is_time_distributed=True,
        is_active=True
    )
    
    session.add(task)
    # Убираем commit, чтобы им управлял middleware
    await session.flush()
    await session.refresh(task)
    
    logger.info(f"Создано задание с временным распределением: ID {task.id}, {distribution_hours}ч, распределение: {hourly_dist}")
    return task

async def get_current_hour_limit(session: AsyncSession, task_id: int) -> int:
    """
    Получает лимит выполнений для текущего часа
    
    Returns:
        Количество доступных выполнений в текущий час (0 если время вышло или задание не найдено)
    """
    task = await get_task_by_id(session, task_id)
    if not task or not task.is_time_distributed or not task.start_time:
        return 0
    
    try:
        # Вычисляем прошедшие часы с момента старта
        current_time = datetime.utcnow()
        time_diff = current_time - task.start_time
        hours_passed = int(time_diff.total_seconds() // 3600)
        
        # Проверяем, не вышло ли время распределения
        if hours_passed >= task.time_distribution_hours:
            return 0
        
        # Получаем распределение
        if not task.hourly_distribution:
            return 0
        
        distribution = json.loads(task.hourly_distribution)
        if hours_passed >= len(distribution):
            return 0
        
        # Возвращаем лимит для текущего часа
        current_limit = distribution[hours_passed]
        logger.debug(f"Task {task_id} hour {hours_passed} limit: {current_limit}")
        return current_limit
        
    except (json.JSONDecodeError, ValueError, IndexError) as e:
        logger.error(f"Ошибка при получении лимита часа для задания {task_id}: {e}")
        return 0

async def get_task_actual_completions_count(session: AsyncSession, task_id: int) -> int:
    """
    Возвращает актуальное количество выполнений задания.
    Считает только из LocalCompletedTask (новая система).
    """
    try:
        # Считаем только из LocalCompletedTask (не оштрафованные)
        local_completions_query = select(func.count(LocalCompletedTask.id)).where(
            (LocalCompletedTask.task_id == task_id) &
            (LocalCompletedTask.penalty_applied == False)
        )
        local_result = await session.execute(local_completions_query)
        local_count = local_result.scalar() or 0
        
        logger.debug(f"Task {task_id} actual completions: {local_count} (from LocalCompletedTask)")
        
        return local_count
        
    except Exception as e:
        logger.error(f"Ошибка при подсчете фактических выполнений для задания {task_id}: {e}")
        return 0

async def get_task_time_info(session: AsyncSession, task_id: int) -> Dict[str, Any]:
    """
    Получает информацию о временном распределении задания.
    """
    task = await session.get(Task, task_id)
    if not task or not task.is_time_distributed:
        return {"error": "Task is not time-distributed or not found."}

    now = datetime.utcnow()
    start_time = task.start_time
    hours = task.time_distribution_hours
    end_time = start_time + timedelta(hours=hours) if start_time and hours else None
    
    time_passed = now - start_time if start_time else timedelta(seconds=-1)
    hours_passed = int(time_passed.total_seconds() // 3600) if time_passed.total_seconds() > 0 else -1

    distribution = json.loads(task.hourly_distribution) if task.hourly_distribution else []

    return {
        "start_time": start_time.isoformat() if start_time else "N/A",
        "end_time": end_time.isoformat() if end_time else "N/A",
        "time_passed_seconds": time_passed.total_seconds(),
        "hours_passed": hours_passed,
        "current_hour_limit": await get_current_hour_limit(session, task_id),
        "current_hour_completions": await get_current_hour_completions(session, task_id),
        "total_completions": task.current_completions,
        "max_completions": task.max_completions,
        "distribution_plan": distribution
    }

async def get_current_hour_completions(session: AsyncSession, task_id: int) -> int:
    """
    Получает количество выполнений задания за текущий слот распределения (1 час).
    Считает из всех источников: user_completed_tasks_table, LocalCompletedTask, SubGramCompletedTask.
    """
    task = await session.get(Task, task_id)
    if not task or not task.is_time_distributed or not task.start_time or not task.hourly_distribution:
        return 0

    now = datetime.utcnow()
    time_passed = now - task.start_time

    if time_passed.total_seconds() < 0:
        return 0 # Распределение еще не началось

    hours_passed = int(time_passed.total_seconds() // 3600)
    
    distribution = json.loads(task.hourly_distribution)
    if hours_passed >= len(distribution):
         return 999999 # Период распределения закончился, возвращаем большое число

    # Определяем начало и конец текущего часового слота
    current_slot_start = task.start_time + timedelta(hours=hours_passed)
    current_slot_end = current_slot_start + timedelta(hours=1)
    
    total_count = 0
    
    try:
        # 1. Считаем из основной таблицы выполненных заданий
        main_query = (
        select(func.count(user_completed_tasks_table.c.user_id))
        .where(user_completed_tasks_table.c.task_id == task_id)
        .where(user_completed_tasks_table.c.completed_at >= current_slot_start)
        .where(user_completed_tasks_table.c.completed_at < current_slot_end)
    )
        main_result = await session.execute(main_query)
        main_count = main_result.scalar_one_or_none() or 0
        total_count += main_count
        
        # 2. Считаем из LocalCompletedTask (локальные задания)
        local_query = (
            select(func.count(LocalCompletedTask.id))
            .where(LocalCompletedTask.task_id == task_id)
            .where(LocalCompletedTask.completed_at >= current_slot_start)
            .where(LocalCompletedTask.completed_at < current_slot_end)
            .where(LocalCompletedTask.penalty_applied == False)
        )
        local_result = await session.execute(local_query)
        local_count = local_result.scalar_one_or_none() or 0
        total_count += local_count
        
        # 3. Считаем из SubGramCompletedTask (SubGram задания)
        subgram_query = (
            select(func.count(SubGramCompletedTask.id))
            .where(SubGramCompletedTask.subgram_task_id == task_id)
            .where(SubGramCompletedTask.completed_at >= current_slot_start)
            .where(SubGramCompletedTask.completed_at < current_slot_end)
            .where(SubGramCompletedTask.penalty_applied == False)
        )
        subgram_result = await session.execute(subgram_query)
        subgram_count = subgram_result.scalar_one_or_none() or 0
        total_count += subgram_count
        
        logger.debug(f"Task {task_id} hour {hours_passed} completions: main={main_count}, local={local_count}, subgram={subgram_count}, total={total_count}")
        
    except Exception as e:
        logger.error(f"Ошибка при подсчете выполнений за час для задания {task_id}: {e}")
        # В случае ошибки возвращаем большое число, чтобы предотвратить выполнение
        return 999999
    
    return total_count

async def check_task_limits(session: AsyncSession, task: Task) -> tuple[bool, str]:
    """Проверяет, можно ли выполнить задание прямо сейчас, с учетом всех лимитов."""
    try:
        # Получаем самую свежую информацию из БД, чтобы избежать race condition
        await session.refresh(task, attribute_names=['current_completions', 'is_active'])
        
        # 1. Проверка активности задания
        if not task.is_active:
            return False, "задание неактивно"
        
        # 2. Проверка общего лимита выполнений
        if task.current_completions >= task.max_completions:
            return False, "лимит выполнений задания исчерпан"

        # 3. Проверка лимитов для заданий с временным распределением
        if task.is_time_distributed:
            current_hour_limit = await get_current_hour_limit(session, task.id)
            
            # Проверяем, что лимит часа больше 0
            if current_hour_limit <= 0:
                logger.debug(f"Task {task.id} time distribution expired: limit={current_hour_limit}")
                return False, "время выполнения задания истекло"
            
            # Получаем актуальное количество выполнений за текущий час
            current_hour_completions = await get_current_hour_completions(session, task.id)
            
            # Проверяем почасовый лимит
            if current_hour_completions >= current_hour_limit:
                logger.debug(f"Task {task.id} hourly limit exceeded: {current_hour_completions}/{current_hour_limit}")
                return False, "лимит выполнений в текущий час исчерпан"
            
            logger.debug(f"Task {task.id} hourly limits OK: {current_hour_completions}/{current_hour_limit}")
        
        return True, "ok"
        
    except Exception as e:
        logger.error(f"Ошибка при проверке лимитов для задания {task.id}: {e}")
        return False, "ошибка проверки лимитов"


async def has_user_completed_task(session: AsyncSession, user_id: int, task_id: int) -> bool:
    """
    Проверяет, выполнял ли пользователь задание ранее.
    Проверяет только в LocalCompletedTask (новая система).
    """
    try:
        # Проверяем только в таблице локальных выполнений (не оштрафованные)
        local_query = select(LocalCompletedTask).where(
            (LocalCompletedTask.user_id == user_id) &
            (LocalCompletedTask.task_id == task_id) &
            (LocalCompletedTask.penalty_applied == False)
        )
        local_result = await session.execute(local_query)
        completed = local_result.first() is not None
        
        logger.debug(f"User {user_id} task {task_id} completion check: {completed} (from LocalCompletedTask)")
        
        return completed
        
    except Exception as e:
        logger.error(f"Ошибка при проверке выполнения задания {task_id} пользователем {user_id}: {e}")
        return False

async def get_all_users_count(session: AsyncSession) -> int:
    """Возвращает общее количество пользователей"""
    query = select(func.count(User.id))
    result = await session.execute(query)
    return result.scalar_one()

# --- Функции для управления 'Показами' (Shows) ---

async def get_all_shows(session: AsyncSession) -> List[Show]:
    """Получить все 'показы'."""
    query = select(Show).order_by(Show.created_at.desc())
    result = await session.execute(query)
    return result.scalars().all()

async def get_show_by_id(session: AsyncSession, show_id: int) -> Optional[Show]:
    """Получить 'показ' по ID."""
    query = select(Show).where(Show.id == show_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def get_active_show(session: AsyncSession) -> Optional[Show]:
    """Получить активный 'показ'."""
    query = select(Show).where(Show.is_active == True)
    result = await session.execute(query)
    return result.scalar_one_or_none()

async def create_show(
    session: AsyncSession,
    name: str,
    text: str,
    photo_file_id: Optional[str] = None,
    keyboard_json: Optional[str] = None
) -> Show:
    """Создать новый 'показ'."""
    new_show = Show(
        name=name,
        text=text,
        photo_file_id=photo_file_id,
        keyboard_json=keyboard_json,
        is_active=False # Новые показы всегда неактивны по умолчанию
    )
    session.add(new_show)
    await session.flush()
    await session.refresh(new_show)
    return new_show

async def set_show_active_status(session: AsyncSession, show_id: int, status: bool) -> Optional[Show]:
    """
    Устанавливает статус активности для 'показа'.
    Если status=True, деактивирует все остальные 'показы'.
    """
    show = await get_show_by_id(session, show_id)
    if not show:
        return None

    if status:
        # Деактивируем все остальные показы
        update_stmt = update(Show).where(Show.id != show_id).values(is_active=False)
        await session.execute(update_stmt)
    
    show.is_active = status
    await session.flush()
    await session.refresh(show)
    return show

async def delete_show_by_id(session: AsyncSession, show_id: int) -> bool:
    """Удалить 'показ' по ID."""
    show = await get_show_by_id(session, show_id)
    if show:
        await session.delete(show)
        await session.flush()
        return True
    return False

# --- Конец функций для 'Показов' ---

async def debug_task_limits(session: AsyncSession, task_id: int) -> dict:
    """
    Функция для отладки лимитов конкретного задания.
    Возвращает подробную информацию о состоянии лимитов.
    """
    try:
        task = await get_task_by_id(session, task_id)
        if not task:
            return {"error": "Task not found"}
        
        result = {
            "task_id": task_id,
            "is_active": task.is_active,
            "is_time_distributed": task.is_time_distributed,
            "current_completions": task.current_completions,
            "max_completions": task.max_completions,
            "time_distribution_hours": task.time_distribution_hours,
            "start_time": task.start_time.isoformat() if task.start_time else None,
            "hourly_distribution": task.hourly_distribution
        }
        
        if task.is_time_distributed:
            current_hour_limit = await get_current_hour_limit(session, task_id)
            current_hour_completions = await get_current_hour_completions(session, task_id)
            
            result.update({
                "current_hour_limit": current_hour_limit,
                "current_hour_completions": current_hour_completions,
                "hourly_limit_exceeded": current_hour_completions >= current_hour_limit if current_hour_limit > 0 else True,
                "total_limit_exceeded": task.current_completions >= task.max_completions
            })
        
        # Проверяем общие лимиты
        can_complete, reason = await check_task_limits(session, task)
        result["can_complete"] = can_complete
        result["reason"] = reason
        
        return result
        
    except Exception as e:
        logger.error(f"Error debugging task limits for task {task_id}: {e}")
        return {"error": str(e)}