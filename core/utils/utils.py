from __future__ import annotations

# bot/utils/newsletter.py
import asyncio
import logging
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup # Импорт клавиатуры
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest
from typing import Optional, Tuple, List
from sqlalchemy.ext.asyncio import AsyncSession
from bot.database.models import Settings
import bot.database.requests as db
from bot.core.config import config

logger = logging.getLogger(__name__)

async def run_newsletter(
    bot: Bot,
    user_ids: list[int],
    session: AsyncSession,
    text: str | None,
    photo_id: str | None,
    reply_markup: InlineKeyboardMarkup | None,
    parse_mode: str = "HTML"
) -> Tuple[int, int, int, List[int]]:
    """
    Асинхронно рассылает сообщение пользователям.
    
    Returns:
        Tuple[успешно_отправлено, ошибок, заблокировавших, список_неудачных_пользователей]
    """
    count = 0
    errors = 0
    blocked_count = 0
    failed_user_ids = []  # Список пользователей, которым не удалось отправить
    
    logger.info(f"Starting newsletter for {len(user_ids)} users.")

    # Получаем промокод один раз для всей рассылки
    promo_code_name = await db.get_promo_code_name(session) or ""

    for i, user_id in enumerate(user_ids):
        if not isinstance(user_id, int) or user_id <= 0:
            logger.warning(f"Invalid user_id: {user_id}, skipping")
            failed_user_ids.append(user_id)
            errors += 1
            continue
            
        success = False
        try:
            logger.debug(f"Preparing to send message to user {user_id} ({i+1}/{len(user_ids)})")
            
            if not text and not photo_id:
                logger.warning(f"No text or photo to send to user {user_id}, skipping")
                failed_user_ids.append(user_id)
                errors += 1
                continue

            # Подставляем реферальную ссылку и промокод для каждого пользователя
            personalized_text = text.replace("ТУТ РЕФЕРАЛЬНАЯ ССЫЛКА ЧЕЛОВЕКА КОТОРЫЙ ПОЛУЧИЛ ЭТУ РАССЫЛКУ", f"https://t.me/Fastfreestarsbot?start={user_id}")
            personalized_text = personalized_text.replace("ПРОМОКОД22", promo_code_name)

            # Обратное преобразование экранированных HTML-тегов, если они уже экранированы
            if '&lt;' in personalized_text or '&gt;' in personalized_text:
                personalized_text = personalized_text.replace('&lt;', '<').replace('&gt;', '>')
                logger.debug(f"Обнаружены экранированные HTML-теги, выполнено обратное преобразование")

            # Отправляем сообщение
            if photo_id:
                await bot.send_photo(
                    chat_id=user_id,
                    photo=photo_id,
                    caption=personalized_text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
            elif text:
                await bot.send_message(
                    chat_id=user_id,
                    text=personalized_text,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                    parse_mode=parse_mode
                )

            count += 1
            success = True
            logger.debug(f"Message sent to user {user_id}. Progress: {count}/{len(user_ids)}")
            
        except TelegramRetryAfter as e:
            logger.warning(f"Flood control hit for user {user_id}. Sleeping for {e.retry_after} seconds.")
            await asyncio.sleep(e.retry_after + 1)  # +1 секунда для надежности
            
            # Повторная попытка после сна
            try:
                if photo_id:
                    await bot.send_photo(
                        chat_id=user_id, 
                        photo=photo_id, 
                        caption=personalized_text, 
                        reply_markup=reply_markup,
                        parse_mode=parse_mode
                    )
                elif text:
                    await bot.send_message(
                        chat_id=user_id, 
                        text=personalized_text, 
                        reply_markup=reply_markup, 
                        disable_web_page_preview=True,
                        parse_mode=parse_mode
                    )
                count += 1
                success = True
                logger.info(f"Message sent to user {user_id} after retry")
            except Exception as retry_error:
                logger.error(f"Failed to send message to user {user_id} after retry: {retry_error}")
                errors += 1
                failed_user_ids.append(user_id)
                
        except TelegramForbiddenError as e:
            logger.info(f"User {user_id} blocked the bot: {e}")
            blocked_count += 1
            failed_user_ids.append(user_id)
            
        except TelegramBadRequest as e:
            logger.error(f"Bad request for user {user_id}: {e}")
            errors += 1
            failed_user_ids.append(user_id)
            
        except Exception as e:
            logger.error(f"Unexpected error sending to user {user_id}: {e}", exc_info=True)
            errors += 1
            failed_user_ids.append(user_id)

        # Интеллектуальные паузы для предотвращения флуд-контроля
        if success:
            if count % 30 == 0:  # Каждые 30 успешных отправок
                await asyncio.sleep(1)
                logger.debug(f"Short pause after {count} successful sends")
            elif count % 100 == 0:  # Каждые 100 успешных отправок
                await asyncio.sleep(2)
                logger.info(f"Long pause after {count} successful sends")

    logger.info(f"Newsletter finished. Sent: {count}, Errors: {errors}, Blocked: {blocked_count}, Failed users: {len(failed_user_ids)}")
    
    return count, errors, blocked_count, failed_user_ids


async def run_newsletter_with_auto_cleanup(
    bot: Bot,
    user_ids: list[int],
    session: AsyncSession,
    text: str | None,
    photo_id: str | None,
    reply_markup: InlineKeyboardMarkup | None,
    parse_mode: str = "HTML",
    auto_delete_inactive: bool = True
) -> Tuple[int, int, int, int]:
    """
    Асинхронно рассылает сообщение пользователям с автоматическим удалением неактивных.
    
    Returns:
        Tuple[успешно_отправлено, ошибок, заблокировавших, удалено_неактивных]
    """
    logger.info(f"Starting newsletter with auto-cleanup for {len(user_ids)} users, auto_delete: {auto_delete_inactive}")
    
    # Запускаем обычную рассылку
    sent_count, errors, blocked_count, failed_user_ids = await run_newsletter(
        bot, user_ids, session, text, photo_id, reply_markup, parse_mode
    )
    
    deleted_count = 0
    
    # Автоматически удаляем неактивных пользователей, если включено
    if auto_delete_inactive and failed_user_ids:
        logger.info(f"Starting auto-deletion of {len(failed_user_ids)} inactive users...")
        
        try:
            # Создаем полностью новую сессию для операции удаления
            async with AsyncSession(session.bind) as delete_session:
                try:
                    logger.debug(f"Created new session for deletion: {id(delete_session)}")
                    
                    # Выполняем удаление
                    deleted_count, total_to_delete = await db.delete_inactive_users_from_list(
                        delete_session, failed_user_ids
                    )
                    
                    # Коммитим изменения в БД
                    await delete_session.commit()
                    logger.info(f"Successfully auto-deleted {deleted_count} out of {total_to_delete} inactive users")
                    
                    if deleted_count < total_to_delete:
                        logger.warning(f"Only {deleted_count} users were deleted out of {total_to_delete} expected")
                        
                except Exception as delete_error:
                    # Откатываем изменения в случае ошибки
                    await delete_session.rollback()
                    logger.error(f"Error during deletion operation, rolled back: {delete_error}", exc_info=True)
                    deleted_count = 0
                    
        except Exception as session_error:
            logger.error(f"Failed to create deletion session: {session_error}", exc_info=True)
            deleted_count = 0
    
    # Собираем информацию для админов
    auto_delete_text = ""
    if auto_delete_inactive:
        if deleted_count > 0:
            auto_delete_text = f"🗑 <b>Автоматически удалено неактивных:</b> <code>{deleted_count}</code>\n"
        else:
            auto_delete_text = f"⚠️ <b>Неактивные пользователи не удалены</b> (ошибка или их не было)\n"
    
    notification_text = (
        "<b>📢 Рассылка завершена!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Всего пользователей:</b> <code>{len(user_ids)}</code>\n"
        f"✅ <b>Успешно отправлено:</b> <code>{sent_count}</code>\n"
        f"❌ <b>Ошибок:</b> <code>{errors}</code>\n"
        f"🚫 <b>Заблокировали бота:</b> <code>{blocked_count}</code>\n"
        f"📊 <b>Процент успеха:</b> <code>{(sent_count/len(user_ids)*100):.1f}%</code>\n"
        f"{auto_delete_text}"
    )
    
    # Отправляем уведомление всем администраторам
    try:
        notification_tasks = []
        for admin_id in config.admin_ids:
            notification_tasks.append(
                bot.send_message(
                    admin_id,
                    notification_text,
                    parse_mode="HTML"
                )
            )
        
        # Ожидаем завершения всех уведомлений, игнорируя ошибки
        if notification_tasks:
            results = await asyncio.gather(*notification_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Не удалось отправить уведомление админу {config.admin_ids[i]}: {result}")
    except Exception as notify_error:
        logger.error(f"Error sending admin notifications: {notify_error}")
    
    logger.info(f"Newsletter completed: sent={sent_count}, errors={errors}, blocked={blocked_count}, deleted={deleted_count}")
    return sent_count, errors, blocked_count, deleted_count
