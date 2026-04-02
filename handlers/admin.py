from __future__ import annotations

from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import html # <-- Добавляем импорт html
import bot.database.models as mdl
import re
import os
import tempfile
import subprocess
import asyncio
from bot.core.utils.logging import logger
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton # Добавляем InlineKeyboardMarkup, InlineKeyboardButton
import bot.database.requests as db
import bot.keyboards.keyboards as kb
from bot.core.config import Config
import bot.core.utils.state as st
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from bot.core.config import config # Импортируем сам объект config
from bot.core.utils.state import AddTaskState, GiftSettingsState, AddIndividualLinkState, AddChannelState, RewardSettingsState, AdminManageUser, AdminTemplateStates, NewsletterStates, PromoCodeStateTemplate
from typing import Optional, List, Union
from bot.database.models import User, IndividualLink, Channel
from bot.keyboards import keyboards as kb # Убедитесь, что клавиатуры импортированы
from bot.database import requests as db # Убедитесь, что запросы к БД импортированы
from bot.core.utils.state import AdminTemplateStates, AddShowState
import json
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InputMediaPhoto
from bot.core.utils.utils import run_newsletter_with_auto_cleanup # Предполагаем, что функция запуска здесь
from bot.database.models import BroadcastTemplate # Импорт модели
from bot.database.models import Channel # Убедитесь в импорте
from bot.core.utils.state import AddChannelState # Импорт состояний для добавления канала
from datetime import datetime
from functools import wraps

from bot.database.requests import get_subgram_webhooks_stats

from bot.database.requests import (
    get_comprehensive_daily_tasks_stats,
    get_daily_tasks_monthly_stats,
    get_user_daily_tasks_history,
    get_user_by_id
)
from bot.keyboards.keyboards import (
    admin_daily_tasks_stats_keyboard
)

# Декоратор для проверки прав администратора
def admin_required(func):
    @wraps(func)
    async def wrapper(event: Union[Message, CallbackQuery], *args, **kwargs):
        user_id = event.from_user.id
        
        # Получаем конфиг из аргументов функции
        config = None
        for arg in args:
            if isinstance(arg, Config):
                config = arg
                break
                
        if 'config' in kwargs:
            config = kwargs['config']
            
        # Если конфига нет в аргументах, пытаемся найти сессию и загрузить конфиг
        if not config and ('session' in kwargs or any(isinstance(arg, AsyncSession) for arg in args)):
            # Здесь можно было бы загрузить конфиг из базы данных,
            # но поскольку конфиг обычно передается в функцию, этот случай маловероятен
            logger.error(f"Конфиг не найден в аргументах функции {func.__name__}")
            
            # Показываем сообщение об ошибке
            if isinstance(event, Message):
                await event.answer("❌ Ошибка доступа: не удалось проверить права администратора")
            else:
                await event.answer("❌ Ошибка доступа", show_alert=True)
            return
            
        # Проверяем права администратора
        if config and user_id in config.admin_ids:
            return await func(event, *args, **kwargs)
        else:
            # Показываем сообщение о нехватке прав
            if isinstance(event, Message):
                await event.answer("❌ У вас недостаточно прав для выполнения этой команды")
            else:
                await event.answer("❌ У вас недостаточно прав", show_alert=True)
            return
            
    return wrapper

router = Router()

# --- Главная Админ-панель ---

@router.message(Command("cancel"))
async def cancel_handler(message: Message, config: Config, state: FSMContext):
    await state.clear()
    await message.answer("Состояния успешно сброшены.")


@router.message(Command("admin"))
@router.message(F.text == "Админ панель 👑")
@router.callback_query(F.data == "admin_back_to_main")
@admin_required
async def cmd_admin_panel(event: Message | CallbackQuery, config: Config, state: FSMContext):
    text = "⚙️ Админ-панель:"
    reply_markup = kb.admin_main_keyboard()

    if isinstance(event, Message):
        await event.answer(text, reply_markup=reply_markup)
    elif isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, reply_markup=reply_markup)
        except Exception as e:
            logger.debug(f"Не удалось изменить сообщение на главную админку: {e}")
        await event.answer()
    await state.clear()

@router.message(Command("admin_download_users"))
@router.callback_query(F.data == "admin_download_users")
@admin_required
async def admin_download_users(message: Message | CallbackQuery, bot: Bot, session: AsyncSession, config: Config):
    """
    Хендлер для команды /admin_download_users.
    Доступен только администраторам.
    Собирает все ID пользователей из БД, сохраняет в .txt файл и отправляет его администратору.
    """
    # Определяем user_id и chat_id универсально
    # (Хотя для @router.message это всегда будет message, сделаем на всякий случай)
    if isinstance(message, CallbackQuery): # Проверка на случай, если сюда попал CallbackQuery
        user_id = message.from_user.id
        chat_id = message.message.chat.id
        event_source = message.message # Используем .message для ответа в тот же чат
    elif isinstance(message, Message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        event_source = message # Используем сам message для ответа
    else:
        logger.error(f"Unknown event type received in admin_download_users: {type(message)}")
        return # Не можем обработать неизвестный тип

    # 1. Проверка прав администратора
    if user_id not in config.admin_ids:
        logger.warning(f"User {user_id} tried to use /admin_download_users without permission.")
        # Можно ничего не отвечать, чтобы не раскрывать команду
        # Или отправить сообщение: await message.reply("У вас нет прав для выполнения этой команды.")
        return

    logger.info(f"Admin {user_id} requested user ID list download.")
    temp_file_path = None # Инициализируем переменную для пути к файлу

    try:
        # 2. Получение ID пользователей из БД
        user_ids = await db.get_all_user_ids(session)

        if not user_ids:
            # Используем event_source для ответа
            await event_source.answer("В базе данных пока нет пользователей.")
            logger.info(f"No users found in DB for admin {user_id}.")
            return

        # 3. Создание временного файла и запись ID
        # Используем tempfile для безопасного создания временного файла
        fd, temp_file_path = tempfile.mkstemp(suffix=".txt", text=True)

        with os.fdopen(fd, 'w', encoding='utf-8') as tmp_file:
            for uid in user_ids:
                tmp_file.write(f"{uid}\n") # Записываем каждый ID на новой строке

        logger.info(f"Created temporary file {temp_file_path} with {len(user_ids)} user IDs for admin {user_id}.")

        # 4. Отправка файла администратору
        file_to_send = FSInputFile(temp_file_path, filename="user_ids.txt")
        await bot.send_document(
            chat_id=chat_id, # Используем chat_id
            document=file_to_send,
            caption=f"📄 Список ID всех пользователей ({len(user_ids)} шт.)"
        )
        logger.info(f"Successfully sent user ID list file to admin {user_id}.")

    except Exception as e:
        logger.error(f"Error during /admin_download_users for admin {user_id}: {e}", exc_info=True)
        # Используем bot.send_message с chat_id вместо message.reply
        try:
            await bot.send_message(
                chat_id=chat_id, # Отправляем в нужный чат
                text="❌ Произошла ошибка при формировании или отправке файла. Пожалуйста, проверьте логи."
            )
        except Exception as send_error:
             logger.error(f"Failed to send error message to chat {chat_id}: {send_error}")

    finally:
        # 5. Удаление временного файла (даже если была ошибка)
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.debug(f"Removed temporary file: {temp_file_path}")
            except OSError as e:
                logger.error(f"Error removing temporary file {temp_file_path}: {e}")

# Обработчик кнопки "Статистика"
@router.callback_query(F.data == "admin_show_stats")
@admin_required
async def admin_show_stats_callback(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        await callback.answer()
        result = await session.execute(select(User))
        users = result.scalars().all()
        total_users = len(users)

        # Подсчитываем количество пользователей, прошедших ОП
        ref_bonus_users = sum(1 for user in users if user.ref_bonus)

        # Подсчитываем количество новых пользователей за сегодня
        today = datetime.utcnow().date()
        new_users_today = [user for user in users if user.registered_at.date() == today]
        total_new_users_today = len(new_users_today)

        # Подсчитываем количество новых пользователей, прошедших ОП
        new_users_with_ref_bonus = sum(1 for user in new_users_today if user.ref_bonus)

        # Подсчитываем количество пользователей, пришедших по реферальной ссылке (саморост)
        invited_by_ref = sum(1 for user in users if user.refferal_id)
        percent_by_ref = (invited_by_ref / total_users * 100)

        await callback.message.edit_text(
            f"📊 Всего пользователей: {total_users}\n"
            f"✅ Пользователей, прошедших ОП: {ref_bonus_users}\n"
            f"🆕 Новых пользователей сегодня: {total_new_users_today}\n"
            f"✅ Новых пользователей, прошедших ОП сегодня: {new_users_with_ref_bonus}\n"
            f"🌱 Приглашено по реф. системе (саморост): {invited_by_ref} ({percent_by_ref:.1f}%)",
            show_alert=True, 
            reply_markup=kb.back_stats_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in stats callback handler: {e}")

@router.callback_query(F.data == "admin_manage_tasks")
@admin_required
async def admin_manage_tasks(callback: CallbackQuery, config: Config, session: AsyncSession):
    tasks_with_counts = await db.get_all_tasks_with_completion_count(session)

    # --- Формируем текстовое представление списка с ограничением длины --- 
    tasks_list_lines = []
    if tasks_with_counts:
        # Ограничиваем количество заданий для отображения в тексте
        max_tasks_to_show = 15  # Максимум 15 заданий в тексте
        tasks_to_show = tasks_with_counts[:max_tasks_to_show]
        
        for task, count in tasks_to_show:
            status_icon = "🟢" if task.is_active else "🔴"
            display_name = task.action_link if task.action_link else task.description
            display_name_short = (display_name[:20] + '...') if len(display_name) > 20 else display_name
            # --- Экранируем HTML перед вставкой в текст --- 
            escaped_display_name = html.escape(display_name_short)
            # ----------------------------------------------
            tasks_list_lines.append(f"{status_icon} #{task.id} - {escaped_display_name} ({count} вып.)")
        
        tasks_list_text = "\n".join(tasks_list_lines)
        
        # Добавляем информацию о количестве заданий
        total_tasks = len(tasks_with_counts)
        if total_tasks > max_tasks_to_show:
            message_text = f"📄 Список заданий (показано {max_tasks_to_show} из {total_tasks}):\n\n{tasks_list_text}\n\n... и еще {total_tasks - max_tasks_to_show} заданий"
        else:
            message_text = f"📄 Список всех заданий ({total_tasks}):\n\n{tasks_list_text}"
    else:
        message_text = "📄 Заданий пока нет."
    # ---------------------------------------------

    # --- Клавиатуру оставляем для навигации и добавления --- 
    reply_markup = kb.admin_tasks_list_keyboard_paginated(tasks_with_counts, total_tasks=len(tasks_with_counts), page=0)

    try:
        # Отправляем сообщение с текстом списка и клавиатурой
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение со списком задач: {e}")
        # Если не удалось отредактировать, попробуем отправить новым
        try:
             await callback.message.answer(message_text, reply_markup=reply_markup)
             # Старое сообщение можно попробовать удалить, но необязательно
             # await callback.message.delete()
        except Exception as send_err:
             logger.error(f"Не удалось отправить новое сообщение со списком задач: {send_err}")

    await callback.answer()

@router.callback_query(F.data.startswith("admin_tasks_page_"))
@admin_required
async def admin_tasks_page_navigation(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Навигация по страницам списка заданий"""
    try:
        page = int(callback.data.split("_")[-1])
        tasks_with_counts = await db.get_all_tasks_with_completion_count(session)
        
        # Формируем текст для текущей страницы
        max_tasks_to_show = 15
        tasks_to_show = tasks_with_counts[:max_tasks_to_show] if tasks_with_counts else []
        
        tasks_list_lines = []
        if tasks_to_show:
            for task, count in tasks_to_show:
                status_icon = "��" if task.is_active else "🔴"
                display_name = task.action_link if task.action_link else task.description
                display_name_short = (display_name[:20] + '...') if len(display_name) > 20 else display_name
                escaped_display_name = html.escape(display_name_short)
                tasks_list_lines.append(f"{status_icon} #{task.id} - {escaped_display_name} ({count} вып.)")
            
            tasks_list_text = "\n".join(tasks_list_lines)
            total_tasks = len(tasks_with_counts)
            if total_tasks > max_tasks_to_show:
                message_text = f"📄 Список заданий (показано {max_tasks_to_show} из {total_tasks}):\n\n{tasks_list_text}\n\n... и еще {total_tasks - max_tasks_to_show} заданий"
            else:
                message_text = f"📄 Список всех заданий ({total_tasks}):\n\n{tasks_list_text}"
        else:
            message_text = "📄 Заданий пока нет."
        
        # Создаем клавиатуру для текущей страницы
        reply_markup = kb.admin_tasks_list_keyboard_paginated(
            tasks_with_counts, 
            total_tasks=len(tasks_with_counts) if tasks_with_counts else 0, 
            page=page
        )
        
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
        
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing page number from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка при навигации по страницам.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in admin_tasks_page_navigation: {e}")
        await callback.answer("❌ Произошла ошибка при загрузке страницы.", show_alert=True)
    
    await callback.answer()


@router.callback_query(F.data == "admin_check_task_limits")
@admin_required
async def admin_check_task_limits(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Проверка лимитов заданий"""
    await callback.message.edit_text(
        "🔍 <b>Проверка лимитов заданий</b>\n\n"
        "Введите ID задания для проверки лимитов:",
        reply_markup=kb.admin_back_to_main()
    )
    
    # Устанавливаем состояние для ожидания ID задания
    await callback.answer()


# @router.message(F.text.regexp(r"^\d+$"))
# async def admin_process_task_id_for_limits(message: Message, config: Config, session: AsyncSession):
#     """Обработка ID задания для проверки лимитов"""
#     try:
#         task_id = int(message.text)
        
#         # Проверяем лимиты задания
#         limits_info = await db.debug_task_limits(session, task_id)
        
#         if "error" in limits_info:
#             await message.answer(f"❌ Ошибка: {limits_info['error']}")
#             return
        
#         # Формируем сообщение с информацией о лимитах
#         info_text = f"""🔍 <b>Информация о лимитах задания #{task_id}</b>

# <b>Статус:</b> {'✅ Активно' if limits_info['is_active'] else '❌ Неактивно'}
# <b>Временное распределение:</b> {'⏰ Включено' if limits_info['is_time_distributed'] else '🔄 Отключено'}
# <b>Выполнения:</b> {limits_info['current_completions']}/{limits_info['max_completions']}
# <b>Можно выполнить:</b> {'✅ Да' if limits_info['can_complete'] else '❌ Нет'}
# <b>Причина:</b> {limits_info['reason']}"""

#         if limits_info['is_time_distributed']:
#             info_text += f"""

# <b>Временное распределение:</b>
# • Период: {limits_info['time_distribution_hours']} часов
# • Начало: {limits_info['start_time']}
# • Лимит текущего часа: {limits_info['current_hour_limit']}
# • Выполнено в текущем часе: {limits_info['current_hour_completions']}
# • Лимит часа превышен: {'❌ Да' if limits_info['hourly_limit_exceeded'] else '✅ Нет'}
# • Общий лимит превышен: {'❌ Да' if limits_info['total_limit_exceeded'] else '✅ Нет'}"""

#         await message.answer(info_text, parse_mode="HTML")
        
#     except ValueError:
#         await message.answer("❌ Пожалуйста, введите корректный ID задания (число)")
#     except Exception as e:
#         logger.error(f"Error checking task limits: {e}")
#         await message.answer(f"❌ Произошла ошибка при проверке лимитов: {e}")


# Просмотр конкретного задания (остается без изменений, т.к. вызывается из списка)
@router.callback_query(F.data.startswith("admin_task_view_"))
@admin_required
async def admin_view_single_task(callback: CallbackQuery, config: Config, session: AsyncSession):
    task_id = int(callback.data.split("_")[3])
    task = await db.get_task_by_id(session, task_id)
    if not task:
        await callback.answer("❌ Задание не найдено.", show_alert=True)
        # Вместо вызова admin_view_tasks, эмулируем нажатие кнопки "Управление заданиями"
        await admin_manage_tasks(callback, config, session)
        return

    status = "Активно 🟢" if task.is_active else "Неактивно 🔴"
    check_sub = "Да" if task.check_subscription else "Нет"
    channel = task.channel_id_to_check if task.channel_id_to_check else "Нет"
    instruction = task.instruction_link if task.instruction_link else "Нет"
    action = task.action_link if task.action_link else "Нет"

    # Добавляем информацию о лимите выполнений
    max_completions = getattr(task, 'max_completions', 1000000)
    current_completions = await db.get_task_actual_completions_count(session, task_id)
    
    # Добавляем информацию о временном распределении
    time_distribution_info = ""
    if getattr(task, 'is_time_distributed', False):
        distribution_hours = getattr(task, 'time_distribution_hours', 0)
        current_hour_limit = await db.get_current_hour_limit(session, task_id)
        time_distribution_info = f"""
<b>Временное распределение:</b> Включено ⏰
<b>Период распределения:</b> {distribution_hours} часов
<b>Лимит текущего часа:</b> {current_hour_limit}"""
    else:
        time_distribution_info = "\n<b>Временное распределение:</b> Отключено"
    
    task_details = f"""
📝 <b>Задание #{task.id}</b> [{status}]
-------------------------------------
<b>Описание:</b>
{task.description}
-------------------------------------
<b>Награда:</b> {task.reward:.2f}⭐️
<b>Инструкция:</b> {instruction}
<b>Ссылка действия:</b> {action}
<b>Проверка подписки:</b> {check_sub}
<b>Канал для проверки:</b> {channel}
<b>Лимит выполнений:</b> {current_completions}/{max_completions}{time_distribution_info}
"""
    try:
        # Используем обновленную клавиатуру с правильной кнопкой назад
        await callback.message.edit_text(task_details, reply_markup=kb.admin_task_manage_keyboard(task))
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение при просмотре задачи {task_id}: {e}")
    await callback.answer()


# Переключение статуса активности задания (остается без изменений)
@router.callback_query(F.data.startswith("admin_task_toggle_"))
async def admin_toggle_task_status(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        task_id = int(callback.data.split("_")[-1]) # Изменено с split("_")[3]
        task = await db.get_task_by_id(session, task_id)
        if not task:
            await callback.answer("❌ Задание не найдено.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
            return

        new_status = not task.is_active
        updated = await db.set_task_active_status(session, task_id, new_status)

        if updated:
            status_text = "активировано" if new_status else "деактивировано"
            await callback.answer(f"✅ Задание #{task_id} {status_text}.")
            await admin_view_single_task(callback, config, session)
        else:
            await callback.answer("❌ Не удалось изменить статус задания.", show_alert=True)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing task_id from toggle callback: {callback.data}")
        await callback.answer("❌ Ошибка обработки запроса.", show_alert=True)


# Запрос на удаление задания (остается без изменений)
@router.callback_query(F.data.startswith("admin_task_delete_"))
async def admin_delete_task_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        task_id = int(callback.data.split("_")[-1])
        task = await db.get_task_by_id(session, task_id)
        if not task:
            await callback.answer("❌ Задание не найдено.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
            return

        text = (f"🗑️ Вы уверены, что хотите удалить Задание #{task_id}?\n\n"
                f"Описание: {task.description[:50]}...\n\n"
                f"⚠️ Это действие необратимо!")
        markup = kb.admin_task_delete_confirm_keyboard(task_id)
        
        try:
            await callback.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                # Ничего не делаем, сообщение уже имеет нужный контент
                pass
            else:
                # Логируем другие ошибки
                logger.debug(f"Ошибка при изменении сообщения для задачи {task_id}: {e}")
        
        await callback.answer()
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing task_id from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка при обработке ID задания.", show_alert=True)


# Подтверждение удаления задания - изменить формат обработки ID
@router.callback_query(F.data.startswith("confirm_admin_task_delete_"))
async def admin_delete_task_confirm(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        task_id = int(callback.data.split("_")[4]) # Изменено с split("_")[4]
        deleted = await db.delete_task_by_id(session, task_id)

        if deleted:
            await callback.answer(f"✅ Задание #{task_id} успешно удалено.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
        else:
            await callback.answer("❌ Не удалось удалить задание.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing task_id from '{callback.data}': {e}")
        await callback.answer("❌ Ошибка при обработке ID задания.", show_alert=True)
        await admin_manage_tasks(callback, config, session) # Возвращаемся к списку

@router.callback_query(F.data == "admin_add_task", StateFilter(None))
async def admin_add_task_start(callback: CallbackQuery, config: Config, state: FSMContext):
    logger.info(f"Admin {callback.from_user.id} starting to add a new task.") # Логирование
    message_text = "✍️ Введите описание нового задания (можно использовать HTML):"
    reply_markup = kb.cancel_state_keyboard()
    await callback.message.answer(message_text, reply_markup=reply_markup)
    await callback.answer()
    await state.set_state(AddTaskState.waiting_for_description)


# Обработчик отмены состояния
@router.callback_query(F.data == "cancel_state", StateFilter("*"))
async def cancel_handler(callback: CallbackQuery, config: Config, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await callback.answer("Нет активного действия для отмены.")
        return

    logger.info(f"Admin {callback.from_user.id} cancelled state {current_state}") # Логирование
    await state.clear()
    try:
        await callback.message.edit_text("Действие отменено.")
    except:
        await callback.answer("Действие отменено.")


# Получение описания
@router.message(AddTaskState.waiting_for_description)
async def process_task_description(message: Message, config: Config, state: FSMContext):
    await state.update_data(description=message.text)
    await message.answer("💰 Введите награду за выполнение (число, например 1.25 или 5):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddTaskState.waiting_for_reward)

# Получение награды
@router.message(AddTaskState.waiting_for_reward)
async def process_task_reward(message: Message, config: Config, state: FSMContext):
    try:
        reward = float(message.text.replace(',', '.'))
        if reward < 0:
             raise ValueError("Награда не может быть отрицательной")
        await state.update_data(reward=reward)
        await message.answer("🔗 Введите ссылку на инструкцию (если нет, введите '-'):", reply_markup=kb.cancel_state_keyboard())
        await state.set_state(AddTaskState.waiting_for_instruction_link)
    except ValueError as e:
        logger.warning(f"Admin {message.from_user.id} entered invalid reward '{message.text}': {e}") # Логирование
        await message.answer(f"❌ Ошибка: {e}. Пожалуйста, введите корректное ПОЛОЖИТЕЛЬНОЕ число для награды (например, 1.25 или 5).", reply_markup=kb.cancel_state_keyboard())


# Получение ссылки на инструкцию
@router.message(AddTaskState.waiting_for_instruction_link)
async def process_task_instruction_link(message: Message, config: Config, state: FSMContext):
    link = message.text if message.text != '-' else None
    await state.update_data(instruction_link=link)
    await message.answer("↗️ Введите ссылку для кнопки 'Выполнить' (если не нужна, введите '-'):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddTaskState.waiting_for_action_link)


# Получение ссылки для кнопки "Выполнить"
@router.message(AddTaskState.waiting_for_action_link)
async def process_task_action_link(message: Message, config: Config, state: FSMContext):
    link = message.text if message.text != '-' else None
    await state.update_data(action_link=link)
    await message.answer("🆔 Введите ID канала для задания (если не требуется, введите '-'):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddTaskState.waiting_for_channel_id)


# Получение ID канала (ОПЦИОНАЛЬНО)
@router.message(AddTaskState.waiting_for_channel_id)
async def process_task_channel_id(message: Message, config: Config, state: FSMContext):
    channel_id_text = message.text
    channel_id = None
    check_subscription = False

    if channel_id_text != '-':
        try:
            channel_id = int(channel_id_text)
            # --- Добавлена проверка на положительное значение --- 
            if channel_id >= 0: # ID каналов обычно отрицательные, чатов положительные. Разрешим только отрицательные? Или все кроме 0?
                 # Пока оставим так: разрешаем любые ненулевые. TG ID могут быть > int32, но BigInt в модели должен справиться.
                 # Если нужны только каналы, можно проверять channel_id < 0
                 pass # ID подходит
            else:
                 # Если нужны только каналы/супергруппы, они начинаются с -100...
                 # Если нужны и обычные чаты/боты, они положительные
                 # Пока допустим любые целые числа, кроме 0, если такая логика нужна.
                 # Для простоты сейчас уберем проверку на знак, оставим только ValueError
                 # Если нужно строже: if channel_id == 0: raise ValueError("ID канала не может быть 0")
                 pass
        except ValueError as e:
            logger.warning(f"Admin {message.from_user.id} entered invalid channel ID '{channel_id_text}': {e}") # Логирование
            await message.answer("❌ Пожалуйста, введите корректный числовой ID канала (например, -100123456789) или '-'.", reply_markup=kb.cancel_state_keyboard())
            return

    await state.update_data(channel_id_to_check=channel_id)

    if channel_id is not None:
        # Сначала спрашиваем про проверку подписки
        await message.answer("❓ Требуется ли проверка подписки на этот канал для выполнения задания?",
                             reply_markup=kb.yes_no_keyboard("addtask_checksub"))
        await state.set_state(AddTaskState.waiting_for_check_subscription)
    else:
        # Если канал не указан, проверка подписки не нужна
        await state.update_data(check_subscription=False)
        # Сразу переходим к выбору Premium требования для задания
        await message.answer(
            "⭐️ Для кого предназначено это задание?",
            reply_markup=kb.admin_task_premium_options_keyboard() # Новая клавиатура
        )
        await state.set_state(AddTaskState.waiting_for_premium_requirement)


# Обработка выбора проверки подписки
@router.callback_query(F.data.startswith("addtask_checksub_"), AddTaskState.waiting_for_check_subscription)
async def process_task_check_subscription_choice(callback: CallbackQuery, config: Config, state: FSMContext):
    check_sub = callback.data.endswith("_yes")
    await state.update_data(check_subscription=check_sub)
    # После выбора проверки подписки, переходим к выбору Premium
    try:
        await callback.message.edit_text(
            "⭐️ Для кого предназначено это задание?",
            reply_markup=kb.admin_task_premium_options_keyboard() # Новая клавиатура
        )
    except Exception as e:
        logger.debug(f"Failed to edit message for task premium query: {e}")
        await callback.message.answer(
             "⭐️ Для кого предназначено это задание?",
             reply_markup=kb.admin_task_premium_options_keyboard()
        )
    await state.set_state(AddTaskState.waiting_for_premium_requirement)
    await callback.answer()

# --- НОВЫЙ обработчик для выбора Premium требования --- 
@router.callback_query(F.data.startswith("addtask_premium_"), AddTaskState.waiting_for_premium_requirement)
async def process_task_premium_requirement(callback: CallbackQuery, config: Config, state: FSMContext):
    # --- Исправлено снова: Используем карту для правильного значения --- 
    prefix = "addtask_premium_"
    extracted_part = None
    if callback.data.startswith(prefix):
        extracted_part = callback.data[len(prefix):] # Получаем 'all', 'only' или 'non_premium'
    
    if not extracted_part:
        logger.error(f"Unexpected callback data format in process_task_premium_requirement: {callback.data}")
        await callback.answer("Внутренняя ошибка.", show_alert=True)
        return
        
    # Карта для сопоставления извлеченной части с полным значением
    requirement_map = {
        "all": "all",
        "only": "premium_only",
        "non_premium": "non_premium_only"
    }
    
    premium_req = requirement_map.get(extracted_part)
    # ---------------------------------------------------------------------

    # --- Валидация --- # Теперь проверяем ПОЛНОЕ значение
    if premium_req is None: # Проверяем, было ли значение найдено в карте
        logger.warning(f"Invalid task premium requirement extracted part: {extracted_part} from {callback.data}")
        await callback.answer("Некорректный выбор. Пожалуйста, выберите из предложенных.", show_alert=True)
        return
    # --- Конец валидации ---

    await state.update_data(premium_requirement=premium_req) # Сохраняем полное значение

    # Теперь запрашиваем лимит выполнений
    await callback.message.edit_text(
        "📊 Введите максимальное количество выполнений для этого задания:\n\n"
        "💡 Например: 1000\n"
        "💡 По умолчанию: 1000000 (миллион выполнений)\n\n"
        "Просто отправьте число или нажмите 'По умолчанию'",
        reply_markup=kb.task_max_completions_keyboard()
    )
    await state.set_state(AddTaskState.waiting_for_max_completions)
    await callback.answer() # Отвечаем на коллбэк

# Обработчик для ввода лимита выполнений
@router.message(AddTaskState.waiting_for_max_completions)
async def process_task_max_completions(message: Message, config: Config, state: FSMContext):
    try:
        max_completions = int(message.text)
        if max_completions < 1:
            raise ValueError("Максимальное количество выполнений должно быть положительным числом")
        await state.update_data(max_completions=max_completions)
        
        # Переходим к выбору временного распределения
        await message.answer(
            "🕐 Хотите использовать временное распределение заданий?\n\n"
            "Временное распределение позволяет автоматически распределить выполнения задания неравномерно по часам.\n"
            "Например, для 20 выполнений на 24 часа: 3 в первый час, 1 во второй, 0 в третий и четвертый, затем 5 и т.д.",
            reply_markup=kb.time_distribution_choice_keyboard()
        )
        await state.set_state(AddTaskState.waiting_for_time_distribution)
        
    except ValueError as e:
        logger.warning(f"Admin {message.from_user.id} entered invalid max completions '{message.text}': {e}")
        await message.answer(f"❌ Ошибка: {e}. Пожалуйста, введите корректное ПОЛОЖИТЕЛЬНОЕ целое число для максимального количества выполнений.", reply_markup=kb.cancel_state_keyboard())

# Обработчик кнопки "По умолчанию" для лимита выполнений
@router.callback_query(F.data == "addtask_default_max_completions", AddTaskState.waiting_for_max_completions)
async def process_task_default_max_completions(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.update_data(max_completions=1000000)
    
    # Переходим к выбору временного распределения
    await callback.message.edit_text(
        "🕐 Хотите использовать временное распределение заданий?\n\n"
        "Временное распределение позволяет автоматически распределить выполнения задания неравномерно по часам.\n"
        "Например, для 20 выполнений на 24 часа: 3 в первый час, 1 во второй, 0 в третий и четвертый, затем 5 и т.д.",
        reply_markup=kb.time_distribution_choice_keyboard()
    )
    await state.set_state(AddTaskState.waiting_for_time_distribution)
    await callback.answer()

# Обработчик выбора временного распределения
@router.callback_query(F.data.startswith("time_dist_"), AddTaskState.waiting_for_time_distribution)
async def process_time_distribution_choice(callback: CallbackQuery, config: Config, state: FSMContext):
    choice = callback.data.split("_")[2]  # yes или no
    
    if choice == "yes":
        await state.update_data(use_time_distribution=True)
        await callback.message.edit_text(
            "⏰ Введите количество часов для распределения:\n\n"
            "Рекомендуемые значения:\n"
            "• 24 часа (1 день)\n"
            "• 48 часов (2 дня)\n"
            "• 72 часа (3 дня)\n\n"
            "Введите число от 1 до 168 (неделя) или используйте быстрый выбор:",
            reply_markup=kb.distribution_hours_keyboard()
        )
        await state.set_state(AddTaskState.waiting_for_distribution_hours)
    else:
        await state.update_data(use_time_distribution=False)
        user_data = await state.get_data()
        await show_task_confirmation(callback, state, user_data, edit=True)
    
    await callback.answer()

# Обработчик ввода количества часов
@router.message(AddTaskState.waiting_for_distribution_hours)
async def process_distribution_hours(message: Message, config: Config, state: FSMContext):
    if not message.text or not message.text.isdigit():
        await message.answer("❗️Пожалуйста, введите корректное число часов (например, 24, 48 или 72).")
        return
        
    hours = int(message.text)
    if hours <= 0:
        await message.answer("❗️Количество часов должно быть больше нуля.")
        return

    data = await state.get_data()
    max_completions = data.get('max_completions')

    if max_completions is not None and hours > max_completions:
        await message.answer(
            f"❗️<b>Ошибка:</b> Количество часов для распределения ({hours}) не может превышать "
            f"общее количество выполнений ({max_completions}).\n\n"
            "Пожалуйста, введите меньшее количество часов или вернитесь и увеличьте лимит выполнений."
        )
        return

    await state.update_data(distribution_hours=hours)
    data['distribution_hours'] = hours # Добавляем в локальный словарь для отображения
    
    await show_task_confirmation(message, state, data)

# Обработчик быстрого выбора часов
@router.callback_query(F.data.startswith("hours_"), AddTaskState.waiting_for_distribution_hours)
async def process_quick_hours_choice(callback: CallbackQuery, config: Config, state: FSMContext):
    hours_data = callback.data.split("_")[1]
    
    if hours_data == "custom":
        # Пользователь хочет ввести своё время
        await callback.message.edit_text(
            "⏰ Введите количество часов для распределения:\n\n"
            "Введите число от 1 до 168 (неделя):",
            reply_markup=kb.cancel_state_keyboard()
        )
        await callback.answer()
        return
    
    # Быстрый выбор предустановленного времени
    hours = int(hours_data)
    data = await state.get_data()
    max_completions = data.get('max_completions')

    if max_completions is not None and hours > max_completions:
        await callback.answer(
            f"Ошибка: Часов ({hours}) больше, чем выполнений ({max_completions}).",
            show_alert=True
        )
        return

    # И затем в user_data нужно добавить 'distribution_hours'
    await state.update_data(distribution_hours=hours)
    user_data = await state.get_data()
    user_data['distribution_hours'] = hours # <--- Вот эта строка
    await show_task_confirmation(callback, state, user_data, edit=True)
    await callback.answer()

# Функция для показа подтверждения (Обновляем)
async def show_task_confirmation(message_event: Message | CallbackQuery, state: FSMContext, data: dict, edit: bool = False):
    desc = data.get('description', 'N/A')
    reward = data.get('reward', 0.0)
    instruction = data.get('instruction_link', 'Нет')
    action = data.get('action_link', 'Нет')
    channel_id = data.get('channel_id_to_check', None)
    check_sub = data.get('check_subscription', False)
    premium_req = data.get('premium_requirement', 'all') # Получаем требование Premium
    max_completions = data.get('max_completions', 1000000) # Получаем лимит выполнений
    use_time_distribution = data.get('use_time_distribution', False)
    distribution_hours = data.get('distribution_hours', None)

    channel_info = ""
    if channel_id:
        channel_info += f"<b>Канал:</b> {channel_id}\n"
        # --- Добавим получение ссылки на канал, если ID известен ---
        channel_info += f"<b>Проверка подписки:</b> {'Да ✅' if check_sub else 'Нет ❌'}"
    else:
        channel_info += "<b>Канал:</b> Не указан (проверка подписки невозможна)"
         
    # Текст для отображения Premium требования
    premium_req_text_map = {
        'all': 'Всем пользователям',
        'premium_only': 'Только Premium [⭐️]',
        'non_premium_only': 'Только НЕ Premium [🚫⭐️]'
    }
    premium_req_display = premium_req_text_map.get(premium_req, premium_req)

    # Информация о временном распределении
    time_dist_info = ""
    if use_time_distribution and distribution_hours:
        time_dist_info = f"\n<b>⏰ Временное распределение:</b> Включено ({distribution_hours} ч.)"
    else:
        time_dist_info = "\n<b>⏰ Временное распределение:</b> Отключено"

    confirm_text = f"""
Проверьте данные нового задания:
-------------------------------------
<b>Описание:</b> {html.escape(desc[:100])}
<b>Награда:</b> {reward:.2f}⭐️
<b>Инструкция:</b> {html.escape(instruction) if instruction and instruction != 'Нет' else 'Нет'}
<b>Ссылка 'Выполнить':</b> {html.escape(action) if action and action != 'Нет' else 'Нет'}
{channel_info}
<b>Доступность:</b> {premium_req_display}
<b>Максимальное количество выполнений:</b> {max_completions}{time_dist_info}
-------------------------------------
Сохранить это задание?
    """
    reply_markup = kb.yes_no_keyboard("addtask_confirm")

    target_message = message_event.message if isinstance(message_event, CallbackQuery) else message_event

    if edit:
        try:
            await target_message.edit_text(confirm_text, reply_markup=reply_markup)
        except Exception as e:
            logger.debug(f"Не удалось отредактировать сообщение подтверждения задачи: {e}")
            # Если не удалось отредактировать (например, текст не изменился), попробуем отправить новым
            try:
                await target_message.answer(confirm_text, reply_markup=reply_markup)
            except Exception as send_e:
                logger.error(f"Не удалось отправить новое сообщение подтверждения задачи: {send_e}")
    else:
        await target_message.answer(confirm_text, reply_markup=reply_markup)

    await state.set_state(AddTaskState.confirming)


# Обработка подтверждения или отмены сохранения (Обновляем)
@router.callback_query(AddTaskState.confirming)
async def process_task_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    user_id = callback.from_user.id # Для логирования
    if callback.data.endswith("_yes"):
        user_data = await state.get_data()
        try:
            use_time_distribution = user_data.get('use_time_distribution', False)
            
            if use_time_distribution and user_data.get('distribution_hours'):
                # Создаем задание с временным распределением
                new_task = await db.create_time_distributed_task(
                    session=session,
                    description=user_data.get('description'),
                    reward=user_data.get('reward'),
                    instruction_link=user_data.get('instruction_link'),
                    action_link=user_data.get('action_link'),
                    channel_id_to_check=user_data.get('channel_id_to_check'),
                    check_subscription=user_data.get('check_subscription'),
                    premium_requirement=user_data.get('premium_requirement', 'all'),
                    max_completions=user_data.get('max_completions', 1000000),
                    distribution_hours=user_data.get('distribution_hours')
                )
                logger.info(f"Admin {user_id} successfully added time-distributed task #{new_task.id} with {user_data.get('distribution_hours')}h distribution")
                await callback.message.edit_text(f"✅ Новое задание с временным распределением #{new_task.id} успешно добавлено!\n⏰ Распределение на {user_data.get('distribution_hours')} часов.")
            else:
                # Создаем обычное задание
                new_task = await db.add_task(
                    session=session,
                    description=user_data.get('description'),
                    reward=user_data.get('reward'),
                    instruction_link=user_data.get('instruction_link'),
                    action_link=user_data.get('action_link'),
                    channel_id_to_check=user_data.get('channel_id_to_check'),
                    check_subscription=user_data.get('check_subscription'),
                    premium_requirement=user_data.get('premium_requirement', 'all'),
                    max_completions=user_data.get('max_completions', 1000000)
                )
                logger.info(f"Admin {user_id} successfully added regular task #{new_task.id}")
                await callback.message.edit_text(f"✅ Новое задание #{new_task.id} успешно добавлено!")
            
            # --- Добавляем коммит --- 
            await session.commit()
            
        except Exception as e:
            # --- Добавляем откат --- 
            await session.rollback()
            logger.error(f"Admin {user_id} failed to add task: {e}", exc_info=True) # Логирование с traceback
            await callback.message.edit_text("❌ Произошла ошибка при сохранении задания.")
        finally:
             await state.clear()

    elif callback.data.endswith("_no"):
        logger.info(f"Admin {user_id} cancelled task addition at confirmation step.") # Логирование
        await state.clear()
        await callback.message.edit_text("Добавление задания отменено.")

    await callback.answer()

# --- Конец Управления Заданиями ---

# --- Управление Индивидуальными Ссылками --- 

# Кнопка "Индивидуальные ссылки" - ТЕПЕРЬ ПОКАЗЫВАЕТ МЕНЮ
@router.callback_query(F.data == "admin_manage_ind_links")
@admin_required
async def admin_manage_ind_links(callback: CallbackQuery, config: Config, session: AsyncSession):
    message_text = "🔗 Специальные реферальные ссылки:"
    reply_markup = kb.admin_ind_links_menu_keyboard()
    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение на меню инд. ссылок: {e}")
    await callback.answer()

# НОВЫЙ обработчик для кнопки "Список ссылок / Статистика"
@router.callback_query(F.data == "admin_view_ind_links_list")
async def admin_view_ind_links_list(callback: CallbackQuery, config: Config, session: AsyncSession):
    links = await db.get_all_individual_links(session)
    links_with_stats = []
    message_text = "📊 Список ссылок и статистика: \n"
    if links:
        for link in links:
            total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
            op_div_3 = passed_op // 3 if passed_op >= 3 else 0
            identifier_short = (link.identifier[:20] + '...') if len(link.identifier) > 20 else link.identifier
            message_text += (
                f"- <code>{html.escape(identifier_short)}</code> "
                f"(Reg: {total_reg} / OP: {op_div_3})\n"
            )
            links_with_stats.append((link, total_reg, passed_op)) # Собираем для клавиатуры
    else:
        message_text = "🔗 Индивидуальных ссылок пока нет."

    reply_markup = kb.admin_ind_links_list_keyboard(links_with_stats)

    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение со списком инд. ссылок: {e}")
        # Если ошибка парсинга, попробуем без HTML
        try:
             await callback.message.edit_text(message_text.replace('<code>', '').replace('</code>', ''), reply_markup=reply_markup)
        except Exception as e2:
             logger.error(f"Не удалось отправить список инд. ссылок даже без HTML: {e2}")

    await callback.answer()

# Просмотр конкретной ссылки
@router.callback_query(F.data.startswith("admin_ind_link_view_"))
async def admin_view_single_ind_link(callback: CallbackQuery, config: Config, session: AsyncSession):
    link_id = int(callback.data.split("_")[4])
    link = await db.get_individual_link_by_id(session, link_id)
    if not link:
        await callback.answer("❌ Ссылка не найдена.", show_alert=True)
        await admin_view_ind_links_list(callback, session) # Обновляем список, если ссылка пропала
        return

    total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
    op_div_3 = passed_op // 3 if passed_op >= 3 else 0

    link_details = f"""
🔗 <b>Индивидуальная ссылка:</b> <code>{html.escape(link.identifier)}</code>
-------------------------------------
<b>Описание:</b> {html.escape(link.description or '(нет)')}
<b>ID:</b> {link.id}
<b>Создана:</b> {link.created_at.strftime('%Y-%m-%d %H:%M')}
-------------------------------------
<b>Статистика:</b>
  Зарегистрировано: {total_reg}
  Прошли ОП: {passed_op}
  (Прошли ОП / 3): {op_div_3}
-------------------------------------
<b>Ссылка для пользователя:</b>
<code>https://t.me/{ (await callback.bot.get_me()).username }?start={html.escape(link.identifier)}</code>
<b>Ссылка для INFO:</b>
<code>https://t.me/{ (await callback.bot.get_me()).username }?start=INFO_{html.escape(link.identifier)}</code>
"""
from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import html # <-- Добавляем импорт html
import bot.database.models as mdl
import re
import os
import tempfile
import subprocess
import asyncio
from bot.core.utils.logging import logger
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton # Добавляем InlineKeyboardMarkup, InlineKeyboardButton
import bot.database.requests as db
import bot.keyboards.keyboards as kb
from bot.core.config import Config
import bot.core.utils.state as st
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from bot.core.config import config # Импортируем сам объект config
from bot.core.utils.state import AddTaskState, GiftSettingsState, AddIndividualLinkState, AddChannelState, RewardSettingsState, AdminManageUser, AdminTemplateStates, NewsletterStates, PromoCodeStateTemplate
from typing import Optional, List, Union
from bot.database.models import User, IndividualLink, Channel
from bot.keyboards import keyboards as kb # Убедитесь, что клавиатуры импортированы
from bot.database import requests as db # Убедитесь, что запросы к БД импортированы
from bot.core.utils.state import AdminTemplateStates, AddShowState
import json
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InputMediaPhoto
from bot.core.utils.utils import run_newsletter_with_auto_cleanup # Предполагаем, что функция запуска здесь
from bot.database.models import BroadcastTemplate # Импорт модели
from bot.database.models import Channel # Убедитесь в импорте
from bot.core.utils.state import AddChannelState # Импорт состояний для добавления канала
from datetime import datetime
from functools import wraps

from bot.database.requests import get_subgram_webhooks_stats

from bot.database.requests import (
    get_comprehensive_daily_tasks_stats,
    get_daily_tasks_monthly_stats,
    get_user_daily_tasks_history,
    get_user_by_id
)
from bot.keyboards.keyboards import (
    admin_daily_tasks_stats_keyboard
)

# Декоратор для проверки прав администратора
def admin_required(func):
    @wraps(func)
    async def wrapper(event: Union[Message, CallbackQuery], *args, **kwargs):
        user_id = event.from_user.id
        
        # Получаем конфиг из аргументов функции
        config = None
        for arg in args:
            if isinstance(arg, Config):
                config = arg
                break
                
        if 'config' in kwargs:
            config = kwargs['config']
            
        # Если конфига нет в аргументах, пытаемся найти сессию и загрузить конфиг
        if not config and ('session' in kwargs or any(isinstance(arg, AsyncSession) for arg in args)):
            # Здесь можно было бы загрузить конфиг из базы данных,
            # но поскольку конфиг обычно передается в функцию, этот случай маловероятен
            logger.error(f"Конфиг не найден в аргументах функции {func.__name__}")
            
            # Показываем сообщение об ошибке
            if isinstance(event, Message):
                await event.answer("❌ Ошибка доступа: не удалось проверить права администратора")
            else:
                await event.answer("❌ Ошибка доступа", show_alert=True)
            return
            
        # Проверяем права администратора
        if config and user_id in config.admin_ids:
            return await func(event, *args, **kwargs)
        else:
            # Показываем сообщение о нехватке прав
            if isinstance(event, Message):
                await event.answer("❌ У вас недостаточно прав для выполнения этой команды")
            else:
                await event.answer("❌ У вас недостаточно прав", show_alert=True)
            return
            
    return wrapper

router = Router()

# --- Главная Админ-панель ---

@router.message(Command("cancel"))
async def cancel_handler(message: Message, config: Config, state: FSMContext):
    await state.clear()
    await message.answer("Состояния успешно сброшены.")


@router.message(Command("admin"))
@router.message(F.text == "Админ панель 👑")
@router.callback_query(F.data == "admin_back_to_main")
@admin_required
async def cmd_admin_panel(event: Message | CallbackQuery, config: Config, state: FSMContext):
    text = "⚙️ Админ-панель:"
    reply_markup = kb.admin_main_keyboard()

    if isinstance(event, Message):
        await event.answer(text, reply_markup=reply_markup)
    elif isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, reply_markup=reply_markup)
        except Exception as e:
            logger.debug(f"Не удалось изменить сообщение на главную админку: {e}")
        await event.answer()
    await state.clear()

@router.message(Command("admin_download_users"))
@router.callback_query(F.data == "admin_download_users")
@admin_required
async def admin_download_users(message: Message | CallbackQuery, bot: Bot, session: AsyncSession, config: Config):
    """
    Хендлер для команды /admin_download_users.
    Доступен только администраторам.
    Собирает все ID пользователей из БД, сохраняет в .txt файл и отправляет его администратору.
    """
    # Определяем user_id и chat_id универсально
    # (Хотя для @router.message это всегда будет message, сделаем на всякий случай)
    if isinstance(message, CallbackQuery): # Проверка на случай, если сюда попал CallbackQuery
        user_id = message.from_user.id
        chat_id = message.message.chat.id
        event_source = message.message # Используем .message для ответа в тот же чат
    elif isinstance(message, Message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        event_source = message # Используем сам message для ответа
    else:
        logger.error(f"Unknown event type received in admin_download_users: {type(message)}")
        return # Не можем обработать неизвестный тип

    # 1. Проверка прав администратора
    if user_id not in config.admin_ids:
        logger.warning(f"User {user_id} tried to use /admin_download_users without permission.")
        # Можно ничего не отвечать, чтобы не раскрывать команду
        # Или отправить сообщение: await message.reply("У вас нет прав для выполнения этой команды.")
        return

    logger.info(f"Admin {user_id} requested user ID list download.")
    temp_file_path = None # Инициализируем переменную для пути к файлу

    try:
        # 2. Получение ID пользователей из БД
        user_ids = await db.get_all_user_ids(session)

        if not user_ids:
            # Используем event_source для ответа
            await event_source.answer("В базе данных пока нет пользователей.")
            logger.info(f"No users found in DB for admin {user_id}.")
            return

        # 3. Создание временного файла и запись ID
        # Используем tempfile для безопасного создания временного файла
        fd, temp_file_path = tempfile.mkstemp(suffix=".txt", text=True)

        with os.fdopen(fd, 'w', encoding='utf-8') as tmp_file:
            for uid in user_ids:
                tmp_file.write(f"{uid}\n") # Записываем каждый ID на новой строке

        logger.info(f"Created temporary file {temp_file_path} with {len(user_ids)} user IDs for admin {user_id}.")

        # 4. Отправка файла администратору
        file_to_send = FSInputFile(temp_file_path, filename="user_ids.txt")
        await bot.send_document(
            chat_id=chat_id, # Используем chat_id
            document=file_to_send,
            caption=f"📄 Список ID всех пользователей ({len(user_ids)} шт.)"
        )
        logger.info(f"Successfully sent user ID list file to admin {user_id}.")

    except Exception as e:
        logger.error(f"Error during /admin_download_users for admin {user_id}: {e}", exc_info=True)
        # Используем bot.send_message с chat_id вместо message.reply
        try:
            await bot.send_message(
                chat_id=chat_id, # Отправляем в нужный чат
                text="❌ Произошла ошибка при формировании или отправке файла. Пожалуйста, проверьте логи."
            )
        except Exception as send_error:
             logger.error(f"Failed to send error message to chat {chat_id}: {send_error}")

    finally:
        # 5. Удаление временного файла (даже если была ошибка)
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.debug(f"Removed temporary file: {temp_file_path}")
            except OSError as e:
                logger.error(f"Error removing temporary file {temp_file_path}: {e}")

# Обработчик кнопки "Статистика"
@router.callback_query(F.data == "admin_show_stats")
@admin_required
async def admin_show_stats_callback(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        await callback.answer()
        result = await session.execute(select(User))
        users = result.scalars().all()
        total_users = len(users)

        # Подсчитываем количество пользователей, прошедших ОП
        ref_bonus_users = sum(1 for user in users if user.ref_bonus)

        # Подсчитываем количество новых пользователей за сегодня
        today = datetime.utcnow().date()
        new_users_today = [user for user in users if user.registered_at.date() == today]
        total_new_users_today = len(new_users_today)

        # Подсчитываем количество новых пользователей, прошедших ОП
        new_users_with_ref_bonus = sum(1 for user in new_users_today if user.ref_bonus)

        # Подсчитываем количество пользователей, пришедших по реферальной ссылке (саморост)
        invited_by_ref = sum(1 for user in users if user.refferal_id)
        percent_by_ref = (invited_by_ref / total_users * 100)

        await callback.message.edit_text(
            f"📊 Всего пользователей: {total_users}\n"
            f"✅ Пользователей, прошедших ОП: {ref_bonus_users}\n"
            f"🆕 Новых пользователей сегодня: {total_new_users_today}\n"
            f"✅ Новых пользователей, прошедших ОП сегодня: {new_users_with_ref_bonus}\n"
            f"🌱 Приглашено по реф. системе (саморост): {invited_by_ref} ({percent_by_ref:.1f}%)",
            show_alert=True, 
            reply_markup=kb.back_stats_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in stats callback handler: {e}")

@router.callback_query(F.data == "admin_manage_tasks")
@admin_required
async def admin_manage_tasks(callback: CallbackQuery, config: Config, session: AsyncSession):
    tasks_with_counts = await db.get_all_tasks_with_completion_count(session)

    # --- Формируем текстовое представление списка с ограничением длины --- 
    tasks_list_lines = []
    if tasks_with_counts:
        # Ограничиваем количество заданий для отображения в тексте
        max_tasks_to_show = 15  # Максимум 15 заданий в тексте
        tasks_to_show = tasks_with_counts[:max_tasks_to_show]
        
        for task, count in tasks_to_show:
            status_icon = "🟢" if task.is_active else "🔴"
            display_name = task.action_link if task.action_link else task.description
            display_name_short = (display_name[:20] + '...') if len(display_name) > 20 else display_name
            # --- Экранируем HTML перед вставкой в текст --- 
            escaped_display_name = html.escape(display_name_short)
            # ----------------------------------------------
            tasks_list_lines.append(f"{status_icon} #{task.id} - {escaped_display_name} ({count} вып.)")
        
        tasks_list_text = "\n".join(tasks_list_lines)
        
        # Добавляем информацию о количестве заданий
        total_tasks = len(tasks_with_counts)
        if total_tasks > max_tasks_to_show:
            message_text = f"📄 Список заданий (показано {max_tasks_to_show} из {total_tasks}):\n\n{tasks_list_text}\n\n... и еще {total_tasks - max_tasks_to_show} заданий"
        else:
            message_text = f"📄 Список всех заданий ({total_tasks}):\n\n{tasks_list_text}"
    else:
        message_text = "📄 Заданий пока нет."
    # ---------------------------------------------

    # --- Клавиатуру оставляем для навигации и добавления --- 
    reply_markup = kb.admin_tasks_list_keyboard_paginated(tasks_with_counts, total_tasks=len(tasks_with_counts), page=0)

    try:
        # Отправляем сообщение с текстом списка и клавиатурой
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение со списком задач: {e}")
        # Если не удалось отредактировать, попробуем отправить новым
        try:
             await callback.message.answer(message_text, reply_markup=reply_markup)
             # Старое сообщение можно попробовать удалить, но необязательно
             # await callback.message.delete()
        except Exception as send_err:
             logger.error(f"Не удалось отправить новое сообщение со списком задач: {send_err}")

    await callback.answer()


@router.callback_query(F.data == "admin_check_task_limits")
@admin_required
async def admin_check_task_limits(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Проверка лимитов заданий"""
    await callback.message.edit_text(
        "🔍 <b>Проверка лимитов заданий</b>\n\n"
        "Введите ID задания для проверки лимитов:",
        reply_markup=kb.admin_back_to_main()
    )
    
    # Устанавливаем состояние для ожидания ID задания
    await callback.answer()

@router.callback_query(F.data.startswith("admin_tasks_page_"))
@admin_required
async def admin_tasks_page_navigation(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Навигация по страницам списка заданий"""
    try:
        page = int(callback.data.split("_")[-1])
        tasks_with_counts = await db.get_all_tasks_with_completion_count(session)
        
        # Формируем текст для текущей страницы
        max_tasks_to_show = 15
        tasks_to_show = tasks_with_counts[:max_tasks_to_show] if tasks_with_counts else []
        
        tasks_list_lines = []
        if tasks_to_show:
            for task, count in tasks_to_show:
                status_icon = "🟢" if task.is_active else "🔴"
                display_name = task.action_link if task.action_link else task.description
                display_name_short = (display_name[:20] + '...') if len(display_name) > 20 else display_name
                escaped_display_name = html.escape(display_name_short)
                tasks_list_lines.append(f"{status_icon} #{task.id} - {escaped_display_name} ({count} вып.)")
            
            tasks_list_text = "\n".join(tasks_list_lines)
            total_tasks = len(tasks_with_counts)
            if total_tasks > max_tasks_to_show:
                message_text = f"📄 Список заданий (показано {max_tasks_to_show} из {total_tasks}):\n\n{tasks_list_text}\n\n... и еще {total_tasks - max_tasks_to_show} заданий"
            else:
                message_text = f"📄 Список всех заданий ({total_tasks}):\n\n{tasks_list_text}"
        else:
            message_text = "📄 Заданий пока нет."
        
        # Создаем клавиатуру для текущей страницы
        reply_markup = kb.admin_tasks_list_keyboard_paginated(
            tasks_with_counts, 
            total_tasks=len(tasks_with_counts) if tasks_with_counts else 0, 
            page=page
        )
        
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
        
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing page number from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка при навигации по страницам.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in admin_tasks_page_navigation: {e}")
        await callback.answer("❌ Произошла ошибка при загрузке страницы.", show_alert=True)
    
    await callback.answer()

# Просмотр конкретного задания (остается без изменений, т.к. вызывается из списка)
@router.callback_query(F.data.startswith("admin_task_view_"))
@admin_required
async def admin_view_single_task(callback: CallbackQuery, config: Config, session: AsyncSession):
    task_id = int(callback.data.split("_")[3])
    task = await db.get_task_by_id(session, task_id)
    if not task:
        await callback.answer("❌ Задание не найдено.", show_alert=True)
        # Вместо вызова admin_view_tasks, эмулируем нажатие кнопки "Управление заданиями"
        await admin_manage_tasks(callback, config, session)
        return

    status = "Активно 🟢" if task.is_active else "Неактивно 🔴"
    check_sub = "Да" if task.check_subscription else "Нет"
    channel = task.channel_id_to_check if task.channel_id_to_check else "Нет"
    instruction = task.instruction_link if task.instruction_link else "Нет"
    action = task.action_link if task.action_link else "Нет"

    # Добавляем информацию о лимите выполнений
    max_completions = getattr(task, 'max_completions', 1000000)
    current_completions = await db.get_task_actual_completions_count(session, task_id)
    
    # Добавляем информацию о временном распределении
    time_distribution_info = ""
    if getattr(task, 'is_time_distributed', False):
        distribution_hours = getattr(task, 'time_distribution_hours', 0)
        current_hour_limit = await db.get_current_hour_limit(session, task_id)
        time_distribution_info = f"""
<b>Временное распределение:</b> Включено ⏰
<b>Период распределения:</b> {distribution_hours} часов
<b>Лимит текущего часа:</b> {current_hour_limit}"""
    else:
        time_distribution_info = "\n<b>Временное распределение:</b> Отключено"
    
    task_details = f"""
📝 <b>Задание #{task.id}</b> [{status}]
-------------------------------------
<b>Описание:</b>
{task.description}
-------------------------------------
<b>Награда:</b> {task.reward:.2f}⭐️
<b>Инструкция:</b> {instruction}
<b>Ссылка действия:</b> {action}
<b>Проверка подписки:</b> {check_sub}
<b>Канал для проверки:</b> {channel}
<b>Лимит выполнений:</b> {current_completions}/{max_completions}{time_distribution_info}
"""
    try:
        # Используем обновленную клавиатуру с правильной кнопкой назад
        await callback.message.edit_text(task_details, reply_markup=kb.admin_task_manage_keyboard(task))
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение при просмотре задачи {task_id}: {e}")
    await callback.answer()


# Переключение статуса активности задания (остается без изменений)
@router.callback_query(F.data.startswith("admin_task_toggle_"))
async def admin_toggle_task_status(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        task_id = int(callback.data.split("_")[-1]) # Изменено с split("_")[3]
        task = await db.get_task_by_id(session, task_id)
        if not task:
            await callback.answer("❌ Задание не найдено.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
            return

        new_status = not task.is_active
        updated = await db.set_task_active_status(session, task_id, new_status)

        if updated:
            status_text = "активировано" if new_status else "деактивировано"
            await callback.answer(f"✅ Задание #{task_id} {status_text}.")
            await admin_view_single_task(callback, config, session)
        else:
            await callback.answer("❌ Не удалось изменить статус задания.", show_alert=True)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing task_id from toggle callback: {callback.data}")
        await callback.answer("❌ Ошибка обработки запроса.", show_alert=True)


# Запрос на удаление задания (остается без изменений)
@router.callback_query(F.data.startswith("admin_task_delete_"))
async def admin_delete_task_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        task_id = int(callback.data.split("_")[-1])
        task = await db.get_task_by_id(session, task_id)
        if not task:
            await callback.answer("❌ Задание не найдено.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
            return

        text = (f"🗑️ Вы уверены, что хотите удалить Задание #{task_id}?\n\n"
                f"Описание: {task.description[:50]}...\n\n"
                f"⚠️ Это действие необратимо!")
        markup = kb.admin_task_delete_confirm_keyboard(task_id)
        
        try:
            await callback.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                # Ничего не делаем, сообщение уже имеет нужный контент
                pass
            else:
                # Логируем другие ошибки
                logger.debug(f"Ошибка при изменении сообщения для задачи {task_id}: {e}")
        
        await callback.answer()
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing task_id from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка при обработке ID задания.", show_alert=True)


# Подтверждение удаления задания - изменить формат обработки ID
@router.callback_query(F.data.startswith("confirm_admin_task_delete_"))
async def admin_delete_task_confirm(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        task_id = int(callback.data.split("_")[4]) # Изменено с split("_")[4]
        deleted = await db.delete_task_by_id(session, task_id)

        if deleted:
            await callback.answer(f"✅ Задание #{task_id} успешно удалено.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
        else:
            await callback.answer("❌ Не удалось удалить задание.", show_alert=True)
            await admin_manage_tasks(callback, config, session)
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing task_id from '{callback.data}': {e}")
        await callback.answer("❌ Ошибка при обработке ID задания.", show_alert=True)
        await admin_manage_tasks(callback, config, session) # Возвращаемся к списку

@router.callback_query(F.data == "admin_add_task", StateFilter(None))
async def admin_add_task_start(callback: CallbackQuery, config: Config, state: FSMContext):
    logger.info(f"Admin {callback.from_user.id} starting to add a new task.") # Логирование
    message_text = "✍️ Введите описание нового задания (можно использовать HTML):"
    reply_markup = kb.cancel_state_keyboard()
    await callback.message.answer(message_text, reply_markup=reply_markup)
    await callback.answer()
    await state.set_state(AddTaskState.waiting_for_description)


# Обработчик отмены состояния
@router.callback_query(F.data == "cancel_state", StateFilter("*"))
async def cancel_handler(callback: CallbackQuery, config: Config, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await callback.answer("Нет активного действия для отмены.")
        return

    logger.info(f"Admin {callback.from_user.id} cancelled state {current_state}") # Логирование
    await state.clear()
    try:
        await callback.message.edit_text("Действие отменено.")
    except:
        await callback.answer("Действие отменено.")


# Получение описания
@router.message(AddTaskState.waiting_for_description)
async def process_task_description(message: Message, config: Config, state: FSMContext):
    await state.update_data(description=message.text)
    await message.answer("💰 Введите награду за выполнение (число, например 1.25 или 5):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddTaskState.waiting_for_reward)

# Получение награды
@router.message(AddTaskState.waiting_for_reward)
async def process_task_reward(message: Message, config: Config, state: FSMContext):
    try:
        reward = float(message.text.replace(',', '.'))
        if reward < 0:
             raise ValueError("Награда не может быть отрицательной")
        await state.update_data(reward=reward)
        await message.answer("🔗 Введите ссылку на инструкцию (если нет, введите '-'):", reply_markup=kb.cancel_state_keyboard())
        await state.set_state(AddTaskState.waiting_for_instruction_link)
    except ValueError as e:
        logger.warning(f"Admin {message.from_user.id} entered invalid reward '{message.text}': {e}") # Логирование
        await message.answer(f"❌ Ошибка: {e}. Пожалуйста, введите корректное ПОЛОЖИТЕЛЬНОЕ число для награды (например, 1.25 или 5).", reply_markup=kb.cancel_state_keyboard())


# Получение ссылки на инструкцию
@router.message(AddTaskState.waiting_for_instruction_link)
async def process_task_instruction_link(message: Message, config: Config, state: FSMContext):
    link = message.text if message.text != '-' else None
    await state.update_data(instruction_link=link)
    await message.answer("↗️ Введите ссылку для кнопки 'Выполнить' (если не нужна, введите '-'):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddTaskState.waiting_for_action_link)


# Получение ссылки для кнопки "Выполнить"
@router.message(AddTaskState.waiting_for_action_link)
async def process_task_action_link(message: Message, config: Config, state: FSMContext):
    link = message.text if message.text != '-' else None
    await state.update_data(action_link=link)
    await message.answer("🆔 Введите ID канала для задания (если не требуется, введите '-'):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddTaskState.waiting_for_channel_id)


# Получение ID канала (ОПЦИОНАЛЬНО)
@router.message(AddTaskState.waiting_for_channel_id)
async def process_task_channel_id(message: Message, config: Config, state: FSMContext):
    channel_id_text = message.text
    channel_id = None
    check_subscription = False

    if channel_id_text != '-':
        try:
            channel_id = int(channel_id_text)
            # --- Добавлена проверка на положительное значение --- 
            if channel_id >= 0: # ID каналов обычно отрицательные, чатов положительные. Разрешим только отрицательные? Или все кроме 0?
                 # Пока оставим так: разрешаем любые ненулевые. TG ID могут быть > int32, но BigInt в модели должен справиться.
                 # Если нужны только каналы, можно проверять channel_id < 0
                 pass # ID подходит
            else:
                 # Если нужны только каналы/супергруппы, они начинаются с -100...
                 # Если нужны и обычные чаты/боты, они положительные
                 # Пока допустим любые целые числа, кроме 0, если такая логика нужна.
                 # Для простоты сейчас уберем проверку на знак, оставим только ValueError
                 # Если нужно строже: if channel_id == 0: raise ValueError("ID канала не может быть 0")
                 pass
        except ValueError as e:
            logger.warning(f"Admin {message.from_user.id} entered invalid channel ID '{channel_id_text}': {e}") # Логирование
            await message.answer("❌ Пожалуйста, введите корректный числовой ID канала (например, -100123456789) или '-'.", reply_markup=kb.cancel_state_keyboard())
            return

    await state.update_data(channel_id_to_check=channel_id)

    if channel_id is not None:
        # Сначала спрашиваем про проверку подписки
        await message.answer("❓ Требуется ли проверка подписки на этот канал для выполнения задания?",
                             reply_markup=kb.yes_no_keyboard("addtask_checksub"))
        await state.set_state(AddTaskState.waiting_for_check_subscription)
    else:
        # Если канал не указан, проверка подписки не нужна
        await state.update_data(check_subscription=False)
        # Сразу переходим к выбору Premium требования для задания
        await message.answer(
            "⭐️ Для кого предназначено это задание?",
            reply_markup=kb.admin_task_premium_options_keyboard() # Новая клавиатура
        )
        await state.set_state(AddTaskState.waiting_for_premium_requirement)


# Обработка выбора проверки подписки
@router.callback_query(F.data.startswith("addtask_checksub_"), AddTaskState.waiting_for_check_subscription)
async def process_task_check_subscription_choice(callback: CallbackQuery, config: Config, state: FSMContext):
    check_sub = callback.data.endswith("_yes")
    await state.update_data(check_subscription=check_sub)
    # После выбора проверки подписки, переходим к выбору Premium
    try:
        await callback.message.edit_text(
            "⭐️ Для кого предназначено это задание?",
            reply_markup=kb.admin_task_premium_options_keyboard() # Новая клавиатура
        )
    except Exception as e:
        logger.debug(f"Failed to edit message for task premium query: {e}")
        await callback.message.answer(
             "⭐️ Для кого предназначено это задание?",
             reply_markup=kb.admin_task_premium_options_keyboard()
        )
    await state.set_state(AddTaskState.waiting_for_premium_requirement)
    await callback.answer()

# --- НОВЫЙ обработчик для выбора Premium требования --- 
@router.callback_query(F.data.startswith("addtask_premium_"), AddTaskState.waiting_for_premium_requirement)
async def process_task_premium_requirement(callback: CallbackQuery, config: Config, state: FSMContext):
    # --- Исправлено снова: Используем карту для правильного значения --- 
    prefix = "addtask_premium_"
    extracted_part = None
    if callback.data.startswith(prefix):
        extracted_part = callback.data[len(prefix):] # Получаем 'all', 'only' или 'non_premium'
    
    if not extracted_part:
        logger.error(f"Unexpected callback data format in process_task_premium_requirement: {callback.data}")
        await callback.answer("Внутренняя ошибка.", show_alert=True)
        return
        
    # Карта для сопоставления извлеченной части с полным значением
    requirement_map = {
        "all": "all",
        "only": "premium_only",
        "non_premium": "non_premium_only"
    }
    
    premium_req = requirement_map.get(extracted_part)
    # ---------------------------------------------------------------------

    # --- Валидация --- # Теперь проверяем ПОЛНОЕ значение
    if premium_req is None: # Проверяем, было ли значение найдено в карте
        logger.warning(f"Invalid task premium requirement extracted part: {extracted_part} from {callback.data}")
        await callback.answer("Некорректный выбор. Пожалуйста, выберите из предложенных.", show_alert=True)
        return
    # --- Конец валидации ---

    await state.update_data(premium_requirement=premium_req) # Сохраняем полное значение

    # Теперь запрашиваем лимит выполнений
    await callback.message.edit_text(
        "📊 Введите максимальное количество выполнений для этого задания:\n\n"
        "💡 Например: 1000\n"
        "💡 По умолчанию: 1000000 (миллион выполнений)\n\n"
        "Просто отправьте число или нажмите 'По умолчанию'",
        reply_markup=kb.task_max_completions_keyboard()
    )
    await state.set_state(AddTaskState.waiting_for_max_completions)
    await callback.answer() # Отвечаем на коллбэк

# Обработчик для ввода лимита выполнений
@router.message(AddTaskState.waiting_for_max_completions)
async def process_task_max_completions(message: Message, config: Config, state: FSMContext):
    try:
        max_completions = int(message.text)
        if max_completions < 1:
            raise ValueError("Максимальное количество выполнений должно быть положительным числом")
        await state.update_data(max_completions=max_completions)
        
        # Переходим к выбору временного распределения
        await message.answer(
            "🕐 Хотите использовать временное распределение заданий?\n\n"
            "Временное распределение позволяет автоматически распределить выполнения задания неравномерно по часам.\n"
            "Например, для 20 выполнений на 24 часа: 3 в первый час, 1 во второй, 0 в третий и четвертый, затем 5 и т.д.",
            reply_markup=kb.time_distribution_choice_keyboard()
        )
        await state.set_state(AddTaskState.waiting_for_time_distribution)
        
    except ValueError as e:
        logger.warning(f"Admin {message.from_user.id} entered invalid max completions '{message.text}': {e}")
        await message.answer(f"❌ Ошибка: {e}. Пожалуйста, введите корректное ПОЛОЖИТЕЛЬНОЕ целое число для максимального количества выполнений.", reply_markup=kb.cancel_state_keyboard())

# Обработчик кнопки "По умолчанию" для лимита выполнений
@router.callback_query(F.data == "addtask_default_max_completions", AddTaskState.waiting_for_max_completions)
async def process_task_default_max_completions(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.update_data(max_completions=1000000)
    
    # Переходим к выбору временного распределения
    await callback.message.edit_text(
        "🕐 Хотите использовать временное распределение заданий?\n\n"
        "Временное распределение позволяет автоматически распределить выполнения задания неравномерно по часам.\n"
        "Например, для 20 выполнений на 24 часа: 3 в первый час, 1 во второй, 0 в третий и четвертый, затем 5 и т.д.",
        reply_markup=kb.time_distribution_choice_keyboard()
    )
    await state.set_state(AddTaskState.waiting_for_time_distribution)
    await callback.answer()

# Обработчик выбора временного распределения
@router.callback_query(F.data.startswith("time_dist_"), AddTaskState.waiting_for_time_distribution)
async def process_time_distribution_choice(callback: CallbackQuery, config: Config, state: FSMContext):
    choice = callback.data.split("_")[2]  # yes или no
    
    if choice == "yes":
        await state.update_data(use_time_distribution=True)
        await callback.message.edit_text(
            "⏰ Введите количество часов для распределения:\n\n"
            "Рекомендуемые значения:\n"
            "• 24 часа (1 день)\n"
            "• 48 часов (2 дня)\n"
            "• 72 часа (3 дня)\n\n"
            "Введите число от 1 до 168 (неделя) или используйте быстрый выбор:",
            reply_markup=kb.distribution_hours_keyboard()
        )
        await state.set_state(AddTaskState.waiting_for_distribution_hours)
    else:
        await state.update_data(use_time_distribution=False)
        user_data = await state.get_data()
        await show_task_confirmation(callback, state, user_data, edit=True)
    
    await callback.answer()

# Обработчик ввода количества часов
@router.message(AddTaskState.waiting_for_distribution_hours)
async def process_distribution_hours(message: Message, config: Config, state: FSMContext):
    if not message.text or not message.text.isdigit():
        await message.answer("❗️Пожалуйста, введите корректное число часов (например, 24, 48 или 72).")
        return
        
    hours = int(message.text)
    if hours <= 0:
        await message.answer("❗️Количество часов должно быть больше нуля.")
        return

    data = await state.get_data()
    max_completions = data.get('max_completions')

    if max_completions is not None and hours > max_completions:
        await message.answer(
            f"❗️<b>Ошибка:</b> Количество часов для распределения ({hours}) не может превышать "
            f"общее количество выполнений ({max_completions}).\n\n"
            "Пожалуйста, введите меньшее количество часов или вернитесь и увеличьте лимит выполнений."
        )
        return

    await state.update_data(distribution_hours=hours)
    data['distribution_hours'] = hours # Добавляем в локальный словарь для отображения
    
    await show_task_confirmation(message, state, data)

# Обработчик быстрого выбора часов
@router.callback_query(F.data.startswith("hours_"), AddTaskState.waiting_for_distribution_hours)
async def process_quick_hours_choice(callback: CallbackQuery, config: Config, state: FSMContext):
    hours_data = callback.data.split("_")[1]
    
    if hours_data == "custom":
        # Пользователь хочет ввести своё время
        await callback.message.edit_text(
            "⏰ Введите количество часов для распределения:\n\n"
            "Введите число от 1 до 168 (неделя):",
            reply_markup=kb.cancel_state_keyboard()
        )
        await callback.answer()
        return
    
    # Быстрый выбор предустановленного времени
    hours = int(hours_data)
    data = await state.get_data()
    max_completions = data.get('max_completions')

    if max_completions is not None and hours > max_completions:
        await callback.answer(
            f"Ошибка: Часов ({hours}) больше, чем выполнений ({max_completions}).",
            show_alert=True
        )
        return

    # И затем в user_data нужно добавить 'distribution_hours'
    await state.update_data(distribution_hours=hours)
    user_data = await state.get_data()
    user_data['distribution_hours'] = hours # <--- Вот эта строка
    await show_task_confirmation(callback, state, user_data, edit=True)
    await callback.answer()

# Функция для показа подтверждения (Обновляем)
async def show_task_confirmation(message_event: Message | CallbackQuery, state: FSMContext, data: dict, edit: bool = False):
    desc = data.get('description', 'N/A')
    reward = data.get('reward', 0.0)
    instruction = data.get('instruction_link', 'Нет')
    action = data.get('action_link', 'Нет')
    channel_id = data.get('channel_id_to_check', None)
    check_sub = data.get('check_subscription', False)
    premium_req = data.get('premium_requirement', 'all') # Получаем требование Premium
    max_completions = data.get('max_completions', 1000000) # Получаем лимит выполнений
    use_time_distribution = data.get('use_time_distribution', False)
    distribution_hours = data.get('distribution_hours', None)

    channel_info = ""
    if channel_id:
        channel_info += f"<b>Канал:</b> {channel_id}\n"
        # --- Добавим получение ссылки на канал, если ID известен ---
        channel_info += f"<b>Проверка подписки:</b> {'Да ✅' if check_sub else 'Нет ❌'}"
    else:
        channel_info += "<b>Канал:</b> Не указан (проверка подписки невозможна)"
         
    # Текст для отображения Premium требования
    premium_req_text_map = {
        'all': 'Всем пользователям',
        'premium_only': 'Только Premium [⭐️]',
        'non_premium_only': 'Только НЕ Premium [🚫⭐️]'
    }
    premium_req_display = premium_req_text_map.get(premium_req, premium_req)

    # Информация о временном распределении
    time_dist_info = ""
    if use_time_distribution and distribution_hours:
        time_dist_info = f"\n<b>⏰ Временное распределение:</b> Включено ({distribution_hours} ч.)"
    else:
        time_dist_info = "\n<b>⏰ Временное распределение:</b> Отключено"

    confirm_text = f"""
Проверьте данные нового задания:
-------------------------------------
<b>Описание:</b> {html.escape(desc[:100])}
<b>Награда:</b> {reward:.2f}⭐️
<b>Инструкция:</b> {html.escape(instruction) if instruction and instruction != 'Нет' else 'Нет'}
<b>Ссылка 'Выполнить':</b> {html.escape(action) if action and action != 'Нет' else 'Нет'}
{channel_info}
<b>Доступность:</b> {premium_req_display}
<b>Максимальное количество выполнений:</b> {max_completions}{time_dist_info}
-------------------------------------
Сохранить это задание?
    """
    reply_markup = kb.yes_no_keyboard("addtask_confirm")

    target_message = message_event.message if isinstance(message_event, CallbackQuery) else message_event

    if edit:
        try:
            await target_message.edit_text(confirm_text, reply_markup=reply_markup)
        except Exception as e:
            logger.debug(f"Не удалось отредактировать сообщение подтверждения задачи: {e}")
            # Если не удалось отредактировать (например, текст не изменился), попробуем отправить новым
            try:
                await target_message.answer(confirm_text, reply_markup=reply_markup)
            except Exception as send_e:
                logger.error(f"Не удалось отправить новое сообщение подтверждения задачи: {send_e}")
    else:
        await target_message.answer(confirm_text, reply_markup=reply_markup)

    await state.set_state(AddTaskState.confirming)


# Обработка подтверждения или отмены сохранения (Обновляем)
@router.callback_query(AddTaskState.confirming)
async def process_task_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    user_id = callback.from_user.id # Для логирования
    if callback.data.endswith("_yes"):
        user_data = await state.get_data()
        try:
            use_time_distribution = user_data.get('use_time_distribution', False)
            
            if use_time_distribution and user_data.get('distribution_hours'):
                # Создаем задание с временным распределением
                new_task = await db.create_time_distributed_task(
                    session=session,
                    description=user_data.get('description'),
                    reward=user_data.get('reward'),
                    instruction_link=user_data.get('instruction_link'),
                    action_link=user_data.get('action_link'),
                    channel_id_to_check=user_data.get('channel_id_to_check'),
                    check_subscription=user_data.get('check_subscription'),
                    premium_requirement=user_data.get('premium_requirement', 'all'),
                    max_completions=user_data.get('max_completions', 1000000),
                    distribution_hours=user_data.get('distribution_hours')
                )
                logger.info(f"Admin {user_id} successfully added time-distributed task #{new_task.id} with {user_data.get('distribution_hours')}h distribution")
                await callback.message.edit_text(f"✅ Новое задание с временным распределением #{new_task.id} успешно добавлено!\n⏰ Распределение на {user_data.get('distribution_hours')} часов.")
            else:
                # Создаем обычное задание
                new_task = await db.add_task(
                    session=session,
                    description=user_data.get('description'),
                    reward=user_data.get('reward'),
                    instruction_link=user_data.get('instruction_link'),
                    action_link=user_data.get('action_link'),
                    channel_id_to_check=user_data.get('channel_id_to_check'),
                    check_subscription=user_data.get('check_subscription'),
                    premium_requirement=user_data.get('premium_requirement', 'all'),
                    max_completions=user_data.get('max_completions', 1000000)
                )
                logger.info(f"Admin {user_id} successfully added regular task #{new_task.id}")
                await callback.message.edit_text(f"✅ Новое задание #{new_task.id} успешно добавлено!")
            
            # --- Добавляем коммит --- 
            await session.commit()
            
        except Exception as e:
            # --- Добавляем откат --- 
            await session.rollback()
            logger.error(f"Admin {user_id} failed to add task: {e}", exc_info=True) # Логирование с traceback
            await callback.message.edit_text("❌ Произошла ошибка при сохранении задания.")
        finally:
             await state.clear()

    elif callback.data.endswith("_no"):
        logger.info(f"Admin {user_id} cancelled task addition at confirmation step.") # Логирование
        await state.clear()
        await callback.message.edit_text("Добавление задания отменено.")

    await callback.answer()

# --- Конец Управления Заданиями ---

# --- Управление Индивидуальными Ссылками --- 

# Кнопка "Индивидуальные ссылки" - ТЕПЕРЬ ПОКАЗЫВАЕТ МЕНЮ
@router.callback_query(F.data == "admin_manage_ind_links")
@admin_required
async def admin_manage_ind_links(callback: CallbackQuery, config: Config, session: AsyncSession):
    message_text = "🔗 Специальные реферальные ссылки:"
    reply_markup = kb.admin_ind_links_menu_keyboard()
    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение на меню инд. ссылок: {e}")
    await callback.answer()

# НОВЫЙ обработчик для кнопки "Список ссылок / Статистика"
@router.callback_query(F.data == "admin_view_ind_links_list")
async def admin_view_ind_links_list(callback: CallbackQuery, config: Config, session: AsyncSession):
    links = await db.get_all_individual_links(session)
    links_with_stats = []
    message_text = "📊 Список ссылок и статистика: \n"
    if links:
        for link in links:
            total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
            op_div_3 = passed_op // 3 if passed_op >= 3 else 0
            identifier_short = (link.identifier[:20] + '...') if len(link.identifier) > 20 else link.identifier
            message_text += (
                f"- <code>{html.escape(identifier_short)}</code> "
                f"(Reg: {total_reg} / OP: {op_div_3})\n"
            )
            links_with_stats.append((link, total_reg, passed_op)) # Собираем для клавиатуры
    else:
        message_text = "🔗 Индивидуальных ссылок пока нет."

    reply_markup = kb.admin_ind_links_list_keyboard(links_with_stats)

    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение со списком инд. ссылок: {e}")
        # Если ошибка парсинга, попробуем без HTML
        try:
             await callback.message.edit_text(message_text.replace('<code>', '').replace('</code>', ''), reply_markup=reply_markup)
        except Exception as e2:
             logger.error(f"Не удалось отправить список инд. ссылок даже без HTML: {e2}")

    await callback.answer()

# Просмотр конкретной ссылки
@router.callback_query(F.data.startswith("admin_ind_link_view_"))
async def admin_view_single_ind_link(callback: CallbackQuery, config: Config, session: AsyncSession):
    link_id = int(callback.data.split("_")[4])
    link = await db.get_individual_link_by_id(session, link_id)
    if not link:
        await callback.answer("❌ Ссылка не найдена.", show_alert=True)
        await admin_view_ind_links_list(callback, session) # Обновляем список, если ссылка пропала
        return

    total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
    op_div_3 = passed_op // 3 if passed_op >= 3 else 0

    link_details = f"""
🔗 <b>Индивидуальная ссылка:</b> <code>{html.escape(link.identifier)}</code>
-------------------------------------
<b>Описание:</b> {html.escape(link.description or '(нет)')}
<b>ID:</b> {link.id}
<b>Создана:</b> {link.created_at.strftime('%Y-%m-%d %H:%M')}
-------------------------------------
<b>Статистика:</b>
  Зарегистрировано: {total_reg}
  Прошли ОП: {passed_op}
  (Прошли ОП / 3): {op_div_3}
-------------------------------------
<b>Ссылка для пользователя:</b>
<code>https://t.me/{ (await callback.bot.get_me()).username }?start={html.escape(link.identifier)}</code>
<b>Ссылка для INFO:</b>
<code>https://t.me/{ (await callback.bot.get_me()).username }?start=INFO_{html.escape(link.identifier)}</code>
"""
    try:
        await callback.message.edit_text(
            link_details,
            reply_markup=kb.admin_ind_link_manage_keyboard(link, total_reg, passed_op)
        )
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение при просмотре инд. ссылки {link_id}: {e}")
    await callback.answer()

# --- FSM для добавления индивидуальной ссылки --- 

@router.callback_query(F.data == "admin_add_ind_link", StateFilter(None))
async def admin_add_ind_link_start(callback: CallbackQuery, config: Config, state: FSMContext):
    logger.info(f"Admin {callback.from_user.id} starting to add a new individual link.")
    await callback.message.edit_text("✍️ Введите уникальный текстовый идентификатор для новой ссылки (только латиница, цифры, _ ):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddIndividualLinkState.waiting_for_identifier)
    await callback.answer()

@router.message(AddIndividualLinkState.waiting_for_identifier)
async def process_ind_link_identifier(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    identifier = message.text.strip()
    # Простая валидация идентификатора (можно усложнить регуляркой)
    if not identifier or not re.match(r'^[a-zA-Z0-9_]+$', identifier):
        await message.answer("❌ Идентификатор может содержать только латинские буквы, цифры и знак подчеркивания. Попробуйте еще раз:", reply_markup=kb.cancel_state_keyboard())
        return

    # Проверка на уникальность
    existing_link = await db.get_individual_link_by_identifier(session, identifier)
    if existing_link:
        await message.answer(f"❌ Идентификатор \"{html.escape(identifier)}\" уже используется. Придумайте другой:", reply_markup=kb.cancel_state_keyboard())
        return

    await state.update_data(identifier=identifier)
    await message.answer("📝 Введите описание для этой ссылки (для вашего удобства, можно '-'):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddIndividualLinkState.waiting_for_description)

@router.message(AddIndividualLinkState.waiting_for_description)
async def process_ind_link_description(message: Message, config: Config, state: FSMContext):
    description = message.text if message.text != '-' else None
    await state.update_data(description=description)
    user_data = await state.get_data()

    confirm_text = f"""
    Проверьте данные новой индивидуальной ссылки:
    -------------------------------------
    <b>Идентификатор:</b> <code>{html.escape(user_data.get('identifier'))}</code>
    <b>Описание:</b> {html.escape(description or '(нет)')}
    -------------------------------------
    Сохранить эту ссылку?
    """
    await message.answer(confirm_text, reply_markup=kb.yes_no_keyboard("addindlink_confirm"))
    await state.set_state(AddIndividualLinkState.confirming)

@router.callback_query(AddIndividualLinkState.confirming)
async def process_ind_link_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    user_id = callback.from_user.id
    if callback.data.endswith("_yes"):
        user_data = await state.get_data()
        try:
            # Используем db.add_individual_link, которая делает commit
            new_link = await db.add_individual_link(
                session=session,
                identifier=user_data.get('identifier'),
                description=user_data.get('description')
            )
            if new_link:
                logger.info(f"Admin {user_id} successfully added individual link #{new_link.id} ({new_link.identifier})")
                
                # --- Получаем юзернейм бота ---
                bot_username = (await callback.bot.get_me()).username
                identifier_escaped = html.escape(new_link.identifier)
                description_escaped = html.escape(new_link.description or '(нет)')
                
                # --- Формируем новое сообщение ---
                success_message = f"""
✅ <b>Новая индивидуальная ссылка успешно добавлена!</b>
-------------------------------------
<b>Идентификатор:</b> <code>{identifier_escaped}</code>
<b>Описание:</b> {description_escaped}
-------------------------------------
<b>Ссылка для пользователя:</b>
<code>https://t.me/{bot_username}?start={identifier_escaped}</code>
<b>Ссылка для INFO:</b>
<code>https://t.me/{bot_username}?start=INFO_{identifier_escaped}</code>
"""
                # --- Редактируем сообщение с новыми данными ---
                await callback.message.edit_text(success_message)
                
                # Очищаем состояние после успешного добавления
                await state.clear() 
                
                # Показываем обновленное меню управления ссылками (можно сделать опциональным или добавить кнопку "Назад")
                # await admin_manage_ind_links(callback, session) # Пока закомментируем, чтобы сообщение с ссылками осталось видно
            else:
                 # ... (обработка дубликата) ...
                 logger.warning(f"Admin {user_id} tried to add duplicate link identifier '{user_data.get('identifier')}' at confirmation step.")
                 await callback.message.edit_text("❌ Ошибка: Этот идентификатор уже был добавлен.")
                 # Не очищаем состояние, даем шанс отменить или вернуться
                 await admin_manage_ind_links(callback, session) # Показываем меню
        except Exception as e:
            # ... (обработка ошибки) ...
            logger.error(f"Admin {user_id} failed to add individual link: {e}", exc_info=True)
            await callback.message.edit_text("❌ Произошла ошибка при сохранении ссылки.")
            await state.clear() # Очищаем состояние при ошибке
            await admin_manage_ind_links(callback, session) # Показываем меню
    elif callback.data.endswith("_no"):
        # ... (обработка отмены) ...
        logger.info(f"Admin {user_id} cancelled individual link addition at confirmation step.")
        await state.clear()
        await callback.message.edit_text("Добавление ссылки отменено.")
        await admin_manage_ind_links(callback, session) # Показываем меню
        
    # Отвечаем на колбэк в любом случае (кроме успешного добавления, где сообщение уже изменено)
    # Если нужно убрать часики после успешного добавления без показа меню, можно добавить callback.answer() после edit_text
    if not callback.data.endswith("_yes") or not new_link: # Отвечаем, если не было успешного добавления
         await callback.answer()
    elif new_link: # Отвечаем без текста после успешного edit_text
         await callback.answer()

# --- Удаление индивидуальной ссылки --- 

# Используем lambda для более точного фильтра: 
# Строка начинается с префикса И пятая часть (индекс 4) является числом
# ... (imports and existing admin router code) ...

# --- Управление Индивидуальными Ссылками --- 

# Кнопка "Индивидуальные ссылки" - ТЕПЕРЬ ПОКАЗЫВАЕТ МЕНЮ
@router.callback_query(F.data == "admin_manage_ind_links", StateFilter(None)) # Добавляем StateFilter(None) для ясности
async def admin_manage_ind_links(callback: CallbackQuery, config: Config, session: AsyncSession):
    message_text = "🔗 Специальные реферальные ссылки:"
    reply_markup = kb.admin_ind_links_menu_keyboard()
    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение на меню инд. ссылок: {e}")
    await callback.answer()

# Кнопка "Список ссылок / Статистика"
@router.callback_query(F.data == "admin_view_ind_links_list", StateFilter(None)) # Добавляем StateFilter(None)
async def admin_view_ind_links_list(callback: CallbackQuery, config: Config, session: AsyncSession):
    links = await db.get_all_individual_links(session)
    links_with_stats = []
    message_text_lines = ["📊 Список ссылок и статистика:"]
    if links:
        message_text_lines.append("") # Пустая строка для отступа
        for link in links:
            total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
            # Используем полное деление для ОП/3
            op_div_3 = passed_op // 3 if passed_op >= 3 else 0
            identifier_short = (link.identifier[:20] + '...') if len(link.identifier) > 20 else link.identifier
            message_text_lines.append(
                f"• <code>{html.escape(identifier_short)}</code> "
                f"(Reg: {total_reg} / OP passed: {passed_op} / OP div 3: {op_div_3})"
            )
            links_with_stats.append((link, total_reg, passed_op)) # Собираем для клавиатуры
        message_text = "\n".join(message_text_lines)
    else:
        message_text = "🔗 Индивидуальных ссылок пока нет."

    reply_markup = kb.admin_ind_links_list_keyboard(links_with_stats)

    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        # Если ошибка из-за неизмененного сообщения, просто отвечаем на колбэк
        if "message is not modified" in str(e):
            await callback.answer("Список уже отображен.")
        else:
            logger.debug(f"Не удалось изменить сообщение со списком инд. ссылок: {e}")
             # Если другая ошибка парсинга, попробуем без HTML
        try:
                  clean_text = '\n'.join(line.replace('<code>', '').replace('</code>', '') for line in message_text_lines)
                  await callback.message.edit_text(clean_text, reply_markup=reply_markup)
        except Exception as e2:
             logger.error(f"Не удалось отправить список инд. ссылок даже без HTML: {e2}")
    except Exception as e:
         logger.error(f"Не удалось отправить список инд. ссылок: {e}", exc_info=True)

    # Всегда отвечаем на колбэк, если не было сделано ранее
    if not callback.is_answered:
        await callback.answer()


# --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ "ПОЛУЧИТЬ СТАТИСТИКУ" ---

@router.callback_query(F.data == "admin_get_ind_link_stats", StateFilter(None))
async def handle_get_ind_link_stats(callback: CallbackQuery, config: Config, state: FSMContext):
    """Запрашивает идентификатор ссылки для получения статистики."""
    logger.info(f"Admin {callback.from_user.id} requested individual link stats.")
    await callback.message.edit_text(
        "📈 Введите <b>уникальный идентификатор</b> ссылки, для которой нужно получить статистику:",
        reply_markup=kb.cancel_state_keyboard() # Кнопка отмены inline
    )
    await state.set_state(st.AdminGetIndLinkStatsState.waiting_for_identifier)
    await callback.answer()

@router.message(st.AdminGetIndLinkStatsState.waiting_for_identifier)
async def process_get_ind_link_stats_identifier(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обрабатывает введенный идентификатор, получает и выводит статистику."""
    identifier = message.text.strip()
    logger.info(f"Admin {message.from_user.id} provided identifier '{identifier}' for stats.")

    # Проверка на отмену через текст ReplyKeyboard (на всякий случай)
    if identifier == "❌ Отменить":
        logger.info(f"Admin {message.from_user.id} cancelled getting stats via text.")
        await message.answer("Действие отменено.", reply_markup=kb.admin_main_keyboard()) # Возврат в гл. меню админки
        await state.clear()
        return

    link = await db.get_individual_link_by_identifier(session, identifier)

    if not link:
        logger.warning(f"Identifier '{identifier}' not found for stats request by admin {message.from_user.id}.")
        await message.answer(
            f"❌ Ссылка с идентификатором <code>{html.escape(identifier)}</code> не найдена. Попробуйте еще раз или отмените.",
            reply_markup=kb.cancel_state_keyboard()
        )
        # Не сбрасываем состояние, даем попробовать еще раз
        return

    total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
    op_div_3 = passed_op // 3 if passed_op >= 3 else 0

    bot_username = (await message.bot.get_me()).username
    
    stats_text = (
        f"📊 <b>Статистика по ссылке:</b> <code>{html.escape(link.identifier)}</code>\n\n"
        f"📝 Описание: {html.escape(link.description or '-')}\n"
        f"🆔 ID ссылки: {link.id}\n"
        # f"👤 Создатель: {link.creator_id}\n" # Если есть поле creator_id в модели
        f"📅 Создана: {link.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
        f"--- Статистика ---\n"
        f"👥 Всего регистраций: {total_reg}\n"
        f"✅ Прошли ОП: {op_div_3}\n"
        f"--- Ссылки ---\n"
        f"🔗 Для пользователя: <code>https://t.me/{bot_username}?start={html.escape(link.identifier)}</code>\n"
        f"ℹ️ Для INFO: <code>https://t.me/{bot_username}?start=INFO_{html.escape(link.identifier)}</code>"
    )
    
    logger.info(f"Displaying stats for link '{identifier}' (ID: {link.id}) to admin {message.from_user.id}.")
    # Отправляем статистику и кнопки главного меню админки
    await message.answer(stats_text, reply_markup=kb.admin_main_keyboard()) 
    await state.clear()

# --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ "УДАЛИТЬ СПЕЦ. ССЫЛКУ" ---

@router.callback_query(F.data == "admin_delete_ind_link_by_id", StateFilter(None))
async def handle_delete_ind_link_by_id(callback: CallbackQuery, config: Config, state: FSMContext):
    """Запрашивает идентификатор ссылки для удаления."""
    logger.info(f"Admin {callback.from_user.id} requested to delete individual link by ID.")
    await callback.message.edit_text(
        "🗑️ Введите <b>уникальный идентификатор</b> ссылки, которую нужно <b>удалить</b>:",
        reply_markup=kb.cancel_state_keyboard() # Кнопка отмены inline
    )
    await state.set_state(st.AdminDeleteIndLinkState.waiting_for_identifier)
    await callback.answer()

@router.message(st.AdminDeleteIndLinkState.waiting_for_identifier)
async def process_delete_ind_link_identifier(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Находит ссылку по идентификатору и запрашивает подтверждение удаления."""
    identifier = message.text.strip()
    logger.info(f"Admin {message.from_user.id} provided identifier '{identifier}' for deletion.")

    if identifier == "❌ Отменить": # Проверка на текстовую отмену
        logger.info(f"Admin {message.from_user.id} cancelled link deletion via text.")
        await message.answer("Действие отменено.", reply_markup=kb.admin_main_keyboard())
        await state.clear()
        return

    link = await db.get_individual_link_by_identifier(session, identifier)

    if not link:
        logger.warning(f"Identifier '{identifier}' not found for deletion request by admin {message.from_user.id}.")
        await message.answer(
            f"❌ Ссылка с идентификатором <code>{html.escape(identifier)}</code> не найдена. Попробуйте еще раз или отмените.",
            reply_markup=kb.cancel_state_keyboard()
        )
        return # Остаемся в состоянии ожидания

    # Сохраняем ID и идентификатор найденной ссылки для подтверждения
    await state.update_data(link_id_to_delete=link.id, link_identifier=link.identifier)

    # Используем существующую клавиатуру подтверждения, передавая ID ссылки
    logger.info(f"Asking admin {message.from_user.id} to confirm deletion of link '{identifier}' (ID: {link.id}).")
    await message.answer(
        f"Вы уверены, что хотите удалить ссылку с идентификатором <code>{html.escape(link.identifier)}</code> (ID: {link.id})?\n\n"
        f"⚠️ Это действие необратимо!",
        reply_markup=kb.admin_ind_link_delete_confirm_keyboard(link.id)
    )
    await state.set_state(st.AdminDeleteIndLinkState.confirm_delete)


# --- ОБРАБОТЧИКИ УДАЛЕНИЯ ИЗ СПИСКА / ПРОСМОТРА ---

# Кнопка "Удалить" из просмотра конкретной ссылки
@router.callback_query(
    lambda c: c.data.startswith("admin_ind_link_delete_") and len(c.data.split("_")) == 5 and c.data.split("_")[4].isdigit(),
    StateFilter(None) # Ловим только если нет активного состояния
) 
async def admin_delete_ind_link_prompt_from_view(callback: CallbackQuery, config: Config, session: AsyncSession):
    link_id = int(callback.data.split("_")[4])
    link = await db.get_individual_link_by_id(session, link_id)
    if not link:
        await callback.answer("❌ Ссылка не найдена.", show_alert=True)
        await admin_manage_ind_links(callback, session) # Показываем меню ссылок
        return

    logger.info(f"Admin {callback.from_user.id} prompted to delete link '{link.identifier}' (ID: {link_id}) from view.")
    try:
        await callback.message.edit_text(
            f"🗑️ Вы уверены, что хотите удалить ссылку <code>{html.escape(link.identifier)}</code> (ID: {link.id})?\n\n"
            f"⚠️ Это действие необратимо!",
            reply_markup=kb.admin_ind_link_delete_confirm_keyboard(link_id)
        )
    except Exception as e:
         logger.debug(f"Не удалось изменить сообщение перед удалением инд. ссылки {link_id}: {e}")
    await callback.answer()
    # Не меняем состояние здесь, т.к. ждем колбэк подтверждения

# Обработка ПОДТВЕРЖДЕНИЯ удаления (ловим в ЛЮБОМ состоянии или без него)
@router.callback_query(
    F.data.startswith("admin_ind_link_delete_confirm_"), 
    StateFilter('*') # Ловим в любом состоянии
)
async def process_delete_ind_link_confirm(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Подтверждает и выполняет удаление индивидуальной ссылки."""
    current_state = await state.get_state() # Получаем текущее состояние для лога
    
    try:
        link_id = int(callback.data.split("_")[-1]) # Получаем ID из callback_data
        logger.info(f"Admin {callback.from_user.id} confirmed deletion for link ID {link_id} (from state: {current_state}).")
    except (ValueError, IndexError):
        logger.error(f"Could not parse link_id from callback_data: {callback.data}")
        await callback.answer("Ошибка при обработке запроса.", show_alert=True)
        await state.clear()
        await cmd_admin_panel(callback) # Возврат в гл. меню админки
        return

    # Получаем идентификатор перед удалением для сообщения
    link = await db.get_individual_link_by_id(session, link_id)
    identifier = link.identifier if link else "UNKNOWN"

    # Вызываем функцию удаления из requests.py
    success = await db.delete_individual_link(session, link_id) 

    if success:
        logger.info(f"Successfully deleted individual link ID {link_id} ('{identifier}') by admin {callback.from_user.id}.")
        # Используем edit_text вместо answer, чтобы изменить сообщение с подтверждением
        await callback.message.edit_text(f"✅ Ссылка '{html.escape(identifier)}' (ID: {link_id}) успешно удалена.")
        # Можно показать обновленный список или меню
        await admin_manage_ind_links(callback, session) # Показываем меню управления ссылками
    else:
        logger.warning(f"Failed to delete individual link ID {link_id} (maybe already deleted?) by admin {callback.from_user.id}.")
        await callback.message.edit_text(f"❌ Не удалось удалить ссылку '{html.escape(identifier)}' (ID: {link_id}). Возможно, она уже удалена.")
        await admin_manage_ind_links(callback, session) # Показываем меню управления ссылками

    await state.clear() # Очищаем состояние в любом случае
    await callback.answer() # Отвечаем на колбэк, чтобы убрать часики


# Обработка ОТМЕНЫ удаления (кнопка "Отмена" на клавиатуре подтверждения)
# Эта кнопка возвращает к просмотру ссылки (admin_ind_link_view_{link_id})
@router.callback_query(
    F.data.startswith("admin_ind_link_view_"), 
    st.AdminDeleteIndLinkState.confirm_delete # Ловим только в состоянии подтверждения удаления
)
async def cancel_delete_ind_link_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Обработка кнопки Отмена при подтверждении удаления ссылки."""
    try:
        link_id = int(callback.data.split("_")[-1])
        logger.info(f"Admin {callback.from_user.id} cancelled deletion for link ID {link_id} at confirmation step.")
    except (ValueError, IndexError):
        logger.error(f"Could not parse link_id from cancel confirmation callback: {callback.data}")
        await callback.answer("Ошибка при отмене.", show_alert=True)
        await state.clear()
        await cmd_admin_panel(callback) # Назад в админку
        return
        
    await state.clear()
    await callback.answer("Удаление отменено.")
    # Вызываем обработчик просмотра этой же ссылки, чтобы вернуть пользователя назад
    await admin_view_single_ind_link(callback, session)


# --- Конец Новых Обработчиков ---

# Просмотр конкретной ссылки (этот обработчик уже был, оставляем)
@router.callback_query(F.data.startswith("admin_ind_link_view_"), StateFilter(None)) # Добавляем StateFilter(None)
async def admin_view_single_ind_link(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        link_id = int(callback.data.split("_")[4])
    except (IndexError, ValueError):
        logger.error(f"Could not parse link_id from callback_data: {callback.data}")
        await callback.answer("Ошибка ID ссылки.", show_alert=True)
        return
        
    link = await db.get_individual_link_by_id(session, link_id)
    if not link:
        await callback.answer("❌ Ссылка не найдена.", show_alert=True)
        await admin_view_ind_links_list(callback, session) # Обновляем список, если ссылка пропала
        return

    total_reg, passed_op = await db.get_individual_link_stats(session, link.id)
    op_div_3 = passed_op // 3 if passed_op >= 3 else 0

    bot_username = (await callback.bot.get_me()).username

    link_details = f"""
🔗 <b>Индивидуальная ссылка:</b> <code>{html.escape(link.identifier)}</code>
-------------------------------------
<b>Описание:</b> {html.escape(link.description or '(нет)')}
<b>ID:</b> {link.id}
<b>Создана:</b> {link.created_at.strftime('%Y-%m-%d %H:%M')}
-------------------------------------
<b>Статистика:</b>
  Зарегистрировано: {total_reg}
  Прошли ОП: {passed_op}
  (Прошли ОП / 3): {op_div_3}
-------------------------------------
<b>Ссылка для пользователя:</b>
<code>https://t.me/{bot_username}?start={html.escape(link.identifier)}</code>
<b>Ссылка для INFO:</b>
<code>https://t.me/{bot_username}?start=INFO_{html.escape(link.identifier)}</code>
"""
    try:
        await callback.message.edit_text(
            link_details,
            reply_markup=kb.admin_ind_link_manage_keyboard(link, total_reg, passed_op)
        )
    except TelegramBadRequest as e:
         if "message is not modified" in str(e):
             await callback.answer("Информация о ссылке уже отображена.")
         else:
             logger.debug(f"Не удалось изменить сообщение при просмотре инд. ссылки {link_id}: {e}")
    except Exception as e:
        logger.error(f"Не удалось изменить сообщение при просмотре инд. ссылки {link_id}: {e}", exc_info=True)
        
    if not callback.is_answered:
        await callback.answer()


# --- FSM для добавления индивидуальной ссылки (оставляем как есть) --- 

@router.callback_query(F.data == "admin_add_ind_link", StateFilter(None))
async def admin_add_ind_link_start(callback: CallbackQuery, config: Config, state: FSMContext):
    logger.info(f"Admin {callback.from_user.id} starting to add a new individual link.")
    await callback.message.edit_text("✍️ Введите уникальный текстовый идентификатор для новой ссылки (только латиница, цифры, _ ):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddIndividualLinkState.waiting_for_identifier)
    await callback.answer()

@router.message(AddIndividualLinkState.waiting_for_identifier)
async def process_ind_link_identifier(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    identifier = message.text.strip()
    # Простая валидация идентификатора (можно усложнить регуляркой)
    if not identifier or not re.match(r'^[a-zA-Z0-9_]+$', identifier):
        await message.answer("❌ Идентификатор может содержать только латинские буквы, цифры и знак подчеркивания. Попробуйте еще раз:", reply_markup=kb.cancel_state_keyboard())
        return

    # Проверка на уникальность
    existing_link = await db.get_individual_link_by_identifier(session, identifier)
    if existing_link:
        await message.answer(f"❌ Идентификатор \"{html.escape(identifier)}\" уже используется. Придумайте другой:", reply_markup=kb.cancel_state_keyboard())
        return

    await state.update_data(identifier=identifier)
    await message.answer("📝 Введите описание для этой ссылки (для вашего удобства, можно '-'):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddIndividualLinkState.waiting_for_description)

@router.message(AddIndividualLinkState.waiting_for_description)
async def process_ind_link_description(message: Message, config: Config, state: FSMContext):
    description = message.text if message.text != '-' else None
    await state.update_data(description=description)
    user_data = await state.get_data()

    confirm_text = f"""
    Проверьте данные новой индивидуальной ссылки:
    -------------------------------------
    <b>Идентификатор:</b> <code>{html.escape(user_data.get('identifier'))}</code>
    <b>Описание:</b> {html.escape(description or '(нет)')}
    -------------------------------------
    Сохранить эту ссылку?
    """
    await message.answer(confirm_text, reply_markup=kb.yes_no_keyboard("addindlink_confirm"))
    await state.set_state(AddIndividualLinkState.confirming)

@router.callback_query(AddIndividualLinkState.confirming)
async def process_ind_link_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    user_id = callback.from_user.id
    if callback.data.endswith("_yes"):
        user_data = await state.get_data()
        try:
            # Используем db.add_individual_link, которая делает commit
            new_link = await db.add_individual_link(
                session=session,
                identifier=user_data.get('identifier'),
                description=user_data.get('description')
            )
            if new_link:
                logger.info(f"Admin {user_id} successfully added individual link #{new_link.id} ({new_link.identifier})")
                
                # --- Получаем юзернейм бота ---
                bot_username = (await callback.bot.get_me()).username
                identifier_escaped = html.escape(new_link.identifier)
                description_escaped = html.escape(new_link.description or '(нет)')
                
                # --- Формируем новое сообщение ---
                success_message = f"""
✅ <b>Новая индивидуальная ссылка успешно добавлена!</b>
-------------------------------------
<b>Идентификатор:</b> <code>{identifier_escaped}</code>
<b>Описание:</b> {description_escaped}
-------------------------------------
<b>Ссылка для пользователя:</b>
<code>https://t.me/{bot_username}?start={identifier_escaped}</code>
<b>Ссылка для INFO:</b>
<code>https://t.me/{bot_username}?start=INFO_{identifier_escaped}</code>
"""
                # --- Редактируем сообщение с новыми данными ---
                await callback.message.edit_text(success_message)
                
                # Очищаем состояние после успешного добавления
                await state.clear() 
                
                # Показываем обновленное меню управления ссылками (можно сделать опциональным или добавить кнопку "Назад")
                # await admin_manage_ind_links(callback, session) # Пока закомментируем, чтобы сообщение с ссылками осталось видно
            else:
                 # ... (обработка дубликата) ...
                 logger.warning(f"Admin {user_id} tried to add duplicate link identifier '{user_data.get('identifier')}' at confirmation step.")
                 await callback.message.edit_text("❌ Ошибка: Этот идентификатор уже был добавлен.")
                 # Не очищаем состояние, даем шанс отменить или вернуться
                 await admin_manage_ind_links(callback, session) # Показываем меню
        except Exception as e:
            # ... (обработка ошибки) ...
            logger.error(f"Admin {user_id} failed to add individual link: {e}", exc_info=True)
            await callback.message.edit_text("❌ Произошла ошибка при сохранении ссылки.")
            await state.clear() # Очищаем состояние при ошибке
            await admin_manage_ind_links(callback, session) # Показываем меню
    elif callback.data.endswith("_no"):
        # ... (обработка отмены) ...
        logger.info(f"Admin {user_id} cancelled individual link addition at confirmation step.")
        await state.clear()
        await callback.message.edit_text("Добавление ссылки отменено.")
        await admin_manage_ind_links(callback, session) # Показываем меню
        
    # Отвечаем на колбэк в любом случае (кроме успешного добавления, где сообщение уже изменено)
    # Если нужно убрать часики после успешного добавления без показа меню, можно добавить callback.answer() после edit_text
    if not callback.data.endswith("_yes") or not new_link: # Отвечаем, если не было успешного добавления
         await callback.answer()
    elif new_link: # Отвечаем без текста после успешного edit_text
         await callback.answer()

# --- Конец Управления Индивидуальными Ссылками --- 

                # ... (rest of the admin handlers: channels, promocodes, rewards, newsletter, user management) ...


# --- ОБЩИЙ ОБРАБОТЧИК ОТМЕНЫ (Убедитесь, что он есть и зарегистрирован) ---
@router.callback_query(F.data == "cancel_state", StateFilter("*"))
@admin_required
async def cancel_any_state_handler(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession): # Добавляем session
    current_state_str = await state.get_state()
    if current_state_str is None:
        await callback.answer("Нет активного действия для отмены.", show_alert=True)
        return

    logger.info(f"Admin {callback.from_user.id} cancelled state {current_state_str}")
    
    # --- Специальная логика отмены для состояний управления пользователем/ссылкой ---
    state_data = await state.get_data()
    user_id_in_state = state_data.get('manage_user_id')
    link_id_in_state = state_data.get('link_id_to_delete') # Используется при удалении по ID

    await state.clear() # Очищаем состояние

    restored = False
    # Пытаемся восстановить профиль пользователя, если отменяли его редактирование
    if current_state_str and current_state_str.startswith("AdminManageUser:") and user_id_in_state:
        user = await db.get_user(session, user_id_in_state)
        if user:
            profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Статус бана:</b> {'Да 🚫' if user.banned else 'Нет ✅'}
<b>Рефералов (всего):</b> {user.refferals_count}
<b>Рефералов (за 24ч):</b> {user.refferals_24h_count}
<b>Бонус за реферала получен:</b> {'Да' if user.ref_bonus else 'Нет'}
<b>Текущее задание ID:</b> {user.current_task_id or 'Нет'}
<b>Пришел по инд. ссылке ID:</b> {user.individual_link_id or 'Нет'}
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
            try:
                await callback.message.edit_text(profile_text, reply_markup=kb.admin_manage_user_keyboard(user))
                await callback.answer("Действие отменено.")
                restored = True
            except Exception as e:
                logger.error(f"Failed to restore profile on cancel state {current_state_str}: {e}")

    # Пытаемся вернуться к меню управления ссылками, если отменяли что-то с ними связанное
    elif current_state_str and (current_state_str.startswith("AdminGetIndLinkStatsState:") or current_state_str.startswith("AdminDeleteIndLinkState:") or current_state_str.startswith("AddIndividualLinkState:")):
         # await callback.message.edit_text("Действие отменено.") # Редактируем или нет?
         await admin_manage_ind_links(callback, session) # Показываем меню ссылок
         await callback.answer("Действие отменено.")
         restored = True

    # Если не удалось восстановить контекст или это было другое состояние
    if not restored:
        try:
            await callback.message.edit_text("Действие отменено.", reply_markup=kb.admin_main_keyboard())
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 pass # Ничего страшного, если сообщение не изменилось
            else: raise e # Перевыбрасываем другую ошибку
        except Exception as e:
             logger.warning(f"Could not edit message on generic cancel state: {e}")
             await callback.message.answer("Действие отменено.", reply_markup=kb.admin_main_keyboard())
        await callback.answer("Действие отменено.")

# Убедитесь, что router зарегистрирован в главном файле:
# dp.include_router(router)

# --- Управление Каналами ОП --- 

# Вспомогательная функция для отображения списка каналов
async def _show_admin_manage_channels(callback: CallbackQuery, session: AsyncSession, check_type: str):
    if check_type == 'start':
        # --- Получаем ВСЕ каналы типа start, включая оба этапа ---
        channels = await db.get_start_check_channels(session) # Эта функция должна возвращать каналы обоих этапов для типа 'start'
        title = "📢 Каналы ОП (Старт - Этапы 1 и 2):"
    elif check_type == 'withdraw':
        channels = await db.get_withdraw_check_channels(session)
        title = "📢 Каналы ОП (Вывод):" # Предполагаем, что у вывода нет этапов
    else:
        await callback.answer("❌ Неизвестный тип каналов ОП.", show_alert=True)
        return

    # --- Сортируем по этапу для наглядности ---
    channels.sort(key=lambda ch: (ch.check_stage, ch.id))

    if channels:
        message_text = f"{title}\n\n"
        current_stage = 0
        for ch in channels:
             # --- Добавляем заголовок этапа при смене ---
             if ch.check_stage != current_stage:
                 message_text += f"\n<b>--- Этап {ch.check_stage} ---</b>\n"
                 current_stage = ch.check_stage

             status_mark = "✅" if ch.channel_status == 'Публичный' else "🔒"
             premium_req_mark = ""
             if ch.premium_requirement == 'premium_only':
                 premium_req_mark = " [⭐️]"
             elif ch.premium_requirement == 'non_premium_only':
                 premium_req_mark = " [🚫⭐️]"
             # --- Отображаем ID базы данных для управления ---
             message_text += f"- {status_mark} {html.escape(ch.channel_name)} (<code>{ch.channel_id}</code> / DB ID: {ch.id}){premium_req_mark}\n"
    else:
        message_text = f"{title}\n\nКаналов этого типа пока нет."

    # --- Используем обновленную клавиатуру ---
    reply_markup = kb.admin_channels_list_keyboard(channels, check_type)

    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение со списком каналов ({check_type}): {e}")
        try:
             # Удаляем старое и отправляем новое, если редактирование не удалось
             await callback.message.delete()
             await callback.message.answer(message_text, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e2:
             logger.error(f"Не удалось отправить список каналов ОП ({check_type}): {e2}")

    await callback.answer()

# Кнопка "Управление каналами (Старт)"
@router.callback_query(F.data == "admin_manage_channels_start")
@admin_required
async def admin_manage_channels_start(callback: CallbackQuery, config: Config, session: AsyncSession):
    await _show_admin_manage_channels(callback, session, 'start')

# Кнопка "Управление каналами (Вывод)"
@router.callback_query(F.data == "admin_manage_channels_withdraw")
@admin_required
async def admin_manage_channels_withdraw(callback: CallbackQuery, config: Config, session: AsyncSession):
    await _show_admin_manage_channels(callback, session, 'withdraw')

# --- FSM для добавления канала --- 

# Старт добавления канала (Старт)
@router.callback_query(F.data == "admin_add_channel_start", StateFilter(None))
async def admin_add_channel_start_start(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.update_data(check_type='start') # Сохраняем тип в FSM
    logger.info(f"Admin {callback.from_user.id} starting to add a new channel (type: start).")
    await callback.message.edit_text("🆔 Введите числовой ID канала/чата (для ОП Старт):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddChannelState.waiting_for_id)
    await callback.answer()

# Старт добавления канала (Вывод)
@router.callback_query(F.data == "admin_add_channel_withdraw", StateFilter(None))
async def admin_add_channel_start_withdraw(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.update_data(check_type='withdraw') # Сохраняем тип в FSM
    logger.info(f"Admin {callback.from_user.id} starting to add a new channel (type: withdraw).")
    await callback.message.edit_text("🆔 Введите числовой ID канала/чата (для ОП Вывод):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddChannelState.waiting_for_id)
    await callback.answer()

@router.message(AddChannelState.waiting_for_id)
async def process_channel_id(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    try:
        channel_id = int(message.text)
        # Проверка на уникальность TG ID
        existing_channel = await db.get_channel(session, channel_id)
        if existing_channel:
             await message.answer(f"❌ Канал с ID <code>{channel_id}</code> уже добавлен. Введите другой ID:", reply_markup=kb.cancel_state_keyboard())
             return
    except ValueError:
        await message.answer("❌ ID канала должен быть числом. Попробуйте еще раз:", reply_markup=kb.cancel_state_keyboard())
        return

    await state.update_data(channel_id=channel_id)
    await message.answer("🏷️ Введите название канала (для отображения в админке):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddChannelState.waiting_for_name)

@router.message(AddChannelState.waiting_for_name)
async def process_channel_name(message: Message, config: Config, state: FSMContext):
    name = message.text.strip()
    if not name:
         await message.answer("❌ Название не может быть пустым. Введите название:", reply_markup=kb.cancel_state_keyboard())
         return

    await state.update_data(channel_name=name)
    await message.answer("🔗 Введите полную ссылку на канал (вида https://t.me/...):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(AddChannelState.waiting_for_link)

# Обработчик ссылки - теперь переходит к выбору Premium
@router.message(AddChannelState.waiting_for_link)
async def process_channel_link(message: Message, config: Config, state: FSMContext):
    link = message.text.strip()
    if not link.startswith("https://t.me/"):
        await message.answer("❌ Ссылка должна начинаться с https://t.me/ . Введите корректную ссылку:", reply_markup=kb.cancel_state_keyboard())
        return

    await state.update_data(channel_link=link)
    # --- Переход к новому состоянию ---
    await message.answer(
        "⭐️ Для кого предназначен этот канал?",
        reply_markup=kb.admin_channel_premium_options_keyboard() # Используем новую клавиатуру
    )
    await state.set_state(AddChannelState.waiting_for_premium_requirement)
    # ----------------------------------

# --- Новый обработчик для выбора Premium ---
@router.callback_query(F.data.startswith("addchannel_premium_"), AddChannelState.waiting_for_premium_requirement)
async def process_channel_premium_requirement(callback: CallbackQuery, config: Config, state: FSMContext):
    # --- Добавляем логирование ---
    logger.debug(f"[Channel Premium Req] Received callback data: {callback.data}")

    value_part = callback.data[len("addchannel_premium_"):]
    logger.debug(f"[Channel Premium Req] Extracted value part: {value_part}")

    requirement_map = {
        'all': 'all',
        'only': 'premium_only',
        'non_premium': 'non_premium_only'
    }
    
    premium_requirement = requirement_map.get(value_part)
    logger.debug(f"[Channel Premium Req] Mapped premium requirement: {premium_requirement}")
    # ---------------------------

    if premium_requirement is None:
        logger.warning(f"[Channel Premium Req] Invalid premium requirement callback data value part: {value_part}")
        await callback.answer("Некорректный выбор требования Premium.", show_alert=True)
        # Не выходим из состояния, просто просим выбрать снова
        return

    await state.update_data(premium_requirement=premium_requirement)
    # --- Переходим к запросу статуса (публичный/не проверять) ---
    try:
        await callback.message.edit_text(
             "❓ Сделать подписку на этот канал обязательной (публичной)?",
             reply_markup=kb.yes_no_keyboard("addchannel_status")
        )
    except Exception as e:
         logger.debug(f"Failed to edit message for channel status query: {e}")
         await callback.message.answer(
              "❓ Сделать подписку на этот канал обязательной (публичной)?",
              reply_markup=kb.yes_no_keyboard("addchannel_status")
         )
    await state.set_state(AddChannelState.waiting_for_status)
    await callback.answer()
# -----------------------------------------

# Обработчик статуса (публичный/не проверять) - теперь идет после Premium
@router.callback_query(F.data.startswith("addchannel_status_"), AddChannelState.waiting_for_status)
async def process_channel_status(callback: CallbackQuery, config: Config, state: FSMContext):
    is_mandatory = callback.data.endswith("_yes")
    channel_status = "Публичный" if is_mandatory else "Не проверять"
    await state.update_data(channel_status=channel_status)

    user_data = await state.get_data()

    # --- Обновляем текст подтверждения ---
    premium_req = user_data.get('premium_requirement', 'all')
    premium_req_text_map = {
        'all': 'Всем пользователям',
        'premium_only': 'Только Premium [⭐️]',
        'non_premium_only': 'Только НЕ Premium [🚫⭐️]'
    }
    premium_req_display = premium_req_text_map.get(premium_req, premium_req)

    confirm_text = f"""
    Проверьте данные нового канала:
    -------------------------------------
    <b>ID:</b> <code>{user_data.get('channel_id')}</code>
    <b>Название:</b> {html.escape(user_data.get('channel_name'))}
    <b>Ссылка:</b> {html.escape(user_data.get('channel_link'))}
    <b>Доступность:</b> {premium_req_display}
    <b>Статус проверки:</b> {channel_status} {'✅' if is_mandatory else '🔒'}
    -------------------------------------
    Сохранить этот канал?
    """
    # ------------------------------------
    try:
        await callback.message.edit_text(confirm_text, reply_markup=kb.yes_no_keyboard("addchannel_confirm"))
    except Exception as e:
        logger.debug(f"Failed to edit message for channel confirmation: {e}")
        await callback.message.answer(confirm_text, reply_markup=kb.yes_no_keyboard("addchannel_confirm"))

    await state.set_state(AddChannelState.confirming)
    await callback.answer()


# Обработка финального подтверждения - добавляем premium_requirement
@router.callback_query(AddChannelState.confirming)
@admin_required
async def process_channel_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    user_id = callback.from_user.id
    if callback.data.endswith("_yes"):
        user_data = await state.get_data()
        channel_status = user_data.get('channel_status', 'Публичный')
        check_type = user_data.get('check_type', 'start')
        premium_requirement = user_data.get('premium_requirement', 'all') # <-- Получаем настройку Premium

        try:
            # Передаем premium_requirement в функцию БД
            new_channel = await db.add_channel(
                session=session,
                channel_id=user_data.get('channel_id'),
                channel_name=user_data.get('channel_name'),
                channel_link=user_data.get('channel_link'),
                channel_status=channel_status,
                check_type=check_type,
                premium_requirement=premium_requirement # <-- Передаем сюда
            )
            if new_channel:
                # Коммитим изменения здесь, после успешной подготовки
                await session.commit()
                logger.info(f"Admin {user_id} successfully added channel ID {new_channel.channel_id} ({new_channel.channel_name}) with status {new_channel.channel_status}, type {new_channel.check_type}, premium {new_channel.premium_requirement}")
                await callback.message.edit_text(f"✅ Канал \"{html.escape(new_channel.channel_name)}\" (Тип: {check_type}, Premium: {premium_requirement}) успешно добавлен!")
            else:
                 await session.rollback() # Откатываем, если add_channel вернул None (ошибка)
                 logger.warning(f"Admin {user_id} failed to add channel (duplicate or other issue) for data: {user_data}")
                 await callback.message.edit_text("❌ Ошибка при добавлении канала (возможно, дубликат?).")
        except Exception as e:
            await session.rollback() # Откатываем при любом исключении
            logger.error(f"Admin {user_id} failed to add channel: {e}", exc_info=True)
            await callback.message.edit_text("❌ Произошла ошибка при сохранении канала.")
        finally:
             await state.clear()
    elif callback.data.endswith("_no"):
        logger.info(f"Admin {user_id} cancelled channel addition at confirmation step.")
        await state.clear()
        await callback.message.edit_text("Добавление канала отменено.")
    await callback.answer()

# --- Удаление канала --- 

# Обновляем фильтр и обработчик запроса на удаление
@router.callback_query(lambda c: c.data.startswith("admin_channel_delete_") and len(c.data.split("_")) == 5 and c.data.split("_")[4].isdigit())
@admin_required
async def admin_delete_channel_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        parts = callback.data.split("_")
        check_type = parts[3] # Получаем тип
        channel_db_id = int(parts[4]) # Получаем ID 
        
        # TODO: Добавить get_channel_by_db_id и использовать его здесь для показа имени канала.
        # channel = await db.get_channel_by_db_id(session, channel_db_id)
        # channel_name_msg = f" канала \"{html.escape(channel.channel_name)}\" (DB ID: {channel_db_id})" if channel else f" канала (DB ID: {channel_db_id})"
        channel_name_msg = f"канала (DB ID: {channel_db_id}, Тип: {check_type})"

        try:
            await callback.message.edit_text(
                f"""🗑️ Вы уверены, что хотите удалить {channel_name_msg} из списка ОП?

    ⚠️ Пользователи больше не будут проверяться на подписку на него для типа '{check_type}'.""",
                # Передаем тип в клавиатуру подтверждения
                reply_markup=kb.admin_channel_delete_confirm_keyboard(channel_db_id, check_type)
            )
        except Exception as e:
            logger.debug(f"Не удалось изменить сообщение перед удалением канала {channel_db_id} ({check_type}): {e}")
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing channel delete callback data: {e}")
        await callback.answer("Ошибка: Неверный формат данных для удаления канала.", show_alert=True)

# Обновляем обработчик подтверждения удаления
# Фильтр теперь должен начинаться с префикса и иметь 6 частей
@router.callback_query(lambda c: c.data.startswith("admin_channel_delete_confirm_") and len(c.data.split("_")) == 6 and c.data.split("_")[5].isdigit())
async def admin_delete_channel_confirm(callback: CallbackQuery, config: Config, session: AsyncSession):
    parts = callback.data.split("_")
    check_type = parts[4] # Получаем тип
    channel_db_id = int(parts[5]) # Получаем ID
    
    deleted = await db.delete_channel(session, channel_db_id)

    if deleted:
        logger.info(f"Admin {callback.from_user.id} deleted channel with DB ID #{channel_db_id} (Type: {check_type})")
        await callback.answer(f"✅ Канал (Тип: {check_type}) удален.", show_alert=True)
        # Показываем обновленный список каналов нужного типа
        if check_type == 'start':
            await admin_manage_channels_start(callback, config, session) 
        elif check_type == 'withdraw':
             await admin_manage_channels_withdraw(callback, config, session)
        else: # На всякий случай, возврат в гл. меню
             await cmd_admin_panel(callback)
    else:
        logger.warning(f"Admin {callback.from_user.id} failed to delete non-existent channel with DB ID {channel_db_id}")
        await callback.answer("❌ Не удалось удалить канал (возможно, он уже удален).", show_alert=True)
        # Показываем список каналов нужного типа
        if check_type == 'start':
            await admin_manage_channels_start(callback, config, session) 
        elif check_type == 'withdraw':
             await admin_manage_channels_withdraw(callback, config, session)
        else:
            await cmd_admin_panel(callback)


# --- Конец Управления Каналами ОП --- 

# --- Управление Промокодами ---

# Кнопка "Управление промокодами"
@router.callback_query(F.data == "admin_manage_promocodes")
@admin_required
async def admin_manage_promocodes(callback: CallbackQuery, config: Config, session: AsyncSession):
    # --- Добавим функцию в requests.py для получения всех промокодов --- 
    promocodes = await db.get_all_promocodes(session) 

    if promocodes:
        message_text = "🎁 Список промокодов:\n\n"
        for promo in promocodes:
            status_icon = "🟢" if promo.is_active else "🔴"
            uses_info = f"{promo.uses_count}"
            if promo.max_uses is not None:
                uses_info += f"/{promo.max_uses}"
            req_refs = f" (ReqRefs: {promo.required_referrals_all_time})" if promo.required_referrals_all_time else ""
            message_text += f"{status_icon} <code>{html.escape(promo.code)}</code> ({uses_info} uses{req_refs}) - {promo.reward}⭐️\n"
    else:
        message_text = "🎁 Промокодов пока нет."

    reply_markup = kb.admin_promocodes_list_keyboard(promocodes)

    try:
        await callback.message.edit_text(message_text, reply_markup=reply_markup)
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение со списком промокодов: {e}")
        try:
             await callback.message.answer(message_text, reply_markup=reply_markup)
        except Exception as e2:
             logger.error(f"Не удалось отправить список промокодов: {e2}")

    await callback.answer()

# --- FSM для добавления промокода --- 

@router.callback_query(F.data == "admin_add_promo", StateFilter(None))
@admin_required
async def admin_add_promo_start(callback: CallbackQuery, config: Config, state: FSMContext):
    logger.info(f"Admin {callback.from_user.id} starting to add a new promocode.")
    await callback.message.edit_text("✍️ Введите текст нового промокода (например, VESNA2025):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(st.AddPromoCodeState.waiting_for_code)
    await callback.answer()

@router.message(st.AddPromoCodeState.waiting_for_code)
@admin_required
async def process_promo_code_text(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    code = message.text.strip()
    if not code:
        await message.answer("❌ Код не может быть пустым.", reply_markup=kb.cancel_state_keyboard())
        return
        
    # Проверка на уникальность
    existing_promo = await db.get_promocode_by_code(session, code) # Используем уже существующую функцию (она ищет активные, но для уникальности надо бы искать все? TODO: Улучшить)
    # TODO: Лучше создать отдельную функцию db.check_promocode_exists(code) которая ищет и активные и неактивные.
    if existing_promo: 
        await message.answer(f"❌ Промокод <code>{html.escape(code)}</code> уже существует. Введите другой.", reply_markup=kb.cancel_state_keyboard())
        return

    await state.update_data(code=code)
    await message.answer("💰 Введите награду за активацию (число, например 5 или 10.5):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(st.AddPromoCodeState.waiting_for_reward)

@router.message(st.AddPromoCodeState.waiting_for_reward)
@admin_required
async def process_promo_reward(message: Message, config: Config, state: FSMContext):
    try:
        reward = float(message.text.replace(',', '.'))
        if reward <= 0:
             raise ValueError("Награда должна быть положительной")
        await state.update_data(reward=reward)
        await message.answer("🔢 Введите максимальное количество активаций (число > 0, или '-' для бесконечного):", reply_markup=kb.cancel_state_keyboard())
        await state.set_state(st.AddPromoCodeState.waiting_for_max_uses)
    except ValueError as e:
        await message.answer(f"❌ Ошибка: Введите корректное ПОЛОЖИТЕЛЬНОЕ число для награды (например, 5 или 10.5).", reply_markup=kb.cancel_state_keyboard())

@router.message(st.AddPromoCodeState.waiting_for_max_uses)
@admin_required
async def process_promo_max_uses(message: Message, config: Config, state: FSMContext):
    max_uses_text = message.text.strip()
    max_uses = None
    if max_uses_text != '-':
        try:
            max_uses = int(max_uses_text)
            if max_uses <= 0:
                raise ValueError("Количество активаций должно быть > 0")
        except ValueError:
             await message.answer("❌ Ошибка: Введите целое число больше нуля или '-' для бесконечного.", reply_markup=kb.cancel_state_keyboard())
             return
    await state.update_data(max_uses=max_uses)
    await message.answer("👥 Введите минимальное количество рефералов (за все время), которое должен иметь пользователь для активации (число >= 0, или '-' если условие не нужно):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(st.AddPromoCodeState.waiting_for_required_referrals)

@router.message(st.AddPromoCodeState.waiting_for_required_referrals)
@admin_required
async def process_promo_req_refs(message: Message, config: Config, state: FSMContext):
    """Обработка требования по количеству рефералов за все время и переход к рефералам за 24ч."""
    req_refs_text = message.text.strip()
    req_refs = None
    if req_refs_text != '-':
        try:
            req_refs = int(req_refs_text)
            if req_refs < 0:
                raise ValueError("Количество рефералов должно быть >= 0")
        except ValueError:
            await message.answer("❌ Ошибка: Введите целое число не меньше нуля или '-' если условие не нужно.", reply_markup=kb.cancel_state_keyboard())
            return
              
    await state.update_data(required_referrals_all_time=req_refs)
    
    # Переходим к запросу рефералов за 24 часа
    await message.answer("👥 Введите минимальное количество рефералов за 24 часа, которое должен иметь пользователь для активации (число >= 0, или '-' если условие не нужно):", reply_markup=kb.cancel_state_keyboard())
    await state.set_state(st.AddPromoCodeState.waiting_for_required_referrals_24h)

@router.message(st.AddPromoCodeState.waiting_for_required_referrals_24h)
@admin_required
async def process_promo_req_refs_24h(message: Message, config: Config, state: FSMContext):
    """Обработка требования по количеству рефералов за 24 часа и переход к подтверждению."""
    req_refs_text = message.text.strip()
    req_refs_24h = None
    if req_refs_text != '-':
        try:
            req_refs_24h = int(req_refs_text)
            if req_refs_24h < 0:
                raise ValueError("Количество рефералов должно быть >= 0")
        except ValueError:
            await message.answer("❌ Ошибка: Введите целое число не меньше нуля или '-' если условие не нужно.", reply_markup=kb.cancel_state_keyboard())
            return
              
    await state.update_data(required_referrals_24h=req_refs_24h)
    
    # Показываем подтверждение
    user_data = await state.get_data()
    await show_promocode_confirmation(message, config, state, user_data, )
    
async def show_promocode_confirmation(message: Message, config: Config, state: FSMContext, data: dict):
    code = data.get('code')
    reward = data.get('reward')
    max_uses = data.get('max_uses')
    req_refs = data.get('required_referrals_all_time')
    req_refs_24h = data.get('required_referrals_24h')

    max_uses_text = str(max_uses) if max_uses is not None else "Безлимитно"
    req_refs_text = str(req_refs) if req_refs is not None else "Не требуется"
    req_refs_24h_text = str(req_refs_24h) if req_refs_24h is not None else "Не требуется"

    confirm_text = f"""
Проверьте данные нового промокода:
-------------------------------------
<b>Код:</b> <code>{html.escape(code)}</code>
<b>Награда:</b> {reward}⭐️
<b>Макс. активаций:</b> {max_uses_text}
<b>Треб. рефералов (всего):</b> {req_refs_text}
<b>Треб. рефералов (за 24ч):</b> {req_refs_24h_text}
-------------------------------------
Сохранить этот промокод (он будет активен)?
"""
    await message.answer(confirm_text, reply_markup=kb.yes_no_keyboard("addpromo_confirm"))
    await state.set_state(st.AddPromoCodeState.confirming)

@router.callback_query(st.AddPromoCodeState.confirming)
async def add_promocode_confirm(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    user_id = callback.from_user.id
    user_data = await state.get_data()
    
    if callback.data.endswith("_yes"):
        try:
            # Получаем данные из состояния
            code = user_data.get('code')
            reward = float(user_data.get('reward', 0))
            max_uses = user_data.get('max_uses')
            required_referrals_all_time = user_data.get('required_referrals_all_time')
            required_referrals_24h = user_data.get('required_referrals_24h')
            
            # Добавляем промокод
            new_promo = await db.add_promocode(
                session=session,
                code=code,
                reward=reward,
                max_uses=max_uses,
                required_referrals_all_time=required_referrals_all_time,
                required_referrals_24h=required_referrals_24h
            )
            
            if new_promo:
                await callback.message.edit_text(f"✅ Промокод <code>{html.escape(code)}</code> успешно создан!", parse_mode="HTML")
                logger.info(f"Admin {user_id} successfully added promocode #{new_promo.id} ({new_promo.code})")
            else:
                await callback.message.edit_text("❌ Не удалось создать промокод. Возможно, код уже существует.")
                logger.warning(f"Admin {user_id} failed to add promocode '{user_data.get('code')}' (maybe duplicate?).")
        except Exception as e:
            await callback.message.edit_text(f"❌ Ошибка при создании промокода: {e}")
            logger.error(f"Admin {user_id} failed to add promocode: {e}", exc_info=True)
    else:
        # Отмена создания
        await callback.message.edit_text("❌ Создание промокода отменено.")
        logger.info(f"Admin {user_id} cancelled promocode addition.")
    
    # В любом случае сбрасываем состояние
    await state.clear()
    # И возвращаемся к меню промокодов
    await admin_manage_promocodes(callback, session)
    
# --- Просмотр, (де)активация, удаление промокода --- 

@router.callback_query(F.data.startswith("admin_promo_view_"))
@admin_required
async def admin_view_single_promo(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        promo_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        logger.error(f"Invalid promo ID in callback data: {callback.data}")
        await callback.answer("Ошибка: Неверный ID промокода.", show_alert=True)
        return

    promo = await db.get_promocode_by_id(session, promo_id)
    if not promo:
        await callback.answer("❌ Промокод не найден.", show_alert=True)
        await admin_manage_promocodes(callback, session)
        return

    uses_info = f"{promo.uses_count}"
    if promo.max_uses is not None:
        uses_info += f"/{promo.max_uses}"
    
    req_refs = f"\n<b>Треб. рефералов:</b> {promo.required_referrals_all_time}" if promo.required_referrals_all_time else ""
    
    status = "Активен ✅" if promo.is_active else "Неактивен ❌"
    promo_details = f"""
🎁 <b>Промокод:</b> <code>{html.escape(promo.code)}</code>
-------------------------------------
<b>ID:</b> {promo.id}
<b>Статус:</b> {status}
<b>Награда:</b> {promo.reward}⭐️
<b>Использований:</b> {uses_info}{req_refs}
<b>Создан:</b> {promo.created_at.strftime('%Y-%m-%d %H:%M')}
"""
    try:
        await callback.message.edit_text(
            promo_details, 
            reply_markup=kb.admin_promocode_manage_keyboard(promo)
        )
    except Exception as e:
        logger.debug(f"Не удалось изменить сообщение при просмотре промокода {promo_id}: {e}")
    await callback.answer()


# --- Запрос на удаление промокода ---
@router.callback_query(F.data.startswith("admin_promo_delete_"))
@admin_required
async def admin_promo_delete_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        promo_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        logger.error(f"Invalid promo ID in callback data: {callback.data}")
        await callback.answer("Ошибка: Неверный ID промокода.", show_alert=True)
        return

    promo = await db.get_promocode_by_id(session, promo_id)
    if not promo:
        await callback.answer("❌ Промокод не найден.", show_alert=True)
        await admin_manage_promocodes(callback, session)
        return

    text = f"❓ Вы уверены, что хотите удалить промокод <code>{html.escape(promo.code)}</code> (ID: {promo.id})?"
    reply_markup = kb.admin_promocode_delete_confirm_keyboard(promo_id)
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest: # Если сообщение не изменилось
        pass
    await callback.answer()


# --- Подтверждение удаления промокода ---
@router.callback_query(F.data.startswith("confirm_admin_promo_delete_"))
@admin_required
async def admin_promo_delete_confirm(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        promo_id = int(callback.data.split("_")[-1])
        admin_id = callback.from_user.id
        logger.info(f"Admin {admin_id} confirmed deletion of promo ID: {promo_id}")
        
        # Используем существующую функцию вместо новой
        deleted = await db.delete_promo_code_by_id(session, promo_id)
        
        if deleted:
            logger.info(f"Successfully deleted promo ID {promo_id}")
            await callback.answer("✅ Промокод успешно удален!", show_alert=True)
        else:
            logger.warning(f"Failed to delete promo ID {promo_id}")
            await callback.answer("❌ Не удалось удалить промокод. Возможно, он уже удален.", show_alert=True)
        
        # В любом случае возвращаемся к списку промокодов
        await admin_manage_promocodes(callback, session)
        
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing promo_id from '{callback.data}': {e}")
        await callback.answer("❌ Некорректный ID промокода", show_alert=True)
        await admin_manage_promocodes(callback, session)
    except Exception as e:
        logger.error(f"Unexpected error during promo deletion: {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при удалении промокода", show_alert=True)
        try:
            await admin_manage_promocodes(callback, session)
        except Exception as menu_e:
            logger.error(f"Failed to show promocodes menu after error: {menu_e}")
            await callback.message.edit_text(
                "Произошла ошибка. Возврат в главное меню админки.",
                reply_markup=kb.admin_main_keyboard()
            )

@router.callback_query(F.data.startswith("admin_promo_toggle_"))
@admin_required
async def admin_toggle_promo_status(callback: CallbackQuery, config: Config, session: AsyncSession):
    promo_id = int(callback.data.split("_")[3])
    # --- Нужна функция для изменения статуса --- 
    updated_promo = await db.set_promocode_active_status(session, promo_id)

    if updated_promo:
        status_text = "активирован" if updated_promo.is_active else "деактивирован"
        await callback.answer(f"✅ Промокод #{promo_id} ({updated_promo.code}) {status_text}.")
        # Обновляем сообщение с деталями промокода
        await admin_view_single_promo(callback, session)
    else:
        await callback.answer("❌ Не удалось изменить статус промокода.", show_alert=True)


# --- Настройки Наград ---

# Обработчик кнопки "Настройки наград"
@router.callback_query(F.data == "admin_manage_rewards", StateFilter(None))
@admin_required
async def admin_manage_rewards(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        current_reward = await db.get_refferal_reward(session)
        text = "💰 Настройки Наград:\n\nЗдесь вы можете изменить значения наград."
        reply_markup = kb.admin_rewards_keyboard(current_reward)
        await callback.message.edit_text(text, reply_markup=reply_markup)
        await callback.answer()
    except Exception as e:
        logger.error(f"Error fetching rewards settings: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при загрузке настроек наград.", show_alert=True)
        # Возвращаем в главное админ-меню в случае ошибки
        await cmd_admin_panel(callback) # Используем cmd_admin_panel для возврата


# Обработчик кнопки "Изменить реф. награду" - старт FSM
@router.callback_query(F.data == "admin_change_ref_reward", StateFilter(None))
@admin_required
async def admin_change_ref_reward_start(callback: CallbackQuery, config: Config, state: FSMContext):
    logger.info(f"Admin {callback.from_user.id} starting to change referral reward.")
    await callback.message.edit_text(
        "💰 Введите новое значение реферальной награды (число, например 1.5 или 2):",
        reply_markup=kb.cancel_state_keyboard() # Добавляем кнопку отмены
    )
    await state.set_state(RewardSettingsState.waiting_for_ref_reward)
    await callback.answer()


# Обработчик ввода новой реферальной награды
@router.message(RewardSettingsState.waiting_for_ref_reward)
@admin_required
async def process_new_ref_reward(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    try:
        new_reward = float(message.text.replace(',', '.'))
        if new_reward < 0:
            raise ValueError("Награда не может быть отрицательной.")

        # Обновляем награду в БД
        updated = await db.update_refferal_reward(session, new_reward)

        if updated:
            logger.info(f"Admin {message.from_user.id} updated referral reward to {new_reward}")
            await message.answer(f"✅ Реферальная награда успешно изменена на {new_reward:.2f}⭐️.")
            await state.clear()
            # Показываем обновленное меню настроек
            # Нужен callback.message, а у нас message. Создадим фейковый CallbackQuery
            # Это не самый чистый способ, но простой. Лучше было бы сохранить ID сообщения.
            # В Aiogram 3 можно использовать bot.edit_message_text по chat_id и message_id, если сохранить их в state.
            # Пока оставим так, админ увидит подтверждение и может нажать "Назад".
            # --- Попробуем показать обновленное меню через новый запрос ---
            current_reward_after_update = await db.get_refferal_reward(session)
            text = "💰 Настройки Наград:\n\nРеферальная награда обновлена."
            reply_markup = kb.admin_rewards_keyboard(current_reward_after_update)
            await message.answer(text, reply_markup=reply_markup) # Отправляем новым сообщением

        else:
            await message.answer("❌ Не удалось обновить реферальную награду в базе данных.")
            # Не очищаем состояние, чтобы админ мог попробовать снова или отменить
    except ValueError as e:
        logger.warning(f"Admin {message.from_user.id} entered invalid referral reward '{message.text}': {e}")
        await message.answer(
            f"❌ Ошибка: Введите корректное ПОЛОЖИТЕЛЬНОЕ число для награды (например, 1.5 или 2).",
            reply_markup=kb.cancel_state_keyboard() # Снова показываем кнопку отмены
        )
    except Exception as e:
        logger.error(f"Error processing new referral reward: {e}", exc_info=True)
        await message.answer("❌ Произошла непредвиденная ошибка при обновлении награды.")
        await state.clear() # Очищаем состояние при неизвестной ошибке

# --- Конец Настроек Наград ---

@router.callback_query(F.data == "newsletter", StateFilter(None))
@admin_required
async def newsletter_start(callback: CallbackQuery, config: Config, state: FSMContext):
    """Начинает процесс рассылки, предлагая выбор источника."""
    await state.clear() # Очищаем предыдущее состояние на всякий случай
    await callback.message.edit_text(
        "Выберите источник для сообщения рассылки:",
        reply_markup=kb.newsletter_source_keyboard()
    )
    # Можно установить начальное состояние, если оно нужно перед выбором
    # await state.set_state(NewsletterStates.choosing_source) # Если есть такое состояние

# Обработчик для кнопки "Создать новую" (переводит в старую логику FSM)
@router.callback_query(F.data == "newsletter_create_new")
@admin_required
async def newsletter_create_new_start(callback: CallbackQuery, config: Config, state: FSMContext):
    """Переход к созданию рассылки с нуля (старая логика)."""
    await callback.answer()
    # Здесь должен быть код, который запрашивает текст сообщения (как было раньше)
    await callback.message.edit_text(
        "Введите текст для рассылки. Используйте HTML-теги для форматирования.",
        reply_markup=kb.cancel_state_keyboard() # Клавиатура отмены
    )
    await state.set_state(NewsletterStates.getting_text) # Переходим в состояние ввода текста

# Обработчик для кнопки "Использовать шаблон"
@router.callback_query(F.data == "newsletter_use_template")
async def newsletter_prompt_select_template(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Запрашивает выбор шаблона для рассылки."""
    templates = await db.get_all_broadcast_templates(session)
    if not templates:
        await callback.answer("Нет доступных шаблонов для использования.", show_alert=True)
        return

    message_text = "Выберите шаблон, который хотите использовать для рассылки:"
    reply_markup = kb.select_newsletter_template_keyboard(templates)

    try:
        # Пытаемся отредактировать исходное сообщение
        await callback.message.edit_text(
            message_text,
            reply_markup=reply_markup
        )
        # Отвечаем на коллбэк после успешного редактирования
        await callback.answer()
    except TelegramBadRequest as e:
        # Если ошибка "нет текста для редактирования", отправляем новое сообщение
        if "there is no text in the message to edit" in str(e):
            logger.warning(f"Cannot edit message (no text?) to show template list. Sending new message. Error: {e}")
            await callback.message.answer( # Отправляем новое сообщение
                message_text,
                reply_markup=reply_markup
            )
            # Отвечаем на коллбэк, чтобы убрать часики
            await callback.answer()
        else:
            # Если другая ошибка BadRequest, логируем и отвечаем
            logger.error(f"TelegramBadRequest while trying to show template list: {e}")
            await callback.answer("Произошла ошибка при отображении шаблонов.", show_alert=True)
            return # Выходим, если была другая ошибка
    except Exception as e:
        # Обработка других непредвиденных ошибок
        logger.error(f"Unexpected error while trying to show template list: {e}", exc_info=True)
        await callback.answer("Произошла непредвиденная ошибка.", show_alert=True)
        return # Выходим

    # Устанавливаем состояние ожидания выбора шаблона
    await state.set_state(NewsletterStates.selecting_template)

# Обработчик выбора конкретного шаблона
@router.callback_query(F.data.startswith("newsletter_select_template_"), NewsletterStates.selecting_template)
async def newsletter_template_selected(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Обрабатывает выбор шаблона, загружает данные и запрашивает подтверждение."""
    template_id = int(callback.data.split("_")[-1])
    template = await db.get_broadcast_template_by_id(session, template_id)

    if not template:
        await callback.answer("❌ Шаблон не найден (возможно, был удален).", show_alert=True)
        # Обновляем список шаблонов
        templates = await db.get_all_broadcast_templates(session)
        await callback.message.edit_text(
            "Выберите шаблон:",
            reply_markup=kb.select_newsletter_template_keyboard(templates)
        )
        return # Остаемся в состоянии выбора

    decoded_text = html.unescape(template.text)
    await state.update_data(
        newsletter_text=decoded_text,
        newsletter_photo_id=template.photo_file_id,
        newsletter_keyboard_json=template.keyboard_json,
        # Можно добавить ID шаблона, если нужно для логов
        selected_template_id=template.id
    )

    # --- Показываем предпросмотр и запрашиваем подтверждение ---
    preview_text = f"<b>Рассылка на основе шаблона '{html.escape(template.name)}'</b>\n\n"
    final_text = decoded_text if template.text else "<i>(Нет текста)</i>"
    preview_text += final_text

    reply_markup = None
    if template.keyboard_json:
        try:
            preview_keyboard = template.get_keyboard() # Используем метод модели
            if preview_keyboard:
                reply_markup = preview_keyboard
            else:
                 preview_text += "\n\n<i>(Не удалось построить клавиатуру из JSON)</i>"
        except Exception as e:
            logger.warning(f"Could not build preview keyboard for newsletter from template {template_id}: {e}")
            preview_text += "\n\n<i>(Ошибка при построении клавиатуры из JSON)</i>"

    # Добавляем кнопки подтверждения/отмены ПОД клавиатурой шаблона (если она есть)
    confirm_builder = InlineKeyboardBuilder()
    confirm_builder.button(text="✅ Запустить рассылку", callback_data="newsletter_confirm_send")
    confirm_builder.button(text="◀️ Выбрать другой шаблон", callback_data="newsletter_use_template") # Возврат к списку
    confirm_builder.button(text="❌ Отменить рассылку", callback_data="cancel_state") # Общая отмена
    confirm_builder.adjust(1)

    if reply_markup:
        # Объединяем клавиатуру шаблона и кнопки управления
        template_builder = InlineKeyboardBuilder.from_markup(reply_markup)
        for row in confirm_builder.export():
            template_builder.row(*row) # Добавляем кнопки управления под кнопками шаблона
        final_reply_markup = template_builder.as_markup()
    else:
        final_reply_markup = confirm_builder.as_markup()


    # Отправляем предпросмотр
    common_params = {
        "text": preview_text, # Не обрезаем здесь, Telegram сам обработает лимиты
        "reply_markup": final_reply_markup,
        "disable_web_page_preview": True,
        "parse_mode": "HTML"
    }

    try:
        # Удаляем предыдущее сообщение (список шаблонов)
        await callback.message.delete()
    except TelegramBadRequest:
        pass # Игнорируем, если не получилось

    if template.photo_file_id:
        try:
            await callback.message.answer_photo(
                photo=template.photo_file_id,
                caption=common_params["text"],
                reply_markup=common_params["reply_markup"],
                parse_mode="HTML"  # Убедитесь, что parse_mode установлен
            )
        except TelegramBadRequest as e:
            # Обработка ошибки длины caption
            if "caption is too long" in str(e):
                logger.warning(f"Caption too long for template {template_id} preview. Sending truncated.")
                await callback.message.answer_photo(
                    photo=template.photo_file_id,
                    caption=common_params["text"][:1024],  # Обрезаем до 1024 для фото
                    reply_markup=common_params["reply_markup"],
                    parse_mode="HTML"  # Убедитесь, что parse_mode установлен
                )
            else:
                logger.error(f"Error sending template preview photo: {e}")
                # Можно отправить текстовое сообщение об ошибке или просто проигнорировать предпросмотр
                await callback.message.answer("Не удалось показать предпросмотр с фото из-за ошибки.")
                # Важно не переходить в состояние подтверждения, если предпросмотр не удался
                return  # Выходим, чтобы пользователь мог выбрать другой шаблон
    else:
        # Для текстовых сообщений parse_mode тоже не помешает
        await callback.message.answer(
            text=common_params["text"][:4096], # Обрезаем для текстового сообщения
            reply_markup=common_params["reply_markup"],
            disable_web_page_preview=common_params["disable_web_page_preview"],
            parse_mode="HTML" # <-- Добавляем parse_mode и сюда
        )

    # Устанавливаем состояние подтверждения
    await state.set_state(NewsletterStates.confirming_send)


# --- СУЩЕСТВУЮЩИЕ ОБРАБОТЧИКИ РАССЫЛКИ (нужно адаптировать) ---

# Обработчик получения текста (если создаем новую)
@router.message(NewsletterStates.getting_text, F.text)
async def newsletter_process_text(message: Message, config: Config, state: FSMContext):
    # ... (старая логика получения текста) ...
    # Сохраняем текст в state (например, newsletter_text)
    await state.update_data(newsletter_text=message.html_text)
    # Запрашиваем фото
    await message.answer(
        "Теперь отправьте <b>фотографию</b> для рассылки.\n"
        "Если фото не нужно, нажмите 'Пропустить'.",
        # --- Используем правильное имя функции ---
        reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_photo")
        # -----------------------------------------
    )
    await state.set_state(NewsletterStates.getting_photo)

# Обработчик пропуска фото (если создаем новую)
@router.callback_query(F.data == "newsletter_skip_photo", NewsletterStates.getting_photo)
async def newsletter_skip_photo_handler(callback: CallbackQuery, config: Config, state: FSMContext):
    # ... (старая логика пропуска фото) ...
    await state.update_data(newsletter_photo_id=None)
    # Запрашиваем клавиатуру
    await callback.message.edit_text( # Используем edit_text для изменения предыдущего сообщения
        "Теперь отправьте <b>JSON-структуру</b> для inline-клавиатуры.\n"
        # --- Добавляем пример JSON ---
        "Пример:\n<pre><code class=\"language-json\">"
        "[[{\"text\": \"Кнопка 1\", \"url\": \"https://t.me/durov\"}],\n"
        " [{\"text\": \"Кнопка 2\", \"callback_data\": \"some_data\"}]]"
        "</code></pre>\n"
        # -----------------------------
        "Или нажмите 'Пропустить', если клавиатура не нужна.",
        # --- Используем правильное имя функции ---
        reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_keyboard"),
        # -----------------------------------------
        disable_web_page_preview=True
    )
    await state.set_state(NewsletterStates.getting_keyboard)

# Обработчик получения фото (если создаем новую)
@router.message(NewsletterStates.getting_photo, F.photo)
async def newsletter_process_photo(message: Message, config: Config, state: FSMContext):
    # ... (старая логика получения фото) ...
    photo_file_id = message.photo[-1].file_id
    await state.update_data(newsletter_photo_id=photo_file_id)
    # Запрашиваем клавиатуру
    await message.answer(
        "Теперь отправьте <b>JSON-структуру</b> для inline-клавиатуры.\n"
        # --- Добавляем пример JSON ---
        "Пример:\n<pre><code class=\"language-json\">"
        "[[{\"text\": \"Кнопка 1\", \"url\": \"https://t.me/durov\"}],\n"
        " [{\"text\": \"Кнопка 2\", \"callback_data\": \"some_data\"}]]"
        "</code></pre>\n"
        # -----------------------------
        "Или нажмите 'Пропустить', если клавиатура не нужна.",
        # --- Используем правильное имя функции ---
        reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_keyboard"),
        # -----------------------------------------
        disable_web_page_preview=True
    )
    await state.set_state(NewsletterStates.getting_keyboard)

# Обработчики получения/пропуска клавиатуры (если создаем новую)
# Обработчик пропуска клавиатуры
@router.callback_query(F.data == "newsletter_skip_keyboard", NewsletterStates.getting_keyboard)
async def newsletter_skip_keyboard_handler(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    await callback.answer("Клавиатура пропущена.")
    await state.update_data(newsletter_keyboard_json=None)
    # Переходим к подтверждению
    await show_manual_newsletter_confirmation(callback.message, config, state) # Используем функцию подтверждения ручного ввода
    await state.set_state(NewsletterStates.confirming_send)

# Обработчик получения JSON клавиатуры

@router.message(NewsletterStates.getting_keyboard, F.text)
async def newsletter_process_keyboard(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обрабатывает введенную клавиатуру в JSON или простом текстовом формате."""
    keyboard_text = message.text
    
    if keyboard_text.strip() == "-":
        # Сброс клавиатуры, если пользователь ввел тире
        await state.update_data(newsletter_keyboard_json=None)
        await message.answer("✅ Клавиатура пропущена.")
        await show_manual_newsletter_confirmation(message, config, state)
        await state.set_state(NewsletterStates.confirming_send)
        return
        
    # Проверяем, использует ли пользователь простой формат
    if "-" in keyboard_text and not keyboard_text.strip().startswith("["):
        try:
            keyboard_markup = BroadcastTemplate.parse_simple_keyboard(keyboard_text)
            if keyboard_markup:
                # Создаем JSON из полученной клавиатуры
                keyboard_data = []
                for row in keyboard_markup.inline_keyboard:
                    row_data = []
                    for button in row:
                        button_dict = {
                            'text': button.text,
                            'url': button.url,
                            'callback_data': button.callback_data,
                        }
                        row_data.append({k: v for k, v in button_dict.items() if v is not None})
                    keyboard_data.append(row_data)
                keyboard_json = json.dumps(keyboard_data, ensure_ascii=False)
                await state.update_data(newsletter_keyboard_json=keyboard_json)
                await message.answer("✅ Клавиатура в простом формате принята.")
                await show_manual_newsletter_confirmation(message, config, state)
                await state.set_state(NewsletterStates.confirming_send)
                return
            else:
                await message.answer(
                    "❌ Не удалось создать клавиатуру из простого формата. Проверьте синтаксис и попробуйте снова.\n"
                    "Пример:\n"
                    "Кнопка1 - https://t.me/link1\n"
                    "Кнопка2 - https://t.me/link2 / Кнопка3 - https://t.me/link3",
                    reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_keyboard")
                )
                return
        except Exception as e:
            logger.error(f"Error processing simple keyboard format for newsletter: {e}", exc_info=True)
            await message.answer(
                "❌ Произошла ошибка при обработке простого формата клавиатуры. Попробуйте снова или используйте JSON формат.",
                reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_keyboard")
            )
            return

    # Если это JSON формат, проверяем его
    keyboard_json_text = keyboard_text
    try:
        parsed_json = json.loads(keyboard_json_text)
        if not isinstance(parsed_json, list) or not all(isinstance(row, list) for row in parsed_json): raise ValueError("JSON structure must be a list of lists.")
        if not all(isinstance(button, dict) for row in parsed_json for button in row): raise ValueError("Each element in inner lists must be a dictionary.")

        await state.update_data(newsletter_keyboard_json=keyboard_json_text)
        await message.answer("✅ JSON клавиатуры принят.")
        await show_manual_newsletter_confirmation(message, config, state)
        await state.set_state(NewsletterStates.confirming_send)

    except (json.JSONDecodeError, ValueError) as e:
        await message.answer(
            f"❌ Ошибка JSON: {e}. Проверьте формат и отправьте снова.\n\n"
            "Вы можете использовать один из двух форматов:\n\n"
            "1. Простой формат:\n"
            "Кнопка1 - https://t.me/link1\n"
            "Кнопка2 - https://t.me/link2 / Кнопка3 - https://t.me/link3\n\n"
            "2. JSON формат:\n"
            "<pre><code class=\"language-json\">[[{\"text\": \"Кнопка 1\", \"url\": \"https://t.me/durov\"}]]</code></pre>",
            reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_keyboard"),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Unexpected error processing newsletter keyboard JSON: {e}", exc_info=True)
        await message.answer(
            "❌ Произошла непредвиденная ошибка при обработке клавиатуры. Попробуйте снова или пропустите шаг.",
            reply_markup=kb.template_creation_skip_keyboard(skip_callback="newsletter_skip_keyboard")
        )


# --- Вспомогательная функция для показа подтверждения РУЧНОГО ввода ---
async def show_manual_newsletter_confirmation(message: Message, config: Config, state: FSMContext):
    """Показывает предпросмотр рассылки, созданной вручную."""
    data = await state.get_data()
    text = data.get("newsletter_text")
    photo_id = data.get("newsletter_photo_id")
    keyboard_json = data.get("newsletter_keyboard_json")

    preview_caption = "<b>Предпросмотр вашей рассылки:</b>\n\n"
    if not text and not photo_id:
        preview_caption += "<i>(Рассылка пустая)</i>"
    elif not text and photo_id:
        preview_caption += "<i>(Только фото и, возможно, кнопки)</i>"
    elif text:
        preview_caption += (text[:1000] + '...') if len(text) > 1000 else text

    reply_markup = kb.newsletter_confirm_keyboard() # Клавиатура Да/Нет/Отмена

    # Пытаемся добавить кнопки из JSON к клавиатуре подтверждения
    if keyboard_json:
        try:
            temp_template = BroadcastTemplate(keyboard_json=keyboard_json)
            preview_keyboard = temp_template.get_keyboard()
            if preview_keyboard:
                confirm_builder = InlineKeyboardBuilder.from_markup(reply_markup)
                preview_builder = InlineKeyboardBuilder.from_markup(preview_keyboard)
                for row in preview_builder.export():
                    confirm_builder.row(*row)
                reply_markup = confirm_builder.as_markup()
            else:
                preview_caption += "\n\n<i>(Не удалось построить клавиатуру из JSON для предпросмотра)</i>"
        except Exception as e:
            logger.warning(f"Could not build preview keyboard for manual newsletter confirmation: {e}")
            preview_caption += "\n\n<i>(Ошибка при построении клавиатуры из JSON для предпросмотра)</i>"
    else:
         preview_caption += "\n\n<i>(Клавиатура отсутствует)</i>"

    # Отправляем предпросмотр
    try:
        if photo_id:
            await message.answer_photo(
                photo=photo_id,
                caption=preview_caption,
                reply_markup=reply_markup,
                parse_mode="HTML" # <-- Добавляем parse_mode
            )
        else:
            await message.answer(
                text=preview_caption,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
                parse_mode="HTML" # <-- Добавляем parse_mode
            )
    except Exception as e:
        logger.error(f"Error sending manual newsletter confirmation: {e}")
        await message.answer(
            "Не удалось показать полный предпросмотр. Отправить эту рассылку?",
            reply_markup=kb.newsletter_confirm_keyboard()
        )


# --- ОБЩИЙ Обработчик подтверждения и запуска рассылки ---
@router.callback_query(F.data == "newsletter_confirm_send", NewsletterStates.confirming_send)
async def newsletter_confirm_and_run(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession, bot: Bot, session_factory):
    """Подтверждает и запускает рассылку."""
    data = await state.get_data()
    text = data.get("newsletter_text")
    photo_id = data.get("newsletter_photo_id")
    keyboard_json = data.get("newsletter_keyboard_json")
    template_id = data.get("selected_template_id", "N/A") # Получаем ID шаблона для логов

    # Получаем список пользователей
    user_ids = await db.get_all_user_ids(session) # Убедитесь, что эта функция есть

    if not user_ids:
        await callback.answer("Нет пользователей для рассылки.", show_alert=True)
        await state.clear()
        # Возвращаем в меню админки
        await callback.message.edit_text("Рассылка отменена (нет пользователей).", reply_markup=kb.admin_main_keyboard())
        return

    # Пытаемся построить клавиатуру из JSON
    reply_markup = None
    if keyboard_json:
        try:
            # Используем временный объект или метод модели для построения
            temp_template = BroadcastTemplate(keyboard_json=keyboard_json)
            reply_markup = temp_template.get_keyboard()
        except Exception as e:
            logger.error(f"Failed to build keyboard for newsletter (template: {template_id}): {e}")
            await callback.answer("Ошибка при построении клавиатуры. Рассылка будет без нее.", show_alert=True)
            # Можно отменить рассылку или продолжить без клавиатуры

    await callback.answer("🚀 Запускаю рассылку...", show_alert=False)
    # Редактируем сообщение, чтобы убрать кнопки подтверждения
    edit_text = f"Рассылка запущена для {len(user_ids)} пользователей..."
    if template_id != "N/A":
        edit_text += f"\n(На основе шаблона ID: {template_id})"

    try:
        if photo_id:
            # Если было фото, редактируем caption
             await callback.message.edit_caption(caption=edit_text, reply_markup=None)
        else:
            # Если было только текст, редактируем текст
             await callback.message.edit_text(edit_text, reply_markup=None)
    except TelegramBadRequest as e:
         logger.warning(f"Could not edit message before newsletter start: {e}")
         # Если не удалось отредактировать, просто продолжаем

    await state.clear()

    # Запускаем рассылку в отдельной задаче с новой сессией
    async def run_newsletter_with_auto_cleanup_task():
        try:
            async with session_factory() as newsletter_session:
                logger.info(f"Начинаем рассылку с новой сессией для {len(user_ids)} пользователей")
                await run_newsletter_with_auto_cleanup(
                    bot=bot,
                    user_ids=user_ids,
                    session=newsletter_session,  # Используем новую сессию
                    text=text,
                    photo_id=photo_id,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Ошибка при выполнении рассылки: {e}", exc_info=True)

    # Запускаем в фоновом режиме
    asyncio.create_task(run_newsletter_with_auto_cleanup_task())


# Обработчик отмены (должен ловить и NewsletterStates)
@router.callback_query(F.data == "cancel_state", StateFilter(AdminTemplateStates, NewsletterStates)) # Ловим во всех состояниях
async def cancel_any_action(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    current_state = await state.get_state()
    logger.info(f"Admin {callback.from_user.id} cancelled action from state {current_state}")
    await state.clear()
    await callback.answer("Действие отменено.")
    # Возвращаем в главное меню админки
    try:
        await callback.message.edit_text(
            "Возврат в главное меню.",
            reply_markup=kb.admin_main_keyboard()
        )
    except TelegramBadRequest: # Если не получилось (например, было фото), отправляем новое
         await callback.message.answer(
             "Возврат в главное меню.",
             reply_markup=kb.admin_main_keyboard()
         )


# --- КОНЕЦ БЛОКА РАССЫЛКИ ---

@router.callback_query(F.data == "admin_search_user", StateFilter(None))
async def admin_search_user_start(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.set_state(AdminManageUser.waiting_for_input)
    await callback.message.edit_text(
        "👤 Введите ID или Username пользователя для поиска:",
        reply_markup=kb.cancel_state_keyboard() # Кнопка отмены
    )
    await callback.answer()

# --- Обработка ввода ID или Username ---
@router.message(AdminManageUser.waiting_for_input)
async def admin_process_user_input(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    user_input = message.text.strip()
    user: Optional[User] = None

    # Пытаемся найти по ID
    try:
        user_id = int(user_input)
        user = await db.get_user(session, user_id)
        logger.info(f"Admin {message.from_user.id} searching user by ID: {user_id}")
    except ValueError:
        # Если не ID, ищем по Username
        logger.info(f"Admin {message.from_user.id} searching user by Username: {user_input}")
        user = await db.get_user_by_username(session, user_input)

    await state.clear() # Очищаем состояние поиска

    if user:
        logger.info(f"User found: ID={user.user_id}, Username={user.username}")
        # Формируем профиль
        profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Статус бана:</b> {'Да 🚫' if user.banned else 'Нет ✅'}
<b>Рефералов (всего):</b> {user.refferals_count}
<b>Рефералов (за 24ч):</b> {user.refferals_24h_count}
<b>Бонус за реферала получен:</b> {'Да' if user.ref_bonus else 'Нет'}
<b>Текущее задание ID:</b> {user.current_task_id or 'Нет'}
<b>Пришел по инд. ссылке ID:</b> {user.individual_link_id or 'Нет'}
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
        await message.answer(
            profile_text,
            reply_markup=kb.admin_manage_user_keyboard(user)
        )
    else:
        logger.warning(f"User not found by input: {user_input}")
        await message.answer(
            f"❌ Пользователь с ID или Username '{html.escape(user_input)}' не найден.",
            reply_markup=kb.admin_main_keyboard() # Возврат в админку
        )

@router.callback_query(F.data.startswith("admin_user_add_stars_"), StateFilter(None))
@admin_required
async def admin_add_stars_start(callback: CallbackQuery, config: Config, state: FSMContext):
    user_id_to_manage = int(callback.data.split("_")[-1])
    await state.set_state(AdminManageUser.waiting_for_add_amount)
    await state.update_data(manage_user_id=user_id_to_manage, action='add')
    await callback.message.edit_text(
        f"➕ Введите сумму звезд для ДОБАВЛЕНИЯ пользователю (ID: <code>{user_id_to_manage}</code>)\n(Положительное число, например 10.5 или 50):",
        reply_markup=kb.cancel_state_keyboard()
    )
    await callback.answer()

# Старт снятия звезд
@router.callback_query(F.data.startswith("admin_user_subtract_stars_"), StateFilter(None))
@admin_required
async def admin_subtract_stars_start(callback: CallbackQuery, config: Config, state: FSMContext):
    user_id_to_manage = int(callback.data.split("_")[-1])
    await state.set_state(AdminManageUser.waiting_for_subtract_amount)
    await state.update_data(manage_user_id=user_id_to_manage, action='subtract')
    await callback.message.edit_text(
        f"➖ Введите сумму звезд для СНЯТИЯ с пользователя (ID: <code>{user_id_to_manage}</code>)\n(Положительное число, например 10.5 или 50):",
        reply_markup=kb.cancel_state_keyboard()
    )
    await callback.answer()

# Обработка ввода суммы для добавления/снятия
@router.message(StateFilter(AdminManageUser.waiting_for_add_amount, AdminManageUser.waiting_for_subtract_amount))
@admin_required
async def admin_process_balance_amount(message: Message, config: Config, state: FSMContext):
    try:
        amount = float(message.text.replace(',', '.'))
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной.")

        state_data = await state.get_data()
        user_id = state_data.get('manage_user_id')
        action = state_data.get('action')

        if not user_id or not action:
            logger.error("State data missing user_id or action for balance change.")
            await message.answer("❌ Произошла внутренняя ошибка состояния. Попробуйте снова.", reply_markup=kb.admin_main_keyboard())
            await state.clear()
            return

        action_text = "добавить" if action == "add" else "снять"
        confirm_text = f"❓ Вы уверены, что хотите {action_text} {amount:.2f}⭐️ пользователю ID <code>{user_id}</code>?"

        await state.update_data(amount=amount)
        await state.set_state(AdminManageUser.confirming_balance_change)
        await message.answer(confirm_text, reply_markup=kb.admin_confirm_balance_change_keyboard(user_id, action, amount))

    except ValueError as e:
        await message.reply(f"❌ Ошибка: Введите корректное ПОЛОЖИТЕЛЬНОЕ число (например, 10.5 или 50).", reply_markup=kb.cancel_state_keyboard())
    except Exception as e:
        logger.error(f"Error processing balance amount: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке суммы.", reply_markup=kb.admin_main_keyboard())
        await state.clear()


# Подтверждение изменения баланса
@router.callback_query(F.data.startswith("admin_confirm_balance_yes_"), AdminManageUser.confirming_balance_change)
@admin_required
async def admin_confirm_balance_change_yes(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    parts = callback.data.split("_")
    user_id = int(parts[4])
    action = parts[5]
    amount = float(parts[6])

    success = False
    commit_needed = False
    try:
        if action == 'add':
            await db.add_balance(session, user_id, amount) # Эта функция не делает коммит
            commit_needed = True
            success = True # add_balance не возвращает статус, предполагаем успех если нет исключения
        elif action == 'subtract':
            # Вызываем обновленную db.minus_balance
            success = await db.minus_balance(session, user_id, amount)
            # Если функция вернула True (баланс достаточен и обновление подготовлено),
            # то коммит нужен.
            if success:
                commit_needed = True
            # Если success == False, коммит не нужен, и мы позже сообщим об ошибке.

        if success and commit_needed:
            await session.commit()
            action_past = "добавлено" if action == "add" else "снято"
            await callback.answer(f"✅ Баланс изменен: {action_past} {amount:.2f}⭐️.", show_alert=True)
            logger.info(f"Admin {callback.from_user.id} changed balance for user {user_id}: {action} {amount}")

            # Обновляем профиль
            user = await db.get_user(session, user_id)
            if user:
                # Дублирование кода профиля...
                profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
                try:
                    await callback.message.edit_text(profile_text, reply_markup=kb.admin_manage_user_keyboard(user))
                except Exception as e:
                     logger.error(f"Failed to update profile after balance change: {e}")
                     await callback.message.answer("Баланс изменен. Не удалось обновить сообщение.")
            else:
                 await callback.message.edit_text("Баланс изменен, но не удалось перезагрузить профиль.")

        elif success and not commit_needed: # Эта ветка больше не должна срабатывать для вычитания
             logger.warning(f"Balance operation for user {user_id} reported success, but no commit was triggered. Action: {action}") # Добавим action в лог
             await callback.answer("Операция выполнена (возможно, без изменений баланса).", show_alert=True)
             # TODO: Обновить профиль здесь тоже?
        else: # Сюда попадем, если success == False (из add_balance/minus_balance)
            await callback.answer("❌ Не удалось изменить баланс (возможно, недостаточно средств для снятия или пользователь не найден).", show_alert=True)
            # TODO: Обновить профиль? Или вернуть как было?

    except Exception as e:
        logger.error(f"Error committing balance change for user {user_id}: {e}", exc_info=True)
        await session.rollback()
        await callback.answer("❌ Произошла ошибка при сохранении изменений.", show_alert=True)

    finally:
        await state.clear() # Очищаем состояние в любом случае

# Отмена изменения баланса
@router.callback_query(F.data.startswith("admin_confirm_balance_no_"), AdminManageUser.confirming_balance_change)
@admin_required
async def admin_confirm_balance_change_no(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    parts = callback.data.split("_")
    user_id = int(parts[4])
    await callback.answer("Изменение баланса отменено.")
    await state.clear()

    # Восстанавливаем профиль пользователя
    user = await db.get_user(session, user_id)
    if user:
        # Дублирование кода профиля...
        profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
        try:
            await callback.message.edit_text(profile_text, reply_markup=kb.admin_manage_user_keyboard(user))
        except Exception as e:
            logger.error(f"Failed to restore profile after cancel balance change: {e}")
            await callback.message.answer("Отмена. Не удалось обновить сообщение.")
    else:
        await callback.message.edit_text("Действие отменено. Не удалось найти пользователя для отображения.")
        await cmd_admin_panel(callback)

# --- Обработчик нажатия кнопки "Удалить из БД" ---
@router.callback_query(F.data.startswith("admin_user_delete_"))
@admin_required
async def handle_delete_user_request(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        user_id_to_delete = int(callback.data.split("_")[-1])
        admin_id = callback.from_user.id
        logger.info(f"Admin {admin_id} initiated delete request for user {user_id_to_delete}.")

        # Получаем данные пользователя для отображения в подтверждении
        user_to_delete = await db.get_user(session, user_id_to_delete)
        if not user_to_delete:
            await callback.answer("❌ Пользователь не найден в БД.", show_alert=True)
            # Можно удалить кнопку удаления из исходного сообщения, если оно еще доступно
            try:
                await callback.message.edit_reply_markup(reply_markup=None) # Убираем кнопки
            except TelegramBadRequest:
                logger.warning(f"Could not edit message after user {user_id_to_delete} not found for deletion.")
            return

        username = user_to_delete.username or f"ID: {user_to_delete.user_id}"
        confirm_text = (f"❓ Вы уверены, что хотите **полностью удалить** пользователя "
                        f"@{username} (ID: {user_id_to_delete}) из базы данных?\n\n"
                        f"⚠️ **Это действие необратимо!** Все данные пользователя будут потеряны.")

        # Отправляем сообщение с подтверждением
        await callback.message.answer(
            confirm_text,
            reply_markup=kb.admin_confirm_delete_keyboard(user_id_to_delete)
        )
        await callback.answer() # Отвечаем на исходный коллбэк

    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing user_id from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка обработки запроса.", show_alert=True)
    except Exception as e:
        logger.error(f"Unexpected error handling delete user request for '{callback.data}': {e}")
        await callback.answer("❌ Произошла непредвиденная ошибка.", show_alert=True)


# --- Обработчик подтверждения удаления ---
@router.callback_query(F.data.startswith("admin_confirm_delete_yes_"))
@admin_required
async def handle_confirm_delete_yes(callback: CallbackQuery, config: Config, session: AsyncSession):
    try:
        user_id_to_delete = int(callback.data.split("_")[-1])
        admin_id = callback.from_user.id
        logger.warning(f"Admin {admin_id} CONFIRMED deletion for user {user_id_to_delete}.")

        user_to_delete = await db.get_user(session, user_id_to_delete)
        if not user_to_delete:
            await callback.answer("❌ Пользователь уже удален или не найден.", show_alert=True)
            try:
                await callback.message.edit_text("Пользователь не найден.", reply_markup=None)
            except TelegramBadRequest: pass # Сообщение могло быть удалено
            return

        username = user_to_delete.username or f"ID: {user_id_to_delete}"

        # --- Непосредственное удаление ---
        try:
            await session.delete(user_to_delete)
            await session.commit()
            logger.info(f"User {user_id_to_delete} successfully deleted from DB by admin {admin_id}.")
            result_text = f"✅ Пользователь @{username} (ID: {user_id_to_delete}) успешно удален из БД."
            await callback.answer("🗑️ Пользователь удален.", show_alert=True)
        except Exception as e_del:
            await session.rollback()
            logger.error(f"Failed to delete user {user_id_to_delete} from DB: {e_del}")
            result_text = f"❌ Ошибка при удалении пользователя @{username} (ID: {user_id_to_delete}) из БД."
            await callback.answer("❌ Ошибка удаления.", show_alert=True)
        # --- Конец удаления ---

        # Редактируем сообщение с подтверждением
        try:
            await callback.message.edit_text(result_text, reply_markup=None)
        except TelegramBadRequest:
            logger.warning(f"Could not edit confirmation message after delete action for user {user_id_to_delete}.")

    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing user_id from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка обработки запроса.", show_alert=True)
    except Exception as e:
        logger.error(f"Unexpected error handling delete confirmation for '{callback.data}': {e}")
        await callback.answer("❌ Произошла непредвиденная ошибка.", show_alert=True)


# --- Обработчик отмены удаления ---
@router.callback_query(F.data.startswith("admin_confirm_delete_no_"))
@admin_required
async def handle_confirm_delete_no(callback: CallbackQuery):
    try:
        user_id_to_delete = int(callback.data.split("_")[-1]) # Получаем ID для логов
        admin_id = callback.from_user.id
        logger.info(f"Admin {admin_id} cancelled deletion for user {user_id_to_delete}.")

        # Редактируем сообщение с подтверждением
        await callback.message.edit_text("🚫 Удаление пользователя отменено.", reply_markup=None)
        await callback.answer("Отменено")
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing user_id from callback data '{callback.data}': {e}")
        await callback.answer("❌ Ошибка обработки запроса.", show_alert=True)
    except TelegramBadRequest:
         logger.warning(f"Could not edit confirmation message after cancelling delete action.")
         await callback.answer("Отменено") # Все равно отвечаем на коллбэк
    except Exception as e:
        logger.error(f"Unexpected error handling delete cancellation for '{callback.data}': {e}")
        await callback.answer("❌ Произошла непредвиденная ошибка.", show_alert=True)

# Бан/разбан - запрос подтверждения
@router.callback_query(F.data.startswith("admin_user_ban_") | F.data.startswith("admin_user_unban_"))
@admin_required
async def admin_ban_user_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    parts = callback.data.split("_")
    action = parts[2] # ban или unban
    user_id_to_manage = int(parts[3])

    user = await db.get_user(session, user_id_to_manage)
    if not user:
        await callback.answer("❌ Пользователь не найден.", show_alert=True)
        await cmd_admin_panel(callback) # Назад в админку
        return

    action_text = "забанить" if action == "ban" else "разбанить"
    confirm_text = f"❓ Вы уверены, что хотите {action_text} пользователя @{html.escape(user.username or 'Нет')} (ID: <code>{user.user_id}</code>)?"

    await callback.message.edit_text(
        confirm_text,
        reply_markup=kb.admin_confirm_ban_keyboard(user_id_to_manage, action)
    )
    await callback.answer()

# Бан/разбан - подтверждение
@router.callback_query(F.data.startswith("admin_confirm_ban_yes_") | F.data.startswith("admin_confirm_unban_yes_"))
@admin_required
async def admin_ban_user_confirm(callback: CallbackQuery, config: Config, session: AsyncSession, bot: Bot): # Добавляем bot
    parts = callback.data.split("_")
    action = parts[2] # ban или unban
    user_id_to_manage = int(parts[4])
    is_banned = (action == "ban")

    success = await db.set_user_ban_status(session, user_id_to_manage, is_banned)

    if success:
        action_past = "забанен" if is_banned else "разбанен"
        await callback.answer(f"✅ Пользователь {action_past}.", show_alert=True)

        # --- Отправка уведомления пользователю при БАНЕ ---
        if is_banned:
            try:
                await bot.send_message(user_id_to_manage, "🚫 Администратор заблокировал ваш доступ к боту.")
                logger.info(f"Sent ban notification to user {user_id_to_manage}")
            except (TelegramForbiddenError, TelegramBadRequest) as e: # Используем конкретные исключения
                 logger.warning(f"Could not send ban notification to user {user_id_to_manage}. Reason: {e}")
            except Exception as e:
                 logger.error(f"Unexpected error sending ban notification to user {user_id_to_manage}: {e}", exc_info=True)
        # -------------------------------------------------

        # Обновляем профиль пользователя в сообщении
        user = await db.get_user(session, user_id_to_manage)
        if user:
            profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Статус бана:</b> {'Да 🚫' if user.banned else 'Нет ✅'}
<b>Рефералов (всего):</b> {user.refferals_count}
<b>Рефералов (за 24ч):</b> {user.refferals_24h_count}
<b>Бонус за реферала получен:</b> {'Да' if user.ref_bonus else 'Нет'}
<b>Текущее задание ID:</b> {user.current_task_id or 'Нет'}
<b>Пришел по инд. ссылке ID:</b> {user.individual_link_id or 'Нет'}
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
            try:
                await callback.message.edit_text(
                    profile_text,
                    reply_markup=kb.admin_manage_user_keyboard(user)
                )
            except Exception as e:
                logger.error(f"Failed to update user profile after ban/unban: {e}")
                # Сообщение об изменении статуса уже было отправлено через callback.answer
                # Можно просто залогировать или отправить новое сообщение админу
                # await callback.message.answer("Статус пользователя изменен. Не удалось обновить сообщение профиля.")
        else:
            # await callback.message.edit_text("Статус пользователя изменен, но не удалось перезагрузить профиль.")
            logger.warning(f"Could not reload profile for user {user_id_to_manage} after ban/unban.")


    else:
        await callback.answer("❌ Ошибка при изменении статуса бана.", show_alert=True)
        # Можно оставить текущее сообщение или вернуть в админку
        # await cmd_admin_panel(callback)

# Бан/разбан - отмена
@router.callback_query(F.data.startswith("admin_confirm_ban_no_") | F.data.startswith("admin_confirm_unban_no_"))
async def admin_ban_user_cancel(callback: CallbackQuery, config: Config, session: AsyncSession):
    parts = callback.data.split("_")
    user_id_to_manage = int(parts[-1]) # Последняя часть всегда ID
    await callback.answer("Действие отменено.")
    # Возвращаем профиль пользователя
    user = await db.get_user(session, user_id_to_manage)
    if user:
        # Дублирование кода профиля... TODO: вынести в отдельную функцию?
        profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Статус бана:</b> {'Да 🚫' if user.banned else 'Нет ✅'}
# ... (остальные поля как выше) ...
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
        try:
            await callback.message.edit_text(
                profile_text,
                reply_markup=kb.admin_manage_user_keyboard(user)
            )
        except Exception as e:
            logger.error(f"Failed to restore user profile after cancel ban/unban: {e}")
            await callback.message.answer("Отмена. Не удалось обновить сообщение.")

# --- НОВЫЙ Обработчик для подтверждения создания ---
@router.callback_query(F.data == "template_create_confirm", AdminTemplateStates.confirm_creation)
@admin_required
async def template_create_confirm(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Сохраняет шаблон в базу данных."""
    data = await state.get_data()
    template_name = data.get('template_name')
    template_text = data.get('template_text')
    photo_file_id = data.get('photo_file_id')
    keyboard_json = data.get('keyboard_json')

    if not template_name: # Доп. проверка
        await callback.answer("❌ Ошибка: Имя шаблона не найдено в данных. Начните заново.", show_alert=True)
        await state.clear()
        # Возвращаем в меню шаблонов
        templates = await db.get_all_broadcast_templates(session)
        await callback.message.edit_text(
            "Меню управления шаблонами рассылок:",
            reply_markup=kb.templates_menu_keyboard(templates)
        )
        return

    try:
        new_template = await db.create_broadcast_template(
            session=session,
            name=template_name,
            text=template_text,
            photo_file_id=photo_file_id,
            keyboard_json=keyboard_json
        )
        await session.commit()
        logger.info(f"Admin {callback.from_user.id} created new template ID: {new_template.id}, Name: {template_name}")
        await callback.answer(f"✅ Шаблон '{html.escape(template_name)}' успешно создан!", show_alert=True)

        # Очищаем состояние и возвращаем в меню шаблонов
        await state.clear()
        templates = await db.get_all_broadcast_templates(session)
        await callback.message.edit_text( # Редактируем сообщение с предпросмотром
            "Меню управления шаблонами рассылок:",
            reply_markup=kb.templates_menu_keyboard(templates)
        )

    except Exception as e:
        await session.rollback()
        logger.error(f"Error creating template '{template_name}': {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при сохранении шаблона в базу данных.", show_alert=True)
        # Можно оставить пользователя в состоянии подтверждения или очистить
        # await state.clear() # Очистить, чтобы избежать повторной попытки с теми же данными


# --- Обработчики для Просмотра, Редактирования, Удаления ---

# --- Просмотр списка ---
@router.callback_query(F.data == "template_list_view", StateFilter(None))
@admin_required
async def template_list_view(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показывает список существующих шаблонов."""
    await callback.answer()
    templates = await db.get_all_broadcast_templates(session)
    if not templates:
        await callback.message.edit_text(
            "Список шаблонов пуст.",
            reply_markup=kb.templates_menu_keyboard(templates) # Показываем меню с кнопкой "Создать"
        )
        return

    list_text = "📋 <b>Список ваших шаблонов:</b>\n\n"
    for t in templates:
        details = []
        if t.text: details.append("Текст")
        if t.photo_file_id: details.append("Фото")
        if t.keyboard_json: details.append("Кнопки")
        details_str = f" ({', '.join(details)})" if details else " (пустой)"
        list_text += f"• <code>{html.escape(t.name)}</code> (ID: {t.id}){details_str}\n"

    await callback.message.edit_text(
        list_text,
        reply_markup=kb.templates_menu_keyboard(templates) # Возвращаем в меню шаблонов
    )

# --- Удаление: Шаг 1 - Выбор шаблона ---
@router.callback_query(F.data == "template_delete_select", StateFilter(None))
@admin_required
async def template_delete_select(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показывает список шаблонов для выбора для удаления."""
    await callback.answer()
    templates = await db.get_all_broadcast_templates(session)
    if not templates:
        await callback.message.edit_text(
            "Нет шаблонов для удаления.",
            reply_markup=kb.templates_menu_keyboard(templates)
        )
        return

    await callback.message.edit_text(
        "🗑️ Выберите шаблон для удаления:",
        reply_markup=kb.select_template_keyboard(templates, action_prefix="template_delete_")
    )

# --- Удаление: Шаг 2 - Подтверждение ---
@router.callback_query(F.data.startswith("template_delete_"), StateFilter(None))
@admin_required
async def template_delete_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Запрашивает подтверждение удаления выбранного шаблона."""
    try:
        template_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        logger.error(f"Invalid template ID in callback data: {callback.data}")
        await callback.answer("Ошибка: Неверный ID шаблона.", show_alert=True)
        return

    template = await db.get_broadcast_template_by_id(session, template_id)
    if not template:
        await callback.answer("❌ Шаблон не найден (возможно, уже удален).", show_alert=True)
        # Обновляем список
        templates = await db.get_all_broadcast_templates(session)
        await callback.message.edit_text(
             "Меню управления шаблонами рассылок:",
             reply_markup=kb.templates_menu_keyboard(templates)
        )
        return

    await callback.message.edit_text(
        f"❓ Вы уверены, что хотите удалить шаблон '<code>{html.escape(template.name)}</code>' (ID: {template_id})?",
        reply_markup=kb.template_delete_confirm_keyboard(template_id)
    )
    await callback.answer()


# --- Удаление: Шаг 3 - Выполнение ---
@router.callback_query(F.data.startswith("s_template_delete_confirm_"), StateFilter(None))
async def template_delete_confirm(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Удаляет шаблон после подтверждения."""
    try:
        template_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        logger.error(f"Invalid template ID in callback data: {callback.data}")
        await callback.answer("Ошибка: Неверный ID шаблона.", show_alert=True)
        return

    try:
        deleted = await db.delete_broadcast_template(session, template_id)
        await session.commit()

        if deleted:
            logger.info(f"Admin {callback.from_user.id} deleted template ID: {template_id}")
            await callback.answer(f"✅ Шаблон #{template_id} успешно удален.", show_alert=True)
        else:
            await callback.answer("❌ Не удалось удалить шаблон (возможно, он уже был удален).", show_alert=True)

    except Exception as e:
        await session.rollback()
        logger.error(f"Error deleting template ID {template_id}: {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при удалении шаблона.", show_alert=True)

    finally:
        # В любом случае обновляем меню шаблонов
        templates = await db.get_all_broadcast_templates(session)
        await callback.message.edit_text(
            "Меню управления шаблонами рассылок:",
            reply_markup=kb.templates_menu_keyboard(templates)
        )


# --- Редактирование: Шаг 1 - Выбор шаблона ---
@router.callback_query(F.data == "template_edit_select", StateFilter(None))
async def template_edit_select(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показывает список шаблонов для выбора для редактирования."""
    await callback.answer()
    templates = await db.get_all_broadcast_templates(session)
    if not templates:
        await callback.message.edit_text(
            "Нет шаблонов для редактирования.",
            reply_markup=kb.templates_menu_keyboard(templates)
        )
        return

    await callback.message.edit_text(
        "✏️ Выберите шаблон для редактирования:",
        reply_markup=kb.select_template_keyboard(templates, action_prefix="template_edit_")
    )

# --- Редактирование: Шаг 2 - Показ меню редактирования ---
@router.callback_query(F.data.startswith("template_edit_"), StateFilter(None))
@admin_required
async def template_edit_menu(callback: CallbackQuery, config: Config, session: AsyncSession, state: FSMContext):
    """Показывает меню редактирования для выбранного шаблона."""
    try:
        template_id = int(callback.data.split("_")[-1])
    except (IndexError, ValueError):
        logger.error(f"Invalid template ID in callback data: {callback.data}")
        await callback.answer("Ошибка: Неверный ID шаблона.", show_alert=True)
        return

    template = await db.get_broadcast_template_by_id(session, template_id)
    if not template:
        await callback.answer("❌ Шаблон не найден.", show_alert=True)
        templates = await db.get_all_broadcast_templates(session)
        await callback.message.edit_text(
             "Меню управления шаблонами рассылок:",
             reply_markup=kb.templates_menu_keyboard(templates)
        )
        return

    # Сохраняем ID редактируемого шаблона в состояние (на всякий случай, если понадобится)
    # Хотя для простого меню это не обязательно, но может пригодиться для след. шагов
    # await state.set_state(AdminTemplateStates.editing_template) # Пока не ставим состояние
    # await state.update_data(editing_template_id=template_id)

    # Формируем текст с текущими данными шаблона
    details = []
    if template.text: details.append("Текст")
    if template.photo_file_id: details.append("Фото")
    if template.keyboard_json: details.append("Кнопки")
    details_str = f" ({', '.join(details)})" if details else " (пустой)"

    await callback.message.edit_text(
        f"✏️ Редактирование шаблона '<code>{html.escape(template.name)}</code>' (ID: {template_id}){details_str}\n\n"
        f"Выберите, что хотите изменить:",
        reply_markup=kb.template_edit_menu_keyboard(template_id)
    )
    await callback.answer()


# --- Редактирование: Шаг 3 - Запрос нового значения ---
@router.callback_query(F.data.startswith("template_edit_field_"), StateFilter(None)) # Пока без состояния
@admin_required
async def template_edit_field_prompt(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Запрашивает новое значение для выбранного поля."""
    parts = callback.data.split("_")
    try:
        field_to_edit = parts[3] # name, text, photo, keyboard
        template_id = int(parts[-1])
    except (IndexError, ValueError):
        logger.error(f"Invalid callback data for field edit: {callback.data}")
        await callback.answer("Ошибка: Неверные данные для редактирования.", show_alert=True)
        return

    # Сохраняем ID и поле в состояние
    await state.set_state(AdminTemplateStates.waiting_for_edit_value)
    await state.update_data(editing_template_id=template_id, field_to_edit=field_to_edit)

    # Формируем сообщение-приглашение
    prompt_text = ""
    reply_markup = kb.cancel_state_keyboard() # Клавиатура отмены

    if field_to_edit == "name":
        prompt_text = "Введите новое <b>уникальное имя</b> для шаблона:"
    elif field_to_edit == "text":
        prompt_text = ("Введите новый <b>текст</b> шаблона (используйте HTML, '-' для удаления текста):")
    elif field_to_edit == "photo":
        prompt_text = ("Отправьте новое <b>фото</b> или нажмите 'Удалить фото', если оно больше не нужно.")
        # Нужна новая клавиатура для удаления фото
        builder = InlineKeyboardBuilder()
        builder.button(text="🗑️ Удалить фото", callback_data="template_edit_delete_photo")
        builder.button(text="❌ Отменить", callback_data="cancel_state")
        reply_markup = builder.adjust(1).as_markup()
    elif field_to_edit == "keyboard":
        prompt_text = ("Отправьте новый <b>JSON клавиатуры</b> или нажмите 'Удалить клавиатуру'.\n"
                       "Пример:\n<pre><code class=\"language-json\">"
                       "[[{\"text\": \"Кнопка\", \"url\": \"...\"}]]"
                       "</code></pre>")
        # Нужна новая клавиатура для удаления клавиатуры
        builder = InlineKeyboardBuilder()
        builder.button(text="🗑️ Удалить клавиатуру", callback_data="template_edit_delete_keyboard")
        builder.button(text="❌ Отменить", callback_data="cancel_state")
        reply_markup = builder.adjust(1).as_markup()
    else:
        await callback.answer("Ошибка: Неизвестное поле для редактирования.", show_alert=True)
        await state.clear()
        return

    await callback.message.edit_text(prompt_text, reply_markup=reply_markup, disable_web_page_preview=True)
    await callback.answer()


# --- Редактирование: Шаг 4 - Обработка нового значения ---
@router.message(AdminTemplateStates.waiting_for_edit_value, F.text | F.photo) # Ловим текст или фото
@admin_required
async def template_process_edit_value(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обрабатывает введенное значение для редактирования."""
    data = await state.get_data()
    template_id = data.get("editing_template_id")
    field_to_edit = data.get("field_to_edit")
    new_value = None

    if not template_id or not field_to_edit:
        logger.error(f"Missing template_id or field_to_edit in state for user {message.from_user.id}")
        await message.answer("❌ Произошла ошибка состояния. Попробуйте начать редактирование заново.")
        await state.clear()
        # TODO: Вернуть в меню шаблонов?
        return

    update_data = {}
    success_message = ""

    try:
        if field_to_edit == "name":
            new_value = message.text.strip()
            if not new_value:
                await message.answer("❌ Имя не может быть пустым. Введите снова:", reply_markup=kb.cancel_state_keyboard())
                return # Остаемся в состоянии
            # Проверка уникальности (кроме текущего шаблона)
            existing = await db.get_broadcast_template_by_name(session, new_value)
            if existing and existing.id != template_id:
                 await message.answer(f"❌ Шаблон с именем '<code>{html.escape(new_value)}</code>' уже существует. Введите другое имя:", reply_markup=kb.cancel_state_keyboard())
                 return # Остаемся в состоянии
            update_data['name'] = new_value
            success_message = "✅ Имя шаблона обновлено."

        elif field_to_edit == "text":
            new_value = message.html_text
            if new_value == "-":
                update_data['text'] = None
                success_message = "✅ Текст шаблона удален."
            elif len(new_value) > 4096:
                 await message.answer("❌ Текст слишком длинный (макс. 4096). Введите снова:", reply_markup=kb.cancel_state_keyboard())
                 return # Остаемся в состоянии
            else:
                update_data['text'] = new_value
                success_message = "✅ Текст шаблона обновлен."

        elif field_to_edit == "photo":
            if message.photo:
                new_value = message.photo[-1].file_id
                update_data['photo_file_id'] = new_value
                success_message = "✅ Фото шаблона обновлено."
            else: # Пришел текст вместо фото
                 await message.answer("❌ Ожидалась фотография. Отправьте фото или нажмите 'Удалить фото'/'Отменить'.", reply_markup=message.reply_markup) # Используем ту же клавиатуру
                 return # Остаемся в состоянии

        elif field_to_edit == "keyboard":
            if message.photo: # Пришло фото вместо текста
                 await message.answer("❌ Ожидался JSON клавиатуры. Отправьте текст или нажмите 'Удалить клавиатуру'/'Отменить'.", reply_markup=message.reply_markup)
                 return # Остаемся в состоянии

            keyboard_json_text = message.text
            if keyboard_json_text == "-": # Позволим тире для удаления
                 update_data['keyboard_json'] = None
                 success_message = "✅ Клавиатура шаблона удалена."
            else:
                try:
                    # Валидация JSON (как при создании)
                    parsed_json = json.loads(keyboard_json_text)
                    if not isinstance(parsed_json, list) or not all(isinstance(row, list) for row in parsed_json): raise ValueError("JSON structure must be a list of lists.")
                    if not all(isinstance(button, dict) for row in parsed_json for button in row): raise ValueError("Each element in inner lists must be a dictionary.")

                    update_data['keyboard_json'] = keyboard_json_text
                    success_message = "✅ Клавиатура шаблона обновлена."
                except (json.JSONDecodeError, ValueError) as e:
                     await message.answer(
                         f"❌ Ошибка JSON: {e}. Проверьте формат и отправьте снова или нажмите 'Удалить'/'Отменить'.\n"
                         "Пример:\n<pre><code class=\"language-json\">[[{\"text\": \"Кнопка\", \"url\": \"...\"}]]</code></pre>",
                         reply_markup=message.reply_markup, disable_web_page_preview=True
                     )
                     return # Остаемся в состоянии
                except Exception as e:
                     logger.error(f"Unexpected error processing edited keyboard JSON: {e}", exc_info=True)
                     await message.answer("❌ Произошла ошибка при обработке JSON клавиатуры. Попробуйте снова или нажмите 'Удалить'/'Отменить'.", reply_markup=message.reply_markup)
                     return # Остаемся в состоянии

    except Exception as e:
        logger.error(f"Unexpected error processing template edit: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при редактировании шаблона. Попробуйте снова или нажмите 'Удалить'/'Отменить'.", reply_markup=message.reply_markup)
        
# ДОБАВЛЯЕМ обработчик для кнопки "Шаблоны рассылок 📬" из главной админ-клавиатуры
@router.callback_query(F.data == "admin_manage_templates")
@admin_required
async def templates_menu_handler(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показывает меню управления шаблонами рассылок."""
    await callback.answer() # Отвечаем на колбэк сразу
    templates = await db.get_all_broadcast_templates(session)
    await callback.message.edit_text(
        "Меню управления шаблонами рассылок:",
        reply_markup=kb.templates_menu_keyboard(templates) # Используем ту же клавиатуру меню шаблонов
    )

# --- Создание нового шаблона (FSM) ---

# Шаг 1: Старт, запрос имени
@router.callback_query(F.data == "template_create_start")
@admin_required
async def template_create_start(callback: CallbackQuery, config: Config, state: FSMContext):
    """Начинает процесс создания нового шаблона, запрашивает имя."""
    await state.clear() # Очищаем предыдущее состояние на всякий случай
    await callback.message.edit_text(
        "Введите <b>уникальное имя</b> для нового шаблона (например, 'Приветствие' или 'Акция_Май'):",
        reply_markup=kb.cancel_state_keyboard() # Клавиатура отмены
    )
    await state.set_state(AdminTemplateStates.waiting_for_name)
    await callback.answer()

# Шаг 2: Обработка имени, запрос текста
@router.message(AdminTemplateStates.waiting_for_name, F.text)
@admin_required
async def template_process_name(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обрабатывает введенное имя, проверяет уникальность и запрашивает текст."""
    template_name = message.text.strip()
    if not template_name:
        await message.answer(
            "❌ Имя шаблона не может быть пустым. Попробуйте еще раз:",
            reply_markup=kb.cancel_state_keyboard()
        )
        return

    # Проверка на уникальность имени
    existing_template = await db.get_broadcast_template_by_name(session, template_name)
    if existing_template:
        await message.answer(
            f"❌ Шаблон с именем '<code>{html.escape(template_name)}</code>' уже существует. Придумайте другое имя:",
            reply_markup=kb.cancel_state_keyboard()
        )
        return

    await state.update_data(template_name=template_name)
    logger.info(f"Admin {message.from_user.id} creating template, name set to: {template_name}")

    # Запрашиваем текст сообщения
    await message.answer(
        "Теперь введите <b>текст сообщения</b> для шаблона.\n"
        "Вы можете использовать HTML-теги для форматирования (<code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;a href='...'&gt;</code> и т.д.).\n"
        "Если текст не нужен (например, только фото и кнопки), отправьте тире (<b>-</b>).",
        reply_markup=kb.cancel_state_keyboard()
    )
    await state.set_state(AdminTemplateStates.waiting_for_text)

# Шаг 3: Обработка текста, запрос фото
@router.message(AdminTemplateStates.waiting_for_text, F.text)
@admin_required
async def template_process_text(message: Message, config: Config, state: FSMContext):
    """Обрабатывает введенный текст, сохраняет его и запрашивает фото."""
    template_text = message.html_text # Используем html_text для сохранения разметки

    if template_text == "-":
        template_text = None # Сохраняем None, если пользователь ввел тире
        logger.info(f"Admin {message.from_user.id} creating template: text skipped.")
        await message.answer("Текст пропущен.")
    else:
        # Проверка длины текста (Telegram имеет лимиты)
        if len(template_text) > 4096:
             await message.answer(
                 "❌ Текст слишком длинный (максимум 4096 символов). Пожалуйста, сократите текст и отправьте снова:",
                 reply_markup=kb.cancel_state_keyboard()
             )
             return # Остаемся в том же состоянии
        logger.info(f"Admin {message.from_user.id} creating template: text set.")

    await state.update_data(template_text=template_text)

    # Запрашиваем фото
    await message.answer(
        "Теперь отправьте <b>фотографию</b> для шаблона.\n"
        "Если фото не нужно, нажмите 'Пропустить'.",
        reply_markup=kb.template_creation_skip_keyboard(skip_callback="template_skip_photo")
    )
    await state.set_state(AdminTemplateStates.waiting_for_photo)

# Шаг 4.1: Пропуск фото, запрос клавиатуры
@router.callback_query(F.data == "template_skip_photo", AdminTemplateStates.waiting_for_photo)
@admin_required
async def template_skip_photo(callback: CallbackQuery, config: Config, state: FSMContext):
    """Пропускает шаг добавления фото и запрашивает клавиатуру."""
    await callback.answer("Фото пропущено.")
    logger.info(f"Admin {callback.from_user.id} creating template: photo skipped.")
    await state.update_data(photo_file_id=None)

    # Запрашиваем клавиатуру
    await callback.message.edit_text( # Используем edit_text для изменения предыдущего сообщения
        "Теперь отправьте клавиатуру для шаблона. Вы можете использовать два формата:\n\n"
        "<b>1. Простой формат:</b>\n"
        "Название1 - https://t.me/link1\n"
        "Название2 - https://t.me/link2 / Название3 - https://t.me/link3\n\n"
        "<b>2. JSON формат:</b>\n"
        "<pre><code class=\"language-json\">"
        "[[{\"text\": \"Кнопка 1\", \"url\": \"https://t.me/durov\"}],\n"
        " [{\"text\": \"Кнопка 2\", \"callback_data\": \"data2\"}]]"
        "</code></pre>\n"
        "Или нажмите 'Пропустить', если клавиатура не нужна.",
        reply_markup=kb.template_creation_skip_keyboard(skip_callback="template_skip_keyboard"),
        disable_web_page_preview=True
    )
    await state.set_state(AdminTemplateStates.waiting_for_keyboard)

# Шаг 4.2: Обработка фото, запрос клавиатуры
@router.message(AdminTemplateStates.waiting_for_photo, F.photo)
@admin_required
async def template_process_photo(message: Message, config: Config, state: FSMContext):
    """Обрабатывает полученное фото, сохраняет его file_id и запрашивает клавиатуру."""
    photo_file_id = message.photo[-1].file_id
    await state.update_data(photo_file_id=photo_file_id)
    logger.info(f"Admin {message.from_user.id} creating template: photo set (file_id: {photo_file_id}).")
    await message.answer("✅ Фото добавлено.")

    # Запрашиваем клавиатуру (так же, как и при пропуске фото)
    await message.answer(
        "Теперь отправьте клавиатуру для шаблона. Вы можете использовать два формата:\n\n"
        "<b>1. Простой формат:</b>\n"
        "Название1 - https://t.me/link1\n"
        "Название2 - https://t.me/link2 / Название3 - https://t.me/link3\n\n"
        "<b>2. JSON формат:</b>\n"
        "<pre><code class=\"language-json\">"
        "[[{\"text\": \"Кнопка 1\", \"url\": \"https://t.me/durov\"}],\n"
        " [{\"text\": \"Кнопка 2\", \"callback_data\": \"data2\"}]]"
        "</code></pre>\n"
        "Или нажмите 'Пропустить', если клавиатура не нужна.",
        reply_markup=kb.template_creation_skip_keyboard(skip_callback="template_skip_keyboard"),
        disable_web_page_preview=True
    )
    await state.set_state(AdminTemplateStates.waiting_for_keyboard)

# Шаг 5.1: Пропуск клавиатуры, переход к подтверждению
@router.callback_query(F.data == "template_skip_keyboard", AdminTemplateStates.waiting_for_keyboard)
@admin_required
async def template_skip_keyboard(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Пропускает шаг добавления клавиатуры и переходит к подтверждению."""
    await callback.answer("Клавиатура пропущена.")
    logger.info(f"Admin {callback.from_user.id} creating template: keyboard skipped.")
    await state.update_data(keyboard_json=None)

    # Переходим к подтверждению
    await show_template_confirmation(callback.message, state, session) # Вызываем функцию подтверждения
    await state.set_state(AdminTemplateStates.confirm_creation)

# Шаг 6: Показ предпросмотра и запрос подтверждения
async def show_template_confirmation(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Показывает предпросмотр создаваемого шаблона и кнопки подтверждения."""
    data = await state.get_data()
    name = data.get("template_name")
    text = data.get("template_text")
    photo_id = data.get("photo_file_id")
    keyboard_json = data.get("keyboard_json")

    preview_caption = f"<b>Предпросмотр шаблона '{html.escape(name)}':</b>\n\n"
    reply_markup = kb.template_confirm_creation_keyboard() # Клавиатура Да/Начать заново/Отмена

    # Пытаемся построить клавиатуру из JSON для предпросмотра
    preview_keyboard = None
    if keyboard_json:
        try:
            temp_template = BroadcastTemplate(keyboard_json=keyboard_json)
            preview_keyboard = temp_template.get_keyboard()
            if not preview_keyboard:
                preview_caption += "\n\n<i>(Не удалось построить клавиатуру из JSON для предпросмотра)</i>"
        except Exception as e:
            logger.warning(f"Could not build preview keyboard for template confirmation: {e}")
            preview_caption += "\n\n<i>(Ошибка при построении клавиатуры из JSON для предпросмотра)</i>"
    else:
         preview_caption += "\n\n<i>(Клавиатура отсутствует)</i>"

    # Добавляем текст в caption, если он есть
    if text:
        preview_caption += (text[:900] + '...') if len(text) > 900 else text # Ограничиваем длину для caption
    elif not photo_id: # Если нет ни текста, ни фото
        preview_caption += "<i>(Шаблон пустой, только имя)</i>"
    elif photo_id and not text: # Если только фото
         preview_caption += "<i>(Только фото и, возможно, кнопки)</i>"


    # Отправляем предпросмотр
    try:
        if photo_id:
            await message.answer_photo(
                photo=photo_id,
                caption=preview_caption,
                reply_markup=reply_markup,
                parse_mode="HTML" # Убедимся, что HTML включен
            )
        else:
            await message.answer(
                text=preview_caption,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
                    parse_mode="HTML"
                )
    except Exception as e:
        logger.error(f"Error sending template confirmation preview: {e}")
        # Если предпросмотр не удался, все равно предлагаем сохранить
        await message.answer(
            f"Не удалось показать полный предпросмотр шаблона '{html.escape(name)}'.\n"
            "Сохранить этот шаблон?",
            reply_markup=kb.template_confirm_creation_keyboard()
        )

# Шаг 7: Подтверждение и сохранение шаблона
@router.callback_query(F.data == "template_create_confirm", AdminTemplateStates.confirm_creation)
@admin_required
async def template_create_confirm(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Сохраняет шаблон в базу данных после подтверждения."""
    data = await state.get_data()
    template_name = data.get("template_name")
    template_text = data.get("template_text")
    photo_file_id = data.get("photo_file_id")
    keyboard_json = data.get("keyboard_json")

    if not template_name: # Дополнительная проверка
        logger.error("Template name missing in state during confirmation.")
        await callback.answer("❌ Ошибка: имя шаблона потеряно. Попробуйте создать заново.", show_alert=True)
        await state.clear()
        await templates_menu_handler(callback, session) # Возврат в меню
        return

    try:
        new_template = await db.create_broadcast_template(
            session=session,
            name=template_name,
            text=template_text,
            photo_file_id=photo_file_id,
            keyboard_json=keyboard_json
        )
        await session.commit()
        logger.info(f"Admin {callback.from_user.id} created new template ID: {new_template.id}, Name: {template_name}")
        await callback.answer(f"✅ Шаблон '{html.escape(template_name)}' успешно создан!", show_alert=True)

    except Exception as e:
        await session.rollback()
        logger.error(f"Error creating template '{template_name}': {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при сохранении шаблона в базу данных.", show_alert=True)

    finally:
        # В любом случае очищаем состояние и возвращаем в меню шаблонов
        await state.clear()
        await templates_menu_handler(callback, session) # Используем существующий хендлер меню

# ... (rest of the handlers like ban/unban confirmation, cancellation) ...

# --- ОБЩИЙ ОБРАБОТЧИК ОТМЕНЫ (Убедитесь, что он есть и зарегистрирован) ---
# Важно, чтобы этот обработчик мог отменить и новое состояние AdminManageUser.subtracting_stars
@router.callback_query(F.data == "cancel_state", StateFilter("*"))
@admin_required
async def cancel_any_state_handler(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession): # Добавляем session
    current_state_str = await state.get_state()
    if current_state_str is None:
        await callback.answer("Нет активного действия для отмены.", show_alert=True)
        return

    logger.info(f"Admin {callback.from_user.id} cancelled state {current_state_str}")

    # --- Специальная логика отмены для состояний управления пользователем/ссылкой ---
    state_data = await state.get_data()
    user_id_in_state = state_data.get('manage_user_id') # Это поле используется и для списания звезд
    link_id_in_state = state_data.get('link_id_to_delete') # Используется при удалении по ID

    await state.clear() # Очищаем состояние

    restored = False
    # Пытаемся восстановить профиль пользователя, если отменяли его редактирование (включая списание звезд)
    if current_state_str and current_state_str.startswith("AdminManageUser:") and user_id_in_state:
        user = await db.get_user(session, user_id_in_state)
        if user:
            profile_text = f"""
👤 <b>Профиль пользователя:</b> @{html.escape(user.username or "Нет")}
-------------------------------------
<b>ID:</b> <code>{user.user_id}</code>
<b>Баланс:</b> {user.balance:.2f}⭐️
<b>Статус бана:</b> {'Да 🚫' if user.banned else 'Нет ✅'}
<b>Рефералов (всего):</b> {user.refferals_count}
<b>Рефералов (за 24ч):</b> {user.refferals_24h_count}
<b>Бонус за реферала получен:</b> {'Да' if user.ref_bonus else 'Нет'}
<b>Текущее задание ID:</b> {user.current_task_id or 'Нет'}
<b>Пришел по инд. ссылке ID:</b> {user.individual_link_id or 'Нет'}
<b>Дата регистрации:</b> {user.registered_at.strftime('%Y-%m-%d %H:%M')}
"""
            try:
                # Редактируем сообщение, где запрашивали сумму или показывали профиль
                await callback.message.edit_text(profile_text, reply_markup=kb.admin_manage_user_keyboard(user))
                await callback.answer("Действие отменено.")
                restored = True
            except Exception as e:
                logger.error(f"Failed to restore profile on cancel state {current_state_str}: {e}")
                # Если не удалось отредактировать, отправим новое сообщение
                try:
                    await callback.message.answer("Действие отменено. Не удалось обновить предыдущее сообщение.", reply_markup=kb.admin_main_keyboard())
                    restored = True # Считаем, что как-то обработали
                except Exception as send_err:
                     logger.error(f"Failed to send new message on cancel state {current_state_str}: {send_err}")


    # Пытаемся вернуться к меню управления ссылками, если отменяли что-то с ними связанное
    elif current_state_str and (current_state_str.startswith("AdminGetIndLinkStatsState:") or current_state_str.startswith("AdminDeleteIndLinkState:") or current_state_str.startswith("AddIndividualLinkState:")):
         # await callback.message.edit_text("Действие отменено.") # Редактируем или нет?
         await admin_manage_ind_links(callback, session) # Показываем меню ссылок
         await callback.answer("Действие отменено.")
         restored = True

    # Если не удалось восстановить контекст или это было другое состояние
    if not restored:
        try:
            await callback.message.edit_text("Действие отменено.", reply_markup=kb.admin_main_keyboard())
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 pass # Ничего страшного, если сообщение не изменилось
            else: raise e # Перевыбрасываем другую ошибку
        except Exception as e:
             logger.warning(f"Could not edit message on generic cancel state: {e}")
             await callback.message.answer("Действие отменено.", reply_markup=kb.admin_main_keyboard())
        await callback.answer("Действие отменено.")

# --- Управление Каналами для Подписки ---

# Кнопка "Каналы для подписки"
@router.callback_query(F.data == "admin_manage_sub_channels")
@admin_required
async def admin_manage_sub_channels_menu(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показывает меню управления каналами для подписки."""
    all_channels = await db.get_all_channels_with_stage(session)
    await callback.message.edit_text(
        "Управление каналами для обязательной подписки:",
        reply_markup=kb.admin_sub_channels_menu_keyboard(all_channels) # Нужна новая клавиатура
    )
    await callback.answer()

# Кнопка "Добавить канал"
@router.callback_query(F.data == "admin_add_sub_channel")
@admin_required
async def admin_add_sub_channel_start(callback: CallbackQuery, config: Config, state: FSMContext):
    """Начинает процесс добавления канала для подписки."""
    # Можно использовать существующий FSM для добавления каналов или создать новый
    # Для простоты пока сделаем без FSM, запросим ID и ссылку в одном сообщении
    await callback.message.edit_text(
        "Введите ID канала и ссылку на него через пробел.\n"
        "Пример: <code>-100123456789 https://t.me/mychannel</code>\n\n"
        "Также укажите этап проверки (1 или 2) через пробел в конце.\n"
        "Пример: <code>-100123456789 https://t.me/mychannel 1</code>",
        reply_markup=kb.admin_back_to_sub_channels_keyboard() # Кнопка "Назад"
    )
    # Устанавливаем состояние ожидания данных канала (если нужен FSM)
    # await state.set_state(AdminChannelStates.waiting_for_channel_data)
    await callback.answer()

# (Если без FSM, нужен обработчик сообщения с данными канала)
@router.message(StateFilter(None), F.text.regexp(r"^(-?\d+)\s+(https?://t\.me/\S+)\s+([12])$")) # Пример регекса
async def admin_process_add_sub_channel(message: Message, config: Config, session: AsyncSession):
     match = re.match(r"^(-?\d+)\s+(https?://t\.me/\S+)\s+([12])$", message.text)
     if not match:
         await message.reply("Неверный формат. Попробуйте еще раз.\nПример: <code>-100123456789 https://t.me/mychannel 1</code>")
         return

     channel_id_str, channel_link_from_msg, stage_str = match.groups() # Получаем ссылку из сообщения
     try:
         channel_id = int(channel_id_str)
         stage = int(stage_str)

         # --- ИСПРАВЛЕННЫЙ ВЫЗОВ ---
         # Передаем channel_id, ссылку из сообщения (channel_link_from_msg) и stage
         # НЕ передаем channel_name
         new_channel = await db.add_channel(
             session=session,
             channel_id=channel_id,
             channel_link=channel_link_from_msg, # Передаем ссылку
             stage=stage
         )
         # --------------------------

         await session.commit()
         logger.info(f"Admin {message.from_user.id} added subscription channel {channel_id} (Stage: {stage})")
         await message.reply(f"✅ Канал {channel_id} (Этап: {stage}) успешно добавлен.")

         # ... (остальной код) ...

     except ValueError:
         await message.reply("Ошибка: ID канала или этап должны быть числами.")
     except Exception as e:
         await session.rollback()
         # --- Улучшенное логирование ошибки ---
         logger.error(f"Error adding subscription channel (ID: {channel_id_str}, Link: {channel_link_from_msg}, Stage: {stage_str}): {e}", exc_info=True)
         # ------------------------------------
         await message.reply("❌ Произошла ошибка при добавлении канала.")

# ... (остальные хендлеры) ...

# --- Пример хендлера подтверждения для FSM AddChannelState ---
# Убедитесь, что фильтр состояния и callback_data соответствуют вашей логике
@router.callback_query(F.data == "addchannel_confirm_yes", StateFilter(AddChannelState.confirming))
@admin_required
async def process_channel_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    """Обрабатывает подтверждение и добавляет канал в БД."""
    data = await state.get_data()
    channel_id = data.get('channel_id')
    channel_link = data.get('channel_link') # Убедитесь, что ссылка сохранена под этим ключом
    # channel_name = data.get('channel_name') # Имя больше не используется в add_channel
    # channel_status = data.get('channel_status') # Статус больше не используется
    # premium_requirement = data.get('premium_requirement') # Требование премиум не используется в новой add_channel
    stage = data.get('channel_stage') # <-- Убедитесь, что этап сохранен под этим ключом

    if not all([channel_id, channel_link, stage]):
        await callback.answer("Ошибка: Не все данные для добавления канала найдены в состоянии.", show_alert=True)
        logger.error(f"Missing data in state for channel confirmation: {data}")
        await state.clear()
        # Можно вернуть в меню админки
        await cmd_admin_panel(callback)
        return

    try:
        # --- ИСПРАВЛЕННЫЙ ВЫЗОВ db.add_channel ---
        # Передаем только channel_id, channel_link и stage
        new_channel = await db.add_channel(
            session=session,
            channel_id=channel_id,
            channel_link=channel_link, # Передаем ссылку
            stage=stage                 # Передаем этап
        )
        # -----------------------------------------

        await session.commit()
        logger.info(f"Admin {callback.from_user.id} confirmed and added channel {channel_id} (Stage: {stage})")
        await callback.message.edit_text(f"✅ Канал {channel_id} (Этап: {stage}) успешно добавлен.")
        # Возвращаемся в меню управления каналами
        await admin_manage_sub_channels_menu(callback, session) # Передаем session

    except Exception as e:
        await session.rollback()
        # --- Улучшенное логирование ---
        logger.error(f"Admin {callback.from_user.id} failed to add channel: {e}", exc_info=True)
        # -----------------------------
        await callback.message.edit_text("❌ Произошла ошибка при добавлении канала.")
        # Возвращаемся в меню управления каналами
        await admin_manage_sub_channels_menu(callback, session) # Передаем session
    finally:
        await state.clear() # Очищаем состояние в любом случае
        await callback.answer() # Отвечаем на коллбэк

# --- НОВЫЙ обработчик для смены этапа канала ---
@router.callback_query(F.data.startswith("admin_channel_set_stage_"))
@admin_required
async def admin_set_channel_stage(callback: CallbackQuery, config: Config, session: AsyncSession):
    # --- Логируем вход в обработчик и данные коллбэка ---
    logger.info(f"Entered admin_set_channel_stage handler. Callback data: {callback.data}")
    try:
        parts = callback.data.split("_")
        channel_db_id = int(parts[4])
        new_stage = int(parts[5])
        if new_stage not in [1, 2]:
            raise ValueError("Invalid stage")
        logger.debug(f"Parsed channel_db_id={channel_db_id}, new_stage={new_stage}")
    except (IndexError, ValueError) as e:
        logger.error(f"Invalid channel set stage callback data: {callback.data}. Error: {e}")
        await callback.answer("Ошибка: Неверные данные для смены этапа.", show_alert=True)
        return

    # --- Используем новую функцию для обновления этапа ---
    logger.debug(f"Attempting to update stage for DB ID {channel_db_id} to {new_stage}...")
    updated = await db.update_channel_stage(session, channel_db_id, new_stage)
    logger.info(f"Database update result for stage change (DB ID {channel_db_id}): {updated}") # Логируем результат

    if updated:
        try:
            await session.commit() # Коммитим изменение этапа
            logger.info(f"Admin {callback.from_user.id} successfully changed stage of channel DB ID {channel_db_id} to {new_stage}")
            await callback.answer(f"✅ Этап канала изменен на {new_stage}.", show_alert=False)
            # Определяем тип канала для возврата к правильному списку
            channel = await db.get_channel_by_db_id(session, channel_db_id)
            check_type = channel.check_type if channel else 'start' # По умолчанию 'start'
            await _show_admin_manage_channels(callback, session, check_type)
        except Exception as e_commit:
             logger.error(f"Error committing stage change for DB ID {channel_db_id}: {e_commit}", exc_info=True)
             await session.rollback()
             await callback.answer("❌ Ошибка при сохранении изменений.", show_alert=True)
             # Пытаемся вернуться к списку 'start' по умолчанию
             await _show_admin_manage_channels(callback, session, 'start')
    else:
        await session.rollback()
        logger.warning(f"Failed to update stage for channel DB ID {channel_db_id}. Channel not found or stage already set?")
        await callback.answer("❌ Не удалось изменить этап канала (возможно, он не найден или этап уже установлен).", show_alert=True)
        # Пытаемся вернуться к списку 'start' по умолчанию
        await _show_admin_manage_channels(callback, session, 'start')

@router.callback_query(F.data == "admin_backup_db")
@admin_required
async def admin_backup_db(callback: CallbackQuery, config: Config):
    try:
        # Путь к файлу бэкапа
        backup_file_path = "/tmp/backup.sql"

        # Устанавливаем переменную окружения для пароля
        os.environ['PGPASSWORD'] = config.db.password

        # Команда для создания дампа базы данных
        command = f"pg_dump -U {config.db.user} -h {config.db.host} -p 5432 {config.db.database} > {backup_file_path}"
        
        # Выполняем команду
        subprocess.run(command, shell=True, check=True)

        # Отправляем файл администратору
        backup_file = FSInputFile(backup_file_path)
        await callback.message.answer_document(backup_file, caption="Бэкап базы данных")

        # Удаляем временный файл
        os.remove(backup_file_path)

    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при создании бэкапа базы данных: {e}")
        await callback.answer("❌ Ошибка при создании бэкапа базы данных.", show_alert=True)
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        await callback.answer("❌ Произошла непредвиденная ошибка.", show_alert=True)

@router.callback_query(F.data == "template_promocode_edit_select")
async def set_promocode(callback: CallbackQuery, config: Config, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("Пожалуйста, введите промокод:", reply_markup=kb.admin_back_to_main_keyboard())
    await state.set_state(PromoCodeStateTemplate.waiting_for_promocode)

@router.message(PromoCodeStateTemplate.waiting_for_promocode)
async def receive_promocode(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    promocode = message.text.strip()
    # Добавляем промокод в базу данных
    new_promo = await db.set_promo_code_name(session, promo_code_name=promocode)
    if new_promo:
        await message.answer(f"Промокод '{promocode}' сохранен в базе данных для шаблона.")
    else:
        await message.answer(f"Промокод '{promocode}' уже существует.")
    await state.clear()

@router.callback_query(F.data == "admin_subgram_stats")
@admin_required
async def admin_subgram_general_stats(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Общая статистика SubGram"""
    try:
        from bot.database.requests import get_subgram_tasks_statistics
        
        stats = await get_subgram_tasks_statistics(session)
        
        text = f"""📊 <b>Общая статистика SubGram</b>

✅ <b>Выполненные задания:</b>
├ Всего заданий: {stats['total_completed_tasks']}
├ Уникальных пользователей: {stats['unique_users']}
└ Среднее заданий на пользователя: {stats['total_completed_tasks'] / max(stats['unique_users'], 1):.1f}

⚠️ <b>Штрафы за отписки:</b>
├ Применено штрафов: {stats['penalties_applied']}
├ Общая сумма: {stats['total_penalty_amount']:.2f}⭐️
└ Процент от выполненных: {stats['penalty_rate']:.1f}%

💰 <b>Экономика:</b>
├ Всего выдано наград: {stats['total_rewards_given']:.2f}⭐️
├ Возвращено штрафами: {stats['total_penalty_amount']:.2f}⭐️
└ <b>Чистые расходы:</b> {stats['total_rewards_given'] - stats['total_penalty_amount']:.2f}⭐️

📈 <b>Эффективность:</b>
├ Возврат средств: {(stats['total_penalty_amount'] / max(stats['total_rewards_given'], 1) * 100):.1f}%
└ Удержание подписчиков: {100 - stats['penalty_rate']:.1f}%"""

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔄 Обновить", callback_data="admin_subgram_stats")
        keyboard.button(text="👤 Проверить пользователя", callback_data="admin_user_subgram_penalties")
        keyboard.button(text="◀️ Назад", callback_data="admin_back_to_main")
        keyboard.adjust(1)
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при получении общей статистики SubGram: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)

@router.callback_query(F.data == "admin_subgram_webhooks")
@admin_required
async def admin_subgram_webhooks_menu(callback: CallbackQuery, config: Config, session: AsyncSession):
    """
    Меню управления webhook'ами SubGram
    """
    try:
        from bot.database.requests import get_subgram_webhooks_stats
        
        stats = await get_subgram_webhooks_stats(session)
        
        text = f"""
📊 <b>Статистика webhook'ов SubGram</b>

📈 <b>Всего событий:</b> {stats['total']}
⏳ <b>Необработанных:</b> {stats['unprocessed']}

📋 <b>По статусам:</b>
✅ Подписки: {stats['subscribed']}
❌ Отписки: {stats['unsubscribed']}
⚠️ Не засчитано: {stats['notgetted']}
        """
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить статистику", callback_data="admin_subgram_webhooks")],
            [InlineKeyboardButton(text="📋 Последние webhook'и", callback_data="admin_subgram_recent")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back_to_main")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в admin_subgram_webhooks_menu: {e}")
        await callback.answer("❌ Произошла ошибка")

@router.callback_query(F.data == "admin_subgram_recent")
@admin_required
async def admin_subgram_recent_webhooks(callback: CallbackQuery, config: Config, session: AsyncSession):
    """
    Показывает последние webhook'и SubGram
    """
    try:
        from bot.database.requests import get_unprocessed_subgram_webhooks
        from bot.database.models import SubGramWebhook
        
        # Получаем последние 10 webhook'ов
        result = await session.execute(
            select(SubGramWebhook)
            .order_by(SubGramWebhook.received_at.desc())
            .limit(10)
        )
        webhooks = result.scalars().all()
        
        if not webhooks:
            text = "❌ Webhook'и не найдены"
        else:
            text = "📋 <b>Последние 10 webhook'ов:</b>\n\n"
            
            for webhook in webhooks:
                status_emoji = {
                    'subscribed': '✅',
                    'unsubscribed': '❌',
                    'notgetted': '⚠️'
                }.get(webhook.status, '❓')
                
                text += f"{status_emoji} <b>ID:</b> {webhook.webhook_id}\n"
                text += f"👤 <b>Пользователь:</b> {webhook.user_id}\n"
                text += f"📅 <b>Дата:</b> {webhook.received_at.strftime('%d.%m.%Y %H:%M')}\n"
                text += f"🔗 <b>Ссылка:</b> {webhook.link[:50]}...\n\n"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_subgram_webhooks")]
        ])
        
        await callback.message.edit_text(text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в admin_subgram_recent_webhooks: {e}")
        await callback.answer("❌ Произошла ошибка")

@router.callback_query(F.data == "admin_subgram_tasks_stats")
@admin_required
async def admin_subgram_tasks_statistics(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Просмотр статистики по заданиям SubGram"""
    try:
        from bot.database.requests import get_subgram_tasks_statistics
        
        stats = await get_subgram_tasks_statistics(session)
        
        text = f"""📊 <b>Статистика заданий SubGram</b>

✅ <b>Всего выполнено заданий:</b> {stats['total_completed_tasks']}
👥 <b>Уникальных пользователей:</b> {stats['unique_users']}

⚠️ <b>Штрафы за отписки:</b>
├ Применено штрафов: {stats['penalties_applied']}
├ Общая сумма штрафов: {stats['total_penalty_amount']:.2f}⭐️
└ Процент штрафов: {stats['penalty_rate']:.1f}%

💰 <b>Награды:</b>
├ Всего выдано: {stats['total_rewards_given']:.2f}⭐️
└ Чистая прибыль: {stats['total_rewards_given'] - stats['total_penalty_amount']:.2f}⭐️"""

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔄 Обновить", callback_data="admin_subgram_tasks_stats")
        keyboard.button(text="📋 Последние webhook'ы", callback_data="admin_subgram_recent")
        keyboard.button(text="◀️ Назад", callback_data="admin_subgram_webhooks")
        keyboard.adjust(1)
        
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики заданий: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)

@router.callback_query(F.data == "admin_user_subgram_penalties")
@admin_required
async def admin_user_subgram_penalties(callback: CallbackQuery, config: Config, state: FSMContext):
    """Запрос статистики штрафов пользователя"""
    
    text = """🔍 <b>Статистика штрафов пользователя</b>

Отправьте User ID пользователя для просмотра его статистики по штрафам SubGram заданий:"""

    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="❌ Отмена", callback_data="admin_back_to_main")
    
    await callback.message.edit_text(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await callback.answer()
    await state.set_state("waiting_for_user_penalties_id")

@router.message(StateFilter("waiting_for_user_penalties_id"))
@admin_required
async def admin_process_user_penalties_id(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обработка User ID для статистики штрафов"""
    try:
        user_id = int(message.text.strip())
        
        from bot.database.requests import get_user_subgram_penalties_stats, get_user
        
        # Проверяем существование пользователя
        user = await get_user(session, user_id)
        if not user:
            await message.reply("❌ Пользователь не найден в базе данных")
            return
        
        # Получаем статистику штрафов
        stats = await get_user_subgram_penalties_stats(session, user_id)
        
        text = f"""👤 <b>Статистика пользователя {user_id}</b>

📝 <b>SubGram задания:</b>
├ Выполнено всего: {stats['total_completed']}
├ Применено штрафов: {stats['penalties_count']}
└ Процент штрафов: {(stats['penalties_count'] / stats['total_completed'] * 100) if stats['total_completed'] > 0 else 0:.1f}%

💰 <b>Финансы:</b>
├ Получено наград: {stats['total_rewards']:.2f}⭐️
├ Штрафы: {stats['total_penalty_amount']:.2f}⭐️
└ Чистая прибыль: {stats['net_earnings']:.2f}⭐️

💳 <b>Текущий баланс:</b> {user.balance:.2f}⭐️"""

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔍 Другой пользователь", callback_data="admin_user_subgram_penalties")
        keyboard.button(text="◀️ Назад", callback_data="admin_back_to_main")
        keyboard.adjust(1)
        
        await message.reply(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
        await state.clear()
        
    except ValueError:
        await message.reply("❌ Неверный формат User ID. Введите число.")
    except Exception as e:
        logger.error(f"Ошибка при получении статистики штрафов пользователя: {e}")
        await message.reply("❌ Произошла ошибка при получении статистики")
        await state.clear()

@router.callback_query(F.data == "admin_daily_tasks_stats")
@admin_required
async def admin_daily_tasks_stats_menu(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показывает статистику ежедневных заданий"""
    try:
        from bot.database.requests import get_comprehensive_daily_tasks_stats
        
        stats = await get_comprehensive_daily_tasks_stats(session)
        
        text = f"""📊 <b>Статистика ежедневных заданий</b>

📈 <b>Общая статистика:</b>
├ Всего выполнено: {stats['total_completions']}
├ Уникальных пользователей: {stats['unique_users']}
└ Всего выдано: {stats['total_rewards']:.2f}⭐️
"""

        from bot.keyboards.keyboards import admin_daily_tasks_stats_keyboard
        
        await callback.message.edit_text(text, reply_markup=admin_daily_tasks_stats_keyboard(), parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики ежедневных заданий: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)


@router.callback_query(F.data == "admin_daily_user_search")
@admin_required
async def admin_daily_user_search(callback: CallbackQuery, config: Config, state: FSMContext):
    """Запрос статистики пользователя по ежедневным заданиям"""
    
    text = """🔍 <b>Статистика пользователя</b>

Отправьте User ID пользователя для просмотра его статистики по ежедневным заданиям:"""

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="❌ Отмена", callback_data="admin_daily_tasks_stats")
    
    await callback.message.edit_text(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")
    await callback.answer()
    await state.set_state("waiting_for_daily_user_id")


@router.message(StateFilter("waiting_for_daily_user_id"))
@admin_required
async def admin_process_daily_user_id(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обработка User ID для статистики ежедневных заданий"""
    try:
        user_id = int(message.text.strip())
        
        from bot.database.requests import get_user_daily_tasks_history, get_user
        
        # Проверяем существование пользователя
        user = await get_user(session, user_id)
        if not user:
            await message.reply("❌ Пользователь не найден в базе данных")
            return
        
        # Получаем статистику пользователя
        stats = await get_user_daily_tasks_history(session, user_id)
        
        text = f"""👤 <b>Статистика пользователя {user_id}</b>

📝 <b>Ежедневные задания:</b>
├ Выполнено всего: {stats.get('total_completions', 0)}
├ Получено наград: {stats.get('total_earned', 0):.2f}⭐️
└ Среднее за задание: {stats.get('total_earned', 0) / max(stats.get('total_completions', 1), 1):.2f}⭐️"""

        # Показываем историю выполнений если есть
        if stats.get('user_history'):
            text += f"\n\n📅 <b>Последние выполнения:</b>"
            for completion in stats['user_history'][:5]:
                task_name = {
                    'bio_link': '🔗',
                    'channel_subscription': '📢',
                    'referral_invite': '👥'
                }.get(completion['task_type'], '📝')
                completion_date = completion['completed_at'].strftime('%d.%m %H:%M')
                text += f"\n├ {task_name} {completion_date} (+{completion['reward']:.1f}⭐️)"

        text += f"\n\n💳 <b>Текущий баланс:</b> {user.balance:.2f}⭐️"
        
        from bot.keyboards.keyboards import admin_daily_user_search_keyboard
        
        await message.reply(text, reply_markup=admin_daily_user_search_keyboard(), parse_mode="HTML")
        await state.clear()
        
    except ValueError:
        await message.reply("❌ Неверный формат User ID. Введите число.")
    except Exception as e:
        logger.error(f"Ошибка при получении статистики пользователя: {e}")
        await message.reply("❌ Произошла ошибка при получении статистики")
        await state.clear()

@router.callback_query(F.data == "admin_gift_settings")
@admin_required
async def admin_gift_settings_menu(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Главное меню настроек подарков"""
    try:
        # Получаем текущие настройки
        settings = await db.get_gift_withdraw_settings(session)
        if not settings:
            # Создаем настройки по умолчанию если их нет
            settings = await db.create_default_gift_settings(session)
        
        text = f"""🎁 <b>Настройки автоматических выплат подарками</b>

📊 <b>Текущие настройки:</b>
• Статус: {'✅ Включено' if settings.enabled else '❌ Отключено'}
• Мин. сумма для подарков: {settings.min_amount_for_gifts} ⭐
• Макс. остаток для возврата: {settings.max_remainder} ⭐

📈 <b>Статистика:</b>
Для просмотра статистики нажмите соответствующую кнопку."""

        keyboard = kb.admin_gift_settings_keyboard(settings)
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в admin_gift_settings_menu: {e}")
        await callback.answer("❌ Произошла ошибка при загрузке настроек", show_alert=True)

@router.callback_query(F.data == "admin_gift_toggle_status")
@admin_required
async def admin_gift_toggle_status(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Переключение статуса автоматических выплат"""
    try:
        settings = await db.get_gift_withdraw_settings(session)
        if not settings:
            settings = await db.create_default_gift_settings(session)
        
        # Переключаем статус
        new_status = not settings.enabled
        success = await db.update_gift_withdraw_settings(session, enabled=new_status)
        
        if success:
            status_text = "включены" if new_status else "отключены"
            await callback.answer(f"✅ Автоматические выплаты подарками {status_text}", show_alert=True)
            
            # Обновляем меню
            await admin_gift_settings_menu(callback, config, session)
        else:
            await callback.answer("❌ Ошибка при обновлении настроек", show_alert=True)
            
    except Exception as e:
        logger.error(f"Ошибка в admin_gift_toggle_status: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@router.callback_query(F.data == "admin_gift_stats")
@admin_required
async def admin_gift_stats(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показать статистику по выплатам подарками"""
    try:
        # Получаем статистику
        stats = await db.get_withdraw_stats_by_type(session)
        
        text = f"""📊 <b>Статистика выплат подарками</b>

🎁 <b>Автоматические выплаты подарками:</b>
• Всего: {stats.get('auto_gifts', 0)} заявок
• Успешно: {stats.get('auto_gifts_success', 0)} заявок
• С ошибками: {stats.get('auto_gifts_failed', 0)} заявок

📝 <b>Ручные выплаты:</b>
• Всего: {stats.get('manual', 0)} заявок
• В ожидании: {stats.get('manual_pending', 0)} заявок

⚠️ <b>Проблемные заявки:</b>
Для просмотра проблемных заявок нажмите соответствующую кнопку."""

        keyboard = kb.admin_gift_stats_keyboard()
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в admin_gift_stats: {e}")
        await callback.answer("❌ Произошла ошибка при загрузке статистики", show_alert=True)

@router.callback_query(F.data == "admin_gift_failed")
@admin_required
async def admin_gift_failed(callback: CallbackQuery, config: Config, session: AsyncSession):
    """Показать проблемные заявки на выплату подарками"""
    try:
        # Получаем проблемные заявки
        failed_withdraws = await db.get_failed_gift_withdraws(session, limit=10)
        
        if not failed_withdraws:
            text = "✅ <b>Проблемных заявок нет</b>\n\nВсе автоматические выплаты подарками прошли успешно."
        else:
            text = "⚠️ <b>Проблемные заявки на выплату подарками</b>\n\n"
            
            for withdraw in failed_withdraws:
                error_text = withdraw.processing_error or "Неизвестная ошибка"
                if len(error_text) > 100:
                    error_text = error_text[:100] + "..."
                
                text += f"🆔 ID: {withdraw.id}\n"
                text += f"👤 Пользователь: {withdraw.user_id}\n"
                text += f"💰 Сумма: {withdraw.withdraw_amount} ⭐\n"
                text += f"❌ Ошибка: {error_text}\n"
                text += f"📅 Дата: {withdraw.withdraw_date.strftime('%d.%m.%Y %H:%M')}\n\n"
        
        keyboard = kb.admin_gift_failed_keyboard()
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в admin_gift_failed: {e}")
        await callback.answer("❌ Произошла ошибка при загрузке проблемных заявок", show_alert=True)

@router.callback_query(F.data == "admin_gift_set_min_amount")
@admin_required
async def admin_gift_set_min_amount(callback: CallbackQuery, config: Config, state: FSMContext):
    """Начать изменение минимальной суммы для подарков"""
    try:
        await state.set_state(GiftSettingsState.waiting_for_min_amount)
        
        text = """🎁 <b>Изменение минимальной суммы</b>

Введите новую минимальную сумму для автоматических выплат подарками.

💡 <b>Рекомендации:</b>
• Минимум: 10 ⭐
• Оптимально: 15-20 ⭐
• Максимум: 50 ⭐

Отправьте число или нажмите "Отмена" для возврата."""

        keyboard = kb.cancel_state_keyboard()
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в admin_gift_set_min_amount: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@router.callback_query(F.data == "admin_gift_set_max_remainder")
@admin_required
async def admin_gift_set_max_remainder(callback: CallbackQuery, config: Config, state: FSMContext):
    """Начать изменение максимального остатка"""
    try:
        await state.set_state(GiftSettingsState.waiting_for_max_remainder)
        
        text = """🎁 <b>Изменение максимального остатка</b>

Введите новый максимальный остаток, который будет возвращен на баланс пользователя.

💡 <b>Рекомендации:</b>
• Минимум: 5 ⭐
• Оптимально: 10-15 ⭐
• Максимум: 20 ⭐

Отправьте число или нажмите "Отмена" для возврата."""

        keyboard = kb.cancel_state_keyboard()
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в admin_gift_set_max_remainder: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@router.message(GiftSettingsState.waiting_for_min_amount)
@admin_required
async def process_gift_min_amount(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обработка новой минимальной суммы"""
    try:
        # Проверяем, что введено число
        try:
            min_amount = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Пожалуйста, введите корректное число")
            return
        
        # Проверяем диапазон
        if min_amount < 10 or min_amount > 100:
            await message.answer("❌ Минимальная сумма должна быть от 10 до 100 звезд")
            return
        
        # Обновляем настройки
        success = await db.update_gift_withdraw_settings(session, min_amount=min_amount)
        
        if success:
            await message.answer(f"✅ Минимальная сумма для подарков установлена: {min_amount} ⭐")
            await state.clear()
            
            # Возвращаемся в меню настроек
            # Создаем фиктивный callback для переиспользования функции
            from types import SimpleNamespace
            fake_callback = SimpleNamespace()
            fake_callback.message = message
            fake_callback.answer = lambda *args, **kwargs: None
            
            await admin_gift_settings_menu(fake_callback, config, session)
        else:
            await message.answer("❌ Ошибка при обновлении настроек")
            
    except Exception as e:
        logger.error(f"Ошибка в process_gift_min_amount: {e}")
        await message.answer("❌ Произошла ошибка при обновлении настроек")

@router.message(GiftSettingsState.waiting_for_max_remainder)
@admin_required
async def process_gift_max_remainder(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    """Обработка нового максимального остатка"""
    try:
        # Проверяем, что введено число
        try:
            max_remainder = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Пожалуйста, введите корректное число")
            return
        
        # Проверяем диапазон
        if max_remainder < 5 or max_remainder > 50:
            await message.answer("❌ Максимальный остаток должен быть от 5 до 50 звезд")
            return
        
        # Обновляем настройки
        success = await db.update_gift_withdraw_settings(session, max_remainder=max_remainder)
        
        if success:
            await message.answer(f"✅ Максимальный остаток установлен: {max_remainder} ⭐")
            await state.clear()
            
            # Возвращаемся в меню настроек
            from types import SimpleNamespace
            fake_callback = SimpleNamespace()
            fake_callback.message = message
            fake_callback.answer = lambda *args, **kwargs: None
            
            await admin_gift_settings_menu(fake_callback, config, session)
        else:
            await message.answer("❌ Ошибка при обновлении настроек")
            
    except Exception as e:
        logger.error(f"Ошибка в process_gift_max_remainder: {e}")
        await message.answer("❌ Произошла ошибка при обновлении настроек")
        await message.answer("❌ Произошла ошибка при обновлении настроек")
        await admin_gift_settings_menu(fake_callback, config, session)
        

# --- Создание нового 'Показа' ---

 # --- Управление 'Показами' ---
    
@router.callback_query(F.data == "admin_manage_shows", StateFilter(None))
@admin_required
async def admin_manage_shows(callback: CallbackQuery, config: Config, session: AsyncSession):
    shows = await db.get_all_shows(session)
    await callback.message.edit_text(
        "<b>🎬 Управление показами</b>\n\n"
        "Здесь вы можете управлять сообщениями, которые пользователи видят при старте бота. "
        "Только один 'показ' может быть активен в один момент времени.",
        reply_markup=kb.admin_shows_list_keyboard(shows),
        parse_mode = 'HTML'
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin_show_view_"), StateFilter(None))
@admin_required
async def admin_view_single_show(callback: CallbackQuery, config: Config, session: AsyncSession):
    show_id = int(callback.data.split("_")[-1])
    show = await db.get_show_by_id(session, show_id)
    if not show:
        await callback.answer("Показ не найден.", show_alert=True)
        return
    
    status = "🟢 Активен" if show.is_active else "⚫️ Неактивен"
    photo_info = "Есть ✅" if show.photo_file_id else "Нет ❌"
    keyboard_info = "Есть ✅" if show.keyboard_json else "Нет ❌"
    
    text = f"""
    <b>🎬 Просмотр показа: "{html.escape(show.name)}"</b>
    
<b>Статус:</b> {status}
<b>Фото:</b> {photo_info}
<b>Клавиатура:</b> {keyboard_info}

<b>Текст сообщения:</b>
{html.escape(show.text)}
    """
    
    await callback.message.edit_text(
        text,
        reply_markup=kb.admin_show_manage_keyboard(show)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin_show_toggle_"), StateFilter(None))
@admin_required
async def admin_toggle_show_status(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    show_id = int(callback.data.split("_")[-1])
    
    updated_show = await db.set_show_active_status(session, show_id, True)
    if not updated_show:
        await callback.answer("Не удалось активировать показ.", show_alert=True)
        return
        
    await callback.answer("✅ Показ успешно активирован!", show_alert=True)
    
    # Обновляем список
    shows = await db.get_all_shows(session)
    await callback.message.edit_text(
        "🎬 Управление показами",
        reply_markup=kb.admin_shows_list_keyboard(shows)
    )
    await state.clear()

@router.callback_query(lambda c: c.data.startswith("admin_show_delete_") and c.data.count('_') == 3, StateFilter(None))
@admin_required
async def admin_delete_show_prompt(callback: CallbackQuery, config: Config, session: AsyncSession):
    show_id = int(callback.data.split("_")[-1])
    show = await db.get_show_by_id(session, show_id)
    if not show:
        await callback.answer("Показ не найден.", show_alert=True)
        return
    
    try:
        await callback.message.edit_text(
            f"Вы уверены, что хотите удалить показ \"{html.escape(show.name)}\"?",
            reply_markup=kb.admin_show_delete_confirm_keyboard(show_id)
        )
    except TelegramBadRequest:
        # Игнорируем ошибку, если сообщение не было изменено (защита от двойных кликов)
        pass 
    await callback.answer()

@router.callback_query(F.data.startswith("admin_show_delete_confirm_"))
@admin_required
async def admin_delete_show_confirm(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    logger.info(f"admin_delete_show_confirm: {callback.data}")
    show_id = int(callback.data.split("_")[-1])
    deleted = await db.delete_show_by_id(session, show_id)
    
    if not deleted:
        await callback.answer("Не удалось удалить показ.", show_alert=True)
        return
        
    await callback.answer("🗑️ Показ успешно удален.", show_alert=True)
    
    # Заменяем edit_text на delete + answer для большей надежности и наглядности
    await callback.message.delete()
    shows = await db.get_all_shows(session)
    await callback.message.answer(
        "🎬 Управление показами",
        reply_markup=kb.admin_shows_list_keyboard(shows),
        parse_mode = 'HTML'
    )
    await state.clear()

@router.callback_query(F.data == "admin_add_show", StateFilter(None))
@admin_required
async def admin_add_show_start(callback: CallbackQuery, config: Config, state: FSMContext):
    await callback.message.edit_text(
        "Введите уникальное имя для нового 'показа' (например, 'Приветствие весна 2024').",
        reply_markup=kb.cancel_state_keyboard()
    )
    await state.set_state(AddShowState.waiting_for_name)
    await callback.answer()

@router.message(AddShowState.waiting_for_name, F.text)
@admin_required
async def process_show_name(message: Message, config: Config, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer(
        "Теперь отправьте текст сообщения. Вы можете использовать HTML-разметку.",
        reply_markup=kb.cancel_state_keyboard()
    )
    await state.set_state(AddShowState.waiting_for_text)

@router.message(AddShowState.waiting_for_text, F.text)
@admin_required
async def process_show_text(message: Message, config: Config, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer(
        "Отправьте фото для сообщения или нажмите 'Пропустить'.",
        reply_markup=kb.template_creation_skip_keyboard("show_skip_photo")
    )
    await state.set_state(AddShowState.waiting_for_photo)

@router.callback_query(F.data == "show_skip_photo", AddShowState.waiting_for_photo)
@admin_required
async def skip_show_photo(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.update_data(photo_file_id=None)
    await callback.message.edit_text(
        "Теперь отправьте клавиатуру в формате 'Текст кнопки - URL', каждая кнопка с новой строки. Или нажмите 'Пропустить'.",
        reply_markup=kb.template_creation_skip_keyboard("show_skip_keyboard")
    )
    await state.set_state(AddShowState.waiting_for_keyboard)
    await callback.answer()

@router.message(AddShowState.waiting_for_photo, F.photo)
@admin_required
async def process_show_photo(message: Message, config: Config, state: FSMContext):
    await state.update_data(photo_file_id=message.photo[-1].file_id)
    await message.answer(
        "Фото получено. Теперь отправьте клавиатуру в формате 'Текст кнопки - URL' или пропустите.",
        reply_markup=kb.template_creation_skip_keyboard("show_skip_keyboard")
    )
    await state.set_state(AddShowState.waiting_for_keyboard)

@router.callback_query(F.data == "show_skip_keyboard", AddShowState.waiting_for_keyboard)
@admin_required
async def skip_show_keyboard(callback: CallbackQuery, config: Config, state: FSMContext):
    await state.update_data(keyboard_json=None)
    data = await state.get_data()
    await show_show_confirmation(callback, state, data, edit=True)
    await callback.answer()

@router.message(AddShowState.waiting_for_keyboard, F.text)
@admin_required
async def process_show_keyboard(message: Message, config: Config, state: FSMContext, session: AsyncSession):
    try:
        keyboard = mdl.BroadcastTemplate.parse_simple_keyboard(message.text)
        if keyboard:
            await state.update_data(keyboard_json=keyboard.model_dump_json())
            data = await state.get_data()
            await show_show_confirmation(message, state, data)
        else:
            raise ValueError("Неверный формат клавиатуры")
    except Exception as e:
        await message.answer(f"❌ Ошибка при парсинге клавиатуры: {e}\n\nПопробуйте еще раз.")
        return

async def show_show_confirmation(event: Message | CallbackQuery, state: FSMContext, data: dict, edit: bool = False):
    name = data.get('name', 'N/A')
    text = data.get('text', 'N/A')
    photo = "Есть ✅" if data.get('photo_file_id') else "Нет ❌"
    keyboard = "Есть ✅" if data.get('keyboard_json') else "Нет ❌"
    
    confirm_text = f"""
<b>🎬 Проверьте данные нового показа:</b>

<b>Имя:</b> {html.escape(name)}
<b>Фото:</b> {photo}
<b>Клавиатура:</b> {keyboard}

<b>Сохранить этот показ?</b>
    """
    
    target_message = event.message if isinstance(event, CallbackQuery) else event
    
    # Отправляем предпросмотр
    await target_message.answer(confirm_text, reply_markup=kb.yes_no_keyboard("addshow_confirm") , parse_mode = 'HTML')
    
    try:
        if data.get('photo_file_id'):
            await target_message.answer_photo(
                photo=data['photo_file_id'],
                caption=data['text'],
                reply_markup=mdl.Show(keyboard_json=data.get('keyboard_json')).get_keyboard(),
                 parse_mode = 'HTML'
            )
        else:
            await target_message.answer(
                text=data['text'],
                reply_markup=mdl.Show(keyboard_json=data.get('keyboard_json')).get_keyboard(),
                 parse_mode = 'HTML'
            )
    except Exception as e:
        await target_message.answer(f"Не удалось отобразить предпросмотр: {e}")

    await state.set_state(AddShowState.confirming)

@router.callback_query(F.data.startswith("addshow_confirm_"), AddShowState.confirming)
@admin_required
async def process_show_confirmation(callback: CallbackQuery, config: Config, state: FSMContext, session: AsyncSession):
    choice = callback.data.split("_")[-1]
    if choice == "no":
        await state.clear()
        await callback.message.edit_text("Создание показа отменено.", reply_markup=kb.admin_back_to_main_keyboard())
        return

    data = await state.get_data()
    try:
        await db.create_show(
            session=session,
            name=data['name'],
            text=data['text'],
            photo_file_id=data.get('photo_file_id'),
            keyboard_json=data.get('keyboard_json')
        )
        await callback.answer("✅ Показ успешно создан!", show_alert=True)
    except Exception as e:
        await callback.answer(f"❌ Ошибка при создании показа: {e}", show_alert=True)
    finally:
        await state.clear()
        shows = await db.get_all_shows(session)
        await callback.message.edit_text(
            "🎬 Управление показами",
            reply_markup=kb.admin_shows_list_keyboard(shows)
        )