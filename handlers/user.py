from __future__ import annotations

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.types import Message, CallbackQuery, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.fsm.context import FSMContext
from typing import Optional, List, Union
import re
import html
import asyncio  # Добавляем импорт asyncio
from flyerapi import Flyer
import hashlib  # Для генерации ID inline результатов
import urllib.parse  # Для кодирования URL параметров
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from datetime import datetime
# --- Добавляем импорт User --- 
from bot.database.models import User, IndividualLink, Channel, TraffyCompletedTask

import aiohttp
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import CommandStart
from bot.core.utils.state import SubscriptionCheckStates, TraffyTaskState

# -----------------------------

from bot.core.utils.logging import logger

import bot.database.requests as db
import bot.keyboards.keyboards as kb
from bot.core.config import Config
import bot.core.utils.state as st
from bot.core.config import Config

# Определение состояния для хранения списка каналов и текущего индекса
from aiogram.fsm.state import State, StatesGroup

class SponsorChannelsState(StatesGroup):
    channels = State()  # Список всех каналов
    current_index = State()  # Текущий индекс
    task_id = State()  # ID задания SubGram

class SubGramState(StatesGroup):
    current_index = State()

class FlyerState(StatesGroup):
    task_id = State()  # ID задания FlyerAPI
    signature = State()  # Подпись задания, необходимая для проверки

router = Router()

# Константы для расчета рефералов
REFERRAL_PERCENTAGE = 0.5  # 50% от стоимости

def calculate_required_referrals(stars_amount: int, previous_withdraws: int = 0) -> int:
    """
    Рассчитывает количество рефералов, необходимых для вывода определенного количества звезд.
    Формула: (количество звезд / 3) * (количество предыдущих выводов + 1)
    
    Например: 
    - 1-й вывод 15 звезд: (15/3) * 1 = 5 рефералов
    - 2-й вывод 15 звезд: (15/3) * 2 = 10 рефералов  
    - 3-й вывод 15 звезд: (15/3) * 3 = 15 рефералов
    """
    import math
    base_referrals = math.ceil(stars_amount / 3)
    multiplier = previous_withdraws + 1
    return base_referrals * multiplier

import os
API_KEY = os.getenv('SUBGRAM_API_KEY', '')
TRAFFY_RESOURCE_ID = os.getenv('TRAFFY_RESOURCE_ID', '')
FLYER_API_KEY = os.getenv('FLYER_API_KEY', '')
TRAFFY_API_URL = "https://api.traffy.site/v1/mixer/bot/pick_tasks"
TRAFFY_CHECK_URL = "https://api.traffy.site/v1/mixer/bot/check_completion"

SUBGRAM_API_URL = "https://api.subgram.org/get-sponsors"

def _get_flyer_task_link(task: dict) -> str:
    """Return a usable task URL from Flyer payload.

    Flyer may return either `link` (string) or `links` (list of strings).
    """
    direct = task.get("link")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    links = task.get("links")
    if isinstance(links, list):
        for item in links:
            if isinstance(item, str) and item.strip():
                return item.strip()

    return ""

async def request_subgram_sponsors(user_id, chat_id, first_name=None, username=None, language_code=None, is_premium=None, action="subscribe"):
    """
    Запрашивает список спонсоров у SubGram API (новый эндпоинт /get-sponsors).
    Возвращает (status, sponsors_list):
      - status: 'ok' (пропускаем), 'warning' (нужна подписка), 'error'
      - sponsors_list: список объектов спонсоров с полями link, status, type, resource_name и т.д.
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Auth': API_KEY,
            'Accept': 'application/json',
        }
        data = {
            'user_id': user_id,
            'chat_id': chat_id,
            'action': action,
            'get_links': 1,
        }

        if first_name:
            data['first_name'] = first_name
        if username:
            data['username'] = username
        if language_code:
            data['language_code'] = language_code
        if is_premium is not None:
            data['is_premium'] = bool(is_premium)

        logger.debug(f"SubGram API request: action={action}, user_id={user_id}, is_premium={is_premium}")

        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            logger.debug(f"Sending request to SubGram API: URL={SUBGRAM_API_URL}, data={data}")

            async with session.post(SUBGRAM_API_URL, headers=headers, json=data) as response:
                response_json = await response.json()
                logger.debug(f"SubGram API full response: {response_json}")

                api_status = response_json.get('status', '')
                sponsors = []
                if 'additional' in response_json and 'sponsors' in response_json['additional']:
                    sponsors = response_json['additional']['sponsors']
                elif 'sponsors' in response_json:
                    sponsors = response_json['sponsors']

                if api_status == 'ok':
                    logger.info(f"SubGram: user {user_id} OK (subscribed or no sponsors)")
                    return 'ok', []

                if api_status == 'warning' and sponsors:
                    active_sponsors = [s for s in sponsors if s.get('available_now', True) and s.get('status') != 'subscribed']
                    if not active_sponsors:
                        logger.info(f"SubGram: user {user_id} all sponsors subscribed/inactive")
                        return 'ok', []
                    logger.info(f"SubGram: user {user_id} needs to subscribe to {len(active_sponsors)} sponsors")
                    return 'warning', active_sponsors

                if api_status == 'error':
                    logger.error(f"SubGram API error for user {user_id}: {response_json.get('message', '')}")
                    return 'ok', []

                logger.warning(f"SubGram: unexpected response for user {user_id}: {response_json}")
                return 'ok', []

    except Exception as e:
        logger.error(f'SubGram API exception for user {user_id}: {str(e)}', exc_info=True)
        return 'ok', []


async def request_op(user_id, chat_id, first_name=None, language_code=None, is_premium=None, action="subscribe"):
    """Legacy wrapper — используется для заданий (action=newtask). Вызывает старый API."""
    try:
        headers = {
            'Content-Type': 'application/json',
            'Auth': API_KEY,
            'Accept': 'application/json',
        }
        data = {
            'UserId': str(user_id),
            'ChatId': str(chat_id),
            'action': action
        }
        if first_name:
            data['first_name'] = first_name
        if language_code:
            data['language_code'] = language_code
        if is_premium is not None:
            data['Premium'] = '1' if is_premium else '0'

        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.post('https://api.subgram.ru/request-op/', headers=headers, json=data) as response:
                response_json = await response.json()
                if response_json.get('code') == 404:
                    return 'ok', 200, []
                if not response.ok:
                    return 'error', response.status, []
                return response_json.get("status"), response_json.get("code"), response_json.get("links", [])
    except Exception as e:
        logger.error(f'SubGram API exception for user {user_id}: {str(e)}', exc_info=True)
        return 'ok', 200, []

async def check_member_with_delay(bot: Bot, channel_id: str, user_id: int) -> bool:
    # Проверяем, является ли пользователь администратором
    from bot.core.config import config
    if user_id in config.admin_ids:
        logger.info(f"check_member_with_delay: Admin user {user_id} detected, skipping subscription check for channel {channel_id}")
        return True
    
    # Дополнительная проверка для конкретного администратора 7631252818
    if user_id == 7631252818:
        logger.info(f"check_member_with_delay: Special admin 7631252818 detected, skipping subscription check for channel {channel_id}")
        return True
    
    await asyncio.sleep(0.1) # Пример
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.warning(f"Failed to check membership for user {user_id} in {channel_id}: {e}")
        return False

async def show_main_menu(message: Union[Message, CallbackQuery], user_data: User, config: Config, bot: Bot):
    """Отправляет главное меню пользователю."""
    user_id = user_data.user_id
    # Пытаемся получить имя пользователя из события (Message или CallbackQuery)
    user_first_name = "Пользователь" # Значение по умолчанию
    if hasattr(message, 'from_user') and message.from_user:
        user_first_name = message.from_user.first_name

    welcome_caption_two = f'''
👋 <b>Добро пожаловать, {html.escape(user_first_name)}!</b>

⭐️ <b>Зарабатывай звёзды</b> - выполняй простые задания и получай вознаграждение!

🎁 <b>Получай подарки</b> - обменивай звёзды на подарки!

👥 <b>Приглашай друзей</b> - получай звёзды за каждого приглашенного друга!

👇 <b>Жми на кнопки ниже, чтобы начать!</b>'''

    # Определяем целевой объект для ответа (Message или message из CallbackQuery)
    target_message = message if isinstance(message, Message) else message.message
    if not target_message:
        logger.warning(f"Could not determine target message to send main menu for user {user_id}")
        return

    try:
        # Отправляем фото с главным меню
        await target_message.answer_photo(
            photo=FSInputFile("bot/assets/images/menu.jpg"), # Убедитесь, что путь к файлу правильный
            caption=welcome_caption_two,
            reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
            parse_mode="HTML"
        )
        # Отправляем доп. сообщение про задания
        await target_message.answer("💡Также можно <b>получать Звёзды</b> за <b>простые задания!</b>", reply_markup=kb.earn_stars_task_keyboard())
        logger.info(f"Sent main menu to user {user_id}")
    except Exception as e_welcome:
         logger.error(f"Error sending main menu for user {user_id}: {e_welcome}", exc_info=True)

async def get_traffy_tasks(resource_id: str, telegram_chat_id: str, max_tasks: int = 10):
    params = {
        "resource_id": resource_id,
        "telegram_id": str(telegram_chat_id),
        "max_tasks": str(max_tasks)
    }
    logger.info(f"[Traffy] Запрос задач: {params}")
    async with aiohttp.ClientSession() as session:
        async with session.get(TRAFFY_API_URL, params=params) as resp:
            data = await resp.json()
            logger.info(f"[Traffy] Ответ: {data}")
            if data.get("success") and data.get("tasks"):
                return data["tasks"]
            return []

async def check_traffy_task(resource_id: str, telegram_chat_id: str, task_id: str):
    params = {
        "resource_id": resource_id,
        "telegram_id": str(telegram_chat_id),
        "task_id": task_id
    }
    logger.info(f"[Traffy] Проверка задания: {params}")
    async with aiohttp.ClientSession() as session:
        async with session.get(TRAFFY_CHECK_URL, params=params) as resp:
            data = await resp.json()
            logger.info(f"[Traffy] Check ответ: {data}")
            return data

async def check_subscription_for_type(
    message_or_callback: Message | CallbackQuery,
    bot: Bot,
    session: AsyncSession,
    check_type: str
) -> bool:
    user = message_or_callback.from_user
    user_id = user.id
    is_premium = user.is_premium
    is_callback = isinstance(message_or_callback, CallbackQuery)
    message = message_or_callback.message if is_callback else message_or_callback
    callback_data = message_or_callback.data if is_callback else None
    recheck_callback_data = f"recheck_sub_{check_type}"

    # Проверяем, является ли пользователь администратором
    from bot.core.config import config
    if user_id in config.admin_ids:
        logger.info(f"Function check_subscription_for_type: Admin user {user_id} detected, skipping subscription check")
        return True
    
    # Дополнительная проверка для конкретного администратора 7631252818
    if user_id == 7631252818:
        logger.info(f"Function check_subscription_for_type: Special admin 7631252818 detected, skipping subscription check")
        return True

    logger.info(f"Checking LOCAL subscription for type '{check_type}', user {user_id} (Premium: {is_premium})")

    channels_to_check: List[Channel] = []
    channels_to_pokaz: List[Channel] = []

    try:
        if check_type == 'start':
            channels_to_check = await db.get_filtered_start_channels(session, is_premium=is_premium)
            channels_to_pokaz = await db.get_filtered_start_channels_all(session, is_premium=is_premium)
        elif check_type == 'withdraw':
            channels_to_check = await db.get_filtered_withdraw_channels(session, is_premium=is_premium)
            channels_to_pokaz = await db.get_filtered_withdraw_channels_all(session, is_premium=is_premium)
        else:
            logger.error(f"Unknown check_type '{check_type}' requested.")
            return False
    except Exception as e_db:
        logger.error(f"Database error fetching channels for check_type '{check_type}', user {user_id}: {e_db}", exc_info=True)
        error_text = "Ошибка при получении списка каналов для проверки. Попробуйте позже."
        try:
            if is_callback: await message_or_callback.answer(error_text, show_alert=True)
            else: await message.answer(error_text)
        except Exception: pass
        return False

    failed_local_channels: List[Channel] = []
    if channels_to_check:
        logger.debug(f"Checking {len(channels_to_check)} local channels for user {user_id} via Telegram API.")
        tasks = [check_member_with_delay(bot, channel.channel_id, user_id) for channel in channels_to_check]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            channel = channels_to_check[i]
            if isinstance(result, Exception) or not result:
                if isinstance(result, Exception):
                    logger.warning(f"Exception checking membership for user {user_id} in {channel.channel_id}: {result}")
                else:
                    logger.info(f"User {user_id} failed check for local channel {channel.channel_id}")
                failed_local_channels.append(channel)

    else:
        logger.debug(f"No local channels to check via Telegram API for user {user_id} and type '{check_type}'.")

    local_check_failed = bool(failed_local_channels)

    if local_check_failed:
        logger.warning(f"User {user_id} failed LOCAL check for type '{check_type}'.")
        items_to_show = channels_to_pokaz
        if not items_to_show:
             logger.error(f"Local check failed for user {user_id}, but no local channels to show.")
             error_text = "Не удалось подтвердить подписку на каналы. Попробуйте позже."
             try:
                 if is_callback: await message_or_callback.answer(error_text, show_alert=True)
                 else: await message.answer(error_text)
             except Exception: pass
             return False

        caption = f'''⏳ Пожалуйста, подпишитесь на каналы спонсоров:'''
        try:
            reply_markup = await kb.get_channels_keyboard(items_to_show, check_type)
        except Exception as e_kb:
            logger.error(f"Error generating local channels keyboard for user {user_id}: {e_kb}")
            error_text = "Ошибка при формировании списка каналов."
            try:
                if is_callback: await message_or_callback.answer(error_text, show_alert=True)
                else: await message.answer(error_text)
            except Exception: pass
            return False

        try:
            if is_callback and callback_data == recheck_callback_data:
                await message.edit_text(caption, reply_markup=reply_markup, disable_web_page_preview=True,)
                await message_or_callback.answer("Пожалуйста, подпишитесь на каналы в списке.", show_alert=True)
            else:
                if is_callback:
                    try: await message.delete()
                    except Exception: pass
                await message.answer(text=caption, reply_markup=reply_markup)
                if is_callback: await message_or_callback.answer()
        except Exception as e:
            logger.error(f"Failed to send/edit LOCAL channels message for user {user_id}: {e}")

        return False

    logger.info(f"User {user_id} PASSED LOCAL check for type '{check_type}'.")

    if is_callback and callback_data == recheck_callback_data:
        try:
            await message_or_callback.answer("✅ Спасибо за подписку!", show_alert=False)
            await message.delete()
            logger.info(f"Deleted subscription prompt message for user {user_id} after successful LOCAL check.")
        except Exception as e:
            logger.warning(f"Could not answer callback or delete message for user {user_id} on successful LOCAL check: {e}")
    elif is_callback:
         try:
             await message_or_callback.answer()
         except Exception: pass

    return True

async def check_two_stage_subscription(
    event: Union[Message, CallbackQuery], # Принимаем Message или CallbackQuery
    bot: Bot,
    session: AsyncSession,
    state: FSMContext
) -> bool:
    """
    Выполняет двухэтапную проверку подписки на каналы типа 'start'.
    Возвращает True, если все проверки пройдены, иначе False.
    При неудаче вызывает show_subscription_channels для показа каналов и установки состояния.
    """
    user = event.from_user
    user_id = user.id
    is_premium = user.is_premium or False
    
    # Проверяем, является ли пользователь администратором
    from bot.core.config import config
    if user_id in config.admin_ids:
        logger.info(f"Function check: Admin user {user_id} detected, skipping subscription check")
        return True
    
    # Дополнительная проверка для конкретного администратора 7631252818
    if user_id == 7631252818:
        logger.info(f"Function check: Special admin 7631252818 detected, skipping subscription check")
        return True
    
    logger.info(f"Performing 2-stage subscription check via function for user {user_id}")

    # --- ЭТАП 1: только локальные каналы ---
    logger.debug(f"Function check: Fetching STAGE 1 channels for user {user_id}.")
    channels_stage1_check = await db.get_filtered_start_channels(session, is_premium=is_premium)
    logger.debug(f"Function check: Found {len(channels_stage1_check)} channels to check for STAGE 1.")

    all_subscribed_stage1 = True
    failed_channels_stage1 = []

    if channels_stage1_check:
        logger.debug(f"Function check: Entering STAGE 1 check loop for user {user_id}.")
        tasks_stage1 = [check_member_with_delay(bot, channel.channel_id, user_id) for channel in channels_stage1_check]
        results_stage1 = await asyncio.gather(*tasks_stage1, return_exceptions=True)

        for i, result in enumerate(results_stage1):
            channel = channels_stage1_check[i]
            if isinstance(result, Exception) or not result:
                all_subscribed_stage1 = False
                failed_channels_stage1.append(channel)
                logger.info(f"Function check: User {user_id} FAILED check for STAGE 1 channel {channel.channel_id}.")
    else:
        logger.debug(f"Function check: No channels to check for STAGE 1 for user {user_id}. Skipping loop.")

    if not all_subscribed_stage1:
        logger.info(f"Function check: User {user_id} failed Stage 1. Showing channels.")
        await show_subscription_channels(event, state, bot, session, stage=1, failed_channels=failed_channels_stage1)
        return False

    logger.info(f"Function check: User {user_id} PASSED STAGE 1. Proceeding.")

    # --- ЭТАП 2: локальные каналы + SubGram ---
    logger.debug(f"Function check: Fetching STAGE 2 channels for user {user_id}.")
    channels_stage2_check = await db.get_filtered_second_stage_channels(session, is_premium=is_premium)
    logger.debug(f"Function check: Found {len(channels_stage2_check)} channels to check for STAGE 2.")

    all_subscribed_stage2 = True
    failed_channels_stage2 = []
    sg_sponsors_stage2 = []

    if channels_stage2_check:
        logger.debug(f"Function check: Entering STAGE 2 check loop for user {user_id}.")
        tasks_stage2 = [check_member_with_delay(bot, channel.channel_id, user_id) for channel in channels_stage2_check]
        results_stage2 = await asyncio.gather(*tasks_stage2, return_exceptions=True)

        for i, result in enumerate(results_stage2):
            channel = channels_stage2_check[i]
            if isinstance(result, Exception) or not result:
                all_subscribed_stage2 = False
                failed_channels_stage2.append(channel)
                logger.info(f"Function check: User {user_id} FAILED check for STAGE 2 channel {channel.channel_id}.")
    else:
        logger.debug(f"Function check: No channels to check for STAGE 2 for user {user_id}. Skipping loop.")

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
            logger.info(f"Function check: User {user_id} has {len(sg_sponsors_stage2)} SubGram sponsors in Stage 2.")
        else:
            logger.info(f"Function check: User {user_id} SubGram status={sg_status}, no sponsors to show.")
    except Exception as e:
        logger.error(f"Function check: SubGram error for user {user_id}: {e}", exc_info=True)

    if not all_subscribed_stage2 or sg_sponsors_stage2:
        logger.info(f"Function check: User {user_id} failed Stage 2 (local={not all_subscribed_stage2}, subgram={len(sg_sponsors_stage2)}).")
        await show_subscription_channels(event, state, bot, session, stage=2, failed_channels=failed_channels_stage2, subgram_sponsors=sg_sponsors_stage2)
        return False

    logger.info(f"Function check: User {user_id} PASSED STAGE 2 (local + SubGram). All checks complete.")
    return True 

@router.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject, bot: Bot, session: AsyncSession, config: Config, state: FSMContext):
    ref_id: Optional[int] = None
    ref_username: Optional[str] = None
    individual_link_identifier: Optional[str] = None
    individual_link_id: Optional[int] = None
    requested_identifier: Optional[str] = None # <-- Добавляем переменную для хранения исходного запроса
    show_info_mode = False

    if command.args:
        args = command.args
        if args == "inline_share":
            # Если пользователь вернулся из inline режима
            await message.answer(
                "Вы можете продолжить делиться своей реферальной ссылкой или вернуться в главное меню",
                reply_markup=kb.get_main_keyboard(user_id, config.admin_ids)
            )
            return
        elif args.startswith("INFO_"):
            show_info_mode = True
            individual_link_identifier = args[len("INFO_"):]
            requested_identifier = individual_link_identifier # <-- Сохраняем исходный запрос
            logger.info(f"User {message.from_user.id} requested info for link: {individual_link_identifier}")
        elif re.match(r'^\d{7,}$', args):
            potential_ref_id = int(args)
            if potential_ref_id != message.from_user.id:
                ref_user = await db.get_user(session, potential_ref_id)
                if ref_user:
                    ref_id = potential_ref_id
                    ref_username = ref_user.username
                    logger.info(f"User {message.from_user.id} came from referral: {ref_id}")
                else:
                    logger.warning(f"User {message.from_user.id} provided non-existent referral ID: {potential_ref_id}")
        else:
            # Если не число и не INFO_, считаем идентификатором инд. ссылки
            individual_link_identifier = args
            requested_identifier = individual_link_identifier # <-- Сохраняем исходный запрос (на всякий случай)
            logger.info(f"User {message.from_user.id} came from potential individual link: {individual_link_identifier}")

    # --- Поиск ID индивидуальной ссылки, если нужно --- 
    link_object: Optional[IndividualLink] = None
    if individual_link_identifier:
        link_object = await db.get_individual_link_by_identifier(session, individual_link_identifier)
        if link_object:
            individual_link_id = link_object.id
        else:
            logger.warning(f"Individual link identifier '{requested_identifier}' not found in DB.") # Используем requested_identifier в логе
            individual_link_identifier = None # Сбрасываем, т.к. ссылка не найдена

    # --- Логика показа INFO или регистрации ---
    user_id = message.from_user.id
    username = message.from_user.username

    if show_info_mode:
        if link_object:
            total_reg, passed_op = await db.get_individual_link_stats(session, link_object.id)
            # INFO-режим: показываем урезанную статистику, исходные данные в БД не меняем.
            display_total_reg = int(total_reg * 0.85)
            op_div_3 = passed_op // 3 if passed_op >= 3 else 0

            info_text = (
                f"ℹ️ <b>Статистика по ссылке:</b> <code>{html.escape(link_object.identifier)}</code>\n\n"
                f"📝 Описание: {html.escape(link_object.description or '-')}\n"
                f"👥 Зарегистрировалось: {display_total_reg}\n"
                f"✅ Прошли ОП: {op_div_3}\n"
            )
            # Отправляем статистику админу (или куда нужно)
            try:
                # Уведомляем пользователя, что запрос обработан
                await message.answer(f"Запрошена информация по ссылке \"{html.escape(link_object.identifier)}\".")
                # Отправляем статистику в лог-канал
                await message.answer(info_text)
                logger.info(f"Sent stats for link '{link_object.identifier}' requested by {user_id}")
            except Exception as e:
                logger.error(f"Failed to send stats for link '{link_object.identifier}': {e}")
        else:
            # Используем requested_identifier вместо individual_link_identifier
            await message.answer(f"❌ Ссылка с идентификатором <code>{html.escape(requested_identifier)}</code> не найдена.")
        # После показа INFO дальнейшая регистрация/приветствие не нужны
        return

     # --- Интеграция 'Показов' ---
    active_show = await db.get_active_show(session)
    if active_show:
        try:
            keyboard = active_show.get_keyboard()
            if active_show.photo_file_id:
                await bot.send_photo(
                    chat_id=user_id,
                    photo=active_show.photo_file_id,
                    caption=active_show.text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=active_show.text,
                    reply_markup=keyboard,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
        except Exception as e:
            logger.error(f"Ошибка при отправке 'показа' пользователю {user_id}: {e}")
    # -----------------------------

    # --- Стандартная регистрация / приветствие ---
    photo = FSInputFile('bot/assets/images/welcome.jpg')
    user = await db.get_user(session, user_id)
    is_new_user = not user

    if is_new_user:
        logger.info(f"Registering new user {user_id} (@{username})")
        user = await db.add_user(
            session,
            user_id=user_id,
            username=username,
            refferal_id=ref_id, # Передаем ref_id
            individual_link_id=individual_link_id # Передаем ID инд. ссылки
        )
        # --- Логирование нового пользователя --- 
        source_info = "Без источника"
        if ref_id:
            # Доп. проверка на существование реферера для лога
            ref_user_log = await db.get_user(session, ref_id)
            ref_username_log = ref_user_log.username if ref_user_log else f"id:{ref_id}(NotFound)"
            source_info = f"Реферер: [{ref_id}] (@{ref_username_log})"
        elif link_object:
             source_info = f"Инд. ссылка: [{link_object.identifier}] ({link_object.id})"
        
        try:
            await bot.send_message(
                config.logs_id,
                f"🎉 <b>Новый пользователь в боте!</b>\n\n"
                f"📌 ID: <code>{user_id}</code>\n"
                f"👤 Пользователь: @{username}\n\n"
                f"🔗 Источник: {source_info}"
            )
        except Exception as log_err:
            logger.error(f"Failed to send new user log: {log_err}")
        # ---------------------------------------

    # --- Проверка подписки типа 'start' ---
    # Явно сообщаем пользователю, что идет проверка, чтобы не было "тишины" на первом /start.
    checking_msg = await message.answer("⏳ Проверяю подписки...")
    if not await check_two_stage_subscription(message, bot, session, state):
        # Если проверка не пройдена, функция check_two_stage_subscription
        # УЖЕ отправила сообщение и установила состояние. Просто выходим.
        try:
            await checking_msg.delete()
        except Exception:
            pass
        logger.info(f"User {user_id} failed 2-stage subscription check in cmd_start. Exiting.")
        return
    try:
        await checking_msg.delete()
    except Exception:
        pass
    # -----------------------------------------

    # --- Если подписка пройдена (или не требовалась), продолжаем ---
    logger.info(f"User {user_id} passed start subscription check.")


    bot_info = await bot.get_me()
    referral_link = f"https://t.me/{bot_info.username}?start={user_id}"
        # Текст для кнопки "Поделиться"
    share_text = f"Привет! Я нашёл отличного бота для заработка подарков! Присоединяйся: {referral_link}"
    refferals_reward = await db.get_reward(session)
    welcome_caption_one = f'<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>'

    welcome_caption_two = f'''

<b><tg-emoji emoji-id="5334607672375254726">👋</tg-emoji> Добро пожаловать!</b>

В нашем боте можно бесплатно зарабатывать звёзды!

• <tg-emoji emoji-id="5310278924616356636">🎯</tg-emoji> Выполняй задания
• <tg-emoji emoji-id="5372926953978341366">👥</tg-emoji> Приглашай друзей по ссылке и получай по {refferals_reward}<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>  за каждого, просто отправь её другу: <b><code>{referral_link}</code></b>

Как только заработаешь минимум 15<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>, выводи их в разделе «<tg-emoji emoji-id="5199749070830197566">🎁</tg-emoji> Вывести звёзды», мы отправим тебе подарок за выбранное количество звёзд, удачи!
'''

    # --- Установка флага ref_bonus и логика рефералов (только после успешной проверки 'start') --- 
    # Переносим логику из check_subscribe_callback сюда
    if user and user.ref_bonus is False:
        logger.info(f"Attempting to set ref_bonus=True for user {user_id} (is_new={is_new_user}) after START CHECK.")
        bonus_set_prepared = await db.set_user_ref_bonus_passed(session, user_id)
        
        ref_id_bonus = user.refferal_id
        reward_info_bonus = ""
        ref_notification_needed_bonus = False
        reward_bonus = None
        commit_needed_bonus = False
        
        if bonus_set_prepared:
            commit_needed_bonus = True # Коммит нужен как минимум для флага ref_bonus
            logger.info(f"Prepared ref_bonus=True update for user {user_id}.")
            
            if ref_id_bonus:
                reward_bonus = await db.get_reward(session)
                if reward_bonus and reward_bonus > 0:
                    try:
                        await db.add_balance(session, ref_id_bonus, reward_bonus)
                        logger.info(f"Prepared adding {reward_bonus} stars to referrer {ref_id_bonus} for user {user_id}.")
                        referral_counts_incremented_bonus = await db.increment_referral_counts(session, ref_id_bonus)
                        if referral_counts_incremented_bonus:
                            logger.info(f"Prepared incrementing referral counts for referrer {ref_id_bonus}.")
                        else:
                            logger.warning(f"Failed to prepare incrementing referral counts for referrer {ref_id_bonus}.")
                        reward_info_bonus = f"🌟 Рефереру подготовлено: {reward_bonus}⭐️"
                        ref_notification_needed_bonus = True
                    except Exception as e_bonus:
                        logger.error(f"Failed to prepare add_balance for referrer {ref_id_bonus}: {e_bonus}", exc_info=True)
                        reward_info_bonus = "⚠️ Ошибка подготовки начисления награды рефереру."
                        commit_needed_bonus = False # Не коммитим если тут ошибка
                        ref_notification_needed_bonus = False
                else:
                    reward_info_bonus = f"ℹ️ Награда рефереру не начислена (сумма награды: {reward_bonus})."
            
            # --- Коммит для флага и награды рефереру --- 
            if commit_needed_bonus:
                try:
                    await session.commit()
                    logger.info(f"Successfully committed ref_bonus=True and potentially ref reward for user {user_id}.")
                    # --- Обновляем reward_info после коммита для лога --- 
                    if ref_notification_needed_bonus and ref_id_bonus:
                        updated_ref_data_bonus = await db.get_user(session, ref_id_bonus) 
                        updated_balance_bonus = updated_ref_data_bonus.balance if updated_ref_data_bonus else "N/A"
                        reward_info_bonus += f" (Новый баланс реферера: {updated_balance_bonus}⭐️)" 
                except Exception as e_commit_bonus:
                    logger.error(f"Failed to commit ref_bonus/reward changes for user {user_id}: {e_commit_bonus}", exc_info=True)
                    await session.rollback()
                    reward_info_bonus += " ⚠️ Ошибка сохранения изменений в БД."
                    ref_notification_needed_bonus = False
        else:
            logger.warning(f"Failed to prepare ref_bonus=True for user {user_id} after start check.")

        # --- Отправка лога о прохождении ОП Start --- 
        try:
            bonus_set_final_status_bonus = "Установлен ✅" if bonus_set_prepared and commit_needed_bonus and "Ошибка сохранения" not in reward_info_bonus else "Ошибка установки/сохранения ❌"
            # Получаем источник снова для лога ОП
            source_info_op = "Неизвестен"
            # Используем user, который уже есть
            if user:
                if user.refferal_id:
                     # Используем ref_id_bonus, если он есть
                     ref_data_op = await db.get_user(session, user.refferal_id) # Получаем реферера снова
                     ref_username_op = ref_data_op.username if ref_data_op else f"id:{user.refferal_id}"
                     source_info_op = f"Реферер: [{user.refferal_id}] (@{ref_username_op})"
                elif user.individual_link_id:
                     # Используем link_object, если он был найден ранее
                     link_identifier_op = link_object.identifier if link_object else "UNKNOWN_LINK"
                     source_info_op = f"Инд. ссылка: [{link_identifier_op}] ({user.individual_link_id})"
                else:
                     source_info_op = "Без источника"

            log_message_op = (
                f"🏁 <b>Пользователь прошел проверку ОП (Start)!</b>\n\n"
                f"📌 ID: <code>{user_id}</code>\n"
                f"👤 Пользователь: @{username}\n\n"
                f"🔗 Источник: {source_info_op}\n"
                f"🚩 Статус флага ref_bonus: {bonus_set_final_status_bonus}\n"
            )

            if reward_info_bonus:
                log_message_op += f"\n{reward_info_bonus}"
            await bot.send_message(config.logs_id, log_message_op)
            await message.answer(
                    text=welcome_caption_one,
                    reply_markup=kb.start_stars_keyboard(share_text),
                    parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
                )

            await message.answer_photo(
            photo=FSInputFile("bot/assets/images/menu.jpg"),
            caption=welcome_caption_two,
            reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
            parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
        )
        except Exception as e_log_op:
            logger.error(f"Error sending OP Start log for user {user_id}: {e_log_op}")
            
        # --- Отправка уведомления рефереру ---
        # Используем reward_bonus вместо reward_recheck
        if ref_notification_needed_bonus and ref_id_bonus and reward_bonus and reward_bonus > 0:
             try:
                 await bot.send_message(
                     ref_id_bonus,
                     "🎉 <b>У вас новый реферал!</b>\n\n"
                     f"💫 Пользователь: @{username}\n\n"
                     # Используем reward_bonus вместо reward_recheck
                     f"- На ваш баланс была начислена награда: {reward_bonus}⭐️"
                 )
                 logger.info(f"Sent OP Start notification to referrer {ref_id_bonus}")
             except Exception as e_notify_bonus:
                 logger.error(f"Failed to send OP Start notification to referrer {ref_id_bonus}: {e_notify_bonus}")
    # --- Конец логики ref_bonus --- 

    # --- Отправляем приветственное сообщение (если новый) или главное меню --- 
    if not is_new_user:
        # Если пользователь не новый, но только что прошел проверку ОП
        if user and user.ref_bonus is False and 'bonus_set_final_status_bonus' in locals() and bonus_set_final_status_bonus == "Установлен ✅":
            await message.answer(
            text=welcome_caption_one,
            reply_markup=kb.start_stars_keyboard(share_text),
            parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
        )

            await message.answer_photo(
            photo=FSInputFile("bot/assets/images/menu.jpg"),
            caption=welcome_caption_two,
            reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
            parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
        )
        # Если пользователь не новый и проверка ОП уже была пройдена ранее
        elif user and user.ref_bonus is True:
            await message.answer(
                    text=welcome_caption_one,
                    reply_markup=kb.start_stars_keyboard(share_text),
                    parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
                )

            await message.answer_photo(
            photo=FSInputFile("bot/assets/images/menu.jpg"),
            caption=welcome_caption_two,
            reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
            parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
        )
        # Дополнительный else на случай, если что-то пошло не так (например, ошибка установки флага)
        else:
            await message.answer(
            text=welcome_caption_one,
            reply_markup=kb.start_stars_keyboard(share_text),
            parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
        )

            await message.answer_photo(
            photo=FSInputFile("bot/assets/images/menu.jpg"),
            caption=welcome_caption_two,
            reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
            parse_mode="HTML" # Указываем parse_mode для обработки HTML тегов
        )

    await message.answer("💡Также можно <b>получать Звёзды</b> за <b>простые задания!</b>", reply_markup=kb.earn_stars_task_keyboard())

@router.message(Command("ban"))
async def ban_user(message: Message, command: CommandObject, session: AsyncSession, bot: Bot, config: Config):
    """Банит пользователя. Только для админов."""
    if message.from_user.id not in config.admin_ids:
        return await message.answer("Эта команда только для администраторов.")

    if not command.args:
        return await message.answer("Пожалуйста, укажите ID пользователя.\nИспользование: /ban <code>user_id</code>", parse_mode="HTML")

    try:
        user_id_to_ban = int(command.args.strip())
    except ValueError:
        return await message.answer("ID пользователя должен быть числом.")

    user_to_ban = await db.get_user(session, user_id_to_ban)
    if not user_to_ban:
        return await message.answer(f"Пользователь с ID {user_id_to_ban} не найден.")
    
    if user_to_ban.banned:
        return await message.answer(f"Пользователь @{user_to_ban.username} (ID: {user_id_to_ban}) уже забанен.")

    success = await db.set_user_ban_status(session, user_id_to_ban, True)
    if success:
        await message.answer(f"✅ Пользователь @{user_to_ban.username} (ID: {user_id_to_ban}) был забанен.")
        try:
            await bot.send_message(user_id_to_ban, "Вы были заблокированы в этом боте.")
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id_to_ban} о бане: {e}")
    else:
        await message.answer(f"❌ Не удалось забанить пользователя с ID {user_id_to_ban}.")

@router.message(F.text == "Купить дешево звезды")
async def start_withdraw(message: Message):
    await message.answer('''⭐️ В нашем боте ты <b>можешь бесплатно заработать звезды</b>, но если тебе лень, можешь их <b>купить у нас</b> <b>дешевле чем в Телеграме</b>!

👉 <a href="https://t.me/starsov?start=r7822690557">@stars</a>''', parse_mode="HTML")

@router.message(F.text == "Поддержка")
async def start_withdraw(message: Message):
    await message.answer("<b>Если у вас возникли трудности, вы можете обратиться к нашей поддержке</b>", parse_mode="HTML", reply_markup=kb.start_support_keyboard_reply())

# --- Обработка Промокодов ---

# Обработчик кнопки "Промокод"
@router.message(F.text == "Промокод", StateFilter(None))
async def promo_code_start(message: Message, state: FSMContext, bot: Bot, session: AsyncSession): # Добавляем bot и session
    user_id = message.from_user.id # Получаем user_id
    # --- Добавляем проверку подписки типа 'start' ---
    if not await check_subscription_for_type(message, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before activating promo code.")
        return
    # -----------------------------------------------
    await message.answer_photo(photo=FSInputFile("bot/assets/images/promo.jpg"), caption="Введите промокод:", reply_markup=kb.cancel_state_keyboard_reply()) # Используем клавиатуру отмены
    await state.set_state(st.PromoCodeState.waiting_for_code)

@router.callback_query(F.data == "promo_code_start", StateFilter(None))
async def promo_code_start(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession): # Добавляем bot и session
    user_id = callback.from_user.id # Получаем user_id
    # --- Добавляем проверку подписки типа 'start' ---
    if not await check_subscription_for_type(callback.message, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before activating promo code.")
        return
    # -----------------------------------------------
    await callback.message.answer_photo(photo=FSInputFile("bot/assets/images/promo.jpg"), caption="Введите промокод:", reply_markup=kb.cancel_state_keyboard_reply()) # Используем клавиатуру отмены
    await state.set_state(st.PromoCodeState.waiting_for_code)


# Обработчик введенного промокода
@router.message(st.PromoCodeState.waiting_for_code)
async def process_promo_code(message: Message, state: FSMContext, session: AsyncSession, bot: Bot, config: Config):
    code = message.text.strip()
    user_id = message.from_user.id

    # admin_ids = Config.admin_ids # Эта строка не используется, можно убрать

    # Ищем активный промокод
    promocode = await db.get_promocode_by_code(session, code)

    # --- Обработка кнопки "Отменить" ---
    if message.text == "❌ Отменить":
        # ... (ваш код для отмены остается без изменений) ...
        bot_info = await bot.get_me()
        referral_link = f"https://t.me/{bot_info.username}?start={message.from_user.id}"
        share_text = f"Привет! Я нашёл отличного бота для заработка подарков! Присоединяйся: {referral_link}"
        refferals_reward = await db.get_reward(session)
        welcome_caption_one = f'⭐️'
        welcome_caption_two = f'''

<b>👋 Добро пожаловать!</b>

В нашем боте можно бесплатно зарабатывать звёзды!

• 🎯 Выполняй задания
• 👥 Приглашай друзей по ссылке и получай по {refferals_reward}⭐️  за каждого, просто отправь её другу: <b><code>{referral_link}</code></b>

Как только заработаешь минимум 15⭐️, выводи их в разделе «🎁 Вывести звёзды», мы отправим тебе подарок за выбранное количество звёзд, удачи!
'''
        await message.answer(
                text=welcome_caption_one,
                reply_markup=kb.start_stars_keyboard(share_text),
                parse_mode="HTML"
            )
        await message.answer_photo(
                photo=FSInputFile("bot/assets/images/menu.jpg"),
                caption=welcome_caption_two,
                reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
                parse_mode="HTML"
            )
        await message.answer("💡Также можно <b>получать Звёзды</b> за <b>простые задания!</b>", reply_markup=kb.earn_stars_task_keyboard())
        await state.clear()
        return
    # --- Конец обработки "Отменить" ---

    # --- Проверка, найден ли промокод ---
    if not promocode or not promocode.is_active: # Добавил проверку is_active на всякий случай
        await message.answer("❌ Промокод не найден или недействителен.", reply_markup=kb.get_main_keyboard(user_id, config.admin_ids))
        await state.clear()
        return
    # --- Конец проверки промокода ---

    # --- Пытаемся активировать промокод ---
    success, activation_message = await db.activate_promocode(session, user_id, promocode)

    # --- Обработка результата активации ---
    reply_markup = kb.get_main_keyboard(user_id, config.admin_ids) # Клавиатура по умолчанию - основное меню

    if not success:
        # Если активация НЕ удалась, проверяем причину
        if "нужно пригласить еще" in activation_message:
            # Если причина - нехватка рефералов, ставим инлайн-кнопку
            reply_markup = kb.error_promo_keyboard()
        # Для других ошибок (уже использован и т.д.) останется основное меню

    prove_message = await message.answer('🕓 Проверка промокода...', reply_markup=ReplyKeyboardRemove())
    await asyncio.sleep(0.5)
    await prove_message.delete()
    await message.answer(activation_message, reply_markup=reply_markup)

    # Очищаем состояние после попытки активации (успешной или нет)
    await state.clear()

# --- Конец обработки Промокодов ---

# --- Вспомогательная функция для отправки задания --- (Обновленная)
async def send_task_message(chat_id: int, user_id: int, is_premium: bool, session: AsyncSession, bot: Bot, state: FSMContext, first_name: str, language_code: str):
    await state.clear()
    logger.debug(f"send_task_message called for user {user_id} (Premium: {is_premium})")

    # --- СНАЧАЛА ПРОБУЕМ TRAFFY ---
    try:
        traffy_tasks = await get_traffy_tasks(TRAFFY_RESOURCE_ID, user_id, max_tasks=10)
        logger.info(f"[Traffy] tasks для пользователя {user_id}: {traffy_tasks}")
        if traffy_tasks:
            task = traffy_tasks[0]
            await state.set_state(TraffyTaskState.waiting_for_task)
            await state.update_data(traffy_task_id=task["id"], traffy_task_link=task["link"], traffy_task_title=task["title"], traffy_task_image=task.get("image_url"))
            task_details_message = f"""🎯 <b>Доступно задание!</b>\n\n<b>{task['title']}</b>\n\n📌 <a href='{task['link']}'>Перейти к заданию</a>\n\n<b>🏆 Награда: 0.25⭐️</b>"""
            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="🔗 Перейти", url=task["link"])
            keyboard.button(text="✅ Я выполнил", callback_data="traffy_task_complete")
            keyboard.button(text="⏩ Пропустить", callback_data="traffy_task_skip")
            keyboard.adjust(1)
            markup = keyboard.as_markup()
            await bot.send_message(chat_id, text=task_details_message, reply_markup=markup, parse_mode="HTML")
            return
    except Exception as e:
        logger.error(f"Error fetching tasks from Traffy for user {user_id}: {e}", exc_info=True)

    # Дальше идёт старая логика: локальные, SubGram, Flyer
    user_stmt = select(User).where(User.user_id == user_id).options(selectinload(User.completed_tasks))
    user_result = await session.execute(user_stmt)
    user = user_result.scalar_one_or_none()
    
    # Явно загружаем completed_tasks для избежания проблем с lazy loading
    if user:
        await session.refresh(user, ['completed_tasks'])

    if not user:
        logger.error(f"Could not get user {user_id} in send_task_message.")
        try:
            await bot.send_message(chat_id, "Произошла ошибка при загрузке задания.")
        except Exception as e:
            logger.error(f"Failed to send error message to chat {chat_id}: {e}")
        return

    logger.debug(f"User {user_id} found: {user}")

    task_to_show = None
    previous_task_id_for_search = None

    if user.current_task_id:
        logger.debug(f"User {user_id} has current task ID: {user.current_task_id}")
        potential_task = await db.get_task_by_id(session, user.current_task_id)
        if potential_task and potential_task.is_active and potential_task.id not in {t.id for t in user.completed_tasks}:
            req = potential_task.premium_requirement
            if (is_premium and req in ('all', 'premium_only')) or (not is_premium and req in ('all', 'non_premium_only')):
                # Дополнительная проверка лимитов для сохраненного задания
                can_complete, reason = await db.check_task_limits(session, potential_task)
                if can_complete:
                    task_to_show = potential_task
                    logger.debug(f"User {user_id} continues with saved task {user.current_task_id} (Premium OK, limits OK)")
                else:
                    logger.debug(f"User {user_id} saved task {user.current_task_id} limits exceeded: {reason}")
                    previous_task_id_for_search = user.current_task_id
            else:
                previous_task_id_for_search = user.current_task_id
        else:
            previous_task_id_for_search = user.current_task_id

    if not task_to_show:
        logger.debug(f"Searching next available task for user {user_id} (Premium: {is_premium}) after task ID: {previous_task_id_for_search}")
        task_to_show = await db.get_next_available_task(session, user_id, is_premium, previous_task_id_for_search)
        next_task_id = task_to_show.id if task_to_show else None
        logger.debug(f"Next task ID for user {user_id}: {next_task_id}")

        if user.current_task_id != next_task_id:
            logger.debug(f"Updating user {user_id}'s current_task_id from {user.current_task_id} to {next_task_id}")
            await db.update_user_current_task(session, user_id, next_task_id)
            # УДАЛЯЕМ COMMIT: Управление транзакцией должно быть на уровне middleware/обработчика
            # try:
            #     await session.commit()
            #     logger.debug(f"Committed new current_task_id {next_task_id} for user {user_id}")
            # except Exception as e:
            #     logger.error(f"Failed to commit new current_task_id for user {user_id}: {e}")
            #     await session.rollback()
            #     try:
            #         await bot.send_message(chat_id, "Произошла ошибка при обновлении вашего прогресса.")
            #     except Exception as send_err:
            #         logger.error(f"Failed to send progress update error message: {send_err}")

    if task_to_show:
        # Дополнительная проверка: убеждаемся, что задание не выполнено пользователем
        if task_to_show.id in {t.id for t in user.completed_tasks}:
            logger.warning(f"Task {task_to_show.id} is already completed by user {user_id}, looking for another task")
            task_to_show = await db.get_next_available_task(session, user_id, is_premium, task_to_show.id)
            if not task_to_show:
                logger.debug(f"No alternative tasks found for user {user_id}")
                await bot.send_message(chat_id, "К сожалению, сейчас нет доступных заданий. Попробуйте позже.")
                return
        
        # Дополнительная проверка лимитов перед показом задания
        can_complete, reason = await db.check_task_limits(session, task_to_show)
        if not can_complete:
            logger.warning(f"Task {task_to_show.id} limits exceeded for user {user_id}: {reason}")
            # Ищем другое задание
            task_to_show = await db.get_next_available_task(session, user_id, is_premium, task_to_show.id)
            if not task_to_show:
                logger.debug(f"No alternative tasks found for user {user_id}")
                await bot.send_message(chat_id, "К сожалению, сейчас нет доступных заданий. Попробуйте позже.")
                return
        
        logger.debug(f"Sending task {task_to_show.id} to user {user_id}")
        task_details_message = f"""🎯 <b>Доступно задание №{task_to_show.id}!</b>\n\n{task_to_show.description}"""
        if task_to_show.instruction_link:
            task_details_message += f"\nИнструкция если возникли сложности -> {task_to_show.instruction_link}"
        task_details_message += f"\n\n🏆 Награда: {task_to_show.reward:.2f}⭐️"
        
        keyboard = InlineKeyboardBuilder()
        if task_to_show.action_link:
            keyboard.button(text="🔗 Перейти в канал", url=task_to_show.action_link)
        keyboard.button(text="✅ Я выполнил", callback_data=f"local_task_complete_{task_to_show.id}")
        keyboard.button(text="⏩ Пропустить", callback_data=f"local_task_skip_{task_to_show.id}")
        keyboard.adjust(1)
        markup = keyboard.as_markup()

        await bot.send_photo(chat_id, caption=task_details_message, photo=FSInputFile("bot/assets/images/quests.jpg"), reply_markup=markup)
    else:
        logger.debug(f"No available tasks found for user {user_id} (Premium: {is_premium}). Looking for SubGram or Flyer tasks.")
        try:
            sg_status, sg_code, sg_links = await request_op(
                user_id=user_id,
                chat_id=chat_id,
                first_name=first_name,
                language_code=language_code,
                is_premium=is_premium,
                action="newtask"
            )
            logger.info(f"SubGram response: status={sg_status}, code={sg_code}, links_count={len(sg_links)}")
            if sg_status in ["success", "warning"] and sg_links:
                await state.update_data(sg_links=sg_links, current_index=0)
                await show_subgram_channel(chat_id, sg_links, 0, bot)
                return
            else:
                # Если SubGram не дал заданий, пробуем получить задания Flyer
                try:
                    flyer = Flyer(FLYER_API_KEY)
                    flyer_tasks = await flyer.get_tasks(
                        user_id=user_id,
                        language_code=language_code,
                        limit=5
                    )
                    logger.info(f"FlyerAPI полный ответ для пользователя {user_id}: {flyer_tasks}")
                    logger.info(f"FlyerAPI количество заданий: {len(flyer_tasks) if flyer_tasks else 0}")
                    if flyer_tasks and len(flyer_tasks) > 0:
                        next_task = None
                        for task in flyer_tasks:
                            if task.get('status') == 'incomplete':
                                next_task = task
                                break
                        if next_task:
                            await state.set_state(FlyerState.task_id)
                            await state.update_data(
                                task_id=next_task.get('signature', ''),
                                signature=next_task.get('signature', '')
                            )
                            task_title = next_task.get('name', 'Задание')
                            task_action = next_task.get('task', 'Подписаться на канал')
                            if task_action == 'give boost':
                                task_details_message = f"""🎯 <b>Доступно задание!</b>\n\n📌 Перейдите по ссылке и проголосуйте за <b><a href=\"{_get_flyer_task_link(next_task) or '#'}\">канал</a></b>.\n\n<b>🏆 Награда: 0.25⭐️</b>"""
                            else:
                                task_details_message = f"""🎯 <b>Доступно задание!</b>\n\n📌 Подпишитесь на <b><a href=\"{_get_flyer_task_link(next_task) or '#'}\">канал</a></b> и не отписывайтесь в течение 7 дней.\n\n<b>🏆 Награда: 0.25⭐️</b>"""
                            keyboard = InlineKeyboardBuilder()
                            if task_action == 'give boost':
                                if _get_flyer_task_link(next_task):
                                    keyboard.button(text="🔗 Голосовать x3", url=_get_flyer_task_link(next_task))
                                keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                                keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                                keyboard.adjust(1)
                                markup = keyboard.as_markup()
                                await bot.send_photo(
                                    chat_id, 
                                    caption=task_details_message, 
                                    photo=FSInputFile("bot/assets/images/quests.jpg"), 
                                    reply_markup=markup
                                )
                                return
                            else:
                                if _get_flyer_task_link(next_task):
                                    keyboard.button(text="🔗 Подписаться", url=_get_flyer_task_link(next_task))
                                keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                                keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                                keyboard.adjust(1)
                                markup = keyboard.as_markup()
                                await bot.send_photo(
                                    chat_id, 
                                    caption=task_details_message, 
                                    photo=FSInputFile("bot/assets/images/quests.jpg"), 
                                    reply_markup=markup
                                )
                                return
                except Exception as e:
                    logger.error(f"Error fetching tasks from FlyerAPI for user {user_id}: {e}", exc_info=True)
                # --- Traffy блок ---
                try:
                    traffy_tasks = await get_traffy_tasks(TRAFFY_RESOURCE_ID, user_id, max_tasks=10)
                    logger.info(f"[Traffy] tasks для пользователя {user_id}: {traffy_tasks}")
                    if traffy_tasks:
                        task = traffy_tasks[0]
                        await state.set_state(TraffyTaskState.waiting_for_task)
                        await state.update_data(traffy_task_id=task["id"], traffy_task_link=task["link"], traffy_task_title=task["title"], traffy_task_image=task.get("image_url"))
                        task_details_message = f"""🎯 <b>Traffy задание!</b>\n\n<b>{task['title']}</b>\n\n📌 <a href='{task['link']}'>Перейти к заданию</a>\n\n<b>🏆 Награда: 0.25⭐️</b>"""
                        keyboard = InlineKeyboardBuilder()
                        keyboard.button(text="🔗 Перейти", url=task["link"])
                        keyboard.button(text="✅ Я выполнил", callback_data="traffy_task_complete")
                        keyboard.button(text="⏩ Пропустить", callback_data="traffy_task_skip")
                        keyboard.adjust(1)
                        markup = keyboard.as_markup()
                        await bot.send_message(chat_id, text=task_details_message, reply_markup=markup, parse_mode="HTML")
                        return
                except Exception as e:
                    logger.error(f"Error fetching tasks from Traffy for user {user_id}: {e}", exc_info=True)
                await bot.send_message(chat_id, "ℹ️ На данный момент доступных заданий для вас нет. Попробуйте зайти позже!", reply_markup=kb.earn_stars_task_again_keyboard())
        except Exception as e:
            logger.error(f"Error fetching task from SubGram for user {user_id}: {e}", exc_info=True)
            await bot.send_message(chat_id, "❌ Произошла ошибка при получении задания. Попробуйте позже.")


async def send_skip_task_message(chat_id: int, user_id: int, is_premium: bool, session: AsyncSession, bot: Bot, state: FSMContext):
    await state.clear()
    """Находит и отправляет текущее/следующее НЕВЫПОЛНЕННОЕ задание или сообщение о завершении,
    учитывая премиум-статус пользователя (после пропуска).
    """
    logger.debug(f"send_skip_task_message called for user {user_id} (Premium: {is_premium})")
    # --- Изменено: Загружаем пользователя сразу с completed_tasks --- 
    user_stmt = select(User).where(User.user_id == user_id).options(selectinload(User.completed_tasks))
    user_result = await session.execute(user_stmt)
    user = user_result.scalar_one_or_none()
    # -------------------------------------------------------------
    if not user:
        logger.error(f"Could not get user {user_id} in send_skip_task_message.")
        try:
            await bot.send_message(chat_id, "Произошла ошибка при загрузке задания.")
        except Exception as e:
            logger.error(f"Failed to send error message to chat {chat_id}: {e}")
        return

    task_to_show = None
    previous_task_id_for_search = user.current_task_id # Ищем после пропущенного

    logger.debug(f"Searching next available task for user {user_id} (Premium: {is_premium}) after skipping task ID: {previous_task_id_for_search}")
    # Передаем is_premium в функцию поиска
    task_to_show = await db.get_next_available_task(session, user_id, is_premium, previous_task_id_for_search)
    next_task_id = task_to_show.id if task_to_show else None

    # Обновляем current_task_id пользователя, если найденное задание отличается
    if user.current_task_id != next_task_id:
        logger.debug(f"Updating user {user_id}'s current_task_id from {user.current_task_id} to {next_task_id}")
        await db.update_user_current_task(session, user_id, next_task_id)
        try:
            await session.commit() # Коммитим изменение current_task_id
            logger.debug(f"Committed new current_task_id {next_task_id} for user {user_id} after skip.")
        except Exception as e:
            logger.error(f"Failed to commit new current_task_id after skip for user {user_id}: {e}")
            await session.rollback()
            try:
                await bot.send_message(chat_id, "Произошла ошибка при обновлении вашего прогресса после пропуска.")
            except Exception as send_err:
                logger.error(f"Failed to send progress update error message after skip: {send_err}")
            return

    # Отправка сообщения с найденным заданием или сообщением о завершении
    if task_to_show:
        # Передаем is_premium для расчета общей награды
        total_reward = await db.get_total_tasks_reward(session, is_premium)
        logger.debug(f"Sending task {task_to_show.id} to user {user_id} after skip.")
        task_details_message = f"""🎯 <b>Доступно задание №{task_to_show.id}!</b>

{task_to_show.description}"""
        if task_to_show.instruction_link:
             task_details_message += f"\nИнструкция если возникли сложности -> {task_to_show.instruction_link}"
        task_details_message += f"\n\n🏆 Награда: {task_to_show.reward:.2f}⭐️"
        await bot.send_photo(chat_id, caption=task_details_message, photo=FSInputFile("bot/assets/images/quests.jpg"), reply_markup=kb.task_keyboard(task_to_show))
    else:
        logger.debug(f"No available tasks found for user {user_id} (Premium: {is_premium}) after skip.")
        any_active_task = await db.get_first_available_task(session, is_premium)
        if any_active_task:
            # Вместо сообщения вызываем send_task_message для показа SubGram/Flyer/и т.д.
            await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
        else:
            await bot.send_message(chat_id, "ℹ️ На данный момент доступных заданий для вас нет. Попробуйте зайти позже!", reply_markup=kb.earn_stars_task_again_keyboard())

@router.callback_query(F.data == "traffy_task_complete", StateFilter(TraffyTaskState.waiting_for_task))
async def handle_traffy_task_complete(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession):
    data = await state.get_data()
    task_id = data.get("traffy_task_id")
    user_id = callback.from_user.id
    check = await check_traffy_task(TRAFFY_RESOURCE_ID, user_id, task_id)
    logger.info(f"[Traffy] check для пользователя {user_id}, task_id {task_id}: {check}")
    if check.get("is_completed"):
        await callback.answer("✅ Задание выполнено! Награда: 0.25⭐️", show_alert=True)
        # Здесь можно начислить награду через вашу систему (пример):
        await db.add_balance(session, user_id, 0.25)
        await session.commit()
        await state.clear()
        await send_task_message(callback.message.chat.id, user_id, callback.from_user.is_premium, session, bot, state, callback.from_user.first_name, callback.from_user.language_code)
    else:
        await callback.answer("❌ Задание ещё не выполнено!", show_alert=True)

@router.callback_query(F.data == "traffy_task_skip", StateFilter(TraffyTaskState.waiting_for_task))
async def handle_traffy_task_skip(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession):
    user_id = callback.from_user.id
    await state.clear()
    await callback.answer("Задание пропущено.")
    await callback.message.delete()
    await send_task_message(callback.message.chat.id, user_id, callback.from_user.is_premium, session, bot, state, callback.from_user.first_name, callback.from_user.language_code)

@router.message(F.text == "Задания")
async def show_tasks(message: Message, session: AsyncSession, bot: Bot, state: FSMContext):
    user = message.from_user
    user_id = user.id

    if not await check_subscription_for_type(message, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before showing tasks.")
        return
    
    task_selection_text = f"""<tg-emoji emoji-id="5310278924616356636">🎯</tg-emoji> <b>Выберите тип заданий:</b>

<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji> <b>Ежедневные задания</b> - выполняйте каждый день и получайте бонусы!

<tg-emoji emoji-id="5420315771991497307">🔥</tg-emoji> <b>Обычные задания</b> - разнообразные задания для заработка звезд!

<tg-emoji emoji-id="5472146462362048818">💡</tg-emoji> <i>Выберите подходящий тип заданий и начинайте зарабатывать!</i>"""
    
    await message.answer(task_selection_text, reply_markup=kb.select_type_task(), parse_mode='HTML')


@router.callback_query(F.data=='daily_task')
async def daily_task(callback: CallbackQuery, session: AsyncSession, bot: Bot, config: Config):
    user = callback.from_user
    user_id = user.id
    
    # Получаем данные пользователя из БД
    user_data = await db.get_user(session, user_id)
    if not user_data:
        await callback.answer("❗️ Пользователь не найден в базе данных.", show_alert=True)
        return
    
    # Проверяем, когда последний раз пользователь получал награду за био
    last_bio_reward = user_data.last_bio_reward_date
    now = datetime.now()
    
    # Проверяем, прошло ли 24 часа с последней награды
    can_claim_reward = True
    if last_bio_reward:
        time_diff = now - last_bio_reward
        if time_diff.total_seconds() < 24 * 3600:  # 24 часа в секундах
            hours_left = 24 - (time_diff.total_seconds() / 3600)
            can_claim_reward = False
    
    # Получаем информацию о боте и формируем реферальную ссылку
    bot_info = await bot.get_me()
    ref_link = f"t.me/{bot_info.username}?start={user_id}"
    
    if can_claim_reward:
        bio_task_text = f"""📅 <b>Ежедневное задание - Реферальная ссылка в профиле</b>

🎯 <b>Задание:</b>
Добавьте свою реферальную ссылку в описание профиля Telegram

🔗 <b>Ваша реферальная ссылка:</b>
<code>{ref_link}</code>

📋 <b>Как выполнить:</b>
1. Скопируйте ссылку выше
2. Перейдите в настройки Telegram
3. Добавьте ссылку в раздел "О себе"
4. Нажмите "Проверить"

🏆 <b>Награда:</b> 0.5⭐️
⏰ <b>Доступно:</b> раз в 24 часа

💡 <i>Привлекайте друзей и зарабатывайте больше звезд!</i>"""
    else:
        bio_task_text = f"""📅 <b>Ежедневное задание - Реферальная ссылка в профиле</b>

⏰ <b>Награда уже получена!</b>
🕐 <b>Следующая награда доступна через:</b> {int(hours_left)} ч. {int((hours_left % 1) * 60)} мин."""

    # Создаем клавиатуру
    keyboard_buttons = []
    
    if can_claim_reward:
        keyboard_buttons.append([InlineKeyboardButton(text="✅ Проверить профиль", callback_data='check_bio_link')])
    
    keyboard_buttons.extend([
        [InlineKeyboardButton(text="🔥 Обычные задания", callback_data='default_task')],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data='back_to_main')]
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.edit_text(bio_task_text, reply_markup=keyboard, parse_mode='HTML')
    await callback.answer()

@router.callback_query(F.data=='default_task')
async def default_task(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    user = callback.from_user
    user_id = user.id
    is_premium = user.is_premium # Получаем статус
    # --- Добавляем проверку подписки типа 'start' ---
    if not await check_subscription_for_type(callback, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before showing tasks.")
        return
    # -----------------------------------------------
    await callback.message.delete()
    await send_task_message(callback.message.chat.id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)

@router.callback_query(F.data.startswith("task_complete_"))
async def process_task_complete(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    task_id = int(callback.data.split("_")[2])
    user = callback.from_user
    user_id = user.id
    is_premium = user.is_premium 
    chat_id = callback.message.chat.id

    logger.info(f"Task completion attempt: user={user_id}, task_id={task_id}, premium={is_premium}")

    # Загружаем user_data из БД, чтобы проверить current_task_id
    user_data = await db.get_user(session, user_id)
    task = await db.get_task_by_id(session, task_id)
    
    logger.debug(f"User current_task_id={user_data.current_task_id if user_data else None}, task_found={task is not None}")

    # Проверяем актуальность задания и соответствие премиум-требованию
    if not user_data or not task or user_data.current_task_id != task_id:
        logger.warning(f"Task {task_id} completion rejected: not current or not found for user {user_id}")
        await callback.answer("❗️ Задание не найдено или уже неактуально.", show_alert=True)
        try: await callback.message.delete() 
        except Exception as e: logger.error(f"Failed to delete task message: {e}")
        return
    
    # Проверяем лимиты задания перед выполнением
    can_complete, reason = await db.check_task_limits(session, task)
    if not can_complete:
        logger.warning(f"Task {task_id} limits exceeded for user {user_id}: {reason}")
        await callback.answer(f"❗️ Задание недоступно: {reason}", show_alert=True)
        try: await callback.message.delete() 
        except Exception as e: logger.error(f"Failed to delete task message: {e}")
        # Показываем следующее доступное задание
        await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
        return
    
    req = task.premium_requirement
    if not ((is_premium and req in ('all', 'premium_only')) or (not is_premium and req in ('all', 'non_premium_only'))):
        await callback.answer("❗️ Это задание недоступно для вашего статуса.", show_alert=True)
        # Можно показать следующее доступное задание?
        try: await callback.message.delete() 
        except: pass
        await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code) # Показываем следующее
        return

    reward_granted = False
    next_task_needed = True
    task_completed_now = False

    # Проверка подписки для заданий с каналами
    if task.check_subscription and task.channel_id_to_check:
        is_subscribed = await check_member_with_delay(bot, task.channel_id_to_check, user_id)
        if is_subscribed:
            await db.add_balance(session, user_id, task.reward)
            reward_granted = True
            task_completed_now = True
            await callback.answer(f"✅ Задание выполнено! Награда: {task.reward:.2f}⭐️", show_alert=True)
        else:
            await callback.answer(f"❗️ Сначала подпишитесь на канал задания, затем нажмите 'Проверить подписку'.", show_alert=True)
            try:
                await callback.message.edit_reply_markup(reply_markup=kb.task_keyboard(task, verification_pending=True))
            except Exception as e:
                 logger.error(f"Не удалось отредактировать клавиатуру для проверки подписки: {e}")
            next_task_needed = False
    # Проверка SubGram заданий (ID 9999 или диапазон 30-100)
    elif task_id == 9999 or (30 <= task_id <= 100):
        logger.info(f"Verifying SubGram task {task_id} completion for user {user_id}")
        
        # Проверяем сначала локальные каналы, если есть
        local_channel_subscribed = True
        
        if task.channel_id_to_check:
            # Проверяем, является ли пользователь администратором
            from bot.core.config import config
            if user_id in config.admin_ids:
                logger.info(f"SubGram task: Admin user {user_id} detected, skipping local channel subscription check")
                local_channel_subscribed = True
            elif user_id == 7631252818:
                logger.info(f"SubGram task: Special admin 7631252818 detected, skipping local channel subscription check")
                local_channel_subscribed = True
            else:
                # Преобразуем ID канала из строки в int для API Telegram
                try:
                    channel_id_int = int(task.channel_id_to_check)
                    
                    # Проверяем подписку на локальный канал через Telegram API
                    chat_member = await bot.get_chat_member(channel_id_int, user_id)
                    if chat_member.status not in ["member", "administrator", "creator"]:
                        local_channel_subscribed = False
                        logger.debug(f"User {user_id} is not subscribed to local channel {channel_id_int}")
                        await callback.answer(f"❗️ Сначала подпишитесь на локальный канал, затем нажмите 'Я выполнил'.", show_alert=True)
                        next_task_needed = False
                        return
                except ValueError as e:
                    logger.error(f"Error converting channel_id '{task.channel_id_to_check}' to int: {e}")
                    local_channel_subscribed = False
                    await callback.answer(f"❗️ Ошибка проверки подписки. Попробуйте позже.", show_alert=True)
                    next_task_needed = False
                    return
                except Exception as e:
                    logger.error(f"Error checking local channel subscription: {e}", exc_info=True)
                    local_channel_subscribed = False
                    await callback.answer(f"❗️ Ошибка проверки подписки. Попробуйте позже.", show_alert=True)
                    next_task_needed = False
                    return
                
        # Если с локальными каналами все в порядке, проверяем через SubGram
        if local_channel_subscribed:
            sg_status, sg_code, sg_links = await request_op(
                user_id,
                callback.message.chat.id,
                first_name=user.first_name,
                language_code=user.language_code,
                is_premium=is_premium,
                action="newtask"
            )
            
            # Логируем результат для отладки
            logger.info(f"SubGram verification response: status={sg_status}, code={sg_code}, links_count={len(sg_links)}")
            
            # Если нет ссылок или статус success, значит пользователь выполнил все подписки
            if sg_status == "success" or not sg_links:
                logger.info(f"User {user_id} successfully completed SubGram task {task_id}")
                # Задание выполнено успешно
                await db.add_balance(session, user_id, task.reward)
                reward_granted = True
                task_completed_now = True
                await callback.answer(f"✅ Задание выполнено! Награда: {task.reward:.2f}⭐️", show_alert=True)
                
                # Удаляем текущее сообщение
                try:
                    await callback.message.delete()
                except Exception as e:
                    logger.error(f"Error deleting message: {e}")
                
                # Показываем новое задание
                await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
            else:
                logger.warning(f"User {user_id} failed SubGram task {task_id} verification")
                # Задание еще не выполнено
                await callback.answer(f"❗️ Сначала подпишитесь на канал спонсора, затем нажмите 'Я выполнил'.", show_alert=True)
                next_task_needed = False
    else:
        # Обычное задание без проверки подписки
        await db.add_balance(session, user_id, task.reward)
        reward_granted = True
        task_completed_now = True
        await callback.answer(f"✅ Задание выполнено! Награда: {task.reward:.2f}⭐️", show_alert=True)

    if task_completed_now and next_task_needed:
        # --- Сначала ищем следующее и обновляем user_data.current_task_id --- # Передаем is_premium
        logger.debug(f"Task {task_id} completed by {user_id}. Finding next (Premium: {is_premium}).")
        next_task = await db.get_next_available_task(session, user_id, is_premium, task_id)
        next_task_id = next_task.id if next_task else None
        logger.debug(f"Next task found: {next_task_id}")
        await db.update_user_current_task(session, user_id, next_task_id)

        # --- Затем отмечаем текущее задание как выполненное --- # Перемещаем вызов сюда
        await db.mark_task_as_completed(session, user_id, task_id)
        logger.debug(f"Called mark_task_as_completed for task {task_id}")

        try:
            # --- Коммитим все изменения вместе ---
            await session.commit()
            logger.debug(f"Committed completion, balance, next task for user {user_id}")
        except Exception as e:
             logger.error(f"Failed to commit changes for user {user_id} after task {task_id}: {e}")
             await session.rollback()
             await callback.answer("❌ Произошла ошибка при сохранении прогресса.", show_alert=True)
             return

        # ... (удаление старого сообщения и вызов send_task_message) ...
        try:
            await callback.message.delete()
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение с выполненным заданием {task_id}: {e}")
        # Передаем is_premium в send_task_message
        await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)


@router.callback_query(F.data.startswith("task_skip_"))
async def process_task_skip(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    task_id = int(callback.data.split("_")[2])
    user = callback.from_user # Получаем пользователя
    user_id = user.id
    is_premium = user.is_premium # Получаем статус
    chat_id = callback.message.chat.id

    user_data = await db.get_user(session, user_id)
    task = await db.get_task_by_id(session, task_id)

    if not user_data or not task or user_data.current_task_id != task_id:
        await callback.answer("❗️ Задание не найдено или уже неактуально.", show_alert=True)
        try:
            # await callback.message.delete(callback.message.id - 1)
            await callback.message.delete()
        except Exception as e:
             logger.warning(f"Не удалось удалить сообщение с неактуальным заданием при пропуске: {e}")
        return

    # --- Ищем следующее НЕВЫПОЛНЕННОЕ задание --- # Передаем is_premium
    logger.debug(f"Task {task_id} skipped by {user_id}. Finding next (Premium: {is_premium}).")
    next_task = await db.get_next_available_task(session, user_id, is_premium, task_id)
    next_task_id = next_task.id if next_task else None
    logger.debug(f"Next task found after skip: {next_task_id}")
    await db.update_user_current_task(session, user_id, next_task_id)

    try:
        await session.commit()
        logger.debug(f"Committed task skip for user {user_id} from task {task_id}")
    except Exception as e:
        logger.error(f"Failed to commit changes for user {user_id} after skipping task {task_id}: {e}")
        await session.rollback()
        await callback.answer("❌ Произошла ошибка при пропуске задания.", show_alert=True)
        return

    await callback.answer("⏭️ Задание пропущено.")

    try:
        await callback.message.delete()
    except Exception as e:
         logger.warning(f"Не удалось удалить сообщение с пропущенным заданием {task_id}: {e}")
    # Передаем is_premium
    await send_skip_task_message(chat_id, user_id, is_premium, session, bot, state)

@router.callback_query(F.data.startswith("task_verify_sub_"))
async def process_task_verify_sub(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    task_id = int(callback.data.split("_")[3]) # Помним, что здесь индекс 3
    user = callback.from_user # Получаем пользователя
    user_id = user.id
    is_premium = user.is_premium # Получаем статус
    chat_id = callback.message.chat.id

    user_data = await db.get_user(session, user_id)
    task = await db.get_task_by_id(session, task_id)

    if not user_data or not task or user_data.current_task_id != task_id or not task.check_subscription or not task.channel_id_to_check:
        await callback.answer("❗️ Задание не найдено, неактуально или не требует проверки подписки.", show_alert=True)
        try: await callback.message.delete() 
        except: pass
        return
        
    # Проверяем премиум требование задачи перед проверкой подписки
    req = task.premium_requirement
    if not ((is_premium and req in ('all', 'premium_only')) or (not is_premium and req in ('all', 'non_premium_only'))):
        await callback.answer("❗️ Это задание недоступно для вашего статуса.", show_alert=True)
        try: await callback.message.delete() 
        except: pass
        await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code) # Показываем следующее
        return

    is_subscribed = await check_member_with_delay(bot, task.channel_id_to_check, user_id)

    if is_subscribed:
        await db.add_balance(session, user_id, task.reward)
        await callback.answer(f"✅ Подписка подтверждена! Награда: {task.reward:.2f}⭐️", show_alert=True)

        # --- Ищем следующее и обновляем user_data.current_task_id --- # Передаем is_premium
        logger.debug(f"Task {task_id} verified for {user_id}. Finding next (Premium: {is_premium}).")
        next_task = await db.get_next_available_task(session, user_id, is_premium, task_id)
        next_task_id = next_task.id if next_task else None
        logger.debug(f"Next task found after verification: {next_task_id}")
        await db.update_user_current_task(session, user_id, next_task_id)

        # --- Отмечаем текущее задание как выполненное --- # Перемещаем вызов сюда
        await db.mark_task_as_completed(session, user_id, task_id)
        logger.debug(f"Called mark_task_as_completed for task {task_id}")

        try:
            # --- Коммитим все изменения вместе ---
            await session.commit()
            logger.debug(f"Committed completion, balance, next task for user {user_id} after verification")
        except Exception as e:
            logger.error(f"Failed to commit changes for user {user_id} after verifying sub for task {task_id}: {e}")
            await session.rollback()
            await callback.answer("❌ Произошла ошибка при сохранении прогресса.", show_alert=True)
            return

        # ... (удаление старого сообщения и вызов send_task_message) ...
        try:
            await callback.message.delete()
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение после проверки подписки для таска {task_id}: {e}")
        # Передаем is_premium
        await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
    else:
        await callback.answer("❗️ Вы все еще не подписаны на канал задания.", show_alert=True)

    await callback.answer() # Отвечаем на колбэк в самом конце

@router.message(F.text == "Заработать звезды")
async def earn_stars(message: Message, session: AsyncSession, bot: Bot, state: FSMContext, config: Config):
    await state.clear()
    user_id = message.from_user.id # Получаем user_id
    # --- Добавляем проверку подписки типа 'start' ---
    if not await check_subscription_for_type(message, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before accessing earn_stars.")
        return
    # -----------------------------------------------

    refferals_count = await db.get_refferals_count(session, message.from_user.id)
    bot_info = await bot.get_me()  # Получаем информацию о боте
    refferals_reward = await db.get_reward(session)
    earn_message =f'''
<b>Приглашай пользователей в бота и получай по {refferals_reward}⭐️, как только они пройдут капчу!</b>

<b>Ваша ссылка:</b>
<code>https://t.me/{bot_info.username}?start={message.from_user.id}</code>

<blockquote><b>❓ Как использовать свою реферальную ссылку?</b>
  • Отправь её друзьям в личные сообщения 👥
  • Поделись ссылкой в своём Telegram-канале 📢
  • Оставь её в комментариях или чатах 🗨
  • Распространяй ссылку в соцсетях: TikTok, Instagram, WhatsApp и других 🕸
</blockquote>

<b>🗣 Вы пригласили: {refferals_count}</b>'''
    
    # Текст для шаринга - более простой, без HTML-тегов
    share_text = f"Привет! Я нашёл отличного бота для заработка подарков! Присоединяйся: https://t.me/{bot_info.username}?start={user_id}"
    
    await message.answer_photo(photo=FSInputFile("bot/assets/images/earn.jpg"), caption=earn_message, reply_markup=kb.earn_stars_keyboard(share_text), disable_web_page_preview=True)
    await message.answer("💡Также можно <b>получать Звёзды</b> за <b>простые задания!</b>", reply_markup=kb.earn_stars_task_keyboard())

@router.callback_query(F.data == "earn_stars")
async def earn_stars(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext, config: Config):
    await state.clear()
    await callback.answer()
    user_id = callback.from_user.id

    refferals_count = await db.get_refferals_count(session, user_id)
    bot_info = await bot.get_me()  # Получаем информацию о боте
    refferals_reward = await db.get_reward(session)
    earn_message =f'''
<b>Приглашай пользователей в бота и получай по {refferals_reward}⭐️, как только они пройдут капчу!</b>

<b>Ваша ссылка:</b>
<code>https://t.me/{bot_info.username}?start={user_id}</code>

<blockquote><b>❓ Как использовать свою реферальную ссылку?</b>
  • Отправь её друзьям в личные сообщения 👥
  • Поделись ссылкой в своём Telegram-канале 📢
  • Оставь её в комментариях или чатах 🗨
  • Распространяй ссылку в соцсетях: TikTok, Instagram, WhatsApp и других 🕸
</blockquote>
<b>🗣 Вы пригласили: {refferals_count}</b>'''
    
    # Текст для шаринга - более простой, без HTML-тегов
    share_text = f"Привет! Я нашёл отличного бота для заработка подарков! Присоединяйся: https://t.me/{bot_info.username}?start={user_id}"
    await callback.message.answer_photo(photo=FSInputFile("bot/assets/images/earn.jpg"), caption=earn_message, reply_markup=kb.earn_stars_keyboard(share_text), disable_web_page_preview=True)
    await callback.message.answer("💡Также можно <b>получать Звёзды</b> за <b>простые задания!</b>", reply_markup=kb.earn_stars_task_keyboard())


@router.message(F.text == "◀️Назад")
async def back_to_main(message: Message, session: AsyncSession, config: Config, bot: Bot):
    try:
        # await asyncio.gather(
        #     message.bot.delete_message(message.chat.id, message.message_id - 1),
        #     message.delete()
        # )
        user_id = message.from_user.id
        bot_info = await bot.get_me()
        referral_link = f"https://t.me/{bot_info.username}?start={user_id}"
        refferals_reward = await db.get_reward(session)
        
        welcome_text = f'''<b><tg-emoji emoji-id="5334607672375254726">👋</tg-emoji> Добро пожаловать!</b>

В нашем боте можно бесплатно зарабатывать звёзды!

• <tg-emoji emoji-id="5310278924616356636">🎯</tg-emoji> Выполняй задания
• <tg-emoji emoji-id="5372926953978341366">👥</tg-emoji> Приглашай друзей по ссылке и получай по {refferals_reward}<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>  за каждого, просто отправь её другу: <b><code>{referral_link}</code></b>

Как только заработаешь минимум 15<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>, выводи их в разделе «<tg-emoji emoji-id="5199749070830197566">🎁</tg-emoji> Вывести звёзды», мы отправим тебе подарок за выбранное количество звёзд, удачи!'''
        
        await message.answer(welcome_text, reply_markup=kb.get_main_keyboard(user_id, config.admin_ids), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка при удалении сообщения: {e}")
        await message.answer("Вы вернулись в главное меню", reply_markup=kb.get_main_keyboard(user_id, config.admin_ids))

@router.callback_query(F.data == "back_to_main")
async def back_to_main_callback(callback: CallbackQuery, session: AsyncSession, config: Config, bot: Bot):
    try:
        # await callback.message.delete()
        user_id = callback.from_user.id
        bot_info = await bot.get_me()
        referral_link = f"https://t.me/{bot_info.username}?start={user_id}"
        refferals_reward = await db.get_reward(session)
        
        welcome_text = f'''<b><tg-emoji emoji-id="5334607672375254726">👋</tg-emoji> Добро пожаловать!</b>

В нашем боте можно бесплатно зарабатывать звёзды!

• <tg-emoji emoji-id="5310278924616356636">🎯</tg-emoji> Выполняй задания
• <tg-emoji emoji-id="5372926953978341366">👥</tg-emoji> Приглашай друзей по ссылке и получай по {refferals_reward}<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>  за каждого, просто отправь её другу: <b><code>{referral_link}</code></b>

Как только заработаешь минимум 15<tg-emoji emoji-id="5179259920754672105">⭐️</tg-emoji>, выводи их в разделе «<tg-emoji emoji-id="5199749070830197566">🎁</tg-emoji> Вывести звёзды», мы отправим тебе подарок за выбранное количество звёзд, удачи!'''
        
        await callback.message.answer(welcome_text, reply_markup=kb.get_main_keyboard(user_id, config.admin_ids), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка при удалении сообщения: {e}")
        await callback.message.answer("Вы вернулись в главное меню", reply_markup=kb.get_main_keyboard(user_id, config.admin_ids))


@router.message(F.text == "Рейтинг")
async def show_top(message: Message, session: AsyncSession, bot: Bot): # Добавляем bot
    user_id = message.from_user.id # Получаем user_id
    # --- Добавляем проверку подписки типа 'start' ---
    if not await check_subscription_for_type(message, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before viewing top.")
        return
    # -----------------------------------------------
    # --- Увеличиваем лимит до 30 ---
    top_users = await db.get_top_users(session, limit=30)
    if not top_users:
        await message.answer("🚫 Рейтинг пока пуст")
        return

    # --- Добавляем эмодзи в заголовок --- 
    top_text = "📊 <b>Топ 30 пользователей за 24 часа:</b>\n\n"
    
    for idx, user in enumerate(top_users, 1):
        referrals = user.refferals_24h_count
        if referrals % 10 == 1 and referrals % 100 != 11:
            word = "реферал"
        elif 2 <= referrals % 10 <= 4 and (referrals % 100 < 10 or referrals % 100 >= 20):
            word = "реферала"
        else:
            word = "рефералов"
            
        # --- Добавляем эмодзи для мест --- 
        place_emoji = ""
        if idx == 1:
            place_emoji = "🥇 "
        elif idx == 2:
            place_emoji = "🥈 "
        elif idx == 3:
            place_emoji = "🥉 "
        else:
            place_emoji = "⭐ " # Для 4-30 места
            
        top_text += f"{place_emoji}{idx}. @{user.username} — {referrals} {word}\n"

    await message.answer_photo(
        photo=FSInputFile("bot/assets/images/top.jpg"),
        caption=top_text,
        reply_markup=kb.top_keyboard()
    )

@router.callback_query(F.data == "top_all_time")
async def show_all_time_top(callback: CallbackQuery, session: AsyncSession):
    # --- Увеличиваем лимит до 30 --- 
    top_users = await db.get_top_users_all_time(session, limit=30)
    if not top_users:
        await callback.answer("🚫 Рейтинг пока пуст", show_alert=True)
        return

    # --- Добавляем эмодзи в заголовок --- 
    top_text = "📊 <b>Топ 30 пользователей за все время:</b>\n\n"
    
    for idx, user in enumerate(top_users, 1):
        referrals = user.refferals_count
        if referrals % 10 == 1 and referrals % 100 != 11:
            word = "реферал"
        elif 2 <= referrals % 10 <= 4 and (referrals % 100 < 10 or referrals % 100 >= 20):
            word = "реферала"
        else:
            word = "рефералов"
            
        # --- Добавляем эмодзи для мест --- 
        place_emoji = ""
        if idx == 1:
            place_emoji = "🥇 "
        elif idx == 2:
            place_emoji = "🥈 "
        elif idx == 3:
            place_emoji = "🥉 "
        else:
            place_emoji = "⭐ " # Для 4-30 места
            
        top_text += f"{place_emoji}{idx}. @{user.username} — {referrals} {word}\n"

    await callback.message.edit_caption(
        caption=top_text,
        reply_markup=kb.top_keyboard()
    )

@router.callback_query(F.data == "top_24h")
async def back_to_24h_top(callback: CallbackQuery, session: AsyncSession, bot: Bot):
    await show_top(callback.message, session, bot)
    await callback.message.delete()

@router.message(F.text == 'Вывести звезды')
async def withdraw_stars(message: Message, session: AsyncSession, state: FSMContext, bot: Bot):
    await state.clear()
    # Получаем ID пользователя, который отправил сообщение
    user_id = message.from_user.id  # ID пользователя, а не бота
    logger.info(f"User {user_id} initiated withdraw process.")
    
    # Проверка подписки
    # if not await check_subscription_for_type(message, bot, session, 'withdraw'):
    #     logger.info(f"User {user_id} failed withdraw subscription check. Exiting withdraw handler.")
    #     return
    
    # Получаем пользователя из БД
    user = await db.get_user(session, user_id)
    if not user:
        logger.error(f"User {user_id} not found after passing withdraw check.")
        await message.answer("Произошла ошибка, не удалось найти ваш профиль.")
        return

    withdraw_text = f'''
<b>Заработано: {round(user.balance, 2)}⭐️</b>

Наши выплаты: <a href="https://t.me/zaberistars">Посмотреть</a>

🎁 Выберите подарок для вывода:'''
    
    await message.answer_photo(
        photo=FSInputFile("bot/assets/images/withdraws.jpg"), 
        caption=withdraw_text, 
        reply_markup=kb.withdraw_gift_selection_keyboard()
    )

# @router.callback_query(F.data.startswith("withdraw_amount_"))
# async def process_withdraw_amount(callback: CallbackQuery, session: AsyncSession, state: FSMContext):
#     # Получаем сумму из callback_data
#     amount = int(callback.data.split("_")[2])
#     user = await db.get_user(session, callback.from_user.id)
    
#     if user.balance < amount:
#         await callback.answer(f"На вашем балансе недостаточно звезд! Ваш баланс: {round(user.balance, 2)}⭐️", show_alert=True)
#         return
    
    
#     # Сохраняем сумму в состоянии
#     await state.update_data(sum=amount)
    
#     # Запрашиваем юзернейм
#     await callback.message.delete()
#     await callback.message.answer('Введите юзернейм для вывода звезд:')
#     await state.set_state(st.WithdrawState.username)

@router.callback_query(F.data.startswith("gift_select_"))
async def process_gift_selection(callback: CallbackQuery, session: AsyncSession, state: FSMContext):
    """Обработчик выбора конкретного подарка"""
    gift_id = callback.data.split("_")[2]  # Получаем ID подарка
    
    # Словарь подарков с их стоимостью
    gifts = {
        "5170145012310081615": {"emoji": "💝", "cost": 15, "name": "Подарок"},
        "5170233102089322756": {"emoji": "🧸", "cost": 15, "name": "Плюшевый мишка"},
        "5168103777563050263": {"emoji": "🌹", "cost": 25, "name": "Роза"},
        "5170250947678437525": {"emoji": "🎁", "cost": 25, "name": "Подарок"},
        "6028601630662853006": {"emoji": "🍾", "cost": 50, "name": "Шампанское"},
        "5170564780938756245": {"emoji": "🚀", "cost": 50, "name": "Ракета"},
        "5170314324215857265": {"emoji": "💐", "cost": 50, "name": "Букет"},
        "5170144170496491616": {"emoji": "🎂", "cost": 50, "name": "Торт"},
        "5170521118301225164": {"emoji": "💎", "cost": 100, "name": "Алмаз"},
        "5168043875654172773": {"emoji": "🏆", "cost": 100, "name": "Кубок"},
        "5170690322832818290": {"emoji": "💍", "cost": 100, "name": "Кольцо"}
    }
    
    if gift_id not in gifts:
        await callback.answer("❌ Неизвестный подарок", show_alert=True)
        return
    
    gift = gifts[gift_id]
    user = await db.get_user(session, callback.from_user.id)
    
    # Проверяем баланс
    if user.balance < gift["cost"]:
        await callback.answer(f"❌ Недостаточно звезд! Нужно: {gift['cost']}⭐, у вас: {round(user.balance, 2)}⭐", show_alert=True)
        return
    
    # Получаем количество предыдущих выводов
    previous_withdraws = await db.get_user_successful_withdraws_count(session, callback.from_user.id)
    
    # Проверяем количество рефералов с учетом предыдущих выводов
    required_referrals = calculate_required_referrals(gift["cost"], previous_withdraws)
    user_referrals = await db.get_refferals_count(session, callback.from_user.id)
    
    if user_referrals < required_referrals:
        await callback.answer(
            f"❌ Недостаточно рефералов!\n"
            f"Для вывода {gift['cost']}⭐ (вывод #{previous_withdraws + 1})\n"
            f"Нужно: {required_referrals} рефералов\n"
            f"У вас: {user_referrals} рефералов\n"
            f"Пригласите еще {required_referrals - user_referrals} друзей!",
            show_alert=True
        )
        return
    
    # Сохраняем выбранный подарок в состоянии
    await state.update_data(
        sum=gift["cost"],
        gift_id=gift_id,
        gift_emoji=gift["emoji"],
        gift_name=gift["name"]
    )
    
    # Удаляем сообщение с фото и отправляем новое текстовое
    await callback.message.delete()
    await callback.message.answer(
        f'Вы выбрали: {gift["emoji"]} {gift["name"]} ({gift["cost"]}⭐)\n'
        f'✅ Рефералов: {user_referrals}/{required_referrals} (вывод #{previous_withdraws + 1})\n\n'
        f'Введите юзернейм для отправки подарка:'
    )
    await state.set_state(st.WithdrawState.username)

@router.message(st.WithdrawState.username)
async def process_withdraw_username(message: Message, state: FSMContext, session: AsyncSession):
    username_input = message.text.strip()

    if not username_input.startswith('@'):
        await message.answer("❌ Неверный формат. Юзернейм должен начинаться с символа @\n\nПожалуйста, введите юзернейм снова:")
        # Остаемся в том же состоянии, ожидая корректного ввода
        return

    await state.update_data(username=message.text)
    
    # Переходим к состоянию подтверждения написания
    await state.set_state(st.WithdrawState.contact_confirmation)
    
    contact_message = f'''
📞 <b>Важно!

ПЕРЕД ПОДТВЕРЖДЕНИЕМ ЗАЯВКИ НЕОБХОДИМО НАПИСАТЬ АДМИНИСТРАТОРУ ДАЖЕ ЕСЛИ ПИСАЛИ ДО ЭТОГО </b><b>@startovsBot</b><b> </b>для выполнения автоматического вывода.

После того как напишете администратору, нажмите кнопку "Написал, продолжить" ниже.
'''
    
    await message.answer(contact_message, reply_markup=kb.contact_confirmation_keyboard(), parse_mode='HTML')

@router.callback_query(F.data == "contact_confirmed")
async def process_contact_confirmation(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    await callback.answer()
    
    state_data = await state.get_data()
    sum = state_data['sum']
    username = state_data['username']
    
    # Проверяем, есть ли данные о подарке
    gift_emoji = state_data.get('gift_emoji')
    gift_name = state_data.get('gift_name')
    
    if gift_emoji and gift_name:
        # Если это подарок
        payments_message = f'''
<b>🎁 Ваша заявка на отправку подарка:</b>

<b>Подарок:</b> {gift_emoji} {gift_name}
<b>Стоимость:</b> {sum}⭐️
<b>Получатель:</b> {username}
'''
    else:
        # Если это обычный вывод звезд
        payments_message = f'''
<b>🧑🏼‍💻 Ваша заявка на вывод:</b>

<b>Сумма:</b> {sum}⭐️
<b>Юзернейм:</b> {username}
'''
    
    await callback.message.edit_text(payments_message, reply_markup=kb.withdraw_keyboard(), parse_mode='HTML')

@router.callback_query(F.data == "withdraw_request")
async def withdraw_request(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext, config: Config, gift_processor):
    user_id = callback.from_user.id
    logger.info(f"User {user_id} initiated withdraw request.")
    
    # Получаем данные из состояния
    data = await state.get_data()
    sum = data.get('sum')
    username = data.get('username')
    
    if not sum or not username:
        await callback.answer("Ошибка: данные заявки не найдены", show_alert=True)
        return
    
    # Проверяем баланс пользователя
    user = await db.get_user(session, user_id)
    if not user or user.balance < sum:
        await callback.answer("❌ Недостаточно звезд на балансе", show_alert=True)
        return
    
    # Удаляем сообщение с подтверждением
    try:
        await callback.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete callback message: {e}")
    
    # Собираем данные подарка, если они есть
    gift_data = None
    if data.get('gift_id'):
        gift_data = {
            'gift_id': data.get('gift_id'),
            'gift_emoji': data.get('gift_emoji'),
            'gift_name': data.get('gift_name')
        }
    
    # Создаем заявку на вывод с передачей gift_processor и данных подарка
    await db.create_withdraw(session, callback.from_user.id, sum, username, bot, config, gift_processor, gift_data)
    
    # Формируем сообщение в зависимости от типа заявки
    if gift_data:
        success_message = f"<b>🎁 Заявка на отправку подарка успешно создана</b>\n\nПодарок: {gift_data['gift_emoji']} {gift_data['gift_name']} ({sum}⭐)\nПолучатель: {username}\n\nЗаявка будет обработана в течение 24 часов"
    else:
        success_message = "<b>Заявка на вывод успешно создана</b>\n\nЗаявка на вывод будет обработана в течение 24 часов"
    
    await callback.message.answer(success_message)
    await state.clear()

@router.callback_query(F.data.startswith("withdraw_confirm_"))
async def withdraw_confirm(callback: CallbackQuery, session: AsyncSession, bot: Bot, config: Config):
    logger.info(f"Received withdraw confirm callback with data: {callback.data}")
    # Проверка на администратора
    if callback.from_user.id not in config.admin_ids:
        logger.warning(f"User {callback.from_user.id} attempted to confirm withdrawal without admin rights.")
        await callback.answer("🚫 У вас нет прав для выполнения этого действия", show_alert=True)
        return
        
    # Обновляем регулярное выражение для извлечения данных
    match = re.match(r"withdraw_confirm_([a-f0-9]+)_(\d+)_(@[\w]+)_(\d+)_(\d+)", callback.data)
    if not match:
        logger.error(f"Failed to match data format for callback: {callback.data}")
        await callback.answer("Ошибка: неверный формат данных.", show_alert=True)
        return

    withdraw_id, sum, username, user_id, id = match.groups()
    logger.info(f"Confirming withdrawal request {withdraw_id} for user {user_id}")
    bot_info = await bot.get_me()
    
    # Подтверждаем вывод в БД
    await db.confirm_withdraw(session, withdraw_id)
    
    # Обновляем сообщение в канале
    await callback.message.edit_text(
    f'''✅ Запрос на вывод №{id}

👤 Пользователь: {html.escape(username)} | ID: {user_id}
🔑 Количество: {sum}⭐️

🔧 Статус: Подарок отправлен 🎁

<a href="https://t.me/{bot_info.username}">Бот</a>''',
    reply_markup=kb.withdraw_confirm_keyboard(), disable_web_page_preview=True  
)
    
    # Отправляем уведомление пользователю
    logger.info(f"Notifying user {user_id} about confirmed withdrawal request.")
    await bot.send_message(
        chat_id=user_id, 
        text=f"🎉 <b>Заявка на вывод была успешно подтверждена</b>\n\nВам было отправлено: {sum}⭐️"
    )

@router.callback_query(F.data.startswith("withdraw_reject_"))
async def withdraw_reject(callback: CallbackQuery, session: AsyncSession, bot: Bot, config: Config):
    # Проверка на администратора
    if callback.from_user.id not in config.admin_ids:
        await callback.answer("🚫 У вас нет прав для выполнения этого действия", show_alert=True)
        return
        
    # Логируем исходные данные callback
    logger.info(f"Raw callback.data: {callback.data}")
    
    # Обновляем регулярное выражение для извлечения данных
    import re
    match = re.match(r"withdraw_reject_([a-f0-9]{8})_(\d+)_(@[\w]+)_(\d+)_(\d+)", callback.data)
    if not match:
        logger.error(f"Failed to match data format for callback: {callback.data}")
        await callback.answer("Ошибка: неверный формат данных.", show_alert=True)
        return

    withdraw_id, sum, username, user_id, id = match.groups()
    logger.info(f"Parsed data - withdraw_id: {withdraw_id}, sum: {sum}, username: {username}, user_id: {user_id}, id: {id}")
    logger.info(f"Rejecting withdrawal request {withdraw_id} for user {user_id}")
    
    # Отклоняем вывод в БД и возвращаем звезды пользователю
    await db.reject_withdraw(session, withdraw_id)
    
    # Обновляем сообщение в канале
    await callback.message.edit_text(
        f'''❌ Запрос на вывод №{id}

👤 Пользователь: {username} | ID: {user_id}
🔑 Количество: {sum}⭐️

🔧 Статус: Отклонено ❌''',
        reply_markup=kb.withdraw_reject_keyboard()
    )
    
    # Отправляем уведомление пользователю
    await bot.send_message(
        chat_id=user_id, 
        text=f"❌ <b>Заявка на вывод была отклонена</b>\n\nЗвезды возвращены на ваш баланс: {sum}⭐️"
    )

@router.callback_query(F.data == "complete_tasks")
async def complete_tasks(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    user_id = callback.from_user.id
    logger.info(f"User {user_id} initiated task completion process.")
    user = callback.from_user
    
    # --- Проверка подписки типа 'start' ---     
    if not await check_subscription_for_type(callback, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before completing tasks.")
        return
    # ------------------------------------------    
    
    await send_task_message(callback.message.chat.id, user_id, True, session, bot, state, user.first_name, user.language_code)
    await callback.message.delete()
    await callback.answer()

@router.callback_query(F.data == "complete_tasks_template")
async def complete_tasks_template(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    user_id = callback.from_user.id
    logger.info(f"User {user_id} initiated task completion process.")
    user = callback.from_user
    
    # --- Проверка подписки типа 'start' ---     
    if not await check_subscription_for_type(callback, bot, session, 'start'):
        logger.info(f"User {user_id} failed start subscription check before completing tasks.")
        return
    # ------------------------------------------    
    
    await send_task_message(callback.message.chat.id, user_id, True, session, bot, state, user.first_name, user.language_code)
    await callback.answer()

# --- Новые обработчики для кнопок перепроверки --- 

async def recheck_subscription_handler(callback: CallbackQuery, bot: Bot, session: AsyncSession, check_type: str, config: Config):
    user_id = callback.from_user.id
    user = callback.from_user
    message = callback.message # Запоминаем исходное сообщение
    is_premium = user.is_premium if user else False
    user_username = user.username or ""

    logger.info(f"User {user_id} (Premium: {is_premium}) initiated recheck for subscription type '{check_type}'")

    # --- Этап 1: Перепроверка ЛОКАЛЬНЫХ каналов ---
    channels_to_check: List[Channel] = []
    channels_to_pokaz: List[Channel] = []

    try:
        if check_type == 'start':
            channels_to_check = await db.get_filtered_start_channels(session, is_premium=is_premium)
            channels_to_pokaz = await db.get_filtered_start_channels_all(session, is_premium=is_premium)
        elif check_type == 'withdraw':
            channels_to_check = await db.get_filtered_withdraw_channels(session, is_premium=is_premium)
            channels_to_pokaz = await db.get_filtered_withdraw_channels_all(session, is_premium=is_premium)
        else:
            logger.error(f"Unknown check_type '{check_type}' in recheck handler.")
            await callback.answer("Ошибка: Неизвестный тип проверки.", show_alert=True)
            return
    except Exception as e_db:
        return

    failed_local_channels: List[Channel] = []
    if channels_to_check:
        logger.debug(f"Rechecking {len(channels_to_check)} local channels for user {user_id} via Telegram API.")
        for channel in channels_to_check:
            is_subscribed = await check_member_with_delay(bot, channel.channel_id, user_id)
            if not is_subscribed:
                logger.info(f"User {user_id} failed recheck for local channel {channel.channel_id}")
                failed_local_channels.append(channel)
    else:
        logger.debug(f"No local channels to recheck via Telegram API for user {user_id} and type '{check_type}'.")

    local_check_failed = bool(failed_local_channels)

    if local_check_failed:
        logger.warning(f"User {user_id} FAILED LOCAL recheck for type '{check_type}'.")
        items_to_show = channels_to_pokaz
        if not items_to_show:
             logger.error(f"Local recheck failed for user {user_id}, but no local channels to show.")
             try: await callback.answer("Не удалось подтвердить подписку на локальные каналы. Попробуйте позже.", show_alert=True)
             except Exception: pass
             try: await message.delete()
             except Exception: pass
             return

        caption = f"⏳ Пожалуйста, подпишитесь на каналы из списка:"
        try:
            reply_markup = await kb.get_channels_keyboard(items_to_show, check_type)
        except Exception as e_kb:
             logger.error(f"Error generating LOCAL keyboard during recheck for user {user_id}: {e_kb}")
             await callback.answer("Ошибка при подготовке списка каналов.", show_alert=True)
             return

        try: await message.delete()
        except Exception as e_del: logger.warning(f"Could not delete original message for user {user_id} on failed local recheck: {e_del}")
        try:
            await callback.message.answer(text=caption, reply_markup=reply_markup)
            logger.info(f"Sent new LOCAL subscription message for user {user_id} on failed recheck.")
            await callback.answer()
        except Exception as e_send:
            logger.error(f"Failed to send new LOCAL subscription message for user {user_id} on failed recheck: {e_send}")
            try: await callback.answer("Не удалось обновить список каналов. Попробуйте еще раз.", show_alert=True)
            except Exception: pass

        return

    logger.info(f"User {user_id} PASSED LOCAL recheck for type '{check_type}'.")

    # Удалите или закомментируйте этот блок, если SubGram не должен использоваться для ОП
    # if check_type == 'start':
    #     sg_status, sg_code, sg_links = await request_op(
    #         user_id,
    #         message.chat.id,
    #         first_name=user.first_name,
    #         language_code=user.language_code,
    #         is_premium=is_premium
    #     )

    #     subgram_failed = sg_status != 'ok'
    #     subgram_failed_with_links_to_show = subgram_failed and sg_links and check_type == 'start'

    #     if subgram_failed:
    #         if subgram_failed_with_links_to_show:
    #             logger.warning(f"User {user_id} passed local, but FAILED SUBGRAM recheck for type '{check_type}' WITH links.")
    #             caption = f"⏳ Подпишитесь на всех спонсоров снизу:"
    #             try:
    #                 reply_markup = await kb.get_combined_channels_keyboard(sg_links, check_type)
    #             except Exception as e_kb:
    #                  logger.error(f"Error generating SUBGRAM keyboard during recheck for user {user_id}: {e_kb}")
    #                  await callback.answer("Ошибка при подготовке списка SubGram.", show_alert=True)
    #                  return

    #             try: await message.delete()
    #             except Exception as e_del: logger.warning(f"Could not delete message for user {user_id} on failed SubGram recheck: {e_del}")
    #             try:
    #                 await callback.message.answer(text=caption, reply_markup=reply_markup)
    #                 logger.info(f"Sent new SUBGRAM subscription message for user {user_id} on failed recheck.")
    #                 await callback.answer()
    #             except Exception as e_send:
    #                 logger.error(f"Failed to send new SUBGRAM subscription message for user {user_id} on failed recheck: {e_send}")
    #                 try: await callback.answer("Не удалось обновить список SubGram. Попробуйте еще раз.", show_alert=True)
    #                 except Exception: pass

    #             return
    #         else:
    #             logger.warning(f"User {user_id} passed local, but FAILED SUBGRAM recheck for type '{check_type}' WITHOUT links (status: {sg_status}, code: {sg_code}).")
    #             error_text = "Не удалось пройти проверку SubGram. Попробуйте позже."
    #             try:
    #                 await callback.answer(error_text, show_alert=True)
    #                 await message.delete()
    #             except Exception: pass
    #             return

    logger.info(f"User {user_id} PASSED ALL rechecks (Local + SubGram) for type '{check_type}'.")
    try:
        await callback.answer("✅ Спасибо за подписку!", show_alert=False)
        # Удаляем сообщение с кнопкой проверки ПОСЛЕДНЕГО этапа
        if message: # Убедимся, что message существует
             try:
                 await message.delete()
                 logger.info(f"Deleted final subscription prompt message for user {user_id}.")
             except Exception as e_del_final:
                  logger.warning(f"Could not delete final subscription prompt message for user {user_id}: {e_del_final}")
    except Exception as e_success_callback:
        logger.warning(f"Could not answer final callback for user {user_id}: {e_success_callback}")

    if check_type == 'start':
        bot_info = await bot.get_me()
        bot_username = bot_info.username
        welcome_caption_two = f'''
👋 <b>Добро пожаловать, {html.escape(user.first_name)}!</b>

⭐️ <b>Зарабатывай звёзды</b> - выполняй простые задания и получай вознаграждение!

🎁 <b>Получай подарки</b> - обменивай звёзды на подарки!

👥 <b>Приглашай друзей</b> - получай звёзды за каждого приглашенного друга!

👇 <b>Жми на кнопки ниже, чтобы начать!</b>'''
        try:
            await callback.message.answer_photo(
                photo=FSInputFile("bot/assets/images/menu.jpg"),
                caption=welcome_caption_two,
                reply_markup=kb.get_main_keyboard(user_id, config.admin_ids),
                parse_mode="HTML"
            )
            await message.answer("💡Также можно <b>получать Звёзды</b> за <b>простые задания!</b>", reply_markup=kb.earn_stars_task_keyboard())
        except Exception as e_welcome:
             logger.error(f"Error sending welcome message after recheck for user {user_id}: {e_welcome}")

        user_data = await db.get_user(session, user_id)
        if user_data and user_data.ref_bonus is False:
            logger.info(f"Attempting to set ref_bonus=True for user {user_id} after START RECHECK.")
            bonus_set_prepared_recheck = await db.set_user_ref_bonus_passed(session, user_id)

            ref_id_recheck = user_data.refferal_id
            reward_info_recheck = ""
            ref_notification_needed_recheck = False
            reward_recheck = None
            commit_needed_recheck = False

            if bonus_set_prepared_recheck:
                commit_needed_recheck = True
                logger.info(f"Prepared ref_bonus=True update for user {user_id} (recheck).")

                if ref_id_recheck:
                    reward_recheck = await db.get_reward(session)
                    if reward_recheck and reward_recheck > 0:
                        try:
                            await db.add_balance(session, ref_id_recheck, reward_recheck)
                            logger.info(f"Prepared adding {reward_recheck} stars to referrer {ref_id_recheck} for user {user_id} (recheck).")

                            referral_counts_incremented_recheck = await db.increment_referral_counts(session, ref_id_recheck)
                            if referral_counts_incremented_recheck:
                                logger.info(f"Prepared incrementing referral counts for referrer {ref_id_recheck} (recheck).")
                            else:
                                 logger.warning(f"Failed to prepare incrementing referral counts for referrer {ref_id_recheck} (recheck).")
                            reward_info_recheck = f"🌟 Рефереру подготовлено: {reward_recheck}⭐️"
                            ref_notification_needed_recheck = True
                        except Exception as e_recheck:
                            logger.error(f"Failed to prepare add_balance for referrer {ref_id_recheck} (recheck): {e_recheck}", exc_info=True)
                            reward_info_recheck = "⚠️ Ошибка подготовки начисления награды рефереру (recheck)."
                            commit_needed_recheck = False
                            ref_notification_needed_recheck = False
                    else:
                        reward_info_recheck = f"ℹ️ Награда рефереру не начислена (сумма награды: {reward_recheck}) (recheck)."

                if commit_needed_recheck:
                    try:
                        await session.commit()
                        logger.info(f"Successfully committed ref_bonus=True and potentially ref reward for user {user_id} (recheck).")
                        if ref_notification_needed_recheck and ref_id_recheck:
                            updated_ref_data_recheck = await db.get_user(session, ref_id_recheck)
                            updated_balance_recheck = updated_ref_data_recheck.balance if updated_ref_data_recheck else "N/A"
                            reward_info_recheck += f" (Новый баланс реферера: {updated_balance_recheck}⭐️)"
                    except Exception as e_commit_recheck:
                        logger.error(f"Failed to commit ref_bonus/reward changes for user {user_id} (recheck): {e_commit_recheck}", exc_info=True)
                        await session.rollback()
                        reward_info_recheck += " ⚠️ Ошибка сохранения изменений в БД (recheck)."
                        ref_notification_needed_recheck = False
                else:
                    logger.warning(f"Failed to prepare ref_bonus=True for user {user_id} after START RECHECK.")

                try:
                    bonus_set_final_status_recheck = "Установлен ✅" if bonus_set_prepared_recheck and commit_needed_recheck and "Ошибка сохранения" not in reward_info_recheck else "Ошибка установки/сохранения ❌"
                    source_info_op_recheck = "Неизвестен"
                    if user_data:
                        if user_data.refferal_id:
                             ref_data_op_recheck = await db.get_user(session, user_data.refferal_id)
                             ref_username_op_recheck = ref_data_op_recheck.username if ref_data_op_recheck else f"id:{user_data.refferal_id}"
                             source_info_op_recheck = f"Реферер: [{user_data.refferal_id}] (@{ref_username_op_recheck})"
                        elif user_data.individual_link_id:
                             link_op_recheck = await db.get_individual_link_by_id(session, user_data.individual_link_id)
                             source_info_op_recheck = f"Инд. ссылка: [{link_op_recheck.identifier if link_op_recheck else 'UNKNOWN'}] ({user_data.individual_link_id})"
                        else:
                             source_info_op_recheck = "Без источника"

                    log_message_op_recheck = (
                        f"🏁 <b>Пользователь прошел проверку ОП (Start)!</b>\n\n"
                        f"📌 ID: <code>{user_id}</code>\n"
                        f"👤 Пользователь: @{user_username}\n\n"
                        f"🔗 Источник: {source_info_op_recheck}\n"
                        f"🚩 Статус флага ref_bonus: {bonus_set_final_status_recheck}\n"
                    )
                    if reward_info_recheck:
                        log_message_op_recheck += f"\n{reward_info_recheck}"
                    await bot.send_message(config.logs_id, log_message_op_recheck)
                except Exception as e_log_op_recheck:
                    logger.error(f"Error sending OP Start RECHECK log for user {user_id}: {e_log_op_recheck}")

                if ref_notification_needed_recheck and ref_id_recheck and reward_recheck and reward_recheck > 0:
                     try:
                         await bot.send_message(
                             ref_id_recheck,
                             "🎉 <b>У вас новый реферал!</b>\n\n"
                             f"💫 Пользователь: @{user_username}\n\n"
                             f"- На ваш баланс была начислена награда: {reward_recheck}⭐️"
                         )
                         logger.info(f"Sent OP Start RECHECK notification to referrer {ref_id_recheck}")
                     except Exception as e_notify_recheck:
                         logger.error(f"Failed to send OP Start RECHECK notification to referrer {ref_id_recheck}: {e_notify_recheck}")

    elif check_type == 'withdraw':
        try:
            await callback.message.answer("✅ Проверка пройдена. Теперь вы можете попробовать вывести средства снова.")
        except Exception as e_withdraw:
            logger.error(f"Error sending withdraw success message after recheck for user {user_id}: {e_withdraw}")

# Регистрируем обработчики для конкретных callback_data (остаются без изменений)
@router.callback_query(F.data == "recheck_sub_start")
async def recheck_sub_start_handler(callback: CallbackQuery, bot: Bot, session: AsyncSession, config: Config):
    await recheck_subscription_handler(callback, bot, session, 'start', config)

@router.callback_query(F.data == "recheck_sub_withdraw")
async def recheck_sub_withdraw_handler(callback: CallbackQuery, bot: Bot, session: AsyncSession, config: Config):
    await recheck_subscription_handler(callback, bot, session, 'withdraw', config)

# -----------------------------------------------------

@router.callback_query(F.data == "withdraw_again")
async def withdraw_again(callback: CallbackQuery, session: AsyncSession, state: FSMContext, bot: Bot): # Добавили bot
    await state.clear()
    user_id = callback.from_user.id
    logger.info(f"User {user_id} initiated withdraw process via 'withdraw_again'.")

    # ... (остальная логика обработчика: получение user, проверка бана, отправка сообщения) ...

@router.message(F.text)
async def handle_all_messages(message: Message, bot: Bot, session: AsyncSession):
    pass

# Создаем специальный обработчик для сброса и обновления задания SubGram
@router.callback_query(F.data == "task_skip")
async def skip_task_handler(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    """Обработчик для кнопки 'Пропустить'"""
    user_id = callback.from_user.id
    is_premium = callback.from_user.is_premium
    user = callback.from_user
    
    # Удаляем текущее сообщение
    try:
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
    
    # Показываем новое задание
    await send_task_message(callback.message.chat.id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
    
    await callback.answer("Задание пропущено")

# Функция для показа канала по индексу
async def show_channel_by_index(chat_id: int, task_id: int, channels: list, index: int, bot: Bot, state: FSMContext):
    # Проверяем, что индекс в пределах списка
    if index >= len(channels):
        # Все каналы просмотрены, показываем сообщение об завершении
        await bot.send_message(
            chat_id,
            "🎉 Вы просмотрели все каналы спонсоров!\nЕсли вы подписались на все, нажмите 'Я выполнил'.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Я выполнил", callback_data=f"task_complete_{task_id}")],
                [InlineKeyboardButton(text="⏩ Пропустить", callback_data="task_skip")]
            ])
        )
        return

    # Создаем клавиатуру для текущего канала
    current_channel = channels[index]
    
    # Создаем клавиатуру с кнопками
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Перейти в канал", url=current_channel)],
        [InlineKeyboardButton(text="✅ Я выполнил", callback_data=f"sg_channel_complete_{task_id}")],
        [InlineKeyboardButton(text="⏩ Пропустить", callback_data="sg_channel_skip")]
    ])
    
    # Сохраняем текущее состояние
    await state.update_data(
        channels=channels,
        current_index=index,
        task_id=task_id
    )
    await state.set_state(SponsorChannelsState.channels)
    
    # Отправляем сообщение с текущим каналом
    await bot.send_message(
        chat_id,
        f"📢 <b>Канал {index + 1}/{len(channels)}</b>\n\nПодпишитесь на канал и нажмите 'Я выполнил':",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

# Обработчик для кнопки "Я выполнил" для канала
@router.callback_query(F.data.startswith("sg_channel_complete_"), StateFilter(SponsorChannelsState.channels))
async def sg_channel_complete_handler(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession):
    # Получаем данные из состояния
    data = await state.get_data()
    channels = data.get('channels', [])
    current_index = data.get('current_index', 0)
    task_id = data.get('task_id')
    
    # Проверяем подписку на текущий канал через SubGram
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id
    is_premium = callback.from_user.is_premium
    user = callback.from_user
    
    # Проверяем подписку на ВЕСЬ список каналов сразу
    sg_status, sg_code, sg_links = await request_op(
        user_id,
        chat_id,
        first_name=callback.from_user.first_name,
        language_code=callback.from_user.language_code,
        is_premium=is_premium,
        action="newtask"
    )
    
    # Если SubGram вернул пустой список или ok, значит все подписки выполнены
    if sg_status == 'ok' or not sg_links:
        # Все подписки выполнены, начисляем награду
        task = await db.get_task_by_id(session, task_id)
        await db.add_balance(session, user_id, task.reward)
        await db.mark_task_as_completed(session, user_id, task_id)
        
        # Ищем следующее задание
        next_task = await db.get_next_available_task(session, user_id, is_premium, task_id)
        next_task_id = next_task.id if next_task else None
        await db.update_user_current_task(session, user_id, next_task_id)
        
        # Коммитим изменения
        await session.commit()
        
        # Удаляем сообщение с кнопками
        try:
            await callback.message.delete()
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
        
        # Показываем сообщение о выполнении
        await bot.send_message(
            chat_id,
            f"🎉 <b>Задание выполнено!</b>\nВы получили {task.reward:.2f}⭐️ за подписку на все каналы спонсоров.",
            parse_mode="HTML"
        )
        
        # Очищаем состояние
        await state.clear()
        
        # Показываем следующее задание
        await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
        
        await callback.answer("✅ Задание выполнено успешно!")
        return
    
    # Есть еще неподписанные каналы, показываем следующий канал или текущий снова
    if current_index < len(sg_links) - 1:
        # Если есть еще каналы в списке, переходим к следующему
        current_index += 1
    else:
        # Если это последний канал, показываем сообщение, что нужно подписаться на все
        await callback.answer("⚠️ Вы не подписались на все каналы. Проверьте ваши подписки.", show_alert=True)
        return
    
    # Удаляем сообщение с текущим каналом
    try:
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
    
    # Показываем следующий канал
    await show_channel_by_index(
        chat_id,
        task_id,
        sg_links,
        current_index,
        bot,
        state
    )
    
    await callback.answer()

# Обработчик для кнопки "Пропустить" для канала
@router.callback_query(F.data == "sg_channel_skip", StateFilter(SponsorChannelsState.channels))
async def sg_channel_skip_handler(callback: CallbackQuery, state: FSMContext, bot: Bot):
    # Получаем данные из состояния
    data = await state.get_data()
    channels = data.get('channels', [])
    current_index = data.get('current_index', 0)
    task_id = data.get('task_id')
    
    # Переходим к следующему каналу
    current_index += 1
    
    # Удаляем сообщение с текущим каналом
    try:
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
    
    # Показываем следующий канал
    await show_channel_by_index(
        callback.message.chat.id,
        task_id,
        channels,
        current_index,
        bot,
        state
    )
    
    await callback.answer()

@router.callback_query(F.data == "subgram_task_complete")
async def handle_subgram_task_complete(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession):
    data = await state.get_data()
    sg_links = data.get('sg_links', [])
    current_index = data.get('current_index', 0)
    user = callback.from_user
    is_premium = user.is_premium

    # Логируем текущий индекс и длину списка
    logger.debug(f"Current index: {current_index}, sg_links length: {len(sg_links)}")
    # Повторный запрос в SubGram для проверки подписки
    sg_status, sg_code, new_sg_links = await request_op(
        callback.from_user.id,
        callback.message.chat.id,
        first_name=callback.from_user.first_name,
        language_code=callback.from_user.language_code,
        action="newtask"
    )

    if sg_links[current_index] not in new_sg_links:
        # Пользователь успешно подписался на канал
        reward_amount = 0.25
        current_channel_link = sg_links[current_index]
        
        # Сохраняем информацию о выполненном задании SubGram
        try:
            # Генерируем ID задания (для SubGram используем хэш ссылки канала + user_id)
            import hashlib
            task_string = f"{current_channel_link}_{callback.from_user.id}_{datetime.now().isoformat()}"
            subgram_task_id = int(hashlib.md5(task_string.encode()).hexdigest()[:8], 16)
            
            # Извлекаем название канала из ссылки (если возможно)
            channel_name = current_channel_link.split('/')[-1] if '/' in current_channel_link else current_channel_link
            
            # Сохраняем выполненное задание для отслеживания штрафов
            task_saved = await db.save_subgram_completed_task(
                session=session,
                user_id=callback.from_user.id,
                subgram_task_id=subgram_task_id,
                channel_link=current_channel_link,
                channel_name=channel_name,
                reward_given=reward_amount
            )
            
            if task_saved:
                logger.info(f"✅ Saved SubGram completed task for user {callback.from_user.id}: {current_channel_link}")
            else:
                logger.error(f"❌ Failed to save SubGram completed task for user {callback.from_user.id}: {current_channel_link}")
            
        except Exception as e:
            logger.error(f"❌ Error saving SubGram completed task: {e}", exc_info=True)
            # Продолжаем выполнение даже при ошибке сохранения
        
        # Начисляем награду
        await callback.answer("Вы получили 0.25⭐️", show_alert=False)
        await db.add_balance(session, callback.from_user.id, reward_amount)
        logger.info(f"User {callback.from_user.id} earned {reward_amount}⭐️ for completing SubGram task")
        
        # Коммитим все изменения
        try:
            await session.commit()
            logger.info(f"💾 Successfully committed changes for user {callback.from_user.id}")
        except Exception as e:
            logger.error(f"❌ Failed to commit changes for user {callback.from_user.id}: {e}")
            await session.rollback()
        
        try:
            await callback.message.delete()
        except Exception as e:
            logger.error(f"Error deleting message: {e}")

        if current_index < len(sg_links) - 1:
            current_index += 1
            await state.update_data(current_index=current_index)
            await show_subgram_channel(callback.message.chat.id, sg_links, current_index, bot)
        else:
            # Повторный запрос в SubGram для новых заданий
            sg_status, sg_code, new_sg_links = await request_op(
                callback.from_user.id,
                callback.message.chat.id,
                first_name=callback.from_user.first_name,
                language_code=callback.from_user.language_code,
                action="newtask"
            )
            if new_sg_links:
                await state.update_data(sg_links=new_sg_links, current_index=0)
                await show_subgram_channel(callback.message.chat.id, new_sg_links, 0, bot)
            else:
                # Очищаем state перед переходом к Flyer, чтобы не было зацикливания на SubGram
                await state.clear()
                # Проверяем наличие локальных заданий
                task_to_show = await db.get_next_available_task(session, callback.from_user.id, False, None)
                if task_to_show:
                    await send_task_message(callback.message.chat.id, callback.from_user.id, is_premium, session, bot, state, user.first_name, user.language_code)
                else:
                    await send_task_message(callback.message.chat.id, callback.from_user.id, is_premium, session, bot, state, user.first_name, user.language_code)
    else:
        await callback.answer("Подпишитесь на канал и попробуйте снова.", show_alert=True)

@router.callback_query(F.data == "subgram_task_skip")
async def handle_subgram_task_skip(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    sg_links = data.get('sg_links', [])
    current_index = data.get('current_index', 0)

    if current_index < len(sg_links) - 1:
        current_index += 1
        await state.update_data(current_index=current_index)
        await callback.message.delete()
        await show_subgram_channel(callback.message.chat.id, sg_links, current_index, bot)
    else:
        await callback.answer("Нет больше заданий для пропуска.", show_alert=True)
        await state.clear()

async def show_subgram_channel(chat_id, sg_links, index, bot):
    current_channel = sg_links[index]
    message_text = f"""🎯 <b>Доступно задание!</b>

📌 Подпишитесь на <b><a href="{current_channel}">канал</a></b> и не отписывайтесь в течение 7 дней.

<b>🏆 Награда: 0.25⭐️</b>"""

    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🔗 Перейти в канал", url=current_channel)
    keyboard.button(text="✅ Я выполнил", callback_data="subgram_task_complete")
    keyboard.button(text="⏩ Пропустить", callback_data="subgram_task_skip")
    keyboard.adjust(1)
    markup = keyboard.as_markup()

    await bot.send_photo(chat_id, caption=message_text, photo=FSInputFile("bot/assets/images/quests.jpg"), reply_markup=markup)

@router.callback_query(F.data.startswith("local_task_complete_"))
async def handle_local_task_complete(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    task_id = int(callback.data.split("_")[-1])
    user = callback.from_user
    is_premium = user.is_premium

    # Блокируем задание для предотвращения race condition
    # Весь обработчик и так в транзакции от middleware, begin_nested() избыточен и может вызывать проблемы
    task = await db.get_task_by_id(session, task_id, for_update=True)

    # Если задание исчезло, пока пользователь думал
    if not task:
        await callback.answer("Задание больше не существует.", show_alert=True)
        try:
            await callback.message.delete()
        except Exception: pass
        # Ищем новое задание
        await send_task_message(callback.message.chat.id, user.id, is_premium, session, bot, state, user.first_name, user.language_code)
        return

    # 1. Проверка на повторное выполнение
    has_completed = await db.has_user_completed_task(session, user.id, task_id)
    if has_completed:
        await callback.answer("Вы уже выполняли это задание.", show_alert=True)
        try:
            await callback.message.delete()
        except Exception: pass
        await send_task_message(callback.message.chat.id, user.id, is_premium, session, bot, state, user.first_name, user.language_code)
        return

    # 2. Проверка лимитов (общих и временных)
    can_complete, reason = await db.check_task_limits(session, task)
    if not can_complete:
        await callback.answer(f"Не удалось выполнить задание: {reason}", show_alert=True)
        try:
            await callback.message.delete()
        except Exception: pass
        # Лимит исчерпан, ищем для пользователя новое задание
        await send_task_message(callback.message.chat.id, user.id, is_premium, session, bot, state, user.first_name, user.language_code)
        return

    # 3. Проверка подписки
    if task.check_subscription and task.channel_id_to_check:
        is_subscribed = await check_member_with_delay(bot, str(task.channel_id_to_check), user.id)
        if not is_subscribed:
            await callback.answer("Для получения награды вы должны быть подписаны на канал.", show_alert=True)
            # Просто выходим, давая шанс подписаться и нажать снова.
            # Middleware откатит транзакцию, если она есть.
            return

    # Все проверки пройдены: выполняем задание
    await db.mark_task_as_completed(session, user.id, task_id)
    # Принудительно отправляем изменения в БД, чтобы они были видны следующему запросу
    await session.flush()
    
    await db.add_balance(session, user.id, task.reward)
    
    try:
        await db.save_local_completed_task(
            session=session, user_id=user.id, task_id=task_id,
            channel_id=int(task.channel_id_to_check) if task.channel_id_to_check else None,
            reward_given=task.reward
        )
    except Exception as e:
        logger.error(f"Ошибка сохранения выполненного локального задания: {e}", exc_info=True)
        # Не делаем commit или rollback, пусть этим занимается middleware

    await callback.answer(f"✅ Задание выполнено! Вы получили {task.reward:.2f}⭐️", show_alert=True)

    try:
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения: {e}")

    # Очищаем current_task_id пользователя, так как задание выполнено
    await db.update_user_current_task(session, user.id, None)
    
    # Ищем и отправляем следующее задание
    await send_task_message(callback.message.chat.id, user.id, is_premium, session, bot, state, user.first_name, user.language_code)


@router.callback_query(F.data.startswith("local_task_skip_"))
async def handle_local_task_skip(callback: CallbackQuery, session: AsyncSession, bot: Bot, state: FSMContext):
    task_id = int(callback.data.split("_")[-1])
    user = callback.from_user
    is_premium = user.is_premium

    try:
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения: {e}")

    # Ищем следующее доступное задание
    next_task = await db.get_next_available_task(session, user.id, is_premium, task_id)
    next_task_id = next_task.id if next_task else None
    
    # Обновляем current_task_id на следующее задание
    if next_task_id:
        await db.update_user_current_task(session, user.id, next_task_id)
    else:
        # Если следующего задания нет, очищаем current_task_id
        await db.update_user_current_task(session, user.id, None)
    
    # Показываем следующее задание
    await send_task_message(
        chat_id=callback.message.chat.id,
        user_id=user.id,
        is_premium=is_premium,
        session=session,
        bot=bot,
        state=state,
        first_name=user.first_name,
        language_code=user.language_code
    )


async def show_subscription_channels(
    event: Union[Message, CallbackQuery],
    state: FSMContext,
    bot: Bot,
    session: AsyncSession,
    stage: int,
    failed_channels: Optional[List[Channel]] = None,
    subgram_sponsors: Optional[list] = None
):
    user = event.from_user
    is_premium = user.is_premium if user else False

    if stage == 1:
        channels_to_show = await db.get_filtered_start_channels_all(session, is_premium)
        check_type = 'start'
        state_to_set = SubscriptionCheckStates.waiting_primary_check
        text = '''⏳ Пожалуйста, подпишитесь на каналы спонсоров:'''
    elif stage == 2:
        channels_to_show = await db.get_filtered_second_stage_channels_all(session, is_premium)
        check_type = 'start'
        state_to_set = SubscriptionCheckStates.waiting_secondary_check
        text = '''⏳ Пожалуйста, подпишитесь на каналы спонсоров:'''
    else:
        logger.warning(f"Invalid stage {stage} requested in show_subscription_channels for user {user.id}")
        return

    sg_sponsors = subgram_sponsors or []

    if not channels_to_show and not sg_sponsors:
        logger.info(f"No channels/sponsors to show for stage {stage} for user {user.id}")
        await state.clear()
        if isinstance(event, CallbackQuery):
            await event.answer("Нет каналов для проверки на данном этапе.", show_alert=True)
        return

    if sg_sponsors:
        await state.update_data(subgram_sponsors=sg_sponsors)

    logger.info(f"show_subscription_channels: Getting keyboard for user {user.id}, stage {stage}, channels={len(channels_to_show or [])}, sponsors={len(sg_sponsors)}")
    
    try:
        reply_markup = kb.get_combined_op_keyboard(
            local_channels=channels_to_show or [],
            subgram_sponsors=sg_sponsors,
            check_type=check_type,
            stage=stage
        )
        logger.info(f"show_subscription_channels: Keyboard created for user {user.id}")
    except Exception as kb_err:
        logger.error(f"show_subscription_channels: Failed to create keyboard for user {user.id}: {kb_err}", exc_info=True)
        return

    message_object = event if isinstance(event, Message) else event.message
    if isinstance(event, CallbackQuery) and not message_object:
        logger.warning(f"show_subscription_channels: message_object is None for callback user {user.id}, event type={type(event)}")
        return

    logger.info(f"show_subscription_channels: Sending message to user {user.id}, is_callback={isinstance(event, CallbackQuery)}")
    
    try:
        if isinstance(event, CallbackQuery):
            await message_object.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
        else:
            # Для /start отправляем отдельным сообщением в чат, чтобы исключить проблемы с reply/answer.
            await bot.send_message(user.id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        logger.info(f"show_subscription_channels: Message sent successfully for user {user.id}")
    except Exception as e:
        logger.warning(f"show_subscription_channels: Could not edit/send subscription message for stage {stage}, user {user.id}: {e}. Sending new.")
        try:
            await bot.send_message(user.id, text, reply_markup=reply_markup)
            logger.info(f"show_subscription_channels: Sent new message via send_message for user {user.id}")
            if isinstance(event, CallbackQuery) and event.message:
                try: await event.message.delete()
                except Exception: pass
        except Exception as e2:
            logger.error(f"show_subscription_channels: Failed to send new subscription message for stage {stage}, user {user.id}: {e2}", exc_info=True)
            return

    await state.set_state(state_to_set)
    logger.info(f"show_subscription_channels: Set state {state_to_set} for user {user.id}")


# --- Обработчик кнопки перепроверки подписки ---
# Фильтр теперь ловит коллбэки вида "recheck_sub_{check_type}_stage_{stage}"
@router.callback_query(F.data.startswith("recheck_sub_"))
async def recheck_subscription_handler(callback: CallbackQuery, bot: Bot, session: AsyncSession, state: FSMContext, config: Config):
    user_id = callback.from_user.id
    user = callback.from_user
    message = callback.message # Запоминаем исходное сообщение
    is_premium = user.is_premium if user else False
    user_username = user.username or ""

    # --- Извлекаем тип и этап из callback_data ---
    try:
        parts = callback.data.split("_")
        check_type = parts[2] # 'start' или 'withdraw'
        stage = int(parts[4]) # 1 или 2
    except (IndexError, ValueError):
        logger.error(f"Invalid recheck callback data format: {callback.data} for user {user_id}")
        await callback.answer("Ошибка формата кнопки проверки.", show_alert=True)
        return

    logger.info(f"User {user_id} initiated recheck for type '{check_type}', stage {stage}.")

    # --- Получаем каналы для проверки И для показа для ТЕКУЩЕГО этапа ---
    if stage == 1:
        channels_to_check = await db.get_filtered_start_channels(session, is_premium)
        channels_to_show = await db.get_filtered_start_channels_all(session, is_premium)
    elif stage == 2:
        channels_to_check = await db.get_filtered_second_stage_channels(session, is_premium)
        channels_to_show = await db.get_filtered_second_stage_channels_all(session, is_premium)
    else: # На случай непредвиденного этапа
        logger.warning(f"Invalid stage {stage} in recheck_subscription_handler for user {user_id}")
        await callback.answer(f"Ошибка: Неизвестный этап проверки {stage}.", show_alert=True)
        return

    # --- Проверяем подписку на обязательные каналы ТЕКУЩЕГО этапа ---
    all_subscribed_current_stage = True
    failed_channels_current_stage = []
    if channels_to_check:
        for channel in channels_to_check:
            try:
                is_subscribed = await check_member_with_delay(bot, channel.channel_id, user_id)
                if not is_subscribed:
                    all_subscribed_current_stage = False
                    failed_channels_current_stage.append(channel)
            except Exception as e:
                logger.error(f"Error checking STAGE {stage} subscription during recheck for channel {channel.channel_id} user {user_id}: {e}")
                all_subscribed_current_stage = False
                if channel not in failed_channels_current_stage:
                    failed_channels_current_stage.append(channel)

    # --- Stage 2: дополнительно проверяем SubGram ---
    sg_sponsors_remaining = []
    if stage == 2:
        try:
            sg_status, sg_sponsors = await request_subgram_sponsors(
                user_id=user_id,
                chat_id=user_id,
                first_name=user.first_name,
                username=user.username,
                language_code=user.language_code,
                is_premium=is_premium,
                action="subscribe"
            )
            if sg_status == 'warning' and sg_sponsors:
                sg_sponsors_remaining = sg_sponsors
                logger.info(f"User {user_id} has {len(sg_sponsors_remaining)} SubGram sponsors remaining in Stage 2.")
        except Exception as e:
            logger.error(f"SubGram check error during Stage 2 recheck for user {user_id}: {e}", exc_info=True)

    if not all_subscribed_current_stage or sg_sponsors_remaining:
        logger.info(f"User {user_id} FAILED recheck for stage {stage} (local={not all_subscribed_current_stage}, subgram={len(sg_sponsors_remaining)}).")
        await callback.answer("Вы не подписаны на все каналы! Пожалуйста, проверьте еще раз.", show_alert=True)
        await show_subscription_channels(
            callback, state, bot, session, stage,
            failed_channels=failed_channels_current_stage,
            subgram_sponsors=sg_sponsors_remaining if stage == 2 else None
        )
        return

    logger.info(f"User {user_id} PASSED recheck for stage {stage}.")

    if stage == 1:
        channels_stage2_exists = await db.get_filtered_second_stage_channels_all(session, is_premium)
        sg_for_stage2 = []
        try:
            sg_status, sg_sponsors = await request_subgram_sponsors(
                user_id=user_id, chat_id=user_id,
                first_name=user.first_name, username=user.username,
                language_code=user.language_code, is_premium=is_premium,
                action="subscribe"
            )
            if sg_status == 'warning' and sg_sponsors:
                sg_for_stage2 = sg_sponsors
        except Exception:
            pass
        if channels_stage2_exists or sg_for_stage2:
            logger.info(f"User {user_id} passed stage 1, proceeding to stage 2.")
            await callback.answer("✅ Осталось совсем немного каналов!", show_alert=False)
            await show_subscription_channels(callback, state, bot, session, stage=2, subgram_sponsors=sg_for_stage2)
            return
        else:
            logger.info(f"User {user_id} passed stage 1, no stage 2 found. All checks complete.")

    # --- Финальная часть: выполняется после успешного прохождения ПОСЛЕДНЕГО этапа ---
    logger.info(f"User {user_id} PASSED ALL subscription stages.")
    await state.clear() # Сбрасываем состояние FSM

    try:
        await callback.answer("✅ Спасибо за подписку!", show_alert=False)
        # Удаляем сообщение с кнопкой проверки ПОСЛЕДНЕГО этапа
        if message: # Убедимся, что message существует
             try:
                 await message.delete()
                 logger.info(f"Deleted final subscription prompt message for user {user_id}.")
             except Exception as e_del_final:
                  logger.warning(f"Could not delete final subscription prompt message for user {user_id}: {e_del_final}")
    except Exception as e_success_callback:
        logger.warning(f"Could not answer final callback for user {user_id}: {e_success_callback}")

    # --- Показываем главное меню ---
    user_data_final = await db.get_user(session, user_id) # Получаем актуальные данные
    if user_data_final:
        await show_main_menu(callback, user_data_final, config, bot) # Используем функцию показа меню
    else:
        logger.error(f"Cannot show main menu for user {user_id}: user data not found after recheck.")


    # --- ЛОГИКА НАЧИСЛЕНИЯ РЕФЕРАЛЬНОГО БОНУСА (ТОЛЬКО ПОСЛЕ ВСЕХ ЭТАПОВ) ---
    # Проверяем тип проверки, чтобы бонус начислялся только при стартовой ОП
    if check_type == 'start':
        user_data_for_bonus = await db.get_user(session, user_id) # Еще раз получаем данные на всякий случай
        if user_data_for_bonus and not user_data_for_bonus.ref_bonus: # Проверяем флаг ref_bonus
            logger.info(f"Attempting to set ref_bonus=True and grant reward for user {user_id} after ALL STAGES passed.")
            bonus_set_prepared = await db.set_user_ref_bonus_passed(session, user_id)

            ref_id = user_data_for_bonus.refferal_id
            reward_info = ""
            ref_notification_needed = False
            reward = None
            commit_needed = False

            if bonus_set_prepared:
                commit_needed = True
                logger.info(f"Prepared ref_bonus=True update for user {user_id}.")

                if ref_id:
                    reward = await db.get_reward(session)
                    if reward and reward > 0:
                        try:
                            # Подготавливаем начисление баланса рефереру
                            await db.add_balance(session, ref_id, reward)
                            logger.info(f"Prepared adding {reward} stars to referrer {ref_id} for user {user_id}.")

                            # Подготавливаем инкремент счетчиков рефералов
                            referral_counts_incremented = await db.increment_referral_counts(session, ref_id)
                            if referral_counts_incremented:
                                logger.info(f"Prepared incrementing referral counts for referrer {ref_id}.")
                            else:
                                logger.warning(f"Failed to prepare incrementing referral counts for referrer {ref_id}.")

                            reward_info = f"🌟 Рефереру подготовлено: {reward}⭐️"
                            ref_notification_needed = True
                        except Exception as e_reward:
                            logger.error(f"Failed to prepare add_balance/increment_counts for referrer {ref_id}: {e_reward}", exc_info=True)
                            reward_info = "⚠️ Ошибка подготовки начисления награды рефереру."
                            commit_needed = False # Отменяем коммит, если награду не подготовили
                            ref_notification_needed = False
                    else:
                        reward_info = f"ℹ️ Награда рефереру не начислена (сумма награды: {reward})."
                else:
                    reward_info = "ℹ️ Пользователь пришел без реферера." # Или по инд. ссылке

            else: # Если не удалось подготовить bonus_set_prepared
                 logger.warning(f"Failed to prepare ref_bonus=True for user {user_id} after ALL STAGES passed.")
                 reward_info = "⚠️ Ошибка подготовки установки флага ref_bonus."
                 commit_needed = False

            # --- Коммит изменений (флаг + награда + счетчики) ---
            if commit_needed:
                try:
                    await session.commit()
                    logger.info(f"Successfully committed ref_bonus=True and potentially ref reward/counts for user {user_id}.")
                    if ref_notification_needed and ref_id:
                        # Получаем обновленный баланс реферера для лога/уведомления
                        try:
                            updated_ref_data = await db.get_user(session, ref_id)
                            updated_balance = updated_ref_data.balance if updated_ref_data else "N/A"
                            reward_info += f" (Новый баланс реферера: {updated_balance}⭐️)"
                        except Exception as e_get_ref:
                             logger.warning(f"Could not get updated referrer balance for log: {e_get_ref}")
                except Exception as e_commit:
                    logger.error(f"Failed to commit ref_bonus/reward/counts changes for user {user_id}: {e_commit}", exc_info=True)
                    await session.rollback()
                    reward_info += " ⚠️ Ошибка сохранения изменений в БД."
                    ref_notification_needed = False # Не отправляем уведомление, если коммит не удался
            else:
                 await session.rollback() # Откатываем, если коммит не требовался (из-за ошибок подготовки)
                 logger.warning(f"Rolling back session for user {user_id} due to preparation errors.")


            # --- Логирование результата операции с бонусом ---
            try:
                bonus_set_final_status = "Установлен ✅" if bonus_set_prepared and commit_needed and "Ошибка сохранения" not in reward_info else "Ошибка установки/сохранения ❌"
                source_info_op = "Неизвестен"
                # Получаем данные пользователя еще раз для актуальной информации об источнике
                final_user_data = await db.get_user(session, user_id)
                if final_user_data:
                    if final_user_data.refferal_id:
                         ref_data_op = await db.get_user(session, final_user_data.refferal_id)
                         ref_username_op = ref_data_op.username if ref_data_op else f"id:{final_user_data.refferal_id}"
                         source_info_op = f"Реферер: [{final_user_data.refferal_id}] (@{ref_username_op})"
                    elif final_user_data.individual_link_id:
                         link_op = await db.get_individual_link_by_id(session, final_user_data.individual_link_id)
                         source_info_op = f"Инд. ссылка: [{link_op.identifier if link_op else 'UNKNOWN'}] ({final_user_data.individual_link_id})"
                    else:
                         source_info_op = "Без источника"

                log_message_op = (
                    f"🏁 <b>Пользователь прошел ВСЕ этапы ОП (Start)!</b>\n\n"
                    f"📌 ID: <code>{user_id}</code>\n"
                    f"👤 Пользователь: @{user_username}\n\n"
                    f"🔗 Источник: {source_info_op}\n"
                    f"🚩 Статус флага ref_bonus: {bonus_set_final_status}\n"
                )
                if reward_info:
                    log_message_op += f"\n{reward_info}"
                await bot.send_message(config.logs_id, log_message_op)
            except Exception as e_log_op:
                logger.error(f"Error sending final OP Start log for user {user_id}: {e_log_op}")

            # --- Уведомление рефереру (если нужно и успешно) ---
            if ref_notification_needed and ref_id and reward and reward > 0:
                 try:
                     await bot.send_message(
                         ref_id,
                         "🎉 <b>У вас новый реферал!</b>\n\n"
                         f"💫 Пользователь: @{user_username}\n\n"
                         f"- На ваш баланс была начислена награда: {reward}⭐️"
                     )
                     logger.info(f"Sent final OP Start notification to referrer {ref_id}")
                 except Exception as e_notify:
                     logger.error(f"Failed to send final OP Start notification to referrer {ref_id}: {e_notify}")
        elif user_data_for_bonus and user_data_for_bonus.ref_bonus:
             logger.info(f"User {user_id} already had ref_bonus=True. No reward action needed after passing stages.")
        elif not user_data_for_bonus:
             logger.error(f"Cannot process ref_bonus for user {user_id}: user data not found.")

    # --- Логика для типа 'withdraw' (если она нужна после речека) ---
    elif check_type == 'withdraw':
        try:
            # Просто уведомляем, что проверка пройдена
            await callback.message.answer("✅ Проверка для вывода пройдена. Теперь вы можете попробовать вывести средства снова.")
            logger.info(f"User {user_id} passed 'withdraw' recheck.")
        except Exception as e_withdraw:
            logger.error(f"Error sending withdraw success message after recheck for user {user_id}: {e_withdraw}")

@router.callback_query(F.data == "flyer_task_complete", StateFilter(FlyerState.task_id))
async def handle_flyer_task_complete(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession):
    """Обработчик кнопки выполнения задания FlyerAPI"""
    user = callback.from_user
    user_id = user.id
    is_premium = user.is_premium
    chat_id = callback.message.chat.id
    
    # Получаем данные о задании
    data = await state.get_data()
    task_id = data.get("task_id")
    signature = data.get("signature")
    
    # Логирование данных состояния
    logger.info(f"FlyerAPI данные состояния для пользователя {user_id}: {data}")
    
    if not task_id or not signature:
        await callback.answer("❌ Ошибка: данные задания не найдены", show_alert=True)
        await state.clear()
        return
    
    logger.info(f"FlyerAPI task completion attempt: user={user_id}, task_id={task_id}")
    
    try:
        # Проверяем выполнение задания через API
        flyer = Flyer(FLYER_API_KEY)
        verification_result = await flyer.check_task(
            user_id=user_id,
            signature=signature
        )
        
        # Подробное логирование результата проверки
        logger.info(f"FlyerAPI результат проверки для пользователя {user_id}, задание {task_id}: {verification_result}")
        
        # Преобразуем результат в словарь, если он строка
        if isinstance(verification_result, str):
            try:
                # Проверка статуса задания (если ответ просто строка 'complete' или 'incomplete')
                if verification_result == 'complete' or verification_result == 'waiting':
                    # Задание выполнено успешно
                    reward = 0.25  # Фиксированная награда
                    await db.add_balance(session, user_id, reward)
                    await session.commit()
                    
                    await callback.answer(f"✅ Задание выполнено! Награда: {reward:.2f}⭐️", show_alert=True)
                    
                    # Удаляем текущее сообщение и показываем новое задание
                    try:
                        await callback.message.delete()
                    except Exception as e:
                        logger.error(f"Error deleting FlyerAPI task message: {e}")
                    
                    # Получаем все доступные задания от Flyer API
                    first_name = user.first_name
                    language_code = user.language_code
                    
                    try:
                        flyer_tasks = await flyer.get_tasks(
                            user_id=user_id,
                            language_code=language_code,
                            limit=10
                        )
                        
                        logger.info(f"FlyerAPI получен список заданий после выполнения: {flyer_tasks}")
                        
                        # Находим первое невыполненное задание
                        next_task = None
                        for task in flyer_tasks:
                            if task.get('status') == 'incomplete':
                                next_task = task
                                break
                        
                        if next_task:
                            # Сохраняем информацию о следующем задании в FSM
                            await state.set_state(FlyerState.task_id)
                            await state.update_data(
                                task_id=next_task.get('signature', ''),
                                signature=next_task.get('signature', '')
                            )
                            
                            # Формируем сообщение с заданием
                            task_title = next_task.get('name', 'Задание')
                            task_action = next_task.get('task', 'Подписаться на канал')
                            
                            if task_action == 'give boost':
                                task_details_message = f"""🎯 <b>Доступно задание!</b>

📌 Перейдите по ссылке и проголосуйте за <b><a href="{_get_flyer_task_link(next_task) or '#'}">канал</a></b>.

<b>🏆 Награда: 0.25⭐️</b>"""
                            else:
                                task_details_message = f"""🎯 <b>Доступно задание!</b>

📌 Подпишитесь на <b><a href="{_get_flyer_task_link(next_task) or '#'}">канал</a></b> и не отписывайтесь в течение 7 дней.

<b>🏆 Награда: 0.25⭐️</b>"""

                            # Создаем клавиатуру с кнопками
                            keyboard = InlineKeyboardBuilder()
                            if task_action == 'give boost':
                                if _get_flyer_task_link(next_task):
                                    keyboard.button(text="🔗 Голосовать x3", url=_get_flyer_task_link(next_task))
                                keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                                keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                                keyboard.adjust(1)
                                markup = keyboard.as_markup()
                                await bot.send_photo(
                                    chat_id, 
                                    caption=task_details_message, 
                                    photo=FSInputFile("bot/assets/images/quests.jpg"), 
                                    reply_markup=markup
                                )
                                return
                            else:
                                if _get_flyer_task_link(next_task):
                                    keyboard.button(text="🔗 Подписаться", url=_get_flyer_task_link(next_task))
                                keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                                keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                                keyboard.adjust(1)
                                markup = keyboard.as_markup()
                                await bot.send_photo(
                                    chat_id, 
                                    caption=task_details_message, 
                                    photo=FSInputFile("bot/assets/images/quests.jpg"), 
                                    reply_markup=markup
                                )
                                return

                    except Exception as e:
                        logger.error(f"Error fetching next Flyer task: {e}", exc_info=True)
                    
                    # Если не удалось получить следующее незавершенное задание, вызываем стандартный обработчик
                    await state.clear()
                    await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
                    return
                elif verification_result == 'incomplete':
                    await callback.answer("❌ Задание ещё не выполнено", show_alert=True)
                    return
                
                # Если не строка статуса, пробуем распарсить как JSON
                import json
                verification_result = json.loads(verification_result)
                logger.info(f"FlyerAPI преобразованный ответ: {verification_result}")
            except json.JSONDecodeError:
                logger.error(f"Не удалось преобразовать строковый ответ в JSON: {verification_result}")
                
                # Если не удалось преобразовать, проверяем ключевые слова в строке
                if 'complete' or 'waiting' in verification_result.lower():
                    # Задание выполнено успешно
                    reward = 0.25  # Фиксированная награда
                    await db.add_balance(session, user_id, reward)
                    await session.commit()
                    
                    await callback.answer(f"✅ Задание выполнено! Награда: {reward:.2f}⭐️", show_alert=True)
                    
                    # Удаляем текущее сообщение и показываем новое задание
                    try:
                        await callback.message.delete()
                    except Exception as e:
                        logger.error(f"Error deleting FlyerAPI task message: {e}")
                    
                    # Очищаем состояние и показываем следующее задание
                    await state.clear()
                    await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
                    return
                else:
                    await callback.answer("❌ Задание не выполнено", show_alert=True)
                    return
        
        # Если ответ уже в формате словаря
        if isinstance(verification_result, dict) and verification_result.get("success", False):
            # Задание выполнено успешно
            reward = 0.25  # Фиксированная награда
            await db.add_balance(session, user_id, reward)
            await session.commit()
            
            await callback.answer(f"✅ Задание выполнено! Награда: {reward:.2f}⭐️", show_alert=True)
            
            # Удаляем текущее сообщение и показываем новое задание
            try:
                await callback.message.delete()
            except Exception as e:
                logger.error(f"Error deleting FlyerAPI task message: {e}")
            
            # Получаем все доступные задания от Flyer API
            first_name = user.first_name
            language_code = user.language_code
            
            try:
                flyer_tasks = await flyer.get_tasks(
                    user_id=user_id,
                    language_code=language_code,
                    limit=5
                )
                
                logger.info(f"FlyerAPI получен список заданий после выполнения: {flyer_tasks}")
                
                # Находим первое невыполненное задание
                next_task = None
                for task in flyer_tasks:
                    if task.get('status') == 'incomplete':
                        next_task = task
                        break
                
                if next_task:
                    # Сохраняем информацию о следующем задании в FSM
                    await state.set_state(FlyerState.task_id)
                    await state.update_data(
                        task_id=next_task.get('signature', ''),
                        signature=next_task.get('signature', '')
                    )
                    
                    # Формируем сообщение с заданием
                    task_title = next_task.get('name', 'Задание')
                    task_action = next_task.get('task', 'Подписаться на канал')
                    
                    # Проверяем тип задания
                    if task_action == 'give boost':
                        task_details_message = f"""🎯 <b>Доступно задание!</b>

📌 Подпишитесь на <b><a href="{_get_flyer_task_link(next_task) or '#'}">канал</a></b> и проголосуйте за него.

<b>🏆 Награда: 0.25⭐️</b>"""
                    else:
                        task_details_message = f"""🎯 <b>Доступно задание!</b>

📌 Подпишитесь на <b><a href="{_get_flyer_task_link(next_task) or '#'}">канал</a></b> и не отписывайтесь в течение 7 дней.

<b>🏆 Награда: 0.25⭐️</b>"""

                    # Создаем клавиатуру с кнопками
                    keyboard = InlineKeyboardBuilder()
                    
                    if _get_flyer_task_link(next_task):
                        keyboard.button(text="🔗 Перейти в канал", url=_get_flyer_task_link(next_task))
                    
                    keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                    keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                    keyboard.adjust(1)
                    markup = keyboard.as_markup()
                    
                    # Отправляем следующее задание
                    await bot.send_photo(
                        chat_id, 
                        caption=task_details_message, 
                        photo=FSInputFile("bot/assets/images/quests.jpg"), 
                        reply_markup=markup
                    )
                    return
            except Exception as e:
                logger.error(f"Error fetching next Flyer task: {e}", exc_info=True)
            
            # Если не удалось получить следующее незавершенное задание, вызываем стандартный обработчик
            await state.clear()
            await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)
        elif isinstance(verification_result, dict):
            error_message = verification_result.get("error", "Не удалось проверить выполнение задания")
            await callback.answer(f"❌ {error_message}", show_alert=True)
        else:
            await callback.answer("❌ Задание не выполнено", show_alert=True)
    except Exception as e:
        logger.error(f"Error verifying FlyerAPI task for user {user_id}: {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при проверке выполнения задания", show_alert=True)

@router.callback_query(F.data == "flyer_task_skip", StateFilter(FlyerState.task_id))
async def handle_flyer_task_skip(callback: CallbackQuery, state: FSMContext, bot: Bot, session: AsyncSession):
    """Обработчик кнопки пропуска задания FlyerAPI"""
    user = callback.from_user
    user_id = user.id
    is_premium = user.is_premium
    chat_id = callback.message.chat.id
    
    logger.info(f"FlyerAPI task skipped by user {user_id}")
    
    await callback.answer("⏭️ Задание пропущено")
    
    # Удаляем текущее сообщение
    try:
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Error deleting FlyerAPI task message: {e}")
    
    # Пробуем получить следующее незавершенное задание от FlyerAPI
    try:
        flyer = Flyer(FLYER_API_KEY)
        flyer_tasks = await flyer.get_tasks(
            user_id=user_id,
            language_code=user.language_code,
            limit=5
        )
        
        logger.info(f"FlyerAPI получен список заданий после пропуска: {flyer_tasks}")
        
        # Находим первое незавершенное задание
        current_data = await state.get_data()
        current_signature = current_data.get("signature", "")
        
        next_task = None
        found_current = False
        
        # Пытаемся найти следующее незавершенное задание после текущего
        for task in flyer_tasks:
            if found_current and task.get('status') == 'incomplete':
                next_task = task
                break
            
            if task.get('signature') == current_signature:
                found_current = True
        
        # Если не нашли задание после текущего, берем первое незавершенное с начала списка
        if not next_task:
            for task in flyer_tasks:
                if task.get('status') == 'incomplete' and task.get('signature') != current_signature:
                    next_task = task
                    break
        
        if next_task:
            # Сохраняем информацию о следующем задании в FSM
            await state.set_state(FlyerState.task_id)
            await state.update_data(
                task_id=next_task.get('signature', ''),
                signature=next_task.get('signature', '')
            )
            
            # Формируем сообщение с заданием
            task_title = next_task.get('name', 'Задание')
            task_action = next_task.get('task', 'Подписаться на канал')
            
            # Проверяем тип задания
            if task_action == 'give boost':
                task_details_message = f"""🎯 <b>Доступно задание!</b>\n\n📌 Перейдите по ссылке и проголосуйте за <b><a href="{_get_flyer_task_link(next_task) or '#'}">канал</a></b>.

<b>🏆 Награда: 0.25⭐️</b>"""
            else:
                task_details_message = f"""🎯 <b>Доступно задание!</b>\n\n📌 Подпишитесь на <b><a href="{_get_flyer_task_link(next_task) or '#'}">канал</a></b> и не отписывайтесь в течение 7 дней.

<b>🏆 Награда: 0.25⭐️</b>"""

                            # Создаем клавиатуру с кнопками
            keyboard = InlineKeyboardBuilder()
            if task_action == 'give boost':
                if _get_flyer_task_link(next_task):
                    keyboard.button(text="🔗 Голосовать x3", url=_get_flyer_task_link(next_task))
                keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                keyboard.adjust(1)
                markup = keyboard.as_markup()
                await bot.send_photo(
                    chat_id, 
                    caption=task_details_message, 
                    photo=FSInputFile("bot/assets/images/quests.jpg"), 
                    reply_markup=markup
                )
                return
            else:
                if _get_flyer_task_link(next_task):
                    keyboard.button(text="🔗 Подписаться", url=_get_flyer_task_link(next_task))
                keyboard.button(text="✅ Я выполнил", callback_data="flyer_task_complete")
                keyboard.button(text="⏩ Пропустить", callback_data="flyer_task_skip")
                keyboard.adjust(1)
                markup = keyboard.as_markup()

                await bot.send_photo(
                        chat_id, 
                        caption=task_details_message, 
                        photo=FSInputFile("bot/assets/images/quests.jpg"), 
                        reply_markup=markup
                        )
                return
    except Exception as e:
        logger.error(f"Error fetching next Flyer task after skip for user {user_id}: {e}", exc_info=True)
    
    # Если не удалось получить следующее незавершенное задание, вызываем стандартный обработчик
    await state.clear()
    await send_task_message(chat_id, user_id, is_premium, session, bot, state, user.first_name, user.language_code)

@router.callback_query(F.data=='check_bio_link')
async def check_bio_link(callback: CallbackQuery, session: AsyncSession, bot: Bot, config: Config):
    user = callback.from_user
    user_id = user.id
    
    try:
        # Получаем информацию о пользователе через Telegram API
        chat_info = await bot.get_chat(user_id)
        user_bio = chat_info.bio if hasattr(chat_info, 'bio') and chat_info.bio else ""
        logger.info(f'User {user_id} bio: {user_bio}')
        
        bot_info = await bot.get_me()
        expected_ref_link = f"t.me/{bot_info.username}?start={user_id}"
        
        # Проверяем, содержит ли био реферальную ссылку
        if expected_ref_link in user_bio:
            # Проверяем, можно ли получить награду (прошло ли 24 часа)
            user_data = await db.get_user(session, user_id)
            last_bio_reward = user_data.last_bio_reward_date
            now = datetime.now()
            
            can_claim = True
            if last_bio_reward:
                time_diff = now - last_bio_reward
                if time_diff.total_seconds() < 24 * 3600:
                    hours_left = 24 - (time_diff.total_seconds() / 3600)
                    can_claim = False
                    
                    # Отправляем новое сообщение вместо alert
                    await callback.message.answer(
                        f"⏰ <b>Награда уже получена сегодня!</b>\n\n"
                        f"🕐 Следующая награда будет доступна через: <b>{int(hours_left)} ч. {int((hours_left % 1) * 60)} мин.</b>\n\n"
                        f"💡 <i>Не забудьте вернуться позже!</i>",
                        parse_mode="HTML"
                    )
                    return
            
            if can_claim:
                # Выдаем награду
                reward = 0.5
                await db.add_balance(session, user_id, reward)
                
                # Обновляем время последней награды за био
                await db.update_user_bio_reward_date(session, user_id, now)

                # Отправляем лог в админ чат
                try:
                    user = await db.get_user(session, user_id)
                    username = user.username if user.username else "Неизвестно"
                    admin_log_text = f"""📋 <b>Ежедневное задание выполнено</b>

👤 <b>Пользователь:</b> @{username} (ID: {user_id})
 <b>Награда:</b> {reward}⭐️"""

                    await bot.send_message(
                        chat_id=config.logs_id,
                        text=admin_log_text,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки лога в админ чат для bio task пользователя {user_id}: {e}")

                # Отправляем сообщение о успехе
                await callback.answer(
                    f"🎉 Отлично! Реферальная ссылка найдена! + {reward}⭐️",
                    # parse_mode="HTML"
                )
                # Обновляем сообщение с заданиями
                await daily_task(callback, session, bot, config)

            
        else:
            # Отправляем подробное сообщение с инструкцией
            instr = await callback.message.answer(
                "❌ <b>Реферальная ссылка не найдена в био</b>\n\n"
                "📝 <b>Как добавить ссылку:</b>\n"
                "1️. Откройте настройки Telegram\n"
                "2️. Перейдите в раздел 'Изменить профиль'\n"
                "3️. Найдите поле 'О себе'\n"
                f"4️. Добавьте эту ссылку: <code>{expected_ref_link}</code>\n\n"
                "⏱️ <i>После добавления подождите 1-2 минуты и попробуйте снова</i>\n\n",
                parse_mode="HTML"
            )
            await asyncio.sleep(10)
            try:
                await bot.delete_message(callback.message.chat.id, instr.message_id)
            except Exception as e:
                logger.error(f"Failed to delete instruction message: {e}")
            
    except Exception as e:
        logger.error(f"Error checking user bio for user {user_id}: {e}")
        await callback.message.answer(
            "⚠️ <b>Временная ошибка при проверке</b>\n\n"
            "🔄 Попробуйте через несколько минут\n\n"
            "💬 Если проблема повторяется, обратитесь в поддержку",
            parse_mode="HTML"
        )

# Обработчики для выбора подарков


# @router.callback_query(F.data == "gift_auto_select")
# async def process_gift_auto_selection(callback: CallbackQuery, session: AsyncSession, state: FSMContext):
#     """Обработчик автоматического выбора подарка"""
#     user = await db.get_user(session, callback.from_user.id)
#     balance = user.balance
    
#     # Список подарков отсортированный по стоимости (от большего к меньшему)
#     gifts = [
#         {"id": "5170521118301225164", "emoji": "💎", "cost": 100, "name": "Алмаз"},
#         {"id": "5168043875654172773", "emoji": "🏆", "cost": 100, "name": "Кубок"},
#         {"id": "5170690322832818290", "emoji": "💍", "cost": 100, "name": "Кольцо"},
#         {"id": "6028601630662853006", "emoji": "🍾", "cost": 50, "name": "Шампанское"},
#         {"id": "5170564780938756245", "emoji": "🚀", "cost": 50, "name": "Ракета"},
#         {"id": "5170314324215857265", "emoji": "💐", "cost": 50, "name": "Букет"},
#         {"id": "5170144170496491616", "emoji": "🎂", "cost": 50, "name": "Торт"},
#         {"id": "5168103777563050263", "emoji": "🌹", "cost": 25, "name": "Роза"},
#         {"id": "5170250947678437525", "emoji": "🎁", "cost": 25, "name": "Подарок"},
#         {"id": "5170145012310081615", "emoji": "💝", "cost": 15, "name": "Подарок"},
#         {"id": "5170233102089322756", "emoji": "🧸", "cost": 15, "name": "Плюшевый мишка"}
#     ]
    
#     # Получаем количество предыдущих выводов
#     previous_withdraws = await db.get_user_successful_withdraws_count(session, callback.from_user.id)
#     user_referrals = await db.get_refferals_count(session, callback.from_user.id)
    
#     # Находим самый дорогой подарок, который можем себе позволить по балансу и рефералам
#     selected_gift = None
#     for gift in gifts:
#         if balance >= gift["cost"]:
#             required_referrals = calculate_required_referrals(gift["cost"], previous_withdraws)
#             if user_referrals >= required_referrals:
#                 selected_gift = gift
#                 break
    
#     if not selected_gift:
#         # Проверяем, есть ли подарки по балансу, но не хватает рефералов
#         affordable_gifts = [gift for gift in gifts if balance >= gift["cost"]]
#         if affordable_gifts:
#             cheapest_affordable = affordable_gifts[-1]  # Самый дешевый из доступных по балансу
#             required_referrals = calculate_required_referrals(cheapest_affordable["cost"], previous_withdraws)
#             await callback.answer(
#                 f"❌ Недостаточно рефералов!\n"
#                 f"Для самого дешевого подарка ({cheapest_affordable['cost']}⭐)\n"
#                 f"Нужно: {required_referrals} рефералов\n"
#                 f"У вас: {user_referrals} рефералов\n"
#                 f"Пригласите еще {required_referrals - user_referrals} друзей!",
#                 show_alert=True
#             )
#         else:
#             await callback.answer("❌ Недостаточно звезд для покупки подарков", show_alert=True)
#         return
    
#     # Сохраняем выбранный подарок в состоянии
#     await state.update_data(
#         sum=selected_gift["cost"],
#         gift_id=selected_gift["id"],
#         gift_emoji=selected_gift["emoji"],
#         gift_name=selected_gift["name"]
#     )
    
#     required_referrals = calculate_required_referrals(selected_gift["cost"], previous_withdraws)
    
#     # Удаляем сообщение с фото и отправляем новое текстовое
#     await callback.message.delete()
#     await callback.message.answer(
#         f'Автоматически выбран: {selected_gift["emoji"]} {selected_gift["name"]} ({selected_gift["cost"]}⭐)\n'
#         f'✅ Рефералов: {user_referrals}/{required_referrals} (вывод #{previous_withdraws + 1})\n\n'
#         f'Введите юзернейм для отправки подарка:'
#     )
#     await state.set_state(st.WithdrawState.username)






