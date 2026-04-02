#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import platform
from pyrogram import Client
from pyrogram.raw import functions, types
from pyrogram import utils
import logging
import json
import random
import time
import os
from typing import List, Dict, Tuple, Optional
from datetime import datetime

# Настройка для Windows
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфигурация из .env
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = "gift_sender"

# Доступные подарки
AVAILABLE_GIFTS = {
    "5170145012310081615": {"emoji": "💝", "stars": 15, "name": "Сердце"},
    "5170233102089322756": {"emoji": "🧸", "stars": 15, "name": "Плюшевый мишка"},
    "5170250947678437525": {"emoji": "🎁", "stars": 25, "name": "Коробка с подарком"},
    "5168103777563050263": {"emoji": "🌹", "stars": 25, "name": "Роза"},
    "5170144170496491616": {"emoji": "🎂", "stars": 50, "name": "Торт"},
    "5170314324215857265": {"emoji": "💐", "stars": 50, "name": "Букет"},
    "5170564780938756245": {"emoji": "🚀", "stars": 50, "name": "Ракета"},
    "6028601630662853006": {"emoji": "🍾", "stars": 50, "name": "Шампанское"},
    "5168043875654172773": {"emoji": "🏆", "stars": 100, "name": "Кубок"},
    "5170690322832818290": {"emoji": "💍", "stars": 100, "name": "Кольцо"},
    "5170521118301225164": {"emoji": "💎", "stars": 100, "name": "Алмаз"}
}

class GiftSender:
    """Класс для отправки подарков через Telegram Stars"""
    
    def __init__(self, client: Client):
        self.client = client
    
    async def send_gift(self, user_id, gift_id: str, message_text: str = None, is_private: bool = True):
        """
        Отправить подарок пользователю
        
        Args:
            user_id: ID пользователя или username
            gift_id: ID подарка из AVAILABLE_GIFTS
            message_text: Текст сообщения к подарку
            is_private: Приватный подарок или нет
            
        Returns:
            bool: True если подарок отправлен успешно
        """
        try:
            if gift_id not in AVAILABLE_GIFTS:
                logger.error(f"Неизвестный gift_id: {gift_id}")
                return False
            
            peer = await self.client.resolve_peer(user_id)
            
            # Устанавливаем комментарий к подарку
            gift_message = "Бесплатно от @Fastfreestarsbot"
            text, entities = (await utils.parse_text_entities(
                self.client, gift_message, None, None
            )).values()
            text_entities = types.TextWithEntities(text=text, entities=entities or [])
            
            invoice = types.InputInvoiceStarGift(
                peer=peer,
                gift_id=int(gift_id),
                hide_name=is_private,
                include_upgrade=False,
                message=text_entities
            )
            
            form = await self.client.invoke(
                functions.payments.GetPaymentForm(invoice=invoice)
            )
            
            result = await self.client.invoke(
                functions.payments.SendStarsForm(
                    form_id=form.form_id,
                    invoice=invoice
                )
            )
            
            if result:
                gift_info = AVAILABLE_GIFTS[gift_id]
                logger.info(f"Подарок отправлен: {gift_info['name']} ({gift_info['stars']} ⭐) -> {user_id}")
                
                # Отправляем дополнительное сообщение после подарка
                follow_up_message = f"Подарок выдал, пожалуйста, оставь отзыв с фото скрином того что я тебе подарил вот тут: https://t.me/FreeStarsNews1/6"
                
                try:
                    await asyncio.sleep(1)  # Небольшая задержка
                    await self.client.send_message(user_id, follow_up_message)
                    logger.info(f"Дополнительное сообщение отправлено пользователю {user_id}")
                except Exception as e:
                    logger.error(f"Ошибка отправки дополнительного сообщения пользователю {user_id}: {e}")
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка отправки подарка: {e}")
            return False

class GiftWithdrawProcessor:
    """Класс для автоматической обработки выплат подарками"""
    
    def __init__(self, gift_sender: GiftSender):
        self.gift_sender = gift_sender
        self.available_gifts = AVAILABLE_GIFTS
        
        # Группируем подарки по номиналам для быстрого доступа
        self.gifts_by_value = {}
        for gift_id, gift_info in AVAILABLE_GIFTS.items():
            value = gift_info['stars']
            if value not in self.gifts_by_value:
                self.gifts_by_value[value] = []
            self.gifts_by_value[value].append((gift_id, gift_info))
    
    def calculate_optimal_gifts(self, amount: int, max_remainder: int = 10) -> Tuple[List[Dict], int]:
        """
        Рассчитывает оптимальную комбинацию подарков для суммы
        
        Args:
            amount: Сумма для выплаты в звездах
            max_remainder: Максимальный остаток для возврата на баланс
            
        Returns:
            Tuple[List[Dict], int]: (список подарков, остаток)
        """
        if amount < 15:
            return [], amount
            
        # Доступные номиналы в порядке убывания
        available_values = sorted(self.gifts_by_value.keys(), reverse=True)
        selected_gifts = []
        remaining_amount = amount
        
        # Жадный алгоритм: берем максимальные номиналы
        for value in available_values:
            while remaining_amount >= value:
                # Выбираем случайный подарок из доступных номиналов
                gift_id, gift_info = random.choice(self.gifts_by_value[value])
                selected_gifts.append({
                    'gift_id': gift_id,
                    'name': gift_info['name'],
                    'emoji': gift_info['emoji'],
                    'stars': gift_info['stars']
                })
                remaining_amount -= value
        
        # Если остаток больше max_remainder, добираем минимальным подарком
        if remaining_amount > max_remainder and 15 in available_values:
            gift_id, gift_info = random.choice(self.gifts_by_value[15])
            selected_gifts.append({
                'gift_id': gift_id,
                'name': gift_info['name'],
                'emoji': gift_info['emoji'],
                'stars': gift_info['stars']
            })
            remaining_amount -= 15
        
        return selected_gifts, max(0, remaining_amount)
    
    async def process_withdraw(self, withdraw_id: str, visual_id: int, user_id: int, amount: int, 
                             session, bot, config, max_remainder: int = 10) -> bool:
        """
        Обрабатывает выплату подарками
        
        Args:
            withdraw_id: Строковый ID заявки на вывод
            user_id: ID пользователя
            amount: Сумма выплаты
            session: Сессия БД
            bot: Экземпляр бота
            config: Конфигурация
            max_remainder: Максимальный остаток
            
        Returns:
            bool: True если обработка прошла успешно
        """
        try:
            from bot.database.requests import get_withdraw, confirm_withdraw, add_balance
            
            # Получаем заявку по строковому ID
            withdraw = await get_withdraw(session, withdraw_id)
            if not withdraw:
                logger.error(f"Заявка {withdraw_id} не найдена")
                return False
            
            # Рассчитываем подарки
            gifts, remainder = self.calculate_optimal_gifts(amount, max_remainder)
            
            if not gifts:
                logger.info(f"Сумма {amount} слишком мала для подарков")
                return False
            
            # Отправляем подарки
            sent_gifts = []
            total_sent_value = 0
            
            for gift in gifts:
                success = await self.gift_sender.send_gift(
                    user_id=user_id,
                    gift_id=gift['gift_id'],
                    is_private=True
                )
                
                if success:
                    sent_gifts.append(gift)
                    total_sent_value += gift['stars']
                    logger.info(f"Отправлен подарок {gift['name']} ({gift['stars']}⭐) пользователю {user_id}")
                else:
                    logger.error(f"Ошибка отправки подарка {gift['name']} пользователю {user_id}")
                    # При ошибке прекращаем отправку
                    break
            
            if not sent_gifts:
                logger.error(f"Не удалось отправить ни одного подарка пользователю {user_id}")
                return False
            
            # Возвращаем остаток на баланс если есть
            if remainder > 0:
                await add_balance(session, user_id, remainder)
                logger.info(f"Возвращен остаток {remainder}⭐ на баланс пользователя {user_id}")
            
            # Обновляем заявку (используем внутренний ID для обновления)
            await self._update_withdraw_success(session, withdraw.id, sent_gifts, remainder)
            
            # Уведомляем пользователя
            await self._notify_user_success(bot, user_id, sent_gifts, remainder, amount)
            
            await self._notify_admins_success(bot, config, user_id, withdraw_id, visual_id, sent_gifts, remainder, amount, session)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обработки выплаты {withdraw_id}: {e}")
            # Для обновления ошибки нужно получить внутренний ID
            try:
                withdraw = await get_withdraw(session, withdraw_id)
                if withdraw:
                    await self._update_withdraw_error(session, withdraw.id, str(e))
            except:
                pass
            await self._notify_admins_error(bot, config, user_id, withdraw_id, amount, str(e), session)
            return False
    
    async def _update_withdraw_success(self, session, withdraw_id: int, gifts: List[Dict], remainder: int):
        """Обновляет заявку при успешной обработке"""
        try:
            # Импортируем здесь чтобы избежать циклических импортов
            from sqlalchemy import update
            from bot.database.models import Withdraws
            
            gift_details = {
                'gifts': gifts,
                'total_gifts': len(gifts),
                'total_value': sum(g['stars'] for g in gifts),
                'remainder': remainder,
                'processed_at': datetime.now().isoformat()
            }
            
            stmt = update(Withdraws).where(Withdraws.id == withdraw_id).values(
                processing_type='auto_gifts',
                gift_details=json.dumps(gift_details, ensure_ascii=False),
                auto_processed_at=datetime.now(),
                remainder_returned=float(remainder),
                withdraw_status=True  # Помечаем как выполненную
            )
            
            await session.execute(stmt)
            await session.commit()
            
        except Exception as e:
            logger.error(f"Ошибка обновления заявки {withdraw_id}: {e}")
    
    async def _update_withdraw_error(self, session, withdraw_id: int, error: str):
        """Обновляет заявку при ошибке обработки"""
        try:
            from sqlalchemy import update
            from bot.database.models import Withdraws
            
            stmt = update(Withdraws).where(Withdraws.id == withdraw_id).values(
                processing_type='requires_manual_processing',
                processing_error=error,
                auto_processed_at=datetime.now()
            )
            
            await session.execute(stmt)
            await session.commit()
            
        except Exception as e:
            logger.error(f"Ошибка обновления заявки с ошибкой {withdraw_id}: {e}")
    
    async def _notify_user_success(self, bot, user_id: int, gifts: List[Dict], remainder: int, original_amount: int):
        """Уведомляет пользователя об успешной выплате"""
        try:
            gifts_text = "\n".join([f"{g['emoji']} {g['name']} ({g['stars']} ⭐)" for g in gifts])
            total_value = sum(g['stars'] for g in gifts)
            
            message = f"✅ <b>Ваша заявка на вывод {original_amount} ⭐ была обработана автоматически!</b>\n\n"
            
            if remainder > 0:
                message += f"<b>Остаток {remainder} ⭐ возвращен на ваш баланс</b>\n\n"
            
            message += "🎉 <b>Спасибо за использование нашего сервиса!</b>"
            
            await bot.send_message(user_id, message, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя {user_id}: {e}")
    
    async def _notify_admins_success(self, bot, config, user_id: int, withdraw_id: str, visual_id: int,
                                    gifts: List[Dict], remainder: int, original_amount: int, session):
        """Обновляет сообщение админов об успешной автоматической выплате"""
        try:
            from bot.database.requests import get_withdraw
            from bot.keyboards import keyboards as kb
            import html
            
            withdraw = await get_withdraw(session, withdraw_id)
            if not withdraw or not withdraw.admin_message_id:
                logger.warning(f"Не найдено сообщение админов для заявки {withdraw_id}")
                return
            
            # Формируем текст с подарками
            gifts_text = "\n".join([f"{gift['name']} ({gift['stars']}⭐)" for gift in gifts])
            remainder_text = f"\n💰 Остаток возвращен на баланс: {remainder}⭐" if remainder > 0 else ""
            
            bot_info = await bot.get_me()
            
            # Обновляем сообщение
            await bot.edit_message_text(
                chat_id=config.withdraw_id,
                message_id=withdraw.admin_message_id,
                text=f'''✅ Запрос на вывод №{visual_id}

👤 Пользователь: {html.escape(withdraw.withdraw_username)} | ID: {user_id}
🔑 Количество: {original_amount}⭐️

🔧 Статус: Подарок отправлен 🎁

🎁 Отправленные подарки:
{gifts_text}{remainder_text}

<a href="https://t.me/+v5fenBe8y2EyZWUy">Основной канал</a> | <a href="https://t.me/+_hJ32Jmtd-lhNDli">Чат</a> | <a href="https://t.me/{bot_info.username}">Бот</a>''',
                reply_markup=kb.withdraw_confirm_keyboard(),
                disable_web_page_preview=True
            )
            
            logger.info(f"Обновлено сообщение админов для заявки {withdraw_id}")
            
        except Exception as e:
            logger.error(f"Ошибка обновления сообщения админов: {e}")

    async def _notify_admins_error(self, bot, config, user_id: int, withdraw_id: str, 
                                  amount: int, error: str, session):
        """Обновляет сообщение админов об ошибке автоматической выплаты"""
        try:
            from bot.database.requests import get_withdraw
            from bot.keyboards import keyboards as kb
            import html
            
            withdraw = await get_withdraw(session, withdraw_id)
            if not withdraw or not withdraw.admin_message_id:
                logger.warning(f"Не найдено сообщение админов для заявки {withdraw_id}")
                return
                
            # Обновляем сообщение с ошибкой
            await bot.edit_message_text(
                chat_id=config.withdraw_id,
                message_id=withdraw.admin_message_id,
                text=f'''❌ Запрос на вывод №{withdraw_id}

👤 Пользователь: {html.escape(withdraw.withdraw_username)} | ID: {user_id}
🔑 Количество: {amount}⭐️

🔧 Статус: Ошибка автоматической обработки ❌

Требуется ручная обработка.''',
                reply_markup=kb.withdraw_admin_keyboard(withdraw.id, amount, withdraw.withdraw_username, user_id, withdraw_id)
            )
            
            logger.info(f"Обновлено сообщение админов с ошибкой для заявки {withdraw_id}")
            
        except Exception as e:
            logger.error(f"Ошибка обновления сообщения админов с ошибкой: {e}")

def create_app():
    """Создание клиента Pyrogram с постоянным именем сессии"""
    return Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH)

def main():
    """Главная функция для тестирования"""
    print("🎁 Gift Sender готов к интеграции")
    
    try:
        app = create_app()
        app.run()
    except KeyboardInterrupt:
        print("\n👋 Остановлено")
    except Exception as e:
        logger.error(f"Ошибка: {e}")

if __name__ == "__main__":
    main() 