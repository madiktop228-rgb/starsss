from __future__ import annotations

import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

from aiohttp import web
from aiohttp.web_request import Request
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram import Bot

from bot.database.requests import (
    save_subgram_webhook,
    get_user,
    minus_balance,
    get_user_subgram_tasks_for_unsubscribe,
    apply_unsubscribe_penalty
)

logger = logging.getLogger(__name__)

import os
ADMIN_NOTIFICATION_ID = int(os.getenv('LOGS_IDSS', '0'))
EXPECTED_API_KEY = os.getenv('SUBGRAM_API_KEY', '')

# Маппинг статусов на эмодзи
STATUS_EMOJI = {
    'subscribed': '✅',
    'unsubscribed': '❌',
    'notgetted': '⚠️'
}

class SubGramWebhookHandler:
    def __init__(self, config, session_maker, bot: Bot = None):
        self.config = config
        self.session_maker = session_maker
        self.bot = bot
        
    async def send_admin_notification(self, message: str, parse_mode: str = 'HTML'):
        """Отправляет уведомление администратору"""
        if not self.bot:
            return
            
        try:
            await self.bot.send_message(
                chat_id=ADMIN_NOTIFICATION_ID,
                text=message,
                parse_mode=parse_mode
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления админу: {e}")
        
    async def send_webhook_notification(self, webhook_data: Dict[str, Any]):
        """Отправляет уведомление админу о новом webhook'е"""
        if not self.bot:
            return
            
        try:
            webhook_id = webhook_data.get('webhook_id')
            user_id = webhook_data.get('user_id')
            status = webhook_data.get('status')
            link = webhook_data.get('link')
            subscribe_date = webhook_data.get('subscribe_date')
            
            status_emoji = STATUS_EMOJI.get(status, '❓')
            
            message = f"""🔔 <b>Новый webhook SubGram</b>

{status_emoji} <b>Статус:</b> {status}
👤 <b>User ID:</b> <code>{user_id}</code>
🔗 <b>Канал:</b> {link}
🆔 <b>Webhook ID:</b> <code>{webhook_id}</code>
📅 <b>Дата:</b> {subscribe_date}"""

            await self.send_admin_notification(message)
            logger.info(f"Уведомление о webhook {webhook_id} отправлено")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления о webhook: {e}")
        
    async def process_webhook(self, request: Request) -> web.Response:
        """Обрабатывает входящий webhook от SubGram"""
        try:
            # Проверка метода запроса
            if request.method != 'POST':
                logger.warning(f"Неправильный метод запроса: {request.method}")
                return web.Response(status=405, text="Method Not Allowed")
            
            # Проверка API ключа
            api_key = request.headers.get('Api-Key')
            if not api_key:
                logger.warning("Отсутствует API ключ")
                return web.Response(status=401, text="Unauthorized: Missing Api-Key")
            
            if api_key != EXPECTED_API_KEY:
                logger.warning(f"Неверный API ключ: {api_key}")
                return web.Response(status=401, text="Unauthorized: Invalid Api-Key")
            
            # Парсинг данных
            try:
                body = await request.text()
                data = json.loads(body)
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON: {e}")
                return web.Response(status=400, text="Bad Request: Invalid JSON")
            
            # Проверка структуры данных
            if 'webhooks' not in data or not isinstance(data['webhooks'], list):
                logger.error("Неверная структура данных webhook'а")
                return web.Response(status=400, text="Bad Request: Invalid webhook structure")
            
            # Обработка webhook'ов
            webhook_count = len(data['webhooks'])
            processed_count = 0
            
            async with self.session_maker() as session:
                for webhook_data in data['webhooks']:
                    await self.send_webhook_notification(webhook_data)
                    
                    if await self.process_single_webhook(session, webhook_data):
                        processed_count += 1
            
            logger.info(f"Обработано {processed_count} из {webhook_count} webhook'ов")
            return web.Response(status=200, text="OK")
            
        except Exception as e:
            logger.error(f"Критическая ошибка при обработке webhook: {e}", exc_info=True)
            return web.Response(status=500, text="Internal Server Error")
    
    async def process_single_webhook(self, session: AsyncSession, webhook_data: Dict[str, Any]) -> bool:
        """Обрабатывает один webhook"""
        try:
            # Извлечение данных
            required_fields = ['webhook_id', 'link', 'user_id', 'bot_id', 'status', 'subscribe_date']
            webhook_id, link, user_id, bot_id, status, subscribe_date = [
                webhook_data.get(field) for field in required_fields
            ]
            
            # Проверка обязательных полей
            if not all([webhook_id, link, user_id, bot_id, status, subscribe_date]):
                missing_fields = [field for field in required_fields if not webhook_data.get(field)]
                logger.error(f"Отсутствуют поля: {missing_fields}")
                return False
            
            # Сохранение в БД
            success = await save_subgram_webhook(
                session=session,
                webhook_id=webhook_id,
                link=link,
                user_id=user_id,
                bot_id=bot_id,
                status=status,
                subscribe_date=subscribe_date
            )
            
            if not success:
                logger.error(f"Не удалось сохранить webhook {webhook_id} в БД")
                return False
            
            # Обработка статуса
            await self.handle_webhook_status(session, user_id, status, link)
            
            logger.info(f"Webhook {webhook_id} обработан успешно")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обработке webhook: {e}", exc_info=True)
            return False
    
    async def handle_webhook_status(self, session: AsyncSession, user_id: int, status: str, link: str):
        """Обрабатывает webhook в зависимости от статуса"""
        try:
            logger.info(f"🔍 Обрабатываем статус '{status}' для пользователя {user_id}, ссылка: {link}")
            
            if status != "unsubscribed":
                logger.info(f"⏭️ Пропускаем статус '{status}' - обрабатываем только 'unsubscribed'")
                return  # Обрабатываем только отписки
                
            user = await get_user(session, user_id)
            if not user:
                logger.warning(f"❌ Пользователь {user_id} не найден в БД")
                return
            
            logger.info(f"✅ Пользователь {user_id} найден, баланс: {user.balance}⭐️")
            
            # Поиск выполненных заданий для штрафа
            completed_tasks = await get_user_subgram_tasks_for_unsubscribe(session, user_id, link)
            logger.info(f"🔍 Найдено заданий для штрафа: {len(completed_tasks) if completed_tasks else 0}")
            
            if not completed_tasks:
                logger.info(f"📋 Для пользователя {user_id} не найдено заданий по ссылке {link}")
                return
            
            total_penalty = 0.0
            tasks_penalized = 0
            
            for i, task in enumerate(completed_tasks, 1):
                penalty_amount = task.reward_given  # Штраф равен награде за задание
                logger.info(f"💰 Задание {i}: награда={task.reward_given}⭐️, штраф={penalty_amount}⭐️")
                
                if user.balance >= penalty_amount:
                    success = await minus_balance(session, user_id, penalty_amount)
                    logger.info(f"💳 Списание {penalty_amount}⭐️: {'успешно' if success else 'неудачно'}")
                    
                    if success:
                        penalty_success = await apply_unsubscribe_penalty(session, task.id, penalty_amount, webhook_id=0)
                        logger.info(f"🏷️ Применение штрафа в БД: {'успешно' if penalty_success else 'неудачно'}")
                        
                        if penalty_success:
                            total_penalty += penalty_amount
                            tasks_penalized += 1
                            logger.info(f"✅ Снят штраф {penalty_amount}⭐️ с пользователя {user_id}")
                        else:
                            logger.warning(f"❌ Не удалось применить штраф в БД для задания {task.id}")
                    else:
                        logger.warning(f"❌ Не удалось снять штраф с пользователя {user_id}")
                else:
                    logger.warning(f"💸 Недостаточно баланса для штрафа пользователя {user_id}: нужно {penalty_amount}⭐️, есть {user.balance}⭐️")
            
            logger.info(f"📊 Итого: заданий обработано={tasks_penalized}, общий штраф={total_penalty}⭐️")
            
            # Уведомления о штрафе
            if tasks_penalized > 0:
                logger.info(f"📤 Отправляем уведомления о штрафе...")
                await self.send_penalty_notifications(user_id, total_penalty, tasks_penalized, link)
            else:
                logger.info(f"⚠️ Штрафы не применены - уведомления не отправляются")
                    
        except Exception as e:
            logger.error(f"💥 Ошибка при обработке статуса webhook: {e}", exc_info=True)
            
    async def send_penalty_notifications(self, user_id: int, total_penalty: float, tasks_count: int, link: str):
        """Отправляет уведомления о штрафе пользователю и админу"""
        if not self.bot:
            return
            
        # Уведомление пользователю
        try:
            penalty_message = f"""⚠️ <b>Штраф за отписку!</b>

Вы отписались от канала, за который получили награду.

📉 <b>Снято звезд:</b> {total_penalty:.2f}⭐️
📝 <b>Заданий затронуто:</b> {tasks_count}
🔗 <b>Канал:</b> {link}

💡 <i>Не отписывайтесь от каналов в течение 7 дней после получения награды.</i>"""

            await self.bot.send_message(
                chat_id=user_id,
                text=penalty_message,
                parse_mode='HTML'
            )
            logger.info(f"Уведомление о штрафе отправлено пользователю {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления пользователю: {e}")
        
        # Уведомление админу
        try:
            admin_message = f"""🚨 <b>Применен штраф за отписку</b>

👤 <b>Пользователь:</b> <code>{user_id}</code>
💰 <b>Сумма штрафа:</b> {total_penalty:.2f}⭐️
📝 <b>Заданий:</b> {tasks_count}
🔗 <b>Канал:</b> {link}"""

            await self.send_admin_notification(admin_message)
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления админу о штрафе: {e}")


def setup_webhook_routes(app: web.Application, config, session_maker, bot: Bot = None):
    """Настраивает маршруты для webhook'ов"""
    
    @web.middleware
    async def logging_middleware(request, handler):
        """Middleware для логирования запросов"""
        logger.info(f"Запрос: {request.method} {request.path} от {request.remote}")
        
        try:
            # Сохраняем тело запроса для повторного использования
            body = await request.read()
            if body:
                logger.info(f"Тело запроса: {body.decode('utf-8', errors='ignore')[:200]}...")
            
            # Создаем новый request с сохраненным телом
            class RequestWithBody:
                def __init__(self, original_request, body):
                    self._original = original_request
                    self._body = body
                    
                def __getattr__(self, name):
                    return getattr(self._original, name)
                    
                async def read(self):
                    return self._body
                    
                async def text(self):
                    return self._body.decode('utf-8')
                    
                async def json(self):
                    return json.loads(self._body.decode('utf-8'))
            
            new_request = RequestWithBody(request, body)
            response = await handler(new_request)
            
        except Exception as e:
            logger.error(f"Ошибка в middleware: {e}")
            response = await handler(request)
            
        logger.info(f"Ответ: {response.status}")
        return response
    
    app.middlewares.append(logging_middleware)
    
    handler = SubGramWebhookHandler(config, session_maker, bot)
    
    # Основные маршруты
    app.router.add_post('/webhook/subgram', handler.process_webhook)
    
    # Тестовые маршруты
    async def test_webhook(request):
        return web.Response(text="Webhook server работает!")
    
    async def test_post_webhook(request):
        body = await request.text()
        logger.info(f"Тестовый POST запрос: {body}")
        return web.Response(text="POST webhook test OK!")
    
    app.router.add_get('/test', test_webhook)
    app.router.add_post('/test-post', test_post_webhook)
    
    logger.info("Webhook маршруты настроены") 