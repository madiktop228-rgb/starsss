from __future__ import annotations

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, Float, Text, ForeignKey, Table, Index, func, Date, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
import json
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

Base = declarative_base()

# --- Ассоциативная таблица User <-> Task (для выполненных заданий) ---
user_completed_tasks_table = Table(
    'user_completed_tasks',
    Base.metadata,
    Column('user_id', BigInteger, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('task_id', Integer, ForeignKey('tasks.id', ondelete='CASCADE'), primary_key=True),
    Column('completed_at', DateTime, default=datetime.utcnow, nullable=False) # <--- ДОБАВЛЕНО
)

# --- Таблица для использованных промокодов --- 
user_used_promocodes_table = Table('user_used_promocodes', Base.metadata,
    Column('user_id', BigInteger, ForeignKey('users.user_id', ondelete='CASCADE'), primary_key=True),
    Column('promocode_id', Integer, ForeignKey('promocodes.id', ondelete='CASCADE'), primary_key=True)
)

# --- Новая модель для Индивидуальных ссылок ---
class IndividualLink(Base):
    __tablename__ = 'individual_links'

    id = Column(Integer, primary_key=True, autoincrement=True)
    identifier = Column(String(100), unique=True, nullable=False, index=True) # Уникальный текстовый идентификатор
    description = Column(String, nullable=True) # Описание для админа
    created_at = Column(DateTime, default=datetime.utcnow)

    # Связь с пользователями, пришедшими по этой ссылке (для возможного использования в будущем)
    users = relationship("User", back_populates="individual_link")
# -------------------------------------------

# --- Новая модель для Промокодов ---
class PromoCode(Base):
    __tablename__ = 'promocodes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False, index=True) # Сам промокод
    reward = Column(Float, nullable=False, default=0.0) # Награда
    max_uses = Column(Integer, nullable=True) # Макс. кол-во использований (NULL = бесконечно)
    uses_count = Column(Integer, default=0, nullable=False) # Текущее кол-во использований
    # --- Новое поле для условия по рефералам --- 
    required_referrals_all_time = Column(Integer, nullable=True) # Требуемое кол-во рефералов за все время (NULL = нет условия)
    required_referrals_24h = Column(Integer, nullable=True) # Требуемое кол-во рефералов за 24 часа (NULL = нет условия)
    is_active = Column(Boolean, default=True, nullable=False, index=True) # Активен ли код
    created_at = Column(DateTime, default=datetime.utcnow)

    # Связь с пользователями, использовавшими код (для возможного использования)
    used_by_users = relationship(
        "User",
        secondary=user_used_promocodes_table,
        back_populates="used_promocodes"
    )
# -------------------------------------


class User(Base):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, unique=True)
    username = Column(String(100))
    balance = Column(Float, default=0.0)
    refferal_id = Column(BigInteger, default=None)
    refferals_count = Column(Integer, default=0)
    refferals_24h_count = Column(Integer, default=0)
    ref_bonus = Column(Boolean, default=False)
    banned = Column(Boolean, default=False)
    registered_at = Column(DateTime, default=datetime.utcnow)
    current_task_id = Column(Integer, ForeignKey('tasks.id', ondelete='SET NULL'), nullable=True)
    last_bio_reward_date = Column(DateTime, nullable=True)  # Дата последней награды за био
    bio_link_penalties = Column(Integer, default=0)  # Количество штрафов за удаление ссылки из био
    last_bio_check_date = Column(DateTime, nullable=True)  # Дата последней проверки био
    last_bio_penalty_date = Column(DateTime, nullable=True)  # Дата последнего штрафа за био

    # --- Новое поле для связи с индивидуальной ссылкой --- 
    individual_link_id = Column(Integer, ForeignKey('individual_links.id', ondelete='SET NULL'), nullable=True)
    individual_link = relationship("IndividualLink", back_populates="users")
    # ------------------------------------------------------

    # --- Новая связь для выполненных заданий ---
    completed_tasks = relationship(
        "Task",
        secondary=user_completed_tasks_table,
        back_populates="users",
        lazy='selectin'
    )

    # --- Новая связь для использованных промокодов --- 
    used_promocodes = relationship(
        "PromoCode",
        secondary=user_used_promocodes_table,
        back_populates="used_by_users",
        lazy='selectin'
    )
    # ----------------------------------------------

class Channel(Base):
    __tablename__ = 'channels'

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(BigInteger, unique=True, nullable=False) # ID канала/чата
    channel_name = Column(String(100))
    channel_link = Column(String(100))
    channel_status = Column(String(100))
    check_type = Column(String(20), default='start', nullable=False, index=True) # 'start', 'withdraw'
    premium_requirement = Column(String(20), default='all', nullable=False, index=True) # 'all', 'premium_only', 'non_premium_only'
    # --- Новое поле для этапа проверки ---
    check_stage = Column(Integer, default=1, nullable=False) # <-- НОВОЕ ПОЛЕ: 1 - основной этап, 2 - второстепенный
    # Добавляем индекс для ускорения выборки по этапу
    __table_args__ = (
        Index('ix_channels_check_stage', 'check_stage'),
    )
    # -----------------------------------

    def __repr__(self):
        return f"<Channel(id={self.id}, channel_id={self.channel_id}, stage={self.check_stage})>"

class Settings(Base):
    __tablename__ = 'settings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    daily_bonus_reward = Column(Integer, default=0)
    daily_bonus_ref = Column(Integer, default=0)
    refferal_reward = Column(Float, default=0)
    promo_code_name = Column(String(100), default='')
    penalty = Column(Integer, default=0)

class DailyBonus(Base):
    __tablename__ = 'daily_bonus'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, unique=True)
    daily_bonus = Column(Integer, default=0)
    bonus_status = Column(Boolean, default=True)

class Withdraws(Base):
    __tablename__ = 'withdraws'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger)
    withdraw_amount = Column(Integer, default=0)
    withdraw_username = Column(String(100))
    withdraw_id = Column(String(100))
    withdraw_status = Column(Boolean, default=False)
    withdraw_date = Column(DateTime, default=datetime.utcnow)
    
    # Новые поля для автоматических выплат подарками
    processing_type = Column(String(50), default='manual')  # 'manual', 'auto_gifts', 'gift_processing'
    gift_details = Column(Text, nullable=True)  # JSON с деталями отправленных подарков
    processing_error = Column(Text, nullable=True)  # Текст ошибки если была
    auto_processed_at = Column(DateTime, nullable=True)  # Время автоматической обработки
    remainder_returned = Column(Float, default=0.0)  # Остаток возвращенный на баланс
    admin_message_id = Column(Integer, nullable=True)  # ID сообщения в канале администраторов

class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String, nullable=False) # Описание задания (может содержать HTML/Markdown)
    reward = Column(Float, nullable=False, default=0.0) # Награда за выполнение
    instruction_link = Column(String, nullable=True) # Ссылка на инструкцию
    action_link = Column(String, nullable=True) # Ссылка для кнопки "Выполнить" (если нужна)
    channel_id_to_check = Column(BigInteger, nullable=True) # ID канала для проверки подписки
    check_subscription = Column(Boolean, default=False, nullable=False) # Нужно ли проверять подписку
    is_active = Column(Boolean, default=True, nullable=False) # Активно ли задание
    # --- Добавляем поле для требования Premium --- 
    premium_requirement = Column(String(20), default='all', nullable=False, index=True) # 'all', 'premium_only', 'non_premium_only'
    # --- Добавляем поля для лимита выполнений ---
    max_completions = Column(Integer, default=1000000, nullable=False) # Максимальное количество выполнений
    current_completions = Column(Integer, default=0, nullable=False) # Текущее количество выполнений
    # --- Новые поля для временного распределения ---
    time_distribution_hours = Column(Integer, nullable=True) # Количество часов для распределения (24, 48, 72 и т.д.)
    hourly_distribution = Column(Text, nullable=True) # JSON с распределением по часам [3,1,0,0,5,2,...]
    start_time = Column(DateTime, nullable=True) # Время начала распределения
    is_time_distributed = Column(Boolean, default=False, nullable=False) # Использует ли задание временное распределение
    created_at = Column(DateTime, default=datetime.utcnow)

    users = relationship(
        "User",
        secondary=user_completed_tasks_table,
        back_populates="completed_tasks"
    )

# Добавляем индексы для внешних ключей для ускорения запросов статистики
Index('ix_users_individual_link_id', User.individual_link_id)
Index('ix_users_ref_bonus', User.ref_bonus) # Индекс по ref_bonus тоже может быть полезен
# Index('ix_tasks_premium_requirement', Task.premium_requirement) # Добавляем индекс для нового поля
# CREATE INDEX IF NOT EXISTS ix_tasks_premium_requirement ON tasks (premium_requirement);

# --- Новая модель для Шаблонов Рассылок (с полем для клавиатуры) ---
class BroadcastTemplate(Base):
    __tablename__ = 'broadcast_templates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False, index=True)
    text = Column(Text, nullable=True)
    photo_file_id = Column(String, nullable=True)
    keyboard_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        has_keyboard = bool(self.keyboard_json)
        return f"<BroadcastTemplate(id={self.id}, name='{self.name}', has_photo={bool(self.photo_file_id)}, has_keyboard={has_keyboard})>"

    def set_keyboard(self, keyboard: InlineKeyboardMarkup | None):
        if keyboard:
            keyboard_data = []
            for row in keyboard.inline_keyboard:
                row_data = []
                for button in row:
                    button_dict = {
                        'text': button.text,
                        'url': button.url,
                        'callback_data': button.callback_data,
                    }
                    row_data.append({k: v for k, v in button_dict.items() if v is not None})
                keyboard_data.append(row_data)
            self.keyboard_json = json.dumps(keyboard_data, ensure_ascii=False)
        else:
            self.keyboard_json = None
            
    @classmethod
    def parse_simple_keyboard(cls, text: str) -> InlineKeyboardMarkup | None:
        """
        Преобразует простой текстовый формат кнопок в InlineKeyboardMarkup
        
        Формат:
        Название - ссылка
        Название - ссылка / Название - ссылка
        
        Кнопки в одной строке разделяются символом '/'
        """
        if not text or text.strip() == '':
            return None
            
        import re
        import logging
        
        logging.info(f"Начало разбора текста клавиатуры: {text[:50]}...")
        
        builder = InlineKeyboardBuilder()
        lines = text.strip().split('\n')
        has_buttons = False
        
        for line_idx, line in enumerate(lines):
            logging.debug(f"Обработка строки {line_idx+1}: {line[:50]}...")
            row_buttons = []
            
            # Разбиваем строку на пары кнопок, разделенные '/'
            button_pairs = []
            current_pair = ""
            for part in line.split(' / '):
                if ' - http' in part or ' - tg:' in part:
                    if current_pair:
                        button_pairs.append(current_pair)
                    current_pair = part
                else:
                    if current_pair:
                        current_pair += ' / ' + part
                    else:
                        current_pair = part
            
            if current_pair:
                button_pairs.append(current_pair)
            
            logging.debug(f"Найдено {len(button_pairs)} пар кнопок в строке {line_idx+1}")
            
            for pair_idx, pair in enumerate(button_pairs):
                pair = pair.strip()
                logging.debug(f"Обработка пары {pair_idx+1}: {pair}")
                
                # Находим последнее вхождение ' - http' или ' - tg:'
                http_pos = pair.rfind(' - http')
                tg_pos = pair.rfind(' - tg:')
                
                split_pos = max(http_pos, tg_pos)
                if split_pos > 0:
                    button_text = pair[:split_pos].strip()
                    button_url = pair[split_pos + 3:].strip()  # +3 чтобы пропустить ' - '
                    
                    logging.debug(f"Разделено на текст: '{button_text}' и URL: '{button_url}'")
                    
                    # Проверка и корректировка URL
                    if button_text and button_url:
                        try:
                            # Проверяем на стандартные протоколы
                            if not re.match(r'^(https?://|tg://)', button_url):
                                # Если URL не начинается с протокола, добавляем https://
                                if not button_url.startswith(('http:', 'https:', 'tg:')):
                                    button_url = f"https://{button_url}"
                                else:
                                    # Исправляем неполные протоколы (http: -> http://)
                                    button_url = re.sub(r'^(https?|tg):/?(?!/)', r'\1://', button_url)
                            
                            # Проверяем, что URL содержит домен
                            if re.match(r'^(https?://|tg://)$', button_url) or not re.search(r'://[^/]+', button_url):
                                logging.warning(f"Некорректный URL без домена: {button_url}")
                                continue
                            
                            # Создаем кнопку с корректным URL
                            row_buttons.append(InlineKeyboardButton(text=button_text, url=button_url))
                            has_buttons = True
                            logging.debug(f"Создана кнопка с текстом '{button_text}' и URL '{button_url}'")
                        except Exception as e:
                            logging.error(f"Ошибка создания кнопки с URL '{button_url}': {e}")
                            continue
                else:
                    logging.warning(f"Не найден разделитель ' - http' или ' - tg:' в паре: {pair}")
            
            if row_buttons:
                builder.row(*row_buttons)
                logging.debug(f"Добавлен ряд с {len(row_buttons)} кнопками")
        
        result = builder.as_markup() if has_buttons else None
        logging.info(f"Завершение разбора клавиатуры. Создано кнопок: {has_buttons}")
        
        # Проверяем, что были добавлены кнопки
        return result

    def get_keyboard(self) -> InlineKeyboardMarkup | None:
        if not self.keyboard_json:
            return None
        try:
            keyboard_data = json.loads(self.keyboard_json)
            builder = InlineKeyboardBuilder()
            for row_data in keyboard_data:
                buttons = []
                for button_dict in row_data:
                    buttons.append(InlineKeyboardButton(**button_dict))
                builder.row(*buttons)
            return builder.as_markup()
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"Error decoding keyboard JSON for template {self.id}: {e}") # Логирование ошибки
            return None

class Show(Base):
    """Модель для управления 'показами' - сообщениями при старте."""
    __tablename__ = 'shows'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False, index=True)
    text = Column(Text, nullable=False)
    photo_file_id = Column(String, nullable=True)
    keyboard_json = Column(Text, nullable=True)
    is_active = Column(Boolean, default=False, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Show(id={self.id}, name='{self.name}', is_active={self.is_active})>"

    def get_keyboard(self) -> InlineKeyboardMarkup | None:
        if self.keyboard_json:
            try:
                # Используем model_validate_json для более надежного парсинга
                return InlineKeyboardMarkup.model_validate_json(self.keyboard_json)
            except Exception:
                # Если парсинг не удался, возвращаем None
                return None
        return None

# --- Новая модель для ежедневных заданий ---
class DailyTask(Base):
    __tablename__ = 'daily_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True) # ID пользователя
    task_type = Column(String(50), nullable=False, index=True) # Тип задания (bio_referral, etc)
    completed_at = Column(DateTime, default=datetime.utcnow) # Время выполнения
    reward = Column(Float, default=0.0) # Полученная награда
    
    # Составной индекс для быстрого поиска по пользователю и типу задания
    __table_args__ = (
        Index('ix_daily_tasks_user_task_type', 'user_id', 'task_type'),
    )

class TaskExtensionState(Base):
    """Хранит дату последнего продления задания с временным распределением."""
    __tablename__ = 'task_extension_state'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey('tasks.id', ondelete='CASCADE'), nullable=False, index=True)
    last_extended_at = Column(DateTime, nullable=True, index=True)

    __table_args__ = (
        UniqueConstraint('task_id', name='uq_task_extension_state_task_id'),
    )

class SubGramWebhook(Base):
    __tablename__ = 'subgram_webhooks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    webhook_id = Column(Integer, nullable=False, index=True)  # ID из SubGram
    link = Column(String, nullable=False)  # Ссылка, через которую взаимодействовал пользователь
    user_id = Column(BigInteger, nullable=False, index=True)  # Telegram user ID
    bot_id = Column(BigInteger, nullable=False)  # ID бота SubGram
    status = Column(String(20), nullable=False, index=True)  # subscribed, unsubscribed, notgetted
    subscribe_date = Column(Date, nullable=False)  # Дата первоначального привлечения
    received_at = Column(DateTime, default=datetime.utcnow)  # Когда получили webhook
    processed = Column(Boolean, default=False, nullable=False, index=True)  # Обработан ли webhook

    def __repr__(self):
        return f"<SubGramWebhook(webhook_id={self.webhook_id}, user_id={self.user_id}, status='{self.status}')>"

class SubGramCompletedTask(Base):
    """Модель для отслеживания выполненных заданий SubGram"""
    __tablename__ = 'subgram_completed_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)  # Telegram user ID
    subgram_task_id = Column(BigInteger, nullable=False, index=True)  # ID задания в SubGram (изменено с Integer на BigInteger)
    channel_link = Column(String, nullable=False)  # Ссылка на канал
    channel_name = Column(String, nullable=True)  # Название канала (если доступно)
    reward_given = Column(Float, nullable=False, default=0.0)  # Награда, которая была выдана
    completed_at = Column(DateTime, default=datetime.utcnow)  # Когда выполнено задание
    penalty_applied = Column(Boolean, default=False, nullable=False)  # Был ли применен штраф за отписку
    penalty_amount = Column(Float, nullable=True)  # Размер штрафа (если применялся)
    penalty_applied_at = Column(DateTime, nullable=True)  # Когда был применен штраф
    webhook_id = Column(Integer, nullable=True, index=True)  # ID webhook'а отписки (для связи)
    
    # Составной индекс для быстрого поиска
    __table_args__ = (
        Index('idx_user_subgram_task', user_id, subgram_task_id),
        Index('idx_penalty_status', penalty_applied),
    )

    def __repr__(self):
        return f"<SubGramCompletedTask(user_id={self.user_id}, task_id={self.subgram_task_id}, reward={self.reward_given})>"

class LocalCompletedTask(Base):
    """Модель для отслеживания выполненных локальных заданий"""
    __tablename__ = 'local_completed_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)  # Telegram user ID
    task_id = Column(Integer, nullable=False, index=True)  # ID локального задания
    channel_id = Column(BigInteger, nullable=True, index=True)  # ID канала, связанного с заданием
    reward_given = Column(Float, nullable=False, default=0.0)  # Награда, которая была выдана
    completed_at = Column(DateTime, default=datetime.utcnow)  # Когда выполнено задание
    penalty_applied = Column(Boolean, default=False, nullable=False)  # Был ли применен штраф за отписку
    penalty_amount = Column(Float, nullable=True)  # Размер штрафа (если применялся)
    penalty_applied_at = Column(DateTime, nullable=True)  # Когда был применен штраф
    
    # Составной индекс для быстрого поиска
    __table_args__ = (
        Index('idx_user_local_task', user_id, task_id),
        Index('idx_local_penalty_status', penalty_applied),
        Index('idx_local_channel_user', channel_id, user_id),
    )

    def __repr__(self):
        return f"<LocalCompletedTask(user_id={self.user_id}, task_id={self.task_id}, channel_id={self.channel_id}, reward={self.reward_given})>"

class TraffyCompletedTask(Base):
    """Модель для отслеживания выполненных Traffy заданий"""
    __tablename__ = 'traffy_completed_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)  # Telegram user ID
    traffy_task_id = Column(String(50), nullable=False, index=True)  # ID задания в Traffy
    task_title = Column(String(255), nullable=True)  # Название задания
    task_link = Column(String, nullable=True)  # Ссылка на задание
    reward_given = Column(Float, nullable=False, default=0.25)  # Награда, которая была выдана
    completed_at = Column(DateTime, default=datetime.utcnow)  # Когда выполнено задание
    
    # Составной индекс для быстрого поиска
    __table_args__ = (
        Index('idx_user_traffy_task', user_id, traffy_task_id),
    )

    def __repr__(self):
        return f"<TraffyCompletedTask(id={self.id}, user_id={self.user_id}, task_id='{self.traffy_task_id}', reward={self.reward_given})>"

class GiftWithdrawSettings(Base):
    """Настройки для автоматических выплат подарками"""
    __tablename__ = 'gift_withdraw_settings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    enabled = Column(Boolean, default=True, nullable=False)  # Включена ли автообработка
    min_amount_for_gifts = Column(Integer, default=15)  # Мин. сумма для автовыплаты
    max_remainder = Column(Integer, default=10)  # Макс. остаток для возврата на баланс
    preferred_gifts = Column(Text, nullable=True)  # JSON с приоритетами подарков
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<GiftWithdrawSettings(enabled={self.enabled}, min_amount={self.min_amount_for_gifts})>"

