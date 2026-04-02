from __future__ import annotations

from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
import hashlib  # Для генерации ID inline результатов
import urllib.parse  # Для кодирования URL параметров
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.database.models import User, Channel, Task, IndividualLink, PromoCode, BroadcastTemplate, Show
from bot.database.requests import get_all_broadcast_templates

from typing import List, Tuple, Optional, Union
from ..database.models import Channel, Task, IndividualLink, PromoCode

from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
import hashlib  # Для генерации ID inline результатов
import urllib.parse  # Для кодирования URL параметров
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.database.models import User

from typing import List, Tuple, Optional, Union
from ..database.models import Channel, Task, IndividualLink, PromoCode

# --- ИСПРАВЛЕННАЯ ФУНКЦИЯ ---
async def get_combined_channels_keyboard(
    items: List[Union[str, Channel]], # Принимает список строк или объектов Channel
    check_type: str, # 'start' или 'withdraw'
    stage: int = 1 # <-- Добавляем параметр этапа
) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру со списком каналов/ссылок SubGram и локальных каналов
    с единой нумерацией для указанного этапа, располагая по две кнопки в ряду.
    """
    builder = InlineKeyboardBuilder()
    channel_counter = 1
    row_buttons = [] # Временный список для кнопок в текущем ряду

    # --- Ограничиваем количество каналов на первом этапе ---
    if stage == 1:
        items_to_show = items[:6]
    else:
        items_to_show = items # На втором этапе показываем все оставшиеся

    for item in items_to_show:
        button_text = f"Канал #{channel_counter}" # Общий текст кнопки
        button = None

        if isinstance(item, str): # Это ссылка от SubGram (URL)
            button = InlineKeyboardButton(text=button_text, url=item)
        elif isinstance(item, Channel): # Это локальный канал
            button = InlineKeyboardButton(text=button_text, url=item.channel_link)

        if button:
            row_buttons.append(button)
            # Если в ряду уже 2 кнопки, добавляем ряд и очищаем список
            if len(row_buttons) == 2:
                builder.row(*row_buttons) # <-- Добавляем ряд из двух кнопок
                row_buttons = [] # Очищаем для следующего ряда

        channel_counter += 1 # Увеличиваем общий счетчик

    # Если осталась одна кнопка (нечетное количество каналов), добавляем ее в отдельный ряд
    if row_buttons:
        builder.row(*row_buttons) # <-- Добавляем оставшуюся кнопку

    # --- Обновляем callback_data кнопки проверки ---
    # Формат: recheck_sub_{check_type}_stage_{stage}
    recheck_callback_data = f"recheck_sub_{check_type}_stage_{stage}"
    check_button_text = f"✅ Проверить подписку"
    builder.row(
        InlineKeyboardButton(text=check_button_text, callback_data=recheck_callback_data)
    )

    return builder.as_markup()
# --- КОНЕЦ ИСПРАВЛЕННОЙ ФУНКЦИИ ---

async def get_channels_keyboard(
    channels: List[Channel],
    check_type: Optional[str] = None,
    stage: int = 1 # <-- Добавляем параметр этапа
) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру со списком каналов/ботов и кнопкой проверки для указанного этапа.
       Кнопки каналов располагаются по две в ряду.
    """
    builder = InlineKeyboardBuilder()
    row_buttons = []
    counter = 1

    # --- Убрано ограничение на количество каналов --- 
    channels_to_show = channels 

    for channel in channels_to_show:
        link = channel.channel_link
        is_bot_link = '_bot' in link.lower() or '?start=' in link
        button_text = f'Бот #{counter}' if is_bot_link else f'Канал #{counter}' # Уточненный текст

        button = InlineKeyboardButton(text=button_text, url=link)
        row_buttons.append(button)

        if len(row_buttons) == 2:
            builder.row(*row_buttons)
            row_buttons = []

        counter += 1

    if row_buttons:
        builder.row(*row_buttons)

    # --- Обновляем callback_data кнопки проверки ---
    # Формат: recheck_sub_{check_type}_stage_{stage}
    if check_type: # Используем check_type если он передан
        callback_data = f"recheck_sub_{check_type}_stage_{stage}"
        check_button_text = f"✅ Проверить подписку"
    else: # Обратная совместимость или общий случай
        callback_data = f"checks_subscribe_stage_{stage}" # Добавляем этап
        check_button_text = f"✅ Проверить подписку"

    builder.row(
        InlineKeyboardButton(
            text=check_button_text,
            callback_data=callback_data
        )
    )

    return builder.as_markup()

ADMIN_PANEL_BUTTON_TEXT = "Админ панель 👑" # Константа для текста кнопки

def get_main_keyboard(user_id: int, admin_ids: list[int]) -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру, добавляя кнопку админ-панели для админов."""
    keyboard_layout = [
        [KeyboardButton(text="Заработать звезды", icon_custom_emoji_id="5179259920754672105", style="primary")],
        [KeyboardButton(text="Задания", icon_custom_emoji_id="5179642890103554696")],
        [KeyboardButton(text="Вывести звезды", icon_custom_emoji_id="5375296873982604963"), KeyboardButton(text="Купить дешево звезды", icon_custom_emoji_id="5472250091332993630")],
        [KeyboardButton(text="Рейтинг", icon_custom_emoji_id="5413566144986503832"), KeyboardButton(text="Поддержка", icon_custom_emoji_id="5818813162815753343"), KeyboardButton(text="Промокод", icon_custom_emoji_id="5199749070830197566")],
        # [KeyboardButton(text="💬Отзывы")], # Убираем кнопку Отзывы, если она была здесь
    ]
    
    # Добавляем кнопку админ-панели, если ID пользователя есть в списке админов
    if user_id in admin_ids:
        keyboard_layout.append([KeyboardButton(text=ADMIN_PANEL_BUTTON_TEXT)]) # Используем константу

    return ReplyKeyboardMarkup(
        keyboard=keyboard_layout,
        resize_keyboard=True
    )

def select_type_task() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ежедневные задания", callback_data='daily_task', icon_custom_emoji_id="5274055917766202507"),
            InlineKeyboardButton(text="Обычные задания", callback_data='default_task', icon_custom_emoji_id="5420315771991497307", style="primary")],
            [InlineKeyboardButton(text="Назад в меню", callback_data='back_to_main', icon_custom_emoji_id="5256247952564825322")]
        ]
    )

def stars_bot_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Купить звезды ⭐️", url="https://t.me/startovsBot")]
        ]
    )

def start_withdraw_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Наши выплаты 💸", url="https://t.me/zaberistars")]
        ]
    )

def start_support_keyboard_reply() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Поддержка 👨‍💻", url="https://t.me/startovsBot")]
        ]
    )

def profile_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎁Промокод")],
            [KeyboardButton(text="◀️Назад")]
        ],
        resize_keyboard=True
    )

def back_to_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="◀️Назад")]
        ],
        resize_keyboard=True
    )

def reviews_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔝Перейти к отзывам", url="https://t.me/startovsBot")]
        ]
    )

# def get_admin_keyboard() -> ReplyKeyboardMarkup:
#     return ReplyKeyboardMarkup(
#         keyboard=[
#             [KeyboardButton(text="Статистика")],
#             [KeyboardButton(text="Рассылка")]
#         ],
#         resize_keyboard=True
#     )

# def get_inline_keyboard(user_id: int) -> InlineKeyboardMarkup:
#     return InlineKeyboardMarkup(
#         inline_keyboard=[
#             [
#                 InlineKeyboardButton(
#                     text="Действие", 
#                     callback_data=f"action_{user_id}"
#                 )
#             ]
#         ]
#     )

def earn_stars_keyboard(share_text) -> InlineKeyboardMarkup:
    # Кодируем текст для URL
    encoded_text = urllib.parse.quote(share_text)
    
    return InlineKeyboardMarkup(
        inline_keyboard=[
            # [InlineKeyboardButton(text="🔥 Выполнить задания", callback_data="complete_tasks")],
            [InlineKeyboardButton(text="📢 Отправить ссылку друзьям", url=f"https://t.me/share/url?url={encoded_text}")]
        ]
    )

def start_stars_keyboard(share_text) -> InlineKeyboardMarkup:
    # Кодируем текст для URL
    encoded_text = urllib.parse.quote(share_text)
    
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Позвать друга", url=f"https://t.me/share/url?url={encoded_text}")]
        ]
    )

def top_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Топ 30 за 24 часа", callback_data="top_24h")],
            [InlineKeyboardButton(text="📊 Топ 30 за все время", callback_data="top_all_time")]
            # [InlineKeyboardButton(text="◀️Назад", callback_data="back_to_main")]
        ]
    )

def withdraw_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить заявку", callback_data="withdraw_request")],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_main")]
    ])
    return keyboard

def withdraw_amounts_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с фиксированными суммами для вывода"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="15⭐️", callback_data="withdraw_amount_15"),
                InlineKeyboardButton(text="25⭐️", callback_data="withdraw_amount_25")
            ],
            [
                InlineKeyboardButton(text="50⭐️", callback_data="withdraw_amount_50"),
                InlineKeyboardButton(text="100⭐️", callback_data="withdraw_amount_100")
            ],
            [
                InlineKeyboardButton(text="350⭐️", callback_data="withdraw_amount_350"),
                InlineKeyboardButton(text="500⭐️", callback_data="withdraw_amount_500")
            ]
        ]
    )

def withdraw_gift_selection_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора подарков при выводе"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="15⭐ (💝)", callback_data="gift_select_5170145012310081615"),
                InlineKeyboardButton(text="15⭐ (🧸)", callback_data="gift_select_5170233102089322756")
            ],
            [
                InlineKeyboardButton(text="25⭐ (🌹)", callback_data="gift_select_5168103777563050263"),
                InlineKeyboardButton(text="25⭐ (🎁)", callback_data="gift_select_5170250947678437525")
            ],
            [
                InlineKeyboardButton(text="50⭐ (🍾)", callback_data="gift_select_6028601630662853006"),
                InlineKeyboardButton(text="50⭐ (🚀)", callback_data="gift_select_5170564780938756245")
            ],
            [
                InlineKeyboardButton(text="50⭐ (💐)", callback_data="gift_select_5170314324215857265"),
                InlineKeyboardButton(text="50⭐ (🎂)", callback_data="gift_select_5170144170496491616")
            ],
            [
                InlineKeyboardButton(text="100⭐ (💎)", callback_data="gift_select_5170521118301225164"),
                InlineKeyboardButton(text="100⭐ (🏆)", callback_data="gift_select_5168043875654172773")
            ],
            [InlineKeyboardButton(text="100⭐ (💍)", callback_data="gift_select_5170690322832818290")],
        ]
    )

def withdraw_admin_keyboard(withdraw_id: str, sum: int, username: str, user_id: int, id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data=f"withdraw_confirm_{withdraw_id}_{sum}_{username}_{user_id}_{id}")],
            [InlineKeyboardButton(text="❌ Отказать", callback_data=f"withdraw_reject_{withdraw_id}_{sum}_{username}_{user_id}_{id}")],
            [InlineKeyboardButton(text="👤 Профиль пользователя", url=f"tg://user?id={user_id}")]
        ]
    )

def withdraw_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтверждено", callback_data=f"withdraw_confirm")]
        ]
    )

def withdraw_reject_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отклонено", callback_data=f"withdraw_reject")]
        ]
    )

def task_keyboard(task: Task, verification_pending: bool = False) -> InlineKeyboardMarkup:
    """
    Клавиатура для отображения задания.
    verification_pending: Если True, показывает кнопку для проверки подписки вместо "Я выполнил".
    """
    buttons = []
    
    # Кнопка "Перейти по ссылке" (если есть ссылка действия)
    if task.action_link:
         buttons.append([InlineKeyboardButton(text="🔗 Перейти по ссылке", url=task.action_link)])

    # Кнопка "Я выполнил" или "Проверить подписку" - теперь всегда на отдельной строке
    if verification_pending:
         # Кнопка для повторной проверки подписки
        buttons.append([InlineKeyboardButton(text="🔍 Проверить подписку", callback_data=f"task_verify_sub_{task.id}")])
    else:
        # Обычная кнопка "Я выполнил"
        buttons.append([InlineKeyboardButton(text="✅ Я выполнил", callback_data=f"task_complete_{task.id}")])

    # Кнопка "Пропустить" - тоже на отдельной строке
    buttons.append([InlineKeyboardButton(text="⏩ Пропустить", callback_data=f"task_skip_{task.id}")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def yes_no_keyboard(callback_prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура Да/Нет с кастомным префиксом"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data=f"{callback_prefix}_yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"{callback_prefix}_no")
        ]
    ])

def cancel_state_keyboard() -> InlineKeyboardMarkup:
     """Кнопка отмены состояния FSM"""
     return InlineKeyboardMarkup(inline_keyboard=[
         [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_state")]
     ])

def cancel_state_keyboard_reply() -> ReplyKeyboardMarkup:
    """Кнопка отмены состояния FSM"""
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="❌ Отменить")]
    ], resize_keyboard=True, one_time_keyboard=True)

def admin_main_keyboard() -> InlineKeyboardMarkup:
    """Главная клавиатура админ-панели"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Задания", callback_data="admin_manage_tasks"),
         InlineKeyboardButton(text="Спец. ссылки", callback_data="admin_manage_ind_links")],
        # --- Разделяем управление каналами ---
        [InlineKeyboardButton(text="Спонсоры ОП (Старт)", callback_data="admin_manage_channels_start")],
        # -------------------------------------
        [InlineKeyboardButton(text="Рассылка", callback_data="newsletter"),
         InlineKeyboardButton(text="Промокоды", callback_data="admin_manage_promocodes")],
        # --- Добавляем кнопку Шаблоны ---
        [InlineKeyboardButton(text="Шаблоны рассылок", callback_data="admin_manage_templates"), # Новая кнопка
         InlineKeyboardButton(text="Настройки наград", callback_data="admin_manage_rewards")],
        # --------------------------------
        [InlineKeyboardButton(text="Статистика", callback_data="admin_show_stats"),
         InlineKeyboardButton(text="Ежедневные задания", callback_data="admin_daily_tasks_stats")],
        [InlineKeyboardButton(text="Управление пользователями", callback_data="admin_search_user"),
         InlineKeyboardButton(text="Скачать юзеров", callback_data="admin_download_users")],
        [InlineKeyboardButton(text="Бэкап БД", callback_data="admin_backup_db"),
        InlineKeyboardButton(text="SubGram статистика", callback_data="admin_subgram_stats")],
        [InlineKeyboardButton(text="Настройка авто-выплат", callback_data="admin_gift_settings"),
        InlineKeyboardButton(text="🎬 Управление показами", callback_data="admin_manage_shows")] # Новая кнопка
    ])

def error_promo_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для ошибки при активации промокода."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Выполнить задания 🔥", callback_data="complete_tasks")]
    ])


def admin_promocodes_list_keyboard(promocodes: List[PromoCode]) -> InlineKeyboardMarkup:
    """Клавиатура для списка промокодов в админке."""
    buttons = []
    for promo in promocodes:
        status_icon = "🟢" if promo.is_active else "🔴"
        uses_info = f"{promo.uses_count}"
        if promo.max_uses is not None:
            uses_info += f"/{promo.max_uses}"
        
        req_refs = f" (ReqRefs: {promo.required_referrals_all_time})" if promo.required_referrals_all_time else ""
        
        button_text = f"{status_icon} {promo.code} ({uses_info} uses{req_refs}) - {promo.reward}⭐️"
        buttons.append([
            InlineKeyboardButton(text=button_text, callback_data=f"admin_promo_view_{promo.id}")
        ])

    buttons.append([InlineKeyboardButton(text="➕ Добавить новый промокод", callback_data="admin_add_promo")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_promocode_manage_keyboard(promo: PromoCode) -> InlineKeyboardMarkup:
    """Клавиатура для управления конкретным промокодом."""
    buttons = []
    status_text = "Деактивировать" if promo.is_active else "Активировать"
    status_icon = "🔴" if promo.is_active else "🟢"
    buttons.append([
        InlineKeyboardButton(text=f"{status_icon} {status_text}", callback_data=f"admin_promo_toggle_{promo.id}")
    ])
    buttons.append([
        InlineKeyboardButton(text="🗑️ Удалить промокод", callback_data=f"admin_promo_delete_{promo.id}")
    ])
    buttons.append([InlineKeyboardButton(text="◀️ К списку промокодов", callback_data="admin_manage_promocodes")]) 
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_promocode_delete_confirm_keyboard(promo_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления промокода."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑️ Да, удалить", callback_data=f"confirm_admin_promo_delete_{promo_id}"),
            InlineKeyboardButton(text="◀️ Отмена", callback_data=f"admin_promo_view_{promo_id}") # Вернуться к просмотру
        ]
    ])

    
def newsletter_source_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора: создать рассылку с нуля или использовать шаблон."""
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Создать новую", callback_data="newsletter_create_new")
    builder.button(text="📬 Использовать шаблон", callback_data="newsletter_use_template")
    builder.button(text="◀️ Отмена", callback_data="admin_back_to_main") # Или просто cancel_state? Зависит от контекста вызова
    builder.adjust(1)
    return builder.as_markup()

def select_newsletter_template_keyboard(templates: List[BroadcastTemplate]) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для выбора шаблона для рассылки."""
    builder = InlineKeyboardBuilder()
    if not templates:
        builder.button(text="Нет доступных шаблонов", callback_data="no_templates_available") # Просто информационная кнопка
    else:
        for template in templates:
            template_name_short = (template.name[:30] + '...') if len(template.name) > 30 else template.name
            builder.button(text=f"📄 {template_name_short}", callback_data=f"newsletter_select_template_{template.id}")

    # Кнопка Назад возвращает к выбору источника рассылки
    builder.button(text="◀️ Назад", callback_data="newsletter") # Возврат к началу выбора рассылки
    builder.adjust(1)
    return builder.as_markup()

def templates_menu_keyboard(templates: List[BroadcastTemplate]) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для меню управления шаблонами."""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Создать новый шаблон", callback_data="template_create_start")

    if templates:
        builder.button(text="✏️ Редактировать Промокод", callback_data="template_promocode_edit_select")
        builder.button(text="🗑️ Удалить шаблон", callback_data="template_delete_select")
        builder.button(text="📋 Список шаблонов", callback_data="template_list_view")

    # Добавляем кнопку "Назад" для возврата в главное меню админки
    builder.button(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")

    builder.adjust(1) # Все кнопки в один столбец
    return builder.as_markup()

def admin_back_to_main_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для возврата в главное меню админки."""
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def template_creation_skip_keyboard(skip_callback: str) -> InlineKeyboardMarkup:
    """Клавиатура с кнопками 'Пропустить' и 'Отмена'."""
    builder = InlineKeyboardBuilder()
    builder.button(text="⏩ Пропустить", callback_data=skip_callback)
    builder.button(text="❌ Отменить создание", callback_data="cancel_state")
    builder.adjust(1)
    return builder.as_markup()

def template_confirm_creation_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения создания шаблона."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Сохранить шаблон", callback_data="template_create_confirm")
    # builder.button(text="🔄 Начать заново", callback_data="template_create_start") # Можно добавить кнопку "Начать заново"
    builder.button(text="❌ Отменить создание", callback_data="cancel_state")
    builder.adjust(1)
    return builder.as_markup()

def template_delete_confirm_keyboard(template_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления шаблона."""
    builder = InlineKeyboardBuilder()
    builder.button(text="🗑️ Да, удалить", callback_data=f"s_template_delete_confirm_{template_id}")
    builder.button(text="❌ Отмена", callback_data="admin_manage_templates") # Возврат в меню шаблонов
    builder.adjust(1)
    return builder.as_markup()

def admin_tasks_list_keyboard(tasks_with_counts: List[Tuple[Task, int]]) -> InlineKeyboardMarkup:
    """Клавиатура для списка заданий в админке с количеством выполнений."""
    buttons = []
    for task, count in tasks_with_counts:
        status_icon = "🟢" if task.is_active else "🔴"
        # --- Изменено: Форматируем текст кнопки ---
        # Используем action_link если есть, иначе начало описания
        display_name = task.action_link if task.action_link else task.description
        # Обрезаем длинные строки
        display_name_short = (display_name[:25] + '...') if len(display_name) > 25 else display_name

        button_text = f"{status_icon} #{task.id} - {display_name_short} ({count} вып.)"
        # -----------------------------------------
        buttons.append([
            InlineKeyboardButton(text=button_text, callback_data=f"admin_task_view_{task.id}")
        ])

    buttons.append([InlineKeyboardButton(text="➕ Добавить новое задание", callback_data="admin_add_task")])
    buttons.append([InlineKeyboardButton(text="🔍 Проверить лимиты", callback_data="admin_check_task_limits")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_tasks_list_keyboard_paginated(tasks_with_counts: List[Tuple[Task, int]], total_tasks: int = 0, page: int = 0) -> InlineKeyboardMarkup:
    """Пагинированная клавиатура для списка заданий в админке с ограничением размера."""
    buttons = []
    
    # Ограничиваем количество заданий в клавиатуре для предотвращения ошибки "reply markup is too long"
    max_tasks_per_page = 8  # Максимум 8 заданий на страницу
    
    # Показываем только задания для текущей страницы
    start_idx = page * max_tasks_per_page
    end_idx = start_idx + max_tasks_per_page
    current_page_tasks = tasks_with_counts[start_idx:end_idx]
    
    for task, count in current_page_tasks:
        status_icon = "🟢" if task.is_active else "🔴"
        display_name = task.action_link if task.action_link else task.description
        # Обрезаем длинные строки еще больше для экономии места
        display_name_short = (display_name[:20] + '...') if len(display_name) > 20 else display_name

        button_text = f"{status_icon} #{task.id} - {display_name_short} ({count} вып.)"
        buttons.append([
            InlineKeyboardButton(text=button_text, callback_data=f"admin_task_view_{task.id}")
        ])

    # Добавляем навигацию по страницам
    total_pages = (total_tasks + max_tasks_per_page - 1) // max_tasks_per_page
    
    if total_pages > 1:
        nav_buttons = []
        
        # Кнопка "Предыдущая страница"
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(
                text="◀️ Пред.", 
                callback_data=f"admin_tasks_page_{page - 1}"
            ))
        
        # Информация о текущей странице
        nav_buttons.append(InlineKeyboardButton(
            text=f"📄 {page + 1}/{total_pages}", 
            callback_data="no_action"
        ))
        
        # Кнопка "Следующая страница"
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(
                text="След. ▶️", 
                callback_data=f"admin_tasks_page_{page + 1}"
            ))
        
        if nav_buttons:
            buttons.append(nav_buttons)

    # Добавляем основные кнопки управления
    buttons.append([InlineKeyboardButton(text="➕ Добавить новое задание", callback_data="admin_add_task")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_task_manage_keyboard(task: Task) -> InlineKeyboardMarkup:
    """Клавиатура для управления конкретным заданием"""
    buttons = []
    status_text = "Деактивировать" if task.is_active else "Активировать"
    status_icon = "🔴" if task.is_active else "🟢"
    
    # --- Добавляем отображение Premium статуса --- 
    premium_req_text = ""
    if task.premium_requirement == 'premium_only':
        premium_req_text = " [⭐️ Prem]"
    elif task.premium_requirement == 'non_premium_only':
        premium_req_text = " [🚫 Prem]"
    # ------------------------------------------
    
    buttons.append([
        # InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"admin_task_edit_{task.id}"), # Кнопка редактирования (пока не реализуем)
        InlineKeyboardButton(text=f"{status_icon} {status_text}{premium_req_text}", callback_data=f"admin_task_toggle_{task.id}")
    ])
    buttons.append([
        InlineKeyboardButton(text="🗑️ Удалить задание", callback_data=f"admin_task_delete_{task.id}")
    ])
    # Обновляем кнопку Назад, чтобы она вела к списку заданий
    buttons.append([InlineKeyboardButton(text="◀️ К списку заданий", callback_data="admin_manage_tasks")]) # Изменено callback_data
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_task_delete_confirm_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления задания"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑️ Да, удалить", callback_data=f"confirm_admin_task_delete_{task_id}"),
            InlineKeyboardButton(text="◀️ Отмена", callback_data=f"admin_task_view_{task_id}") # Вернуться к просмотру этого задания
        ]
    ])

def admin_task_premium_options_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора требования Premium для задачи."""
    builder = InlineKeyboardBuilder()
    # Используем префикс addtask_premium_
    builder.button(text="Всем пользователям", callback_data="addtask_premium_all")
    builder.button(text="Только Premium [⭐️]", callback_data="addtask_premium_only")
    builder.button(text="Только НЕ Premium [🚫⭐️]", callback_data="addtask_premium_non_premium")
    builder.button(text="❌ Отменить", callback_data="cancel_state")
    builder.adjust(1) # Каждая кнопка на новой строке
    return builder.as_markup()

def task_max_completions_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для лимита выполнений задания."""
    builder = InlineKeyboardBuilder()
    # Кнопка "По умолчанию" (1,000,000)
    builder.button(text="По умолчанию (1,000,000)", callback_data="addtask_default_max_completions")
    builder.button(text="❌ Отменить", callback_data="cancel_state")
    builder.adjust(1) # Каждая кнопка на новой строке
    return builder.as_markup()

def time_distribution_choice_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора использования временного распределения."""
    builder = InlineKeyboardBuilder()
    builder.button(text="⏰ Включить временное распределение", callback_data="time_dist_yes")
    builder.button(text="🚫 Обычное задание", callback_data="time_dist_no")
    builder.button(text="❌ Отменить", callback_data="cancel_state")
    builder.adjust(1) # Каждая кнопка на новой строке
    return builder.as_markup()

def distribution_hours_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура быстрого выбора количества часов для распределения."""
    builder = InlineKeyboardBuilder()
    builder.button(text="24 часа", callback_data="hours_24")
    builder.button(text="48 часов", callback_data="hours_48")
    builder.button(text="72 часа", callback_data="hours_72")
    builder.button(text="168 часов (неделя)", callback_data="hours_168")
    builder.button(text="✏️ Ввести своё время", callback_data="hours_custom")
    builder.button(text="❌ Отменить", callback_data="cancel_state")
    builder.adjust(2, 2, 1, 1) # Первые 4 кнопки по 2 в ряду, потом по одной
    return builder.as_markup()

def earn_stars_task_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Выполнить задание", callback_data="complete_tasks")]
    ])

def earn_stars_task_again_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Попробовать снова", callback_data="complete_tasks")]
    ])

# --- Клавиатуры для Индивидуальных Ссылок ---
# ... existing code ...

def admin_ind_links_menu_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура главного меню управления индивидуальными ссылками."""
    builder = InlineKeyboardBuilder()
    # Кнопки как на скриншоте (заменяем "Изменить статистику")
    builder.button(text="📊 Список ссылок / Статистика", callback_data="admin_view_ind_links_list")
    # --- Раскомментируем кнопки ---
    builder.button(text="➕ Добавить новую ссылку", callback_data="admin_add_ind_link")
    builder.button(text="🗑️ Удалить спец. ссылку", callback_data="admin_delete_ind_link_by_id") 
    
    builder.button(text="📈 Получить статистику", callback_data="admin_get_ind_link_stats") 
    # ------------------------------
    builder.button(text="◀️ Назад", callback_data="admin_back_to_main")
    builder.adjust(1, 2, 1) # Скорректируем расположение: Добавить | Удалить, Статистика | Список | Назад
    return builder.as_markup()

def admin_ind_links_list_keyboard(links_with_stats: List[Tuple[IndividualLink, int, int]]) -> InlineKeyboardMarkup:
    """Клавиатура для списка индивидуальных ссылок со статистикой."""
    buttons = []
    for link, total_reg, passed_op in links_with_stats:
        op_div_3 = passed_op // 3 if passed_op >= 3 else 0
        # Ограничиваем длину идентификатора и описания
        identifier_short = (link.identifier[:20] + '...') if len(link.identifier) > 20 else link.identifier
        # Кнопка показывает идентификатор и базовую стату (Зарег./Прошли ОП)
        button_text = f"🔗 {identifier_short} (Reg: {total_reg} / OP: {passed_op})"
        buttons.append([
            InlineKeyboardButton(text=button_text, callback_data=f"admin_ind_link_view_{link.id}")
        ])

    # Оставляем кнопку Назад, ведущую в меню спец. ссылок
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_manage_ind_links")])
    # Убираем кнопку Добавить отсюда, т.к. она есть в меню
    # buttons.append([InlineKeyboardButton(text="➕ Добавить новую ссылку", callback_data="admin_add_ind_link")])
    # Убираем кнопку Назад в админ-панель отсюда
    # buttons.append([InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_ind_link_manage_keyboard(link: IndividualLink, total_reg: int, passed_op: int) -> InlineKeyboardMarkup:
    """Клавиатура для просмотра/удаления конкретной индивидуальной ссылки."""
    buttons = []
    # Добавить кнопку Редактировать, если нужно
    buttons.append([InlineKeyboardButton(text="🗑️ Удалить ссылку", callback_data=f"admin_ind_link_delete_{link.id}")])
    # Обновляем кнопку Назад, чтобы она вела к списку ссылок
    buttons.append([InlineKeyboardButton(text="◀️ К списку ссылок", callback_data="admin_view_ind_links_list")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_ind_link_delete_confirm_keyboard(link_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления индивидуальной ссылки."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑️ Да, удалить", callback_data=f"admin_ind_link_delete_confirm_{link_id}"),
            InlineKeyboardButton(text="◀️ Отмена", callback_data=f"admin_ind_link_view_{link_id}") # Вернуться к просмотру этой ссылки
        ]
    ])

# --- Конец Клавиатур для Индивидуальных Ссылок ---

# --- Клавиатура выбора Premium для канала ---

def admin_channel_premium_options_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора требования Premium для канала."""
    builder = InlineKeyboardBuilder()
    builder.button(text="Всем пользователям", callback_data="addchannel_premium_all")
    builder.button(text="Только Premium", callback_data="addchannel_premium_only")
    builder.button(text="Только НЕ Premium", callback_data="addchannel_premium_non_premium")
    builder.button(text="❌ Отменить", callback_data="cancel_state")
    builder.adjust(1) # Каждая кнопка на новой строке
    return builder.as_markup()

def admin_channels_list_keyboard(channels: List[Channel], check_type: str) -> InlineKeyboardMarkup:
    """Клавиатура для списка каналов ОП в админке (с указанием типа, Premium и этапа)."""
    buttons = []
    # --- Сортируем каналы по этапу, затем по ID для наглядности ---
    channels.sort(key=lambda ch: (ch.check_stage, ch.id))

    for channel in channels:
        status_icon = "✅" if channel.channel_status == 'Публичный' else "🔒"
        premium_req_text = ""
        if channel.premium_requirement == 'premium_only':
            premium_req_text = " [⭐️]"
        elif channel.premium_requirement == 'non_premium_only':
            premium_req_text = " [🚫⭐️]"

        # --- Добавляем этап в callback и текст ---
        stage_text = f" [Э{channel.check_stage}]"
        delete_callback_data = f"admin_channel_delete_{check_type}_{channel.id}" # ID базы данных уникален, этап не нужен для удаления
        channel_name_short = (channel.channel_name[:15] + '...') if len(channel.channel_name) > 15 else channel.channel_name

        # Кнопка для переключения этапа
        next_stage = 2 if channel.check_stage == 1 else 1
        switch_stage_callback = f"admin_channel_set_stage_{channel.id}_{next_stage}"
        switch_stage_text = f"-> Э{next_stage}"

        buttons.append([
            InlineKeyboardButton(
                text=f"{status_icon}{stage_text} {channel_name_short}{premium_req_text}",
                url=channel.channel_link if channel.channel_link else None,
            ),
            # --- Кнопка переключения этапа ---
            InlineKeyboardButton(
                text=switch_stage_text,
                callback_data=switch_stage_callback
            ),
            InlineKeyboardButton(
                text="🗑️",
                callback_data=delete_callback_data
            )
        ])

    add_callback_data = f"admin_add_channel_{check_type}"
    type_text = check_type.capitalize()
    buttons.append([InlineKeyboardButton(text=f"➕ Добавить канал ({type_text})", callback_data=add_callback_data)])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_channel_delete_confirm_keyboard(channel_db_id: int, check_type: str) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления канала ОП (с указанием типа)."""
    cancel_callback_data = f"admin_manage_channels_{check_type}" 
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑️ Да, удалить", callback_data=f"admin_channel_delete_confirm_{check_type}_{channel_db_id}"),
            InlineKeyboardButton(text="◀️ Отмена", callback_data=cancel_callback_data)
        ]
    ])

# --- Клавиатура для проверки подписки (с этапами) ---
def get_subscription_keyboard(channels: List[Channel], stage: int) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру со ссылками на каналы и кнопкой проверки подписки для указанного этапа.
    На первом этапе выводит не более 6 каналов.
    """
    builder = InlineKeyboardBuilder()

    # --- Ограничиваем количество каналов на первом этапе ---
    if stage == 1:
        channels_to_show = channels[:6]
    else:
        channels_to_show = channels # На втором этапе показываем все

    for i, channel in enumerate(channels_to_show):
        # --- Используем channel_link из модели ---
        builder.button(text=f"Канал {i+1}", url=channel.channel_link) # Используем channel_link

    # Кнопка проверки подписки с указанием этапа
    builder.button(text=f"✅ Я подписался", callback_data=f"check_subscription_stage_{stage}")

    # Распределяем кнопки: по одной на канал, кнопка проверки внизу
    builder.adjust(*([1] * len(channels_to_show)), 1) # Используем channels_to_show
    return builder.as_markup()

# --- Клавиатура для меню управления каналами подписки в админке ---
def admin_sub_channels_menu_keyboard(channels: List[Channel]) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру для управления каналами подписки в админ-панели.
    """
    builder = InlineKeyboardBuilder()
    if channels:
        builder.row(InlineKeyboardButton(text="--- Список Каналов ---", callback_data="noop")) # Просто заголовок
        for channel in channels:
            # Показываем ID канала и текущий этап
            stage_text = f"Этап: {channel.check_stage}"
            # Кнопка для переключения этапа
            toggle_button = InlineKeyboardButton(
                text=f"🔄 {stage_text}",
                callback_data=f"admin_toggle_sub_channel_stage_{channel.id}"
            )
            # Кнопка для удаления
            delete_button = InlineKeyboardButton(
                text="🗑️ Удалить",
                callback_data=f"admin_delete_sub_channel_{channel.id}"
            )
            # Добавляем информацию о канале и кнопки в строку
            builder.row(
                InlineKeyboardButton(text=f"ID: {channel.channel_id}", url=channel.channel_url), # Ссылка на канал
                toggle_button,
                delete_button
            )
        builder.row(InlineKeyboardButton(text="-"*20, callback_data="noop")) # Разделитель

    # Кнопки управления
    builder.row(InlineKeyboardButton(text="➕ Добавить канал", callback_data="admin_add_sub_channel"))
    builder.row(InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")) # Кнопка назад

    return builder.as_markup()

# --- Клавиатура "Назад к списку каналов" ---
def admin_back_to_sub_channels_keyboard() -> InlineKeyboardMarkup:
    """
    Создает клавиатуру с кнопкой "Назад к списку каналов".
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад к списку каналов", callback_data="admin_manage_sub_channels")
    return builder.as_markup()

# --- Клавиатура подтверждения/отмены рассылки ---
def newsletter_confirm_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения запуска рассылки."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Запустить рассылку", callback_data="newsletter_confirm_send")
    builder.button(text="❌ Отменить", callback_data="cancel_state")
    builder.adjust(1)
    return builder.as_markup()

def admin_rewards_keyboard(current_ref_reward: float) -> InlineKeyboardMarkup:
    """Клавиатура для управления наградами."""
    buttons = [
        [InlineKeyboardButton(text=f"Реф. награда: {current_ref_reward:.2f}⭐️ - Изменить", callback_data="admin_change_ref_reward")],
        # Сюда можно добавить кнопки для других наград (ежедневный бонус и т.д.)
        [InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- Клавиатура для выбора шаблона (для удаления/редактирования) ---
def select_template_keyboard(templates: List[BroadcastTemplate], action_prefix: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора шаблона для определенного действия."""
    builder = InlineKeyboardBuilder()
    for template in templates:
        builder.button(text=template.name, callback_data=f"{action_prefix}{template.id}")
    builder.button(text="◀️ Назад в меню шаблонов", callback_data="admin_manage_templates")
    builder.adjust(1)
    return builder.as_markup()


# --- Клавиатура меню редактирования шаблона ---
def template_edit_menu_keyboard(template_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для выбора поля для редактирования шаблона."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Изменить имя", callback_data=f"template_edit_field_name_{template_id}")
    builder.button(text="📄 Изменить текст", callback_data=f"template_edit_field_text_{template_id}")
    builder.button(text="🖼️ Изменить/удалить фото", callback_data=f"template_edit_field_photo_{template_id}")
    builder.button(text="⌨️ Изменить/удалить клавиатуру", callback_data=f"template_edit_field_keyboard_{template_id}")
    builder.button(text="◀️ Назад к выбору шаблона", callback_data="template_edit_select")
    builder.adjust(1)
    return builder.as_markup()

# --- Новая клавиатура для выбора этапа при добавлении канала ---
def admin_channel_stage_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора этапа проверки канала."""
    buttons = [
        [
            InlineKeyboardButton(text="Этап 1", callback_data="addchannel_stage_1"),
            InlineKeyboardButton(text="Этап 2", callback_data="addchannel_stage_2")
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel_state")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_manage_user_keyboard(user: User) -> InlineKeyboardMarkup:
    """Клавиатура для управления найденным пользователем."""
    builder = InlineKeyboardBuilder()

    # Кнопка бана/разбана
    ban_action = "unban" if user.banned else "ban"
    ban_text = "✅ Разбанить" if user.banned else "🚫 Забанить"
    builder.button(text=ban_text, callback_data=f"admin_user_{ban_action}_{user.user_id}")

    # Кнопки управления балансом
    builder.button(text="➕ Добавить ⭐️", callback_data=f"admin_user_add_stars_{user.user_id}")
    builder.button(text="➖ Снять ⭐️", callback_data=f"admin_user_subtract_stars_{user.user_id}")
    builder.button(text="🗑️ Удалить из БД", callback_data=f"admin_user_delete_{user.user_id}")

    # Кнопка назад к поиску или в гл. меню (решим в хендлере)
    builder.button(text="◀️ Назад", callback_data="admin_back_to_main") # Пока ведет в главное меню

    # Расположение: Бан | Добавить / Снять | Назад
    builder.adjust(1, 2, 1, 1)
    return builder.as_markup()

def admin_shows_list_keyboard(shows: List[Show]) -> InlineKeyboardMarkup:
    """Клавиатура для списка 'показов' в админке."""
    builder = InlineKeyboardBuilder()
    for show in shows:
        status_icon = "🟢" if show.is_active else "⚫️"
        button_text = f"{status_icon} {show.name}"
        builder.button(text=button_text, callback_data=f"admin_show_view_{show.id}")

    builder.button(text="➕ Добавить новый показ", callback_data="admin_add_show")
    builder.button(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def admin_show_manage_keyboard(show: Show) -> InlineKeyboardMarkup:
    """Клавиатура для управления конкретным 'показом'."""
    builder = InlineKeyboardBuilder()
    
    if not show.is_active:
        builder.button(text="🟢 Активировать", callback_data=f"admin_show_toggle_{show.id}")
    
    builder.button(text="🗑️ Удалить показ", callback_data=f"admin_show_delete_{show.id}")
    builder.button(text="◀️ К списку показов", callback_data="admin_manage_shows")
    builder.adjust(1)
    return builder.as_markup()

def admin_show_delete_confirm_keyboard(show_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления 'показа'."""
    builder = InlineKeyboardBuilder()
    builder.button(text="🗑️ Да, удалить", callback_data=f"admin_show_delete_confirm_{show_id}")
    builder.button(text="◀️ Отмена", callback_data=f"admin_show_view_{show_id}")
    builder.adjust(1)
    return builder.as_markup()

def admin_confirm_delete_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления пользователя."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_confirm_delete_yes_{user_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_confirm_delete_no_{user_id}")
        ]
    ])

def back_stats_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для возврата в админ-панель."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад в админ-панель", callback_data="admin_back_to_main")]
    ])

def admin_confirm_ban_keyboard(user_id: int, ban_action: str) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения бана/разбана."""
    confirm_text = "Да, забанить" if ban_action == "ban" else "Да, разбанить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"✅ {confirm_text}", callback_data=f"admin_confirm_{ban_action}_yes_{user_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_confirm_{ban_action}_no_{user_id}")
        ]
    ])

def admin_confirm_balance_change_keyboard(user_id: int, action: str, amount: float) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения изменения баланса."""
    action_text = "добавить" if action == "add" else "снять"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"✅ Да, {action_text} {amount}⭐️", callback_data=f"admin_confirm_balance_yes_{user_id}_{action}_{amount}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_confirm_balance_no_{user_id}_{action}_{amount}")
        ]
    ])

def admin_daily_tasks_stats_keyboard():
    """Клавиатура для статистики ежедневных заданий"""
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🔄 Обновить", callback_data="admin_daily_tasks_stats")
    keyboard.button(text="👤 Проверить пользователя", callback_data="admin_daily_user_search")
    keyboard.button(text="◀️ Назад", callback_data="admin_back_to_main")
    keyboard.adjust(1)
    return keyboard.as_markup()

def admin_daily_user_search_keyboard():
    """Клавиатура для поиска пользователя в статистике ежедневных заданий"""
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🔍 Другой пользователь", callback_data="admin_daily_user_search")
    keyboard.button(text="📊 Общая статистика", callback_data="admin_daily_tasks_stats")
    keyboard.button(text="◀️ Назад", callback_data="admin_back_to_main")
    keyboard.adjust(1)
    return keyboard.as_markup()

def admin_daily_user_search_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_daily_tasks_stats")]
    ])
    return keyboard

def admin_gift_settings_keyboard(settings) -> InlineKeyboardMarkup:
    """Клавиатура для управления настройками подарков"""
    status_text = "✅ Включено" if settings and settings.enabled else "❌ Выключено"
    min_amount = settings.min_amount_for_gifts if settings else 15
    max_remainder = settings.max_remainder if settings else 10
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Статус: {status_text}", callback_data="admin_gift_toggle_status")],
        [InlineKeyboardButton(text=f"Мин. сумма: {min_amount} ⭐", callback_data="admin_gift_set_min_amount")],
        [InlineKeyboardButton(text=f"Макс. остаток: {max_remainder} ⭐", callback_data="admin_gift_set_max_remainder")],
        [InlineKeyboardButton(text="📊 Статистика выплат", callback_data="admin_gift_stats")],
        [InlineKeyboardButton(text="❌ Проблемные заявки", callback_data="admin_gift_failed")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back_to_main")]
    ])
    return keyboard

def admin_gift_stats_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для статистики подарков"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_gift_settings")]
    ])
    return keyboard

def admin_gift_failed_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для проблемных заявок"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_gift_failed")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_gift_settings")]
    ])
    return keyboard

def contact_confirmation_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Написал, продолжить", callback_data="contact_confirmed")],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_main")]
    ])
    return keyboard


def get_combined_op_keyboard(
    local_channels: List[Channel],
    subgram_sponsors: list,
    check_type: str,
    stage: int = 1
) -> InlineKeyboardMarkup:
    """
    Клавиатура ОП: объединяет локальные каналы и спонсоров SubGram в один блок.
    """
    builder = InlineKeyboardBuilder()
    counter = 1
    row_buttons = []

    for channel in local_channels:
        link = channel.channel_link
        is_bot_link = '_bot' in link.lower() or '?start=' in link
        text = f'Бот #{counter}' if is_bot_link else f'Канал #{counter}'
        button = InlineKeyboardButton(text=text, url=link)
        row_buttons.append(button)
        if len(row_buttons) == 2:
            builder.row(*row_buttons)
            row_buttons = []
        counter += 1

    for sponsor in subgram_sponsors:
        link = sponsor.get('link', '')
        s_type = sponsor.get('type', 'channel')
        text = f'Бот #{counter}' if s_type == 'bot' else f'Канал #{counter}'
        button = InlineKeyboardButton(text=text, url=link)
        row_buttons.append(button)
        if len(row_buttons) == 2:
            builder.row(*row_buttons)
            row_buttons = []
        counter += 1

    if row_buttons:
        builder.row(*row_buttons)

    recheck_callback = f"recheck_sub_{check_type}_stage_{stage}"
    builder.row(
        InlineKeyboardButton(text="✅ Проверить подписку", callback_data=recheck_callback)
    )

    return builder.as_markup()