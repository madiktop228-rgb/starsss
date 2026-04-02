from aiogram.fsm.state import State, StatesGroup

class WithdrawState(StatesGroup):
    sum = State()
    username = State()
    contact_confirmation = State()  # Новое состояние для подтверждения написания аккаунту
    gift_selection = State()  # Новое состояние для выбора подарков
    # id_payments = State() # Возможно, это состояние больше не нужно?

class AddTaskState(StatesGroup):
    waiting_for_description = State()
    waiting_for_reward = State()
    waiting_for_instruction_link = State()
    waiting_for_action_link = State()
    waiting_for_check_subscription = State()
    waiting_for_status = State()
    waiting_for_channel_id = State()
    waiting_for_premium_requirement = State()
    waiting_for_max_completions = State() # Новое состояние для лимита выполнений
    waiting_for_time_distribution = State() # Выбор: использовать временное распределение или нет
    waiting_for_distribution_hours = State() # Количество часов для распределения
    confirming = State() # Для подтверждения

# Можно добавить EditTaskState по аналогии, если нужна редакция

# --- Состояния для добавления индивидуальной ссылки --- 
class AddIndividualLinkState(StatesGroup):
    waiting_for_identifier = State()
    waiting_for_description = State()
    confirming = State()
# ----------------------------------------------------

# --- Состояния для добавления канала --- 
class AddChannelState(StatesGroup):
    waiting_for_type = State()
    waiting_for_id = State()
    waiting_for_link = State()
    waiting_for_name = State()
    waiting_for_premium_requirement = State()
    waiting_for_stage = State()
    waiting_for_status = State()
    confirming = State()
# -------------------------------------

# --- Состояния для активации промокода ---
class PromoCodeState(StatesGroup):
    waiting_for_code = State()

# --- Новое состояние для настроек наград ---
class RewardSettingsState(StatesGroup):
    waiting_for_ref_reward = State()
    # Сюда можно добавить состояния для других наград
# -----------------------------------------

# --- Состояния для добавления промокода --- 
class AddPromoCodeState(StatesGroup):
    waiting_for_code = State()
    waiting_for_reward = State()
    waiting_for_max_uses = State()
    waiting_for_required_referrals = State()
    waiting_for_required_referrals_24h = State() # Добавляем состояние для рефералов за 24 часа
    waiting_for_ref_24h_condition = State()  # Состояние для требования по рефералам за 24 часа
    confirming = State()
# ------------------------------------------

# --- Добавляем состояния для рассылки ---
class Sletter(StatesGroup):
    message = State()
    buttons = State()

class SletterImage(StatesGroup):
    image = State()
    message = State()
    buttons = State()

class SletterVideo(StatesGroup):
    video = State()
    message = State()
    buttons = State()

class SletterButtons(StatesGroup):
    message = State()
    buttons = State()

class SletterButtonsImage(StatesGroup):
    photo = State()
    buttons = State()
    message = State()
# --------------------------------------

class AdminManageUser(StatesGroup):
    waiting_for_input = State() # Ожидание ID или Username
    # Состояния для добавления/вычитания звезд
    waiting_for_add_amount = State()
    waiting_for_subtract_amount = State()
    confirming_balance_change = State()
    # Добавляем новое состояние для ожидания суммы списания
    subtracting_stars = State()

class AdminDeleteIndLinkState(StatesGroup):
    waiting_for_identifier = State()
    confirm_delete = State() # Добавим состояние подтверждения

class AdminGetIndLinkStatsState(StatesGroup):
    waiting_for_identifier = State()

# --- Новые состояния для управления шаблонами рассылок ---
class AdminTemplateStates(StatesGroup):
    # Создание
    waiting_for_name = State()
    waiting_for_text = State()
    waiting_for_photo = State()
    waiting_for_keyboard = State()
    confirm_creation = State()

    # Редактирование
    selecting_for_edit = State()
    editing_field = State()
    waiting_for_edit_value = State()

    # Удаление
    selecting_for_delete = State()
    confirm_delete = State()

class NewsletterStates(StatesGroup):
    # choosing_source = State() # Можно добавить, если нужно состояние до выбора
    selecting_template = State() # Выбор шаблона из списка
    getting_text = State()       # Ввод текста вручную
    getting_photo = State()      # Ввод фото вручную
    getting_keyboard = State()   # Ввод клавиатуры вручную
    confirming_send = State()    # Финальное подтверждение перед запуском

class SubscriptionCheckStates(StatesGroup):
    waiting_primary_check = State()   # Ожидание проверки 1-го этапа
    waiting_secondary_check = State() # Ожидание проверки 2-го этапа

class PromoCodeStateTemplate(StatesGroup):
    waiting_for_promocode = State()

class TraffyTaskState(StatesGroup):
    waiting_for_task = State()

class GiftSettingsState(StatesGroup):
    waiting_for_min_amount = State()
    waiting_for_max_remainder = State()


class AddShowState(StatesGroup):
    waiting_for_name = State()
    waiting_for_text = State()
    waiting_for_photo = State()
    waiting_for_keyboard = State()
    confirming = State()