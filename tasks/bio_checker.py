import asyncio
import logging
from datetime import datetime, timedelta
from bot.database.requests import (
    get_users_for_bio_check, 
    apply_bio_penalty, 
    update_bio_check_date,
    check_referral_link_in_bio
)
from bot.database.models import User

logger = logging.getLogger(__name__)

async def check_users_bio(bot, config, session_factory):
    """
    Проверяет био пользователей на наличие реферальной ссылки.
    - Штрафует, если ссылка удалена и штрафа еще не было.
    - Сбрасывает флаг штрафа, если ссылка восстановлена.
    """
    try:
        async with session_factory() as session:
            # Получаем пользователей для проверки, которые получали награду за био
            users = await get_users_for_bio_check(session, hours_ago=24)
            
            if not users:
                logger.info("Нет пользователей для проверки био.")
                return
                
            logger.info(f"Начинаем проверку био у {len(users)} пользователей.")
            
            penalties_applied_count = 0
            
            for user in users:
                try:
                    bot_info = await bot.get_me()
                    referral_link = f"t.me/{bot_info.username}?start={user.user_id}"
                    
                    has_link = await check_referral_link_in_bio(bot, user.user_id, referral_link)
                    
                    if has_link:
                        # Если ссылка есть, а штраф был - сбрасываем дату штрафа
                        if user.last_bio_penalty_date:
                            logger.info(f"Пользователь {user.user_id} восстановил ссылку в био. Сбрасываем флаг штрафа.")
                        else:
                            logger.debug(f"У пользователя {user.user_id} ссылка на месте. Все в порядке.")
                    else:
                        # Если ссылки нет, проверяем, был ли уже штраф
                        if user.last_bio_penalty_date:
                            logger.info(f"Пользователь {user.user_id} уже был оштрафован и не восстановил ссылку. Пропускаем.")
                        else:
                            # Штрафа не было - применяем
                            logger.info(f"У пользователя {user.user_id} нет ссылки в био. Применяем штраф.")
                            success = await apply_bio_penalty(session, user.user_id, penalty_amount=0.2)
                            
                            if success:
                                penalties_applied_count += 1
                                logger.info(f"Штраф успешно применен к {user.user_id}.")
                                try:
                                    await bot.send_message(
                                        user.user_id,
                                        "⚠️ Вы удалили реферальную ссылку из своего био!\n"
                                        "С вашего баланса было списано 0.20 звезд.\n\n"
                                        "Чтобы избежать дальнейших штрафов и снова получать награды, пожалуйста, верните ссылку в био."
                                    )
                                except Exception as msg_error:
                                    logger.error(f"Не удалось отправить уведомление о штрафе пользователю {user.user_id}: {msg_error}")
                            else:
                                logger.error(f"Не удалось применить штраф к пользователю {user.user_id}.")

                    # В любом случае обновляем дату последней проверки
                    await update_bio_check_date(session, user.user_id)
                    
                    await asyncio.sleep(0.5)  # Пауза для избежания флуда
                    
                except Exception as e:
                    logger.error(f"Критическая ошибка при проверке пользователя {user.user_id}: {e}", exc_info=True)
                    continue
            
            # Сохраняем все изменения в базе данных
            await session.commit()
            
            if penalties_applied_count > 0:
                admin_message = (
                    f"🔍 **Проверка био завершена**\n\n"
                    f"👤 Проверено пользователей: `{len(users)}`\n"
                    f"🚫 Применено штрафов: `{penalties_applied_count}`"
                )
                for admin_id in config.admins:
                    try:
                        await bot.send_message(admin_id, admin_message, parse_mode="Markdown")
                    except Exception as admin_msg_error:
                        logger.warning(f"Не удалось отправить отчет админу {admin_id}: {admin_msg_error}")
                        
            logger.info(f"Проверка био завершена. Применено {penalties_applied_count} штрафов.")
            
    except Exception as e:
        logger.critical(f"Глобальная ошибка в задаче `check_users_bio`: {e}", exc_info=True) 