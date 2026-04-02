from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import List, Dict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.core.utils.logging import logger
from bot.database.models import Task, TaskExtensionState
from bot.database.requests import get_task_actual_completions_count


async def extend_overdue_time_distributed_tasks(session: AsyncSession, append_hours: int = 4) -> Dict:
    """
    Пролонгирует на append_hours часов задания с временным распределением, у которых закончился период,
    но общий лимит выполнений ещё не исчерпан. Повторное продление одного задания возможно не чаще,
    чем раз в append_hours часов.

    Условия:
    - Не трогаем задания, у которых ещё есть оставшееся время (now < start_time + time_distribution_hours)
    - Повторное продление одного и того же задания разрешено только если прошло не меньше append_hours
      с момента последнего продления (хранится в таблице task_extension_state)
    - Распределяем оставшиеся выполнения равномерно по добавляемым часам
    """
    # Получаем список активных заданий с временным распределением
    query = (
        select(Task)
        .where(Task.is_time_distributed == True)
        .where(Task.is_active == True)
    )
    result = await session.execute(query)
    tasks: List[Task] = list(result.scalars().all())

    now = datetime.utcnow()
    if not tasks:
        return {
            "extended_count": 0,
            "total_remaining": 0,
            "append_hours": append_hours,
            "execution_time": now.isoformat(),
            "extended_tasks_details": [],
            "total_tasks_checked": 0
        }

    extended_count = 0
    extended_tasks_details = []  # Детали первых 5 продленных заданий
    total_remaining_before = 0
    total_remaining_after = 0

    for task in tasks:
        try:
            # Базовые проверки корректности данных
            if not task.start_time or not task.time_distribution_hours or not task.hourly_distribution:
                logger.warning(f"Task {task.id} has invalid time distribution data, skipping")
                continue

            # Не продлеваем те, у кого период ещё не закончился
            end_time = task.start_time + timedelta(hours=task.time_distribution_hours)
            if now < end_time:
                continue

            # Не продлеваем слишком часто: разрешаем повторное продление только спустя append_hours с момента прошлого
            ext_state_result = await session.execute(
                select(TaskExtensionState).where(TaskExtensionState.task_id == task.id)
            )
            ext_state: TaskExtensionState | None = ext_state_result.scalar_one_or_none()
            if ext_state and ext_state.last_extended_at:
                if (now - ext_state.last_extended_at) < timedelta(hours=append_hours):
                    # ещё не прошло достаточно времени с прошлого продления
                    continue

            # Правильно рассчитываем оставшиеся выполнения из hourly_distribution
            try:
                distribution: List[int] = json.loads(task.hourly_distribution) if task.hourly_distribution else []
            except Exception:
                distribution = []

            if not isinstance(distribution, list) or not distribution:
                logger.warning(f"Task {task.id} has invalid hourly_distribution format, skipping")
                continue
                
            # Проверяем, что распределение соответствует заявленному количеству часов
            if len(distribution) != task.time_distribution_hours:
                logger.warning(f"Task {task.id} distribution length ({len(distribution)}) doesn't match time_distribution_hours ({task.time_distribution_hours}), skipping")
                continue

            # Вычисляем прошедшие часы с момента старта
            time_passed = now - task.start_time
            hours_passed = int(time_passed.total_seconds() // 3600)
            
            # План: вся сумма распределения
            planned_total = sum(distribution)
            # Факт: актуальные выполнения из БД (по новой системе)
            actual_total = await get_task_actual_completions_count(session, task.id)
            # Остаток: план - факт
            remaining = max(0, planned_total - actual_total)
            
            logger.debug(
                f"Task {task.id}: hours_passed={hours_passed}, planned_total={planned_total}, actual_total={actual_total}, remaining={remaining}, distribution_length={len(distribution)}"
            )
            
            if remaining <= 0:
                logger.debug(f"Task {task.id}: no remaining completions, skipping")
                continue

            # Сохраняем детали для первых 5 заданий
            task_details = {
                "task_id": task.id,
                "description": task.description[:100] + "..." if len(task.description) > 100 else task.description,
                "hours_passed": hours_passed,
                "remaining_before": remaining,
                "old_distribution_hours": task.time_distribution_hours,
                "old_distribution": distribution.copy()
            }

            # Равномерно распределяем оставшиеся выполнения по новым часам
            base = remaining // append_hours
            extra = remaining % append_hours
            appended = [base] * append_hours
            for i in range(extra):
                appended[i] += 1

            # Создаем новое распределение:
            # - докидываем нули до текущего часа, если с момента старта прошло больше часов, чем длина плана
            # - добавляем новые часы прямо с ТЕКУЩЕГО часа, чтобы пользователям сразу выдавались лимиты
            if hours_passed > len(distribution):
                gap = hours_passed - len(distribution)
                padded = distribution + ([0] * gap)
            else:
                padded = distribution[:hours_passed]
            new_distribution = padded + appended
            
            # Общее количество часов должно соответствовать длине нового распределения
            new_total_hours = len(new_distribution)

            # Обновляем поля задания
            task.hourly_distribution = json.dumps(new_distribution)
            task.time_distribution_hours = new_total_hours

            # Фиксируем факт продления в БД
            if ext_state:
                ext_state.last_extended_at = now
            else:
                ext_state = TaskExtensionState(task_id=task.id, last_extended_at=now)
                session.add(ext_state)
            
            # Обновляем статистику
            total_remaining_before += remaining
            total_remaining_after += remaining  # Остается то же количество, просто перераспределяется
            
            # Дополняем детали задания
            try:
                limit_now = new_distribution[hours_passed] if hours_passed < len(new_distribution) else 0
            except Exception:
                limit_now = 0
            task_details.update({
                "new_distribution_hours": task.time_distribution_hours,
                "new_distribution": new_distribution,
                "appended_distribution": appended,
                "extended_at": now.isoformat(),
                "current_hour_index": hours_passed,
                "current_hour_limit": limit_now
            })
            
            # Сохраняем детали для первых 5 заданий
            if len(extended_tasks_details) < 5:
                extended_tasks_details.append(task_details)
            
            logger.info(
                f"Extended task {task.id}: {remaining} completions over {append_hours}h; "
                f"hours {task_details['old_distribution_hours']} -> {new_total_hours}; "
                f"current_slot=({hours_passed}), limit_now={limit_now}"
            )
            extended_count += 1
        except Exception as e:
            logger.error(f"Error extending task {getattr(task, 'id', 'unknown')}: {e}", exc_info=True)


    # Общая статистика
    if extended_count:
        logger.info(f"Extended {extended_count} time-distributed task(s) by {append_hours} hours.")

    # Возвращаем статистику для отправки администратору
    return {
        "extended_count": extended_count,
        "total_remaining": total_remaining_before,
        "append_hours": append_hours,
        "execution_time": now.isoformat(),
        "extended_tasks_details": extended_tasks_details,
        "total_tasks_checked": len(tasks)
    }


async def find_tasks_to_extend(session: AsyncSession, append_hours: int = 4) -> List[Dict]:
    """
    Dry-run: возвращает список заданий, которые были бы продлены по текущим правилам, без изменения БД.
    Формат элемента: {"task_id": int, "remaining": int, "ended_at": datetime, "last_extended_at": datetime|None}
    """
    query = (
        select(Task)
        .where(Task.is_time_distributed == True)
        .where(Task.is_active == True)
    )
    result = await session.execute(query)
    tasks: List[Task] = list(result.scalars().all())

    now = datetime.utcnow()
    candidates: List[Dict] = []

    for task in tasks:
        if not task.start_time or not task.time_distribution_hours or not task.hourly_distribution:
            continue

        end_time = task.start_time + timedelta(hours=task.time_distribution_hours)
        if now < end_time:
            continue

        ext_state_result = await session.execute(
            select(TaskExtensionState).where(TaskExtensionState.task_id == task.id)
        )
        ext_state: TaskExtensionState | None = ext_state_result.scalar_one_or_none()
        last_extended_at = ext_state.last_extended_at if ext_state else None
        if last_extended_at and (now - last_extended_at) < timedelta(hours=append_hours):
            continue

        # Правильно рассчитываем оставшиеся выполнения из hourly_distribution
        try:
            distribution: List[int] = json.loads(task.hourly_distribution) if task.hourly_distribution else []
        except Exception:
            distribution = []

        if not isinstance(distribution, list) or not distribution:
            continue
            
        # Проверяем, что распределение соответствует заявленному количеству часов
        if len(distribution) != task.time_distribution_hours:
            continue

        # Вычисляем прошедшие часы с момента старта
        time_passed = now - task.start_time
        hours_passed = int(time_passed.total_seconds() // 3600)
        
        # План: вся сумма распределения
        planned_total = sum(distribution)
        # Факт: актуальные выполнения из БД (по новой системе)
        actual_total = await get_task_actual_completions_count(session, task.id)
        remaining = max(0, planned_total - actual_total)
        
        if remaining <= 0:
            continue

        candidates.append({
            "task_id": task.id,
            "remaining": remaining,
            "ended_at": end_time,
            "last_extended_at": last_extended_at
        })

    return candidates


async def send_extension_stats_to_admin(bot, stats: Dict) -> None:
    """
    Отправляет статистику продления заданий администратору
    """
    admin_id = 7631252818
    
    if stats["extended_count"] == 0:
        message = f"📊 <b>Статистика продления заданий</b>\n\n"
        message += f"⏰ Время: {stats['execution_time']}\n"
        message += f"🔍 Проверено заданий: {stats['total_tasks_checked']}\n"
        message += f"✅ Продлено заданий: {stats['extended_count']}\n"
        message += f"ℹ️ Нет заданий для продления"
    else:
        message = f"📊 <b>Статистика продления заданий</b>\n\n"
        message += f"⏰ Время: {stats['execution_time']}\n"
        message += f"🔍 Проверено заданий: {stats['total_tasks_checked']}\n"
        message += f"✅ Продлено заданий: {stats['extended_count']}\n"
        message += f"⏱️ Добавлено часов: {stats['append_hours']}\n"
        message += f"🎯 Всего оставшихся выполнений: {stats['total_remaining']}\n\n"
        
        if stats['extended_tasks_details']:
            message += f"<b>Детали первых {len(stats['extended_tasks_details'])} заданий:</b>\n"
            for i, details in enumerate(stats['extended_tasks_details'], 1):
                message += f"\n{i}. <b>ID {details['task_id']}</b>\n"
                message += f"   📝 {details['description']}\n"
                message += f"   ⏳ Прошло часов: {details['hours_passed']}\n"
                message += f"   🎯 Осталось выполнений: {details['remaining_before']}\n"
                message += f"   📈 Часов: {details['old_distribution_hours']} → {details['new_distribution_hours']}\n"
                message += f"   📊 Новое распределение: {details['appended_distribution']}"
    
    try:
        await bot.send_message(admin_id, message, parse_mode='HTML')
        logger.info(f"Sent extension stats to admin {admin_id}")
    except Exception as e:
        logger.error(f"Failed to send stats to admin {admin_id}: {e}")


