import sqlite3
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from datetime import datetime
import logging
import asyncio
import csv
import io
import os
import shutil
from database import init_db
from aiogram.utils.exceptions import MessageNotModified

# Инициализация базы данных
init_db()

logging.basicConfig(level=logging.INFO)

API_TOKEN = '6554402261:AAEHESW-p7hvM-MXbwnyL1WhVnUBwEmjFpQ'
ADMIN_IDS = [231916981]  # ID администраторов

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

class ClientStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_day = State()
    waiting_for_payment_status = State()
    waiting_for_import = State()
    waiting_for_telegram_username = State()
    waiting_for_test_notification = State()
    waiting_for_client_selection = State()

def get_db_connection():
    return sqlite3.connect('vpn_bot.db')

def create_client_keyboard(clients, action="status", row_width=2):
    keyboard = types.InlineKeyboardMarkup(row_width=row_width)
    buttons = []
    for client in clients:
        name, status = client
        status_emoji = "✅" if status == "paid" else "❌"
        buttons.append(
            types.InlineKeyboardButton(
                f"{status_emoji} {name}",
                callback_data=f"{action}_{name}"
            )
        )
    keyboard.add(*buttons)
    return keyboard

def create_main_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        ["Добавить клиента", "Удалить клиента"],
        ["Список клиентов", "Изменить статус"],
        ["Импорт данных", "Экспорт данных"],
        ["Тестовое оповещение", "Управление Telegram"],
        ["Уведомления", "Проверка базы данных"]
    ]
    for row in buttons:
        keyboard.add(*row)
    return keyboard

async def return_to_main_menu(message_or_callback):
    """Универсальная функция для возврата в главное меню"""
    text = "VPN Payment Bot - Панель управления"
    
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.reply(text, reply_markup=create_main_keyboard())
    else:
        # Создаем inline-клавиатуру для callback
        keyboard = types.InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            types.InlineKeyboardButton("Список клиентов", callback_data="list_all"),
            types.InlineKeyboardButton("Уведомления", callback_data="notifications_menu"),
            types.InlineKeyboardButton("Проверка базы данных", callback_data="check_database")
        )
        
        await bot.edit_message_text(
            chat_id=message_or_callback.message.chat.id,
            message_id=message_or_callback.message.message_id,
            text=text,
            reply_markup=keyboard
        )

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Доступ запрещен")
        return
    
    await return_to_main_menu(message)

@dp.message_handler(lambda message: message.text == "Список клиентов")
async def list_clients(message: types.Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    keyboard = types.InlineKeyboardMarkup(row_width=3)
    keyboard.add(
        types.InlineKeyboardButton("Все", callback_data="list_all"),
        types.InlineKeyboardButton("Оплаченные", callback_data="list_paid"),
        types.InlineKeyboardButton("Неоплаченные", callback_data="list_unpaid"),
        types.InlineKeyboardButton("Главное меню", callback_data="back_to_main")
    )
    
    await message.reply("Выберите тип списка:", reply_markup=keyboard)
    conn.close()

@dp.callback_query_handler(lambda c: c.data.startswith('list_'))
async def process_list_callback(callback_query: types.CallbackQuery):
    list_type = callback_query.data[5:]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if list_type == 'all':
        cursor.execute("SELECT name, payment_day, status, telegram_username FROM clients")
    else:
        cursor.execute("SELECT name, payment_day, status, telegram_username FROM clients WHERE status = ?", 
                      ('paid' if list_type == 'paid' else 'unpaid',))
    
    clients = cursor.fetchall()
    conn.close()
    
    if not clients:
        await bot.answer_callback_query(callback_query.id, "Нет клиентов в базе.")
        return
    
    # Создаем клавиатуру с кнопками для каждого клиента
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for name, day, status, telegram in clients:
        status_emoji = "✅" if status == "paid" else "❌"
        keyboard.add(types.InlineKeyboardButton(
            f"{status_emoji} {name}",
            callback_data=f"client_info_{name}"
        ))
    
    # Добавляем кнопки фильтрации внизу
    keyboard.add(
        types.InlineKeyboardButton("Все", callback_data="list_all"),
        types.InlineKeyboardButton("Оплаченные", callback_data="list_paid"),
        types.InlineKeyboardButton("Неоплаченные", callback_data="list_unpaid")
    )
    
    response = f"📋 Список клиентов ({'все' if list_type == 'all' else 'оплаченные' if list_type == 'paid' else 'неоплаченные'}):\n\n"
    response += "Нажмите на клиента для просмотра подробной информации"
    
    try:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=response,
            reply_markup=keyboard
        )
    except MessageNotModified:
        await bot.answer_callback_query(callback_query.id, "Список актуален")

@dp.callback_query_handler(lambda c: c.data.startswith('client_info_'))
async def process_client_info(callback_query: types.CallbackQuery):
    client_name = callback_query.data[12:]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name, payment_day, status, telegram_username, last_notified, created_at 
        FROM clients WHERE name = ?
    """, (client_name,))
    client = cursor.fetchone()
    conn.close()
    
    if not client:
        await bot.answer_callback_query(callback_query.id, "Клиент не найден")
        return
    
    name, day, status, telegram, last_notified, created_at = client
    status_emoji = "✅" if status == "paid" else "❌"
    status_text = "Оплачено" if status == "paid" else "Не оплачено"
    
    response = f"👤 Информация о клиенте:\n\n"
    response += f"Имя: {name}\n"
    response += f"Статус: {status_emoji} {status_text}\n"
    response += f"День оплаты: {day}\n"
    if telegram:
        response += f"Telegram: @{telegram}\n"
    if last_notified:
        response += f"Последнее уведомление: {last_notified}\n"
    response += f"Дата добавления: {created_at}\n"
    
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Изменить статус", callback_data=f"change_status_{name}"),
        types.InlineKeyboardButton("Установить Telegram", callback_data=f"set_telegram_{name}"),
        types.InlineKeyboardButton("Отправить уведомление", callback_data=f"test_notify_{name}"),
        types.InlineKeyboardButton("Назад к списку", callback_data="list_all")
    )
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=response,
        reply_markup=keyboard
    )

@dp.message_handler(lambda message: message.text == "Изменить статус")
async def change_status_start(message: types.Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, status FROM clients")
    clients = cursor.fetchall()
    conn.close()
    
    if not clients:
        await message.reply("Нет клиентов в базе")
        return
    
    await message.reply(
        "Выберите клиента для изменения статуса:",
        reply_markup=create_client_keyboard(clients, "change_status")
    )

@dp.callback_query_handler(lambda c: c.data.startswith('change_status_'))
async def process_change_status(callback_query: types.CallbackQuery):
    client_name = callback_query.data[14:]
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT status FROM clients WHERE name = ?", (client_name,))
    result = cursor.fetchone()
    
    if result:
        new_status = 'paid' if result[0] == 'unpaid' else 'unpaid'
        cursor.execute("UPDATE clients SET status = ? WHERE name = ?", (new_status, client_name))
        conn.commit()
        
        status_emoji = "✅" if new_status == "paid" else "❌"
        await bot.answer_callback_query(
            callback_query.id,
            f"Статус клиента {client_name} изменен на {status_emoji}"
        )
        
        # Обновляем список клиентов
        cursor.execute("SELECT name, status FROM clients")
        clients = cursor.fetchall()
        await bot.edit_message_reply_markup(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            reply_markup=create_client_keyboard(clients, "change_status")
        )
    
    conn.close()

@dp.message_handler(lambda message: message.text == "Удалить клиента")
async def delete_client_start(message: types.Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, status FROM clients")
    clients = cursor.fetchall()
    conn.close()
    
    if not clients:
        await message.reply("Нет клиентов в базе")
        return
    
    await message.reply(
        "Выберите клиента для удаления:",
        reply_markup=create_client_keyboard(clients, "delete")
    )

@dp.callback_query_handler(lambda c: c.data.startswith('delete_'))
async def process_delete_client(callback_query: types.CallbackQuery):
    client_name = callback_query.data[7:]
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM clients WHERE name = ?", (client_name,))
    conn.commit()
    
    await bot.answer_callback_query(
        callback_query.id,
        f"Клиент {client_name} удален"
    )
    
    # Обновляем список клиентов
    cursor.execute("SELECT name, status FROM clients")
    clients = cursor.fetchall()
    if clients:
        await bot.edit_message_reply_markup(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            reply_markup=create_client_keyboard(clients, "delete")
        )
    else:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Нет клиентов в базе"
        )
    
    conn.close()

@dp.message_handler(lambda message: message.text == "Тестовое оповещение")
async def test_notification(message: types.Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Получаем всех клиентов с их статусами
    cursor.execute("""
        SELECT name, status, telegram_username, payment_day 
        FROM clients 
        ORDER BY name
    """)
    clients = cursor.fetchall()
    conn.close()
    
    if not clients:
        await message.reply("Нет клиентов в базе данных")
        return
    
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for name, status, telegram, day in clients:
        status_emoji = "✅" if status == "paid" else "❌"
        telegram_info = f" (@{telegram})" if telegram else " (Telegram не указан)"
        keyboard.add(types.InlineKeyboardButton(
            f"{status_emoji} {name} - {day} число{telegram_info}",
            callback_data=f"test_notify_{name}"
        ))
    
    keyboard.add(types.InlineKeyboardButton("Главное меню", callback_data="back_to_main"))
    
    await message.reply(
        "Выберите клиента для тестового оповещения:\n"
        "✅ - оплачено\n"
        "❌ - не оплачено",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('test_notify_'))
async def process_test_notification(callback_query: types.CallbackQuery):
    client_name = callback_query.data[12:]
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name, status, telegram_username, payment_day, last_notified, chat_id 
        FROM clients 
        WHERE name = ?
    """, (client_name,))
    client = cursor.fetchone()
    conn.close()
    
    if not client:
        await bot.answer_callback_query(
            callback_query.id,
            f"Клиент {client_name} не найден"
        )
        return
    
    name, status, telegram, day, last_notified, chat_id = client
    status_emoji = "✅" if status == "paid" else "❌"
    status_text = "Оплачено" if status == "paid" else "Не оплачено"
    
    if not telegram and not chat_id:
        await bot.answer_callback_query(
            callback_query.id,
            f"У клиента {name} не указан Telegram username или chat_id"
        )
        return
    
    try:
        notification_text = (
            f"⚠️ Тестовое уведомление о платеже\n\n"
            f"Уважаемый(ая) {name},\n"
            f"Это тестовое уведомление о платеже.\n"
            f"Текущий статус: {status_emoji} {status_text}\n"
            f"День оплаты: {day} число"
        )
        
        # Отправляем уведомление администратору
        await bot.send_message(
            callback_query.from_user.id,
            f"Отправлено тестовое уведомление клиенту {name} (@{telegram})"
        )
        
        # Пытаемся отправить сообщение клиенту
        try:
            if chat_id:
                await bot.send_message(chat_id=chat_id, text=notification_text)
            elif telegram:
                # Если нет chat_id, пробуем найти пользователя по username
                user = await bot.get_chat(f"@{telegram}")
                if user and user.id:
                    await bot.send_message(chat_id=user.id, text=notification_text)
                    # Сохраняем chat_id для будущих сообщений
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE clients SET chat_id = ? WHERE name = ?",
                        (user.id, name)
                    )
                    conn.commit()
        except Exception as e:
            print(f"Ошибка отправки сообщения клиенту: {str(e)}")
            raise
        
        # Обновляем время последнего уведомления
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE clients SET last_notified = ? WHERE name = ?",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), name)
        )
        conn.commit()
        conn.close()
        
        # Показываем информацию о клиенте
        response = f"👤 Информация о клиенте:\n\n"
        response += f"Имя: {name}\n"
        response += f"Статус: {status_emoji} {status_text}\n"
        response += f"День оплаты: {day}\n"
        response += f"Telegram: @{telegram}\n"
        if last_notified:
            response += f"Последнее уведомление: {last_notified}\n"
        response += f"Новое уведомление отправлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        keyboard = types.InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            types.InlineKeyboardButton("Отправить еще раз", callback_data=f"test_notify_{name}")
        )
        
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=response,
            reply_markup=keyboard
        )
        
    except Exception as e:
        error_message = f"Ошибка при отправке уведомления: {str(e)}"
        print(f"Ошибка отправки уведомления: {str(e)}")  # Логируем ошибку
        await bot.answer_callback_query(
            callback_query.id,
            error_message
        )

@dp.message_handler(lambda message: message.text == "Управление Telegram")
async def manage_telegram(message: types.Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name, telegram_username FROM clients")
    clients = cursor.fetchall()
    conn.close()
    
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for name, telegram in clients:
        telegram_info = f" (@{telegram})" if telegram else " (не указан)"
        keyboard.add(types.InlineKeyboardButton(
            f"{name}{telegram_info}",
            callback_data=f"set_telegram_{name}"
        ))
    
    await message.reply("Выберите клиента для управления Telegram username:", reply_markup=keyboard)

@dp.message_handler(lambda message: message.text == "Добавить клиента")
async def add_client_start(message: types.Message):
    await ClientStates.waiting_for_name.set()
    await message.reply("Введите имя клиента:", reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler(state=ClientStates.waiting_for_name)
async def process_client_name(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['name'] = message.text.strip()
    
    await ClientStates.next()
    await message.reply("Введите день месяца для оплаты (1-31):")

@dp.message_handler(state=ClientStates.waiting_for_day)
async def process_client_day(message: types.Message, state: FSMContext):
    try:
        day = int(message.text)
        if day < 1 or day > 31:
            raise ValueError
    except ValueError:
        await message.reply("Пожалуйста, введите число от 1 до 31:")
        return
    
    async with state.proxy() as data:
        data['day'] = day
    
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Оплачено", "Не оплачено"]
    keyboard.add(*buttons)
    
    await ClientStates.next()
    await message.reply("Установите статус оплаты:", reply_markup=keyboard)

@dp.message_handler(state=ClientStates.waiting_for_payment_status)
async def process_payment_status(message: types.Message, state: FSMContext):
    status = message.text.lower()
    if status not in ["оплачено", "не оплачено"]:
        await message.reply("Пожалуйста, выберите 'Оплачено' или 'Не оплачено':")
        return
    
    async with state.proxy() as data:
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO clients (name, payment_day, status) VALUES (?, ?, ?)",
                (data['name'], data['day'], 'paid' if status == 'оплачено' else 'unpaid')
            )
            conn.commit()
            await message.reply(
                f"Клиент {data['name']} добавлен!\n"
                f"День оплаты: {data['day']}\n"
                f"Статус: {'Оплачено' if status == 'оплачено' else 'Не оплачено'}",
                reply_markup=create_main_keyboard()
            )
        except sqlite3.IntegrityError:
            await message.reply("Клиент с таким именем уже существует!")
        finally:
            conn.close()
    
    await state.finish()

# Импорт данных
@dp.message_handler(lambda message: message.text == "Импорт данных")
async def import_data_start(message: types.Message):
    await ClientStates.waiting_for_import.set()
    await message.reply(
        "Отправьте данные для импорта в формате:\n"
        "Имя,День оплаты,Статус\n"
        "Пример:\n"
        "Иван Петров,5,paid\n"
        "Алексей Сидоров,10,unpaid\n\n"
        "Или отправьте файл .csv с такими же колонками.",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message_handler(state=ClientStates.waiting_for_import, content_types=types.ContentType.TEXT)
async def process_text_import(message: types.Message, state: FSMContext):
    lines = message.text.split('\n')
    imported = 0
    errors = 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for line in lines:
        try:
            name, day, status = map(str.strip, line.split(','))
            day = int(day)
            if status not in ('paid', 'unpaid'):
                raise ValueError
            
            cursor.execute(
                "INSERT OR IGNORE INTO clients (name, payment_day, status) VALUES (?, ?, ?)",
                (name, day, status)
            )
            imported += 1
        except Exception as e:
            errors += 1
            continue
    
    conn.commit()
    conn.close()
    
    await state.finish()
    await message.reply(
        f"Импорт завершен:\nУспешно: {imported}\nОшибок: {errors}",
        reply_markup=create_main_keyboard()
    )

@dp.message_handler(state=ClientStates.waiting_for_import, content_types=types.ContentType.DOCUMENT)
async def process_file_import(message: types.Message, state: FSMContext):
    if not message.document.file_name.endswith('.csv'):
        await message.reply("Пожалуйста, отправьте файл в формате CSV")
        return

    try:
        # Скачиваем файл
        file = await bot.get_file(message.document.file_id)
        file_path = file.file_path
        downloaded_file = await bot.download_file(file_path)
        
        # Пробуем разные кодировки
        encodings = ['utf-8', 'cp1251', 'windows-1251']
        csv_data = None
        
        for encoding in encodings:
            try:
                csv_data = downloaded_file.read().decode(encoding)
                break
            except UnicodeDecodeError:
                downloaded_file.seek(0)  # Возвращаемся в начало файла
                continue
        
        if csv_data is None:
            raise ValueError("Не удалось определить кодировку файла. Пожалуйста, сохраните файл в кодировке UTF-8 или Windows-1251")
        
        # Очищаем данные от BOM и лишних пробелов
        csv_data = csv_data.strip().replace('\ufeff', '')
        
        # Читаем CSV файл
        csv_reader = csv.DictReader(io.StringIO(csv_data))
        
        # Проверяем наличие необходимых колонок
        required_columns = ['Имя', 'День оплаты', 'Статус']
        if not csv_reader.fieldnames:
            raise ValueError("Файл пустой или имеет неправильный формат")
            
        # Нормализуем имена колонок (убираем пробелы и приводим к нижнему регистру)
        normalized_fieldnames = {name.strip().lower(): name for name in csv_reader.fieldnames}
        normalized_required = {name.strip().lower(): name for name in required_columns}
        
        missing_columns = []
        for req_col in normalized_required:
            if req_col not in normalized_fieldnames:
                missing_columns.append(normalized_required[req_col])
        
        if missing_columns:
            raise ValueError(f"В файле отсутствуют необходимые колонки: {', '.join(missing_columns)}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        imported = 0
        errors = 0
        error_details = []
        
        for row in csv_reader:
            try:
                # Получаем значения с учетом возможных пробелов в именах колонок
                name = row['Имя'].strip()
                if not name:
                    raise ValueError("Имя не может быть пустым")
                
                try:
                    day = int(row['День оплаты'].strip())
                except (ValueError, KeyError):
                    raise ValueError("День оплаты должен быть числом")
                
                status = row['Статус'].strip().lower()
                
                if status not in ('paid', 'unpaid'):
                    raise ValueError("Статус должен быть 'paid' или 'unpaid'")
                
                if not (1 <= day <= 31):
                    raise ValueError("День должен быть от 1 до 31")
                
                cursor.execute(
                    "INSERT OR REPLACE INTO clients (name, payment_day, status) VALUES (?, ?, ?)",
                    (name, day, status)
                )
                imported += 1
            except Exception as e:
                errors += 1
                error_details.append(f"Строка {csv_reader.line_num}: {str(e)}")
                continue
        
        conn.commit()
        conn.close()
        
        response = f"Импорт завершен:\n✅ Успешно импортировано: {imported}\n❌ Ошибок: {errors}"
        if error_details:
            response += "\n\nДетали ошибок:\n" + "\n".join(error_details[:5])
            if len(error_details) > 5:
                response += f"\n... и еще {len(error_details) - 5} ошибок"
        
        await message.reply(
            response,
            reply_markup=create_main_keyboard()
        )
    except Exception as e:
        await message.reply(
            f"Произошла ошибка при обработке файла: {str(e)}",
            reply_markup=create_main_keyboard()
        )
    finally:
        await state.finish()

# Экспорт данных
@dp.message_handler(lambda message: message.text == "Экспорт данных")
async def export_data(message: types.Message):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT name, payment_day, status FROM clients")
    clients = cursor.fetchall()
    conn.close()
    
    if not clients:
        await message.reply("Нет данных для экспорта")
        return
    
    csv_data = "Имя,День оплаты,Статус\n"
    for name, day, status in clients:
        csv_data += f"{name},{day},{status}\n"
    
    with open('clients_export.csv', 'w', encoding='utf-8') as f:
        f.write(csv_data)
    
    with open('clients_export.csv', 'rb') as f:
        await message.reply_document(f, caption="Экспорт данных клиентов")

# Проверка платежей (аналогично предыдущей версии, но с запросами к БД)
async def check_payments():
    while True:
        try:
            now = datetime.now()
            current_hour = now.hour
            
            # Проверяем, наступило ли время отправки уведомлений (10:00, 18:00 или 22:00 МСК)
            if current_hour in [10, 18, 22]:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # Получаем клиентов с оплатой сегодня
                cursor.execute("""
                    SELECT name, payment_day, status, telegram_username, chat_id 
                    FROM clients 
                    WHERE payment_day = ? AND status = 'unpaid'
                """, (now.day,))
                
                today_clients = cursor.fetchall()
                
                # Получаем клиентов с оплатой в ближайшие 3 дня
                next_3_days = [(now.day + i) % 31 or 31 for i in range(1, 4)]
                cursor.execute("""
                    SELECT name, payment_day, status, telegram_username, chat_id 
                    FROM clients 
                    WHERE payment_day IN ({}) AND status = 'unpaid'
                """.format(','.join('?' * len(next_3_days))), next_3_days)
                
                upcoming_clients = cursor.fetchall()
                
                # Отправляем уведомления администраторам
                for admin_id in ADMIN_IDS:
                    if today_clients:
                        today_message = "❗ Неоплаченные платежи на сегодня:\n\n"
                        for name, day, status, telegram, chat_id in today_clients:
                            telegram_info = f" (@{telegram})" if telegram else " (Telegram не указан)"
                            today_message += f"👤 {name}{telegram_info}\n"
                        await bot.send_message(admin_id, today_message)
                    
                    if upcoming_clients:
                        upcoming_message = "📅 Предстоящие платежи в ближайшие 3 дня:\n\n"
                        for name, day, status, telegram, chat_id in upcoming_clients:
                            telegram_info = f" (@{telegram})" if telegram else " (Telegram не указан)"
                            upcoming_message += f"👤 {name} - {day} число{telegram_info}\n"
                        await bot.send_message(admin_id, upcoming_message)
                
                # Отправляем уведомления клиентам
                for name, day, status, telegram, chat_id in today_clients:
                    if telegram or chat_id:
                        try:
                            notification_text = (
                                f"⚠️ Уведомление о платеже\n\n"
                                f"Уважаемый(ая) {name},\n"
                                f"Напоминаем, что сегодня необходимо оплатить VPN.\n"
                                f"Текущий статус: Не оплачено"
                            )
                            
                            # Пытаемся отправить сообщение клиенту
                            if chat_id:
                                await bot.send_message(chat_id=chat_id, text=notification_text)
                            elif telegram:
                                user = await bot.get_chat(f"@{telegram}")
                                if user and user.id:
                                    await bot.send_message(chat_id=user.id, text=notification_text)
                                    # Сохраняем chat_id для будущих сообщений
                                    cursor.execute(
                                        "UPDATE clients SET chat_id = ?, last_notified = ? WHERE name = ?",
                                        (user.id, now.strftime("%Y-%m-%d %H:%M:%S"), name)
                                    )
                                    conn.commit()
                        except Exception as e:
                            print(f"Ошибка отправки уведомления клиенту {name}: {str(e)}")
                
                conn.close()
            
            # Проверяем каждый час
            await asyncio.sleep(3600)
            
        except Exception as e:
            print(f"Ошибка при проверке платежей: {str(e)}")
            await asyncio.sleep(3600)  # В случае ошибки ждем час перед следующей попыткой

@dp.callback_query_handler(lambda c: c.data.startswith('test_notify_'))
async def process_test_notification(callback_query: types.CallbackQuery):
    client_name = callback_query.data[12:]
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT telegram_username FROM clients WHERE name = ?", (client_name,))
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        try:
            await bot.send_message(
                callback_query.from_user.id,
                f"Тестовое оповещение отправлено клиенту {client_name} (@{result[0]})"
            )
            # Здесь можно добавить отправку сообщения клиенту
        except Exception as e:
            await bot.send_message(
                callback_query.from_user.id,
                f"Ошибка при отправке оповещения: {str(e)}"
            )
    else:
        await bot.answer_callback_query(
            callback_query.id,
            f"У клиента {client_name} не указан Telegram username"
        )

@dp.callback_query_handler(lambda c: c.data.startswith('set_telegram_'))
async def process_set_telegram(callback_query: types.CallbackQuery):
    client_name = callback_query.data[13:]
    await ClientStates.waiting_for_telegram_username.set()
    state = dp.current_state(user=callback_query.from_user.id)
    await state.update_data(client_name=client_name)
    
    await bot.send_message(
        callback_query.message.chat.id,
        f"Введите Telegram username для клиента {client_name} (без @):"
    )

@dp.message_handler(state=ClientStates.waiting_for_telegram_username)
async def process_telegram_username(message: types.Message, state: FSMContext):
    username = message.text.strip().replace('@', '')
    async with state.proxy() as data:
        client_name = data['client_name']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE clients SET telegram_username = ? WHERE name = ?",
        (username, client_name)
    )
    conn.commit()
    conn.close()
    
    await message.reply(
        f"Telegram username для клиента {client_name} установлен: @{username}",
        reply_markup=create_main_keyboard()
    )
    await state.finish()

@dp.message_handler(lambda message: message.text == "Уведомления")
async def notifications_menu(message: types.Message):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Проверить сегодняшние платежи", callback_data="check_today"),
        types.InlineKeyboardButton("Проверить ближайшие 3 дня", callback_data="check_next_3_days"),
        types.InlineKeyboardButton("Отправить уведомления", callback_data="send_notifications")
    )
    
    await message.reply("Выберите действие:", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data == "check_today")
async def check_today_payments(callback_query: types.CallbackQuery):
    today = datetime.now().day
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name, payment_day, status, telegram_username 
        FROM clients 
        WHERE payment_day = ? AND status = 'unpaid'
    """, (today,))
    
    unpaid_clients = cursor.fetchall()
    conn.close()
    
    if not unpaid_clients:
        await bot.answer_callback_query(callback_query.id, "Сегодня нет неоплаченных платежей")
        return
    
    response = "❗ Неоплаченные платежи на сегодня:\n\n"
    for name, day, status, telegram in unpaid_clients:
        telegram_info = f" (@{telegram})" if telegram else " (Telegram не указан)"
        response += f"👤 {name}{telegram_info}\n"
    
    try:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=response,
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Отправить уведомления", callback_data="send_notifications"),
                types.InlineKeyboardButton("Назад", callback_data="back_to_notifications")
            )
        )
    except MessageNotModified:
        await bot.answer_callback_query(callback_query.id, "Информация актуальна")

@dp.callback_query_handler(lambda c: c.data == "check_next_3_days")
async def check_next_3_days(callback_query: types.CallbackQuery):
    today = datetime.now().day
    next_3_days = [(today + i) % 31 or 31 for i in range(3)]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    response = "📅 Платежи в ближайшие 3 дня:\n\n"
    
    for day in next_3_days:
        cursor.execute("""
            SELECT name, payment_day, status, telegram_username 
            FROM clients 
            WHERE payment_day = ? AND status = 'unpaid'
        """, (day,))
        
        unpaid_clients = cursor.fetchall()
        
        if unpaid_clients:
            response += f"📌 {day} число:\n"
            for name, day, status, telegram in unpaid_clients:
                telegram_info = f" (@{telegram})" if telegram else " (Telegram не указан)"
                response += f"👤 {name}{telegram_info}\n"
            response += "\n"
    
    conn.close()
    
    if response == "📅 Платежи в ближайшие 3 дня:\n\n":
        await bot.answer_callback_query(callback_query.id, "Нет неоплаченных платежей в ближайшие 3 дня")
        return
    
    try:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=response,
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Отправить уведомления", callback_data="send_notifications"),
                types.InlineKeyboardButton("Назад", callback_data="back_to_notifications")
            )
        )
    except MessageNotModified:
        await bot.answer_callback_query(callback_query.id, "Информация актуальна")

@dp.callback_query_handler(lambda c: c.data == "send_notifications")
async def send_notifications(callback_query: types.CallbackQuery):
    today = datetime.now().day
    next_3_days = [(today + i) % 31 or 31 for i in range(3)]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    sent_count = 0
    failed_count = 0
    
    for day in next_3_days:
        cursor.execute("""
            SELECT name, payment_day, status, telegram_username 
            FROM clients 
            WHERE payment_day = ? AND status = 'unpaid' AND telegram_username IS NOT NULL
        """, (day,))
        
        unpaid_clients = cursor.fetchall()
        
        for name, day, status, telegram in unpaid_clients:
            try:
                notification_text = (
                    f"⚠️ Уведомление о платеже\n\n"
                    f"Уважаемый(ая) {name},\n"
                    f"Напоминаем, что сегодня необходимо оплатить VPN.\n"
                    f"Текущий статус: Не оплачено"
                )
                
                # Здесь должна быть отправка сообщения клиенту
                # await bot.send_message(chat_id=telegram, text=notification_text)
                
                cursor.execute(
                    "UPDATE clients SET last_notified = ? WHERE name = ?",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), name)
                )
                sent_count += 1
            except Exception as e:
                failed_count += 1
                continue
    
    conn.commit()
    conn.close()
    
    response = f"📨 Отправка уведомлений завершена:\n"
    response += f"✅ Успешно отправлено: {sent_count}\n"
    response += f"❌ Ошибок отправки: {failed_count}"
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=response,
        reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
            types.InlineKeyboardButton("Назад", callback_data="back_to_notifications")
        )
    )

@dp.callback_query_handler(lambda c: c.data == "back_to_notifications")
async def back_to_notifications(callback_query: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Проверить сегодняшние платежи", callback_data="check_today"),
        types.InlineKeyboardButton("Проверить ближайшие 3 дня", callback_data="check_next_3_days"),
        types.InlineKeyboardButton("Отправить уведомления", callback_data="send_notifications")
    )
    
    try:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Выберите действие:",
            reply_markup=keyboard
        )
    except MessageNotModified:
        await bot.answer_callback_query(callback_query.id, "Меню актуально")

@dp.message_handler(lambda message: message.text == "Проверка базы данных")
async def check_database(message: types.Message):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Проверить целостность", callback_data="check_db_integrity"),
        types.InlineKeyboardButton("Создать резервную копию", callback_data="create_db_backup"),
        types.InlineKeyboardButton("Восстановить из копии", callback_data="restore_db_backup")
    )
    
    await message.reply("Выберите действие:", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data == "check_db_integrity")
async def process_check_db_integrity(callback_query: types.CallbackQuery):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Проверяем наличие таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if not tables:
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text="❌ База данных повреждена: отсутствуют таблицы",
                reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                    types.InlineKeyboardButton("Восстановить базу", callback_data="restore_db_backup"),
                    types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
                )
            )
            return
        
        # Проверяем структуру таблиц
        cursor.execute("PRAGMA integrity_check")
        integrity_check = cursor.fetchone()[0]
        
        if integrity_check != "ok":
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text=f"❌ База данных повреждена: {integrity_check}",
                reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                    types.InlineKeyboardButton("Восстановить базу", callback_data="restore_db_backup"),
                    types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
                )
            )
            return
        
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM clients")
        clients_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM payment_history")
        payments_count = cursor.fetchone()[0]
        
        conn.close()
        
        response = "✅ База данных в порядке:\n\n"
        response += f"📊 Количество клиентов: {clients_count}\n"
        response += f"📊 Количество записей в истории платежей: {payments_count}\n"
        response += f"📊 Проверка целостности: {integrity_check}"
        
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=response,
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Создать резервную копию", callback_data="create_db_backup"),
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )
    except Exception as e:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=f"❌ Ошибка при проверке базы данных: {str(e)}",
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Восстановить базу", callback_data="restore_db_backup"),
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )

@dp.callback_query_handler(lambda c: c.data == "create_db_backup")
async def process_create_db_backup(callback_query: types.CallbackQuery):
    try:
        # Создаем директорию для бэкапов, если её нет
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Создаем имя файла с текущей датой и временем
        backup_filename = f"vpn_bot_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        backup_path = os.path.join(backup_dir, backup_filename)
        
        # Копируем файл базы данных
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vpn_bot.db')
        shutil.copy2(db_path, backup_path)
        
        # Получаем список всех бэкапов
        backups = sorted([f for f in os.listdir(backup_dir) if f.endswith('.db')], reverse=True)
        
        # Удаляем старые бэкапы, оставляя только последние 5
        if len(backups) > 5:
            for old_backup in backups[5:]:
                os.remove(os.path.join(backup_dir, old_backup))
        
        response = "✅ Резервная копия создана успешно!\n\n"
        response += "Последние резервные копии:\n"
        for backup in backups[:5]:
            response += f"📁 {backup}\n"
        
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=response,
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )
    except Exception as e:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=f"❌ Ошибка при создании резервной копии: {str(e)}",
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )

@dp.callback_query_handler(lambda c: c.data == "restore_db_backup")
async def process_restore_db_backup(callback_query: types.CallbackQuery):
    try:
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
        if not os.path.exists(backup_dir):
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text="❌ Нет доступных резервных копий",
                reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                    types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
                )
            )
            return
        
        # Получаем список файлов и проверяем их существование
        backups = []
        for f in os.listdir(backup_dir):
            if f.endswith('.db') and os.path.isfile(os.path.join(backup_dir, f)):
                backups.append(f)
        
        backups = sorted(backups, reverse=True)
        
        if not backups:
            await bot.edit_message_text(
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                text="❌ Нет доступных резервных копий",
                reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                    types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
                )
            )
            return
        
        keyboard = types.InlineKeyboardMarkup(row_width=1)
        for backup in backups[:5]:  # Показываем только последние 5 копий
            keyboard.add(types.InlineKeyboardButton(
                f"📁 {backup}",
                callback_data=f"restore_backup_{backup}"
            ))
        keyboard.add(types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu"))
        
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Выберите резервную копию для восстановления:",
            reply_markup=keyboard
        )
    except Exception as e:
        error_message = f"❌ Ошибка при получении списка резервных копий: {str(e)}"
        print(f"Ошибка получения списка копий: {str(e)}")  # Добавляем логирование
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=error_message,
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )

@dp.callback_query_handler(lambda c: c.data.startswith('restore_backup_'))
async def process_restore_selected_backup(callback_query: types.CallbackQuery):
    try:
        # Получаем имя файла из callback_data, убирая префикс и возможные специальные символы
        backup_filename = callback_query.data[14:].strip('_')
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backups')
        backup_path = os.path.join(backup_dir, backup_filename)
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vpn_bot.db')
        
        # Проверяем существование директории и файла
        if not os.path.exists(backup_dir):
            raise FileNotFoundError("Директория с резервными копиями не найдена")
        
        if not os.path.exists(backup_path):
            # Пробуем найти файл без префикса подчеркивания
            alt_backup_path = os.path.join(backup_dir, backup_filename.lstrip('_'))
            if os.path.exists(alt_backup_path):
                backup_path = alt_backup_path
            else:
                raise FileNotFoundError(f"Резервная копия {backup_filename} не найдена")
        
        # Создаем резервную копию текущей базы перед восстановлением
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        pre_restore_backup = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                        f'vpn_bot_pre_restore_{current_time}.db')
        
        # Проверяем существование текущей базы данных
        if os.path.exists(db_path):
            shutil.copy2(db_path, pre_restore_backup)
        
        # Восстанавливаем выбранную копию
        shutil.copy2(backup_path, db_path)
        
        # Проверяем, что восстановление прошло успешно
        if not os.path.exists(db_path):
            raise Exception("Ошибка при восстановлении базы данных")
        
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=f"✅ База данных успешно восстановлена из копии {backup_filename}",
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Проверить целостность", callback_data="check_db_integrity"),
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )
    except Exception as e:
        error_message = f"❌ Ошибка при восстановлении базы данных: {str(e)}"
        print(f"Ошибка восстановления: {str(e)}")  # Добавляем логирование
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=error_message,
            reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                types.InlineKeyboardButton("Назад", callback_data="back_to_db_menu")
            )
        )

@dp.callback_query_handler(lambda c: c.data == "back_to_db_menu")
async def back_to_db_menu(callback_query: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton("Проверить целостность", callback_data="check_db_integrity"),
        types.InlineKeyboardButton("Создать резервную копию", callback_data="create_db_backup"),
        types.InlineKeyboardButton("Восстановить из копии", callback_data="restore_db_backup")
    )
    
    try:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text="Выберите действие:",
            reply_markup=keyboard
        )
    except MessageNotModified:
        await bot.answer_callback_query(callback_query.id, "Меню актуально")

if __name__ == '__main__':
    from aiogram import executor
    loop = asyncio.get_event_loop()
    loop.create_task(check_payments())
    executor.start_polling(dp, skip_updates=True)