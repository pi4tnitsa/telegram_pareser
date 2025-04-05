from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from datetime import datetime
import openpyxl
import pytz
import os
import asyncio
import json
import sqlite3
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel
from config import api_hash, api_id, BOT_TOKEN

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Инициализация клиента Telethon
client = TelegramClient('session_name', api_id, api_hash)

# Путь к базе данных SQLite
DB_FILE = 'channel_posts.db'

# Инициализация базы данных
def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        channel_name TEXT,
        content TEXT,
        is_comment BOOLEAN,
        parent_id INTEGER
    )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized successfully")

# Добавление данных в базу данных
def add_to_database(date, channel_name, content, is_comment=False, parent_id=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO posts (date, channel_name, content, is_comment, parent_id)
    VALUES (?, ?, ?, ?, ?)
    ''', (date, channel_name, content, is_comment, parent_id))
    last_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return last_id

# Обработчик новых сообщений в каналах
@client.on(events.NewMessage)
async def new_post_listener(event):
    if isinstance(event.peer_id, PeerChannel):
        try:
            channel = await client.get_entity(event.peer_id)
            channel_name = channel.title
            if event.is_group or event.is_channel:
                post_date_utc = event.message.date
                moscow_tz = pytz.timezone('Europe/Moscow')
                post_date_moscow = post_date_utc.astimezone(moscow_tz)
                post_date = post_date_moscow.strftime('%Y-%m-%d')
                post_content = event.message.text or "Сообщение без текста"

                # Проверяем, является ли сообщение комментарием
                if event.message.reply_to:
                    try:
                        replied_msg = await client.get_messages(event.peer_id, ids=event.message.reply_to.reply_to_msg_id)
                        # Найдем ID исходного поста в нашей базе
                        conn = sqlite3.connect(DB_FILE)
                        cursor = conn.cursor()
                        cursor.execute('SELECT id FROM posts WHERE content = ? AND channel_name = ? AND is_comment = 0', 
                                      (replied_msg.text or "Сообщение без текста", channel_name))
                        result = cursor.fetchone()
                        conn.close()
                        parent_id = None
                        if result:
                            parent_id = result[0]
                        # Сохраняем как комментарий
                        add_to_database(post_date, channel_name, post_content, is_comment=True, parent_id=parent_id)
                    except Exception as e:
                        print(f"Ошибка при обработке комментария: {e}")
                        # Если не удалось определить, что это комментарий, сохраняем как обычное сообщение
                        add_to_database(post_date, channel_name, post_content, is_comment=False)
                else:
                    # Сохраняем как обычное сообщение
                    add_to_database(post_date, channel_name, post_content, is_comment=False)

                print(f"Добавлен{'о сообщение' if event.message.reply_to else ' пост'} из канала {channel_name} за {post_date} (Московское время)")
        except Exception as e:
            print(f"Ошибка при обработке сообщения: {e}")

# Команда /start
@dp.message(Command("start"))
async def start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Выгрузить посты за текущий месяц")],
            [KeyboardButton(text="Выгрузить всю таблицу")],
            [KeyboardButton(text="Выгрузить посты за определённый месяц")],
            [KeyboardButton(text="Экспорт в JSON")]
        ],
        resize_keyboard=True
    )
    await message.answer("Привет! Выберите действие:", reply_markup=keyboard)

# Функция для получения данных из базы данных
def get_posts_by_month(month, option="with"):
    current_year = datetime.now().year
    month_start = f"{current_year}-{month:02d}-01"
    next_month = month + 1 if month < 12 else 1
    next_year = current_year if month < 12 else current_year + 1
    month_end = f"{next_year}-{next_month:02d}-01"
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = '''
    SELECT * FROM posts 
    WHERE date >= ? AND date < ?
    '''
    if option == "postsonly":
        query += " AND is_comment = 0"
    elif option == "commentsonly":
        query += " AND is_comment = 1"
    query += " ORDER BY date"
    cursor.execute(query, (month_start, month_end))
    rows = cursor.fetchall()
    conn.close()
    posts = []
    for row in rows:
        post = dict(row)
        posts.append(post)
    return posts

def get_all_posts(option="with"):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = 'SELECT * FROM posts'
    if option == "postsonly":
        query += " WHERE is_comment = 0"
    elif option == "commentsonly":
        query += " WHERE is_comment = 1"
    query += " ORDER BY date"
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    posts = []
    for row in rows:
        post = dict(row)
        posts.append(post)
    return posts

# Функции для создания Excel-файлов
def create_monthly_excel(month, option="with"):
    current_year = datetime.now().year
    posts = get_posts_by_month(month, option)
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Posts"
    headers = ["Дата", "Название канала", "Содержание"]
    if option in ["with", "without"]:
        headers.insert(3, "Тип")
    if option in ["with", "commentsonly"]:
        headers.append("ID родительского поста")
    sheet.append(headers)
    for post in posts:
        row_data = [
            post['date'],
            post['channel_name'],
            post['content']
        ]
        if option in ["with", "without"]:
            row_data.insert(3, "Комментарий" if post['is_comment'] else "Пост")
        if option in ["with", "commentsonly"] and post['is_comment']:
            row_data.append(post['parent_id'] or "Неизвестно")
        elif option in ["with", "commentsonly"]:
            row_data.append("N/A")
        sheet.append(row_data)
    for col in sheet.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            if cell.value:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
        adjusted_width = (max_length + 2)
        sheet.column_dimensions[column].width = min(adjusted_width, 100)
    option_suffix = {
        "with": "_с_комментариями",
        "without": "_без_комментариев",
        "postsonly": "_только_посты",
        "commentsonly": "_только_комментарии"
    }
    monthly_file = f"посты_{current_year}-{month:02d}{option_suffix[option]}.xlsx"
    workbook.save(monthly_file)
    return monthly_file

def create_full_excel(option="with"):
    posts = get_all_posts(option)
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "All Posts"
    headers = ["Дата", "Название канала", "Содержание"]
    if option in ["with", "without"]:
        headers.insert(3, "Тип")
    if option in ["with", "commentsonly"]:
        headers.append("ID родительского поста")
    sheet.append(headers)
    for post in posts:
        row_data = [
            post['date'],
            post['channel_name'],
            post['content']
        ]
        if option in ["with", "without"]:
            row_data.insert(3, "Комментарий" if post['is_comment'] else "Пост")
        if option in ["with", "commentsonly"] and post['is_comment']:
            row_data.append(post['parent_id'] or "Неизвестно")
        elif option in ["with", "commentsonly"]:
            row_data.append("N/A")
        sheet.append(row_data)
    for col in sheet.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            if cell.value:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
        adjusted_width = (max_length + 2)
        sheet.column_dimensions[column].width = min(adjusted_width, 100)
    option_suffix = {
        "with": "_с_комментариями",
        "without": "_без_комментариев",
        "postsonly": "_только_посты",
        "commentsonly": "_только_комментарии"
    }
    full_file = f"все_посты{option_suffix[option]}.xlsx"
    workbook.save(full_file)
    return full_file

# Функции для создания JSON-файлов
def create_monthly_json(month, option="with"):
    current_year = datetime.now().year
    posts = get_posts_by_month(month, option)
    option_suffix = {
        "with": "_с_комментариями",
        "without": "_без_комментариев",
        "postsonly": "_только_посты",
        "commentsonly": "_только_комментарии"
    }
    monthly_file = f"посты_{current_year}-{month:02d}{option_suffix[option]}.json"
    with open(monthly_file, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    return monthly_file

def create_full_json(option="with"):
    posts = get_all_posts(option)
    option_suffix = {
        "with": "_с_комментариями",
        "without": "_без_комментариев",
        "postsonly": "_только_посты",
        "commentsonly": "_только_комментарии"
    }
    full_file = f"все_посты{option_suffix[option]}.json"
    with open(full_file, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    return full_file

# Обработчики кнопок и запросов
@dp.message(lambda message: message.text == "Выгрузить посты за текущий месяц")
async def send_monthly_posts(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data="monthcurrent_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data="monthcurrent_without")
        ],
        [
            InlineKeyboardButton(text="Только посты", callback_data="monthcurrent_postsonly"),
            InlineKeyboardButton(text="Только комментарии", callback_data="monthcurrent_commentsonly")
        ]
    ])
    await message.answer("Выберите вариант выгрузки:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Выгрузить всю таблицу")
async def send_full_table(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data="full_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data="full_without")
        ],
        [
            InlineKeyboardButton(text="Только посты", callback_data="full_postsonly"),
            InlineKeyboardButton(text="Только комментарии", callback_data="full_commentsonly")
        ]
    ])
    await message.answer("Выберите вариант выгрузки:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Выгрузить посты за определённый месяц")
async def choose_month(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Январь", callback_data="month_1"),
            InlineKeyboardButton(text="Февраль", callback_data="month_2"),
            InlineKeyboardButton(text="Март", callback_data="month_3")
        ],
        [
            InlineKeyboardButton(text="Апрель", callback_data="month_4"),
            InlineKeyboardButton(text="Май", callback_data="month_5"),
            InlineKeyboardButton(text="Июнь", callback_data="month_6")
        ],
        [
            InlineKeyboardButton(text="Июль", callback_data="month_7"),
            InlineKeyboardButton(text="Август", callback_data="month_8"),
            InlineKeyboardButton(text="Сентябрь", callback_data="month_9")
        ],
        [
            InlineKeyboardButton(text="Октябрь", callback_data="month_10"),
            InlineKeyboardButton(text="Ноябрь", callback_data="month_11"),
            InlineKeyboardButton(text="Декабрь", callback_data="month_12")
        ],
    ])
    await message.answer("Выберите месяц за текущий год:", reply_markup=keyboard)

@dp.callback_query(lambda query: query.data.startswith("monthcurrent_"))
async def handle_current_month_with_option(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    current_month = datetime.now().month
    try:
        monthly_file = create_monthly_excel(current_month, option=option)
        input_file = FSInputFile(monthly_file)
        caption_option = {
            "with": "с комментариями",
            "without": "без комментариев",
            "postsonly": "только посты",
            "commentsonly": "только комментарии"
        }
        await query.message.answer_document(input_file, caption=f"Посты за {datetime.now().year}-{current_month:02d} ({caption_option[option]})")
        os.remove(monthly_file)
    except Exception as e:
        await query.message.answer(f"Ошибка при создании Excel файла: {str(e)}")
    finally:
        await query.answer()

@dp.callback_query(lambda query: query.data.startswith("full_"))
async def handle_full_with_option(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    try:
        full_file = create_full_excel(option=option)
        input_file = FSInputFile(full_file)
        caption_option = {
            "with": "с комментариями",
            "without": "без комментариев",
            "postsonly": "только посты",
            "commentsonly": "только комментарии"
        }
        await query.message.answer_document(input_file, caption=f"Полная таблица ({caption_option[option]})")
        os.remove(full_file)
    except Exception as e:
        await query.message.answer(f"Ошибка при создании Excel файла: {str(e)}")
    finally:
        await query.answer()

@dp.callback_query(lambda query: query.data.startswith("month_"))
async def handle_month_selection(query: types.CallbackQuery):
    month = int(query.data.split("_")[1])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data=f"month_{month}_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data=f"month_{month}_without")
        ],
        [
            InlineKeyboardButton(text="Только посты", callback_data=f"month_{month}_postsonly"),
            InlineKeyboardButton(text="Только комментарии", callback_data=f"month_{month}_commentsonly")
        ]
    ])
    await query.message.answer(f"Выберите вариант выгрузки для месяца {month}:", reply_markup=keyboard)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("month_") and query.data.count("_") == 2)
async def handle_month_with_option(query: types.CallbackQuery):
    parts = query.data.split("_")
    month = int(parts[1])
    option = parts[2]
    try:
        monthly_file = create_monthly_excel(month, option=option)
        input_file = FSInputFile(monthly_file)
        caption_option = {
            "with": "с комментариями",
            "without": "без комментариев",
            "postsonly": "только посты",
            "commentsonly": "только комментарии"
        }
        await query.message.answer_document(input_file, caption=f"Посты за {datetime.now().year}-{month:02d} ({caption_option[option]})")
        os.remove(monthly_file)
    except Exception as e:
        await query.message.answer(f"Ошибка при создании Excel файла: {str(e)}")
    finally:
        await query.answer()

@dp.message(lambda message: message.text == "Экспорт в JSON")
async def export_json(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Текущий месяц", callback_data="json_current"),
            InlineKeyboardButton(text="Все данные", callback_data="json_all")
        ],
        [InlineKeyboardButton(text="Выбрать месяц", callback_data="json_choose")]
    ])
    await message.answer("Выберите данные для экспорта в JSON:", reply_markup=keyboard)

@dp.callback_query(lambda query: query.data.startswith("json_"))
async def handle_json_selection(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    if option == "current":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="С комментариями", callback_data="jsoncurrent_with"),
                InlineKeyboardButton(text="Без комментариев", callback_data="jsoncurrent_without")
            ],
            [
                InlineKeyboardButton(text="Только посты", callback_data="jsoncurrent_postsonly"),
                InlineKeyboardButton(text="Только комментарии", callback_data="jsoncurrent_commentsonly")
            ]
        ])
        await query.message.answer("Выберите вариант выгрузки JSON:", reply_markup=keyboard)
    elif option == "all":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="С комментариями", callback_data="jsonall_with"),
                InlineKeyboardButton(text="Без комментариев", callback_data="jsonall_without")
            ],
            [
                InlineKeyboardButton(text="Только посты", callback_data="jsonall_postsonly"),
                InlineKeyboardButton(text="Только комментарии", callback_data="jsonall_commentsonly")
            ]
        ])
        await query.message.answer("Выберите вариант выгрузки JSON:", reply_markup=keyboard)
    elif option == "choose":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Январь", callback_data="jsonmonth_1"),
                InlineKeyboardButton(text="Февраль", callback_data="jsonmonth_2"),
                InlineKeyboardButton(text="Март", callback_data="jsonmonth_3")
            ],
            [
                InlineKeyboardButton(text="Апрель", callback_data="jsonmonth_4"),
                InlineKeyboardButton(text="Май", callback_data="jsonmonth_5"),
                InlineKeyboardButton(text="Июнь", callback_data="jsonmonth_6")
            ],
            [
                InlineKeyboardButton(text="Июль", callback_data="jsonmonth_7"),
                InlineKeyboardButton(text="Август", callback_data="jsonmonth_8"),
                InlineKeyboardButton(text="Сентябрь", callback_data="jsonmonth_9")
            ],
            [
                InlineKeyboardButton(text="Октябрь", callback_data="jsonmonth_10"),
                InlineKeyboardButton(text="Ноябрь", callback_data="jsonmonth_11"),
                InlineKeyboardButton(text="Декабрь", callback_data="jsonmonth_12")
            ],
        ])
        await query.message.answer("Выберите месяц для экспорта JSON:", reply_markup=keyboard)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsoncurrent_"))
async def handle_json_current_with_option(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    current_month = datetime.now().month
    json_file = create_monthly_json(current_month, option=option)
    input_file = FSInputFile(json_file)
    caption_option = {
        "with": "с комментариями",
        "without": "без комментариев",
        "postsonly": "только посты",
        "commentsonly": "только комментарии"
    }
    await query.message.answer_document(input_file, caption=f"JSON посты за {datetime.now().year}-{current_month:02d} ({caption_option[option]})")
    os.remove(json_file)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonall_"))
async def handle_json_all_with_option(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    json_file = create_full_json(option=option)
    input_file = FSInputFile(json_file)
    caption_option = {
        "with": "с комментариями",
        "without": "без комментариев",
        "postsonly": "только посты",
        "commentsonly": "только комментарии"
    }
    await query.message.answer_document(input_file, caption=f"Все посты в формате JSON ({caption_option[option]})")
    os.remove(json_file)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonmonth_"))
async def handle_json_month_selection(query: types.CallbackQuery):
    month = int(query.data.split("_")[1])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data=f"jsonmonth_{month}_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data=f"jsonmonth_{month}_without")
        ],
        [
            InlineKeyboardButton(text="Только посты", callback_data=f"jsonmonth_{month}_postsonly"),
            InlineKeyboardButton(text="Только комментарии", callback_data=f"jsonmonth_{month}_commentsonly")
        ]
    ])
    await query.message.answer(f"Выберите вариант выгрузки JSON для месяца {month}:", reply_markup=keyboard)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonmonth_") and query.data.count("_") == 2)
async def handle_json_month_with_option(query: types.CallbackQuery):
    parts = query.data.split("_")
    month = int(parts[1])
    option = parts[2]
    json_file = create_monthly_json(month, option=option)
    input_file = FSInputFile(json_file)
    caption_option = {
        "with": "с комментариями",
        "without": "без комментариев",
        "postsonly": "только посты",
        "commentsonly": "только комментарии"
    }
    await query.message.answer_document(input_file, caption=f"JSON посты за {datetime.now().year}-{month:02d} ({caption_option[option]})")
    os.remove(json_file)
    await query.answer()

# Запуск бота и клиента Telethon
async def main():
    initialize_database()
    bot_task = asyncio.create_task(dp.start_polling(bot))
    phone_number = input("Введите номер телефона (в формате +79998887766): ")
    await client.start(phone_number)
    print("Бот запущен и мониторит каналы...")
    client_task = asyncio.create_task(client.run_until_disconnected())
    await asyncio.gather(bot_task, client_task)

if __name__ == '__main__':
    asyncio.run(main())
