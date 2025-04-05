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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

client = TelegramClient('session_name', api_id, api_hash)

DB_FILE = 'channel_posts.db'

def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        channel_name TEXT,
        content TEXT,
        reactions INTEGER,
        views INTEGER,
        media_type TEXT,
        media_path TEXT,
        comments INTEGER,
        sentiment_score REAL,
        keywords TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

def add_to_database(date, channel_name, content, comments=None, reactions=0, views=0, media_type=None, media_path=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO posts (date, channel_name, content, comments, reactions, views, media_type, media_path)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (date, channel_name, content, comments, reactions, views, media_type, media_path))
    
    conn.commit()
    conn.close()

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

                post_content = event.message.text or "Media message (no text)"
                comments = None
                
                # Проверяем, является ли сообщение комментарием
                if event.message.reply_to:
                    try:
                        replied_msg = await client.get_messages(event.peer_id, ids=event.message.reply_to.reply_to_msg_id)
                        # Сохраняем как комментарий к исходному посту
                        comments = post_content
                        post_content = replied_msg.text or "Original media message (no text)"
                    except:
                        pass
                
                reactions = 0
                if hasattr(event.message, 'reactions') and event.message.reactions:
                    reactions = event.message.reactions.count
                
                views = 0
                if hasattr(event.message, 'views'):
                    views = event.message.views
                
                media_type = None
                media_path = None
                
                if event.message.media:
                    if hasattr(event.message.media, 'photo'):
                        media_type = 'photo'
                    elif hasattr(event.message.media, 'document'):
                        media_type = 'document'
                    elif hasattr(event.message.media, 'video'):
                        media_type = 'video'
                
                add_to_database(post_date, channel_name, post_content, comments, reactions, views, media_type, media_path)
                print(f"Added post from channel {channel_name} on {post_date} (Moscow time)")
        except Exception as e:
            print(f"Error processing post: {e}")

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

@dp.message(lambda message: message.text == "Выгрузить посты за текущий месяц")
async def send_monthly_posts(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data="monthcurrent_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data="monthcurrent_without")
        ]
    ])
    await message.answer("Выберите вариант выгрузки:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Выгрузить всю таблицу")
async def send_full_table(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data="full_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data="full_without")
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
    current_year = datetime.now().year
    try:
        monthly_file = create_monthly_excel(current_month, with_comments=(option == "with"))
        input_file = FSInputFile(monthly_file)
        await query.message.answer_document(input_file, caption=f"Посты за {current_year}-{current_month:02d}")
        os.remove(monthly_file)  
    except Exception as e:
        await query.message.answer(f"Ошибка при создании Excel файла: {str(e)}")
    finally:
        await query.answer()  

@dp.callback_query(lambda query: query.data.startswith("full_"))
async def handle_full_with_option(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    try:
        full_file = create_full_excel(with_comments=(option == "with"))
        input_file = FSInputFile(full_file)
        await query.message.answer_document(input_file, caption="Полная таблица постов")
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
        monthly_file = create_monthly_excel(month, with_comments=(option == "with"))
        input_file = FSInputFile(monthly_file)
        await query.message.answer_document(input_file, caption=f"Посты за {datetime.now().year}-{month:02d}")
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
            ]
        ])
        await query.message.answer("Выберите вариант выгрузки JSON:", reply_markup=keyboard)
    elif option == "all":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="С комментариями", callback_data="jsonall_with"),
                InlineKeyboardButton(text="Без комментариев", callback_data="jsonall_without")
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
    json_file = create_monthly_json(current_month, with_comments=(option == "with"))
    input_file = FSInputFile(json_file)
    await query.message.answer_document(input_file, caption=f"JSON посты за {datetime.now().year}-{current_month:02d}")
    os.remove(json_file)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonall_"))
async def handle_json_all_with_option(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    json_file = create_full_json(with_comments=(option == "with"))
    input_file = FSInputFile(json_file)
    await query.message.answer_document(input_file, caption="Все посты в формате JSON")
    os.remove(json_file)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonmonth_"))
async def handle_json_month_selection(query: types.CallbackQuery):
    month = int(query.data.split("_")[1])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="С комментариями", callback_data=f"jsonmonth_{month}_with"),
            InlineKeyboardButton(text="Без комментариев", callback_data=f"jsonmonth_{month}_without")
        ]
    ])
    await query.message.answer(f"Выберите вариант выгрузки JSON для месяца {month}:", reply_markup=keyboard)
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonmonth_") and query.data.count("_") == 2)
async def handle_json_month_with_option(query: types.CallbackQuery):
    parts = query.data.split("_")
    month = int(parts[1])
    option = parts[2]
    json_file = create_monthly_json(month, with_comments=(option == "with"))
    input_file = FSInputFile(json_file)
    await query.message.answer_document(input_file, caption=f"JSON посты за {datetime.now().year}-{month:02d}")
    os.remove(json_file)
    await query.answer()

def get_posts_by_month(month):
    current_year = datetime.now().year
    month_start = f"{current_year}-{month:02d}-01"
    
    next_month = month + 1 if month < 12 else 1
    next_year = current_year if month < 12 else current_year + 1
    month_end = f"{next_year}-{next_month:02d}-01"
    
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM posts 
    WHERE date >= ? AND date < ?
    ORDER BY date
    ''', (month_start, month_end))
    
    rows = cursor.fetchall()
    conn.close()
    
    posts = []
    for row in rows:
        post = dict(row)
        posts.append(post)
    
    return posts

def get_all_posts():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM posts ORDER BY date')
    rows = cursor.fetchall()
    conn.close()
    
    posts = []
    for row in rows:
        post = dict(row)
        posts.append(post)
    
    return posts

def create_monthly_excel(month, with_comments=True):
    current_year = datetime.now().year
    posts = get_posts_by_month(month)
    
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Posts"
    
    if with_comments:
        sheet.append(["Дата", "Название канала", "Содержание", "Комментарии", "Реакции", "Просмотры", "Тип медиа"])
    else:
        sheet.append(["Дата", "Название канала", "Содержание", "Реакции", "Просмотры", "Тип медиа"])
    
    for post in posts:
        if with_comments:
            sheet.append([
                post['date'],
                post['channel_name'],
                post['content'],
                post['comments'] or "Нет комментариев",
                post['reactions'],
                post['views'],
                post['media_type'] or "Нет"
            ])
        else:
            sheet.append([
                post['date'],
                post['channel_name'],
                post['content'],
                post['reactions'],
                post['views'],
                post['media_type'] or "Нет"
            ])
    
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
    
    comments_suffix = "_с_комментариями" if with_comments else "_без_комментариев"
    monthly_file = f"посты_{current_year}-{month:02d}{comments_suffix}.xlsx"
    workbook.save(monthly_file)
    return monthly_file

def create_full_excel(with_comments=True):
    posts = get_all_posts()
    
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "All Posts"
    
    if with_comments:
        sheet.append(["Дата", "Название канала", "Содержание", "Комментарии", "Реакции", "Просмотры", "Тип медиа"])
    else:
        sheet.append(["Дата", "Название канала", "Содержание", "Реакции", "Просмотры", "Тип медиа"])
    
    for post in posts:
        if with_comments:
            sheet.append([
                post['date'],
                post['channel_name'],
                post['content'],
                post['comments'] or "Нет комментариев",
                post['reactions'],
                post['views'],
                post['media_type'] or "Нет"
            ])
        else:
            sheet.append([
                post['date'],
                post['channel_name'],
                post['content'],
                post['reactions'],
                post['views'],
                post['media_type'] or "Нет"
            ])
    
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
    
    comments_suffix = "_с_комментариями" if with_comments else "_без_комментариев"
    full_file = f"все_посты{comments_suffix}.xlsx"
    workbook.save(full_file)
    return full_file

def create_monthly_json(month, with_comments=True):
    current_year = datetime.now().year
    posts = get_posts_by_month(month)
    
    output_posts = []
    for post in posts:
        if not with_comments:
            # Создаем копию словаря без поля comments
            filtered_post = {key: value for key, value in post.items() if key != 'comments'}
            output_posts.append(filtered_post)
        else:
            output_posts.append(post)
    
    comments_suffix = "_с_комментариями" if with_comments else "_без_комментариев"
    monthly_file = f"посты_{current_year}-{month:02d}{comments_suffix}.json"
    with open(monthly_file, 'w', encoding='utf-8') as f:
        json.dump(output_posts, f, ensure_ascii=False, indent=2)
    
    return monthly_file

def create_full_json(with_comments=True):
    posts = get_all_posts()
    
    output_posts = []
    for post in posts:
        if not with_comments:
            # Создаем копию словаря без поля comments
            filtered_post = {key: value for key, value in post.items() if key != 'comments'}
            output_posts.append(filtered_post)
        else:
            output_posts.append(post)
    
    comments_suffix = "_с_комментариями" if with_comments else "_без_комментариев"
    full_file = f"все_посты{comments_suffix}.json"
    with open(full_file, 'w', encoding='utf-8') as f:
        json.dump(output_posts, f, ensure_ascii=False, indent=2)
    
    return full_file

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
