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
        media_path TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

def add_to_database(date, channel_name, content, reactions, views=0, media_type=None, media_path=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO posts (date, channel_name, content, reactions, views, media_type, media_path)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (date, channel_name, content, reactions, views, media_type, media_path))
    
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
                
                add_to_database(post_date, channel_name, post_content, reactions, views, media_type, media_path)
                print(f"Added post from channel {channel_name} on {post_date} (Moscow time)")
        except Exception as e:
            print(f"Error processing post: {e}")

@dp.message(Command("start"))
async def start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Export posts for current month")],
            [KeyboardButton(text="Export all data")],
            [KeyboardButton(text="Export posts for specific month")],
            [KeyboardButton(text="Export as JSON")]
        ],
        resize_keyboard=True
    )
    await message.answer("Hello! Choose an action:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Export posts for current month")
async def send_monthly_posts(message: types.Message):
    current_month = datetime.now().month
    current_year = datetime.now().year
    try:
        monthly_file = create_monthly_excel(current_month)
        input_file = FSInputFile(monthly_file)
        await message.answer_document(input_file, caption=f"Posts for {current_year}-{current_month:02d}")
        os.remove(monthly_file)  
    except Exception as e:
        await message.answer(f"Error generating Excel file: {str(e)}")

@dp.message(lambda message: message.text == "Export all data")
async def send_full_table(message: types.Message):
    try:
        full_file = create_full_excel()
        input_file = FSInputFile(full_file)
        await message.answer_document(input_file, caption="Complete table of posts")
        os.remove(full_file)  
    except Exception as e:
        await message.answer(f"Error generating Excel file: {str(e)}")

@dp.message(lambda message: message.text == "Export posts for specific month")
async def choose_month(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="January", callback_data="month_1"),
            InlineKeyboardButton(text="February", callback_data="month_2"),
            InlineKeyboardButton(text="March", callback_data="month_3")
        ],
        [
            InlineKeyboardButton(text="April", callback_data="month_4"),
            InlineKeyboardButton(text="May", callback_data="month_5"),
            InlineKeyboardButton(text="June", callback_data="month_6")
        ],
        [
            InlineKeyboardButton(text="July", callback_data="month_7"),
            InlineKeyboardButton(text="August", callback_data="month_8"),
            InlineKeyboardButton(text="September", callback_data="month_9")
        ],
        [
            InlineKeyboardButton(text="October", callback_data="month_10"),
            InlineKeyboardButton(text="November", callback_data="month_11"),
            InlineKeyboardButton(text="December", callback_data="month_12")
        ],
    ])
    await message.answer("Select a month for this year:", reply_markup=keyboard)

@dp.callback_query(lambda query: query.data.startswith("month_"))
async def handle_month_selection(query: types.CallbackQuery):
    month = int(query.data.split("_")[1])  
    try:
        monthly_file = create_monthly_excel(month)
        input_file = FSInputFile(monthly_file)
        await query.message.answer_document(input_file, caption=f"Posts for {datetime.now().year}-{month:02d}")
        os.remove(monthly_file)  
    except Exception as e:
        await query.message.answer(f"Error generating Excel file: {str(e)}")
    finally:
        await query.answer()  

@dp.message(lambda message: message.text == "Export as JSON")
async def export_json(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Current month", callback_data="json_current"),
            InlineKeyboardButton(text="All data", callback_data="json_all")
        ],
        [InlineKeyboardButton(text="Specific month", callback_data="json_choose")]
    ])
    await message.answer("Choose what data to export as JSON:", reply_markup=keyboard)

@dp.callback_query(lambda query: query.data.startswith("json_"))
async def handle_json_selection(query: types.CallbackQuery):
    option = query.data.split("_")[1]
    
    if option == "current":
        current_month = datetime.now().month
        json_file = create_monthly_json(current_month)
        input_file = FSInputFile(json_file)
        await query.message.answer_document(input_file, caption=f"JSON posts for {datetime.now().year}-{current_month:02d}")
        os.remove(json_file)
    elif option == "all":
        json_file = create_full_json()
        input_file = FSInputFile(json_file)
        await query.message.answer_document(input_file, caption="All posts in JSON format")
        os.remove(json_file)
    elif option == "choose":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="January", callback_data="jsonmonth_1"),
                InlineKeyboardButton(text="February", callback_data="jsonmonth_2"),
                InlineKeyboardButton(text="March", callback_data="jsonmonth_3")
            ],
            [
                InlineKeyboardButton(text="April", callback_data="jsonmonth_4"),
                InlineKeyboardButton(text="May", callback_data="jsonmonth_5"),
                InlineKeyboardButton(text="June", callback_data="jsonmonth_6")
            ],
            [
                InlineKeyboardButton(text="July", callback_data="jsonmonth_7"),
                InlineKeyboardButton(text="August", callback_data="jsonmonth_8"),
                InlineKeyboardButton(text="September", callback_data="jsonmonth_9")
            ],
            [
                InlineKeyboardButton(text="October", callback_data="jsonmonth_10"),
                InlineKeyboardButton(text="November", callback_data="jsonmonth_11"),
                InlineKeyboardButton(text="December", callback_data="jsonmonth_12")
            ],
        ])
        await query.message.answer("Select a month for JSON export:", reply_markup=keyboard)
    
    await query.answer()

@dp.callback_query(lambda query: query.data.startswith("jsonmonth_"))
async def handle_json_month_selection(query: types.CallbackQuery):
    month = int(query.data.split("_")[1])
    json_file = create_monthly_json(month)
    input_file = FSInputFile(json_file)
    await query.message.answer_document(input_file, caption=f"JSON posts for {datetime.now().year}-{month:02d}")
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

def create_monthly_excel(month):
    current_year = datetime.now().year
    posts = get_posts_by_month(month)
    
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Posts"
    
    sheet.append(["Date", "Channel Name", "Content", "Reactions", "Views", "Media Type"])
    
    for post in posts:
        sheet.append([
            post['date'],
            post['channel_name'],
            post['content'],
            post['reactions'],
            post['views'],
            post['media_type'] or "None"
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
    
    monthly_file = f"posts_{current_year}-{month:02d}.xlsx"
    workbook.save(monthly_file)
    return monthly_file

def create_full_excel():
    posts = get_all_posts()
    
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "All Posts"
    
    sheet.append(["Date", "Channel Name", "Content", "Reactions", "Views", "Media Type"])
    
    for post in posts:
        sheet.append([
            post['date'],
            post['channel_name'],
            post['content'],
            post['reactions'],
            post['views'],
            post['media_type'] or "None"
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
    
    full_file = "all_posts.xlsx"
    workbook.save(full_file)
    return full_file

def create_monthly_json(month):
    current_year = datetime.now().year
    posts = get_posts_by_month(month)
    
    monthly_file = f"posts_{current_year}-{month:02d}.json"
    with open(monthly_file, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    
    return monthly_file

def create_full_json():
    posts = get_all_posts()
    
    full_file = "all_posts.json"
    with open(full_file, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    
    return full_file

async def main():
    initialize_database()
    
    bot_task = asyncio.create_task(dp.start_polling(bot))

    phone_number = input("Enter your phone number (in format +79998887766): ")
    await client.start(phone_number)
    print("Bot started and monitoring channels...")
    client_task = asyncio.create_task(client.run_until_disconnected())

    await asyncio.gather(bot_task, client_task)

if __name__ == '__main__':
    asyncio.run(main())
