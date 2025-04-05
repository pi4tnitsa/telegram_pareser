from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from datetime import datetime, timedelta
import openpyxl
import pytz
import os
import asyncio
import json
import sqlite3
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel, PeerUser
from config import api_hash, api_id, BOT_TOKEN

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Initialize Telethon client
client = TelegramClient('session_name', api_id, api_hash)

# Path to SQLite database
DB_FILE = 'telegram_content.db'

# Database initialization
def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create posts table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        channel_name TEXT,
        content TEXT
    )
    ''')
    
    # Create comments table with reference to posts
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        channel_name TEXT,
        post_content TEXT,
        comment TEXT,
        user_id INTEGER,
        username TEXT
    )
    ''')
    
    # Create messages table for group chat messages
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        source TEXT,
        content TEXT,
        user_id INTEGER,
        username TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

# Functions to add data to respective tables
def add_post(date, channel_name, content):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO posts (date, channel_name, content)
    VALUES (?, ?, ?)
    ''', (date, channel_name, content))
    post_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return post_id

def add_comment(date, channel_name, post_content, comment, user_id, username):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO comments (date, channel_name, post_content, comment, user_id, username)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (date, channel_name, post_content, comment, user_id, username))
    comment_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return comment_id

def add_message(date, source, content, user_id, username):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO messages (date, source, content, user_id, username)
    VALUES (?, ?, ?, ?, ?)
    ''', (date, source, content, user_id, username))
    message_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return message_id

# Event handler for new messages in channels and groups
@client.on(events.NewMessage)
async def new_content_listener(event):
    if isinstance(event.peer_id, PeerChannel):
        try:
            channel = await client.get_entity(event.peer_id)
            channel_name = channel.title
            
            # Get Moscow time for the post
            post_date_utc = event.message.date
            moscow_tz = pytz.timezone('Europe/Moscow')
            post_date_moscow = post_date_utc.astimezone(moscow_tz)
            post_date = post_date_moscow.strftime('%Y-%m-%d %H:%M:%S')
            
            # Get sender info
            sender = await event.get_sender()
            user_id = None
            username = None
            if sender:
                user_id = sender.id
                username = sender.username or f"User_{sender.id}"
            
            content = event.message.text or "Message without text"
            
            # Check if this is a comment to a post
            if event.message.reply_to:
                try:
                    # Get the original post
                    replied_msg = await client.get_messages(event.peer_id, ids=event.message.reply_to.reply_to_msg_id)
                    original_post = replied_msg.text or "Message without text"
                    
                    # This is a comment
                    add_comment(post_date, channel_name, original_post, content, user_id, username)
                    print(f"Added comment in channel {channel_name} at {post_date}")
                except Exception as e:
                    print(f"Error processing comment: {e}")
            else:
                # Determine if this is a channel post or a group message
                if event.is_channel and not event.is_group:
                    # This is a channel post
                    add_post(post_date, channel_name, content)
                    print(f"Added post from channel {channel_name} at {post_date}")
                elif event.is_group:
                    # This is a group message
                    add_message(post_date, channel_name, content, user_id, username)
                    print(f"Added message from group {channel_name} at {post_date}")
        except Exception as e:
            print(f"Error processing message: {e}")

# Command /start
@dp.message(Command("start"))
async def start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Export Posts")],
            [KeyboardButton(text="Export Comments")],
            [KeyboardButton(text="Export Messages")],
            [KeyboardButton(text="Export All Content")]
        ],
        resize_keyboard=True
    )
    await message.answer("Hello! Choose an action:", reply_markup=keyboard)

# Get data from database based on type and date range
def get_data_by_period(data_type, start_date, end_date):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if data_type == "posts":
        query = '''
        SELECT date, channel_name, content 
        FROM posts 
        WHERE date >= ? AND date <= ?
        ORDER BY date
        '''
    elif data_type == "comments":
        query = '''
        SELECT date, channel_name, post_content, comment, user_id, username 
        FROM comments 
        WHERE date >= ? AND date <= ?
        ORDER BY date
        '''
    elif data_type == "messages":
        query = '''
        SELECT date, source, content, user_id, username 
        FROM messages 
        WHERE date >= ? AND date <= ?
        ORDER BY date
        '''
    else:  # All data
        results = {
            "posts": get_data_by_period("posts", start_date, end_date),
            "comments": get_data_by_period("comments", start_date, end_date),
            "messages": get_data_by_period("messages", start_date, end_date)
        }
        return results
    
    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        data.append(dict(row))
    
    return data

# Calculate date range based on period
def get_date_range(period):
    current_date = datetime.now()
    end_date = current_date.strftime('%Y-%m-%d %H:%M:%S')
    
    if period == "week":
        start_date = (current_date - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    elif period == "two_weeks":
        start_date = (current_date - timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')
    elif period == "month":
        start_date = (current_date - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        # Default to one week if period is not recognized
        start_date = (current_date - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    return start_date, end_date

# Functions to create Excel files
def create_excel_file(data_type, data, filename):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    
    if data_type == "posts":
        sheet.title = "Posts"
        headers = ["Date", "Channel Name", "Content"]
        sheet.append(headers)
        
        for post in data:
            row_data = [
                post['date'],
                post['channel_name'],
                post['content']
            ]
            sheet.append(row_data)
    
    elif data_type == "comments":
        sheet.title = "Comments"
        headers = ["Date", "Channel Name", "Post Content", "Comment", "User ID", "Username"]
        sheet.append(headers)
        
        for comment in data:
            row_data = [
                comment['date'],
                comment['channel_name'],
                comment['post_content'],
                comment['comment'],
                comment['user_id'],
                comment['username']
            ]
            sheet.append(row_data)
    
    elif data_type == "messages":
        sheet.title = "Messages"
        headers = ["Date", "Source", "Content", "User ID", "Username"]
        sheet.append(headers)
        
        for message in data:
            row_data = [
                message['date'],
                message['source'],
                message['content'],
                message['user_id'],
                message['username']
            ]
            sheet.append(row_data)
    
    else:  # All data - create multiple sheets
        for content_type, content_data in data.items():
            if content_type == "posts":
                sheet = workbook.active
                sheet.title = "Posts"
                headers = ["Date", "Channel Name", "Content"]
                sheet.append(headers)
                
                for post in content_data:
                    row_data = [
                        post['date'],
                        post['channel_name'],
                        post['content']
                    ]
                    sheet.append(row_data)
            
            else:
                sheet = workbook.create_sheet(title=content_type.capitalize())
                
                if content_type == "comments":
                    headers = ["Date", "Channel Name", "Post Content", "Comment", "User ID", "Username"]
                    sheet.append(headers)
                    
                    for comment in content_data:
                        row_data = [
                            comment['date'],
                            comment['channel_name'],
                            comment['post_content'],
                            comment['comment'],
                            comment['user_id'],
                            comment['username']
                        ]
                        sheet.append(row_data)
                
                elif content_type == "messages":
                    headers = ["Date", "Source", "Content", "User ID", "Username"]
                    sheet.append(headers)
                    
                    for message in content_data:
                        row_data = [
                            message['date'],
                            message['source'],
                            message['content'],
                            message['user_id'],
                            message['username']
                        ]
                        sheet.append(row_data)
    
    # Adjust column widths
    for sheet in workbook.worksheets:
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
    
    workbook.save(filename)
    return filename

# Functions to create JSON files
def create_json_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filename

# Message handlers for main buttons
@dp.message(lambda message: message.text == "Export Posts")
async def export_posts_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Last Week", callback_data="posts_week"),
            InlineKeyboardButton(text="Last Two Weeks", callback_data="posts_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Last Month", callback_data="posts_month"),
            InlineKeyboardButton(text="Custom Period", callback_data="posts_custom")
        ],
        [
            InlineKeyboardButton(text="Excel Format", callback_data="posts_format_xlsx"),
            InlineKeyboardButton(text="JSON Format", callback_data="posts_format_json")
        ]
    ])
    await message.answer("Choose export period and format for Posts:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Export Comments")
async def export_comments_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Last Week", callback_data="comments_week"),
            InlineKeyboardButton(text="Last Two Weeks", callback_data="comments_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Last Month", callback_data="comments_month"),
            InlineKeyboardButton(text="Custom Period", callback_data="comments_custom")
        ],
        [
            InlineKeyboardButton(text="Excel Format", callback_data="comments_format_xlsx"),
            InlineKeyboardButton(text="JSON Format", callback_data="comments_format_json")
        ]
    ])
    await message.answer("Choose export period and format for Comments:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Export Messages")
async def export_messages_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Last Week", callback_data="messages_week"),
            InlineKeyboardButton(text="Last Two Weeks", callback_data="messages_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Last Month", callback_data="messages_month"),
            InlineKeyboardButton(text="Custom Period", callback_data="messages_custom")
        ],
        [
            InlineKeyboardButton(text="Excel Format", callback_data="messages_format_xlsx"),
            InlineKeyboardButton(text="JSON Format", callback_data="messages_format_json")
        ]
    ])
    await message.answer("Choose export period and format for Messages:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Export All Content")
async def export_all_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Last Week", callback_data="all_week"),
            InlineKeyboardButton(text="Last Two Weeks", callback_data="all_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Last Month", callback_data="all_month"),
            InlineKeyboardButton(text="Custom Period", callback_data="all_custom")
        ],
        [
            InlineKeyboardButton(text="Excel Format", callback_data="all_format_xlsx"),
            InlineKeyboardButton(text="JSON Format", callback_data="all_format_json")
        ]
    ])
    await message.answer("Choose export period and format for All Content:", reply_markup=keyboard)

# Store user preferences
user_preferences = {}

# Callback query handlers for period selection
@dp.callback_query(lambda query: query.data.split('_')[1] in ["week", "two_weeks", "month"])
async def handle_period_selection(query: types.CallbackQuery):
    parts = query.data.split('_')
    data_type = parts[0]
    period = parts[1]
    
    # Store the user's data type and period preference
    user_id = query.from_user.id
    if user_id not in user_preferences:
        user_preferences[user_id] = {}
    
    user_preferences[user_id]['data_type'] = data_type
    user_preferences[user_id]['period'] = period
    
    await query.message.answer(f"Selected {period} period for {data_type}. Now choose the format.")
    await query.answer()

# Callback query handlers for custom period
@dp.callback_query(lambda query: query.data.endswith("_custom"))
async def handle_custom_period(query: types.CallbackQuery):
    data_type = query.data.split('_')[0]
    
    # Store the user's data type preference
    user_id = query.from_user.id
    if user_id not in user_preferences:
        user_preferences[user_id] = {}
    
    user_preferences[user_id]['data_type'] = data_type
    user_preferences[user_id]['waiting_for'] = 'start_date'
    
    await query.message.answer("Enter start date in format YYYY-MM-DD:")
    await query.answer()

# Handler for receiving custom start date
@dp.message(lambda message: message.from_user.id in user_preferences and user_preferences[message.from_user.id].get('waiting_for') == 'start_date')
async def handle_custom_start_date(message: types.Message):
    user_id = message.from_user.id
    
    try:
        # Validate date format
        start_date = f"{message.text} 00:00:00"
        datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        user_preferences[user_id]['start_date'] = start_date
        user_preferences[user_id]['waiting_for'] = 'end_date'
        await message.answer("Enter end date in format YYYY-MM-DD:")
    except ValueError:
        await message.answer("Invalid date format. Please enter date in format YYYY-MM-DD:")

# Handler for receiving custom end date
@dp.message(lambda message: message.from_user.id in user_preferences and user_preferences[message.from_user.id].get('waiting_for') == 'end_date')
async def handle_custom_end_date(message: types.Message):
    user_id = message.from_user.id
    
    try:
        # Validate date format
        end_date = f"{message.text} 23:59:59"
        datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        user_preferences[user_id]['end_date'] = end_date
        user_preferences[user_id]['waiting_for'] = 'format'
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Excel Format", callback_data=f"{user_preferences[user_id]['data_type']}_format_xlsx"),
                InlineKeyboardButton(text="JSON Format", callback_data=f"{user_preferences[user_id]['data_type']}_format_json")
            ]
        ])
        await message.answer("Now choose the export format:", reply_markup=keyboard)
    except ValueError:
        await message.answer("Invalid date format. Please enter date in format YYYY-MM-DD:")

# Callback query handlers for format selection
@dp.callback_query(lambda query: query.data.split('_')[1] == "format")
async def handle_format_selection(query: types.CallbackQuery):
    parts = query.data.split('_')
    data_type = parts[0]
    export_format = parts[2]  # xlsx or json
    
    user_id = query.from_user.id
    if user_id not in user_preferences:
        await query.message.answer("Please select data type and period first.")
        await query.answer()
        return
    
    user_prefs = user_preferences[user_id]
    user_prefs['format'] = export_format
    
    # Process export based on saved preferences
    try:
        # Get date range either from custom dates or predefined period
        if 'start_date' in user_prefs and 'end_date' in user_prefs:
            start_date = user_prefs['start_date']
            end_date = user_prefs['end_date']
        else:
            start_date, end_date = get_date_range(user_prefs.get('period', 'week'))
        
        # Get data from database
        data = get_data_by_period(user_prefs['data_type'], start_date, end_date)
        
        # Create export file
        if export_format == 'xlsx':
            filename = f"{user_prefs['data_type']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            export_file = create_excel_file(user_prefs['data_type'], data, filename)
        else:  # json
            filename = f"{user_prefs['data_type']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            export_file = create_json_file(data, filename)
        
        # Send the file
        input_file = FSInputFile(export_file)
        period_str = user_prefs.get('period', 'custom period')
        if period_str == 'custom period':
            period_str = f"{user_prefs.get('start_date', '').split()[0]} to {user_prefs.get('end_date', '').split()[0]}"
        
        await query.message.answer_document(
            input_file, 
            caption=f"{user_prefs['data_type'].capitalize()} export for {period_str} in {export_format.upper()} format"
        )
        
        # Clean up the file
        os.remove(export_file)
        
    except Exception as e:
        await query.message.answer(f"Error creating export: {str(e)}")
    
    # Clear user preferences
    if user_id in user_preferences:
        del user_preferences[user_id]
    
    await query.answer()

# Main function to run both bot and client
async def main():
    initialize_database()
    bot_task = asyncio.create_task(dp.start_polling(bot))
    
    # Start the Telethon client
    phone_number = input("Enter your phone number (format: +79998887766): ")
    await client.start(phone_number)
    print("Bot is running and monitoring channels...")
    
    client_task = asyncio.create_task(client.run_until_disconnected())
    await asyncio.gather(bot_task, client_task)

if __name__ == '__main__':
    asyncio.run(main())
