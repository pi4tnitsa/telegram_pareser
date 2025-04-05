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

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Инициализация клиента Telethon
client = TelegramClient('session_name', api_id, api_hash)

# Путь к базе данных SQLite
DB_FILE = 'telegram_content.db'

# Инициализация базы данных
def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Создание таблицы постов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        channel_name TEXT,
        content TEXT,
        message_id INTEGER
    )
    ''')
    
    # Создание таблицы комментариев со ссылкой на посты
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        channel_name TEXT,
        post_content TEXT,
        comment TEXT,
        user_id INTEGER,
        username TEXT,
        post_id INTEGER
    )
    ''')
    
    # Создание таблицы сообщений для групповых чатов
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
    print("База данных успешно инициализирована")

# Функции для добавления данных в соответствующие таблицы
def add_post(date, channel_name, content, message_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO posts (date, channel_name, content, message_id)
    VALUES (?, ?, ?, ?)
    ''', (date, channel_name, content, message_id))
    post_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return post_id

def add_comment(date, channel_name, post_content, comment, user_id, username, post_id=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO comments (date, channel_name, post_content, comment, user_id, username, post_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (date, channel_name, post_content, comment, user_id, username, post_id))
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

# Функция для поиска поста по id сообщения
def find_post_by_message_id(channel_name, message_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id, content FROM posts 
    WHERE channel_name = ? AND message_id = ?
    ''', (channel_name, message_id))
    result = cursor.fetchone()
    conn.close()
    return result

# Обработчик событий для новых сообщений в каналах и группах
@client.on(events.NewMessage)
async def new_content_listener(event):
    if isinstance(event.peer_id, PeerChannel):
        try:
            channel = await client.get_entity(event.peer_id)
            channel_name = channel.title
            
            # Получение московского времени для поста
            post_date_utc = event.message.date
            moscow_tz = pytz.timezone('Europe/Moscow')
            post_date_moscow = post_date_utc.astimezone(moscow_tz)
            post_date = post_date_moscow.strftime('%Y-%m-%d %H:%M:%S')
            
            # Получение информации о отправителе
            sender = await event.get_sender()
            user_id = None
            username = None
            if sender:
                user_id = sender.id
                username = sender.username or f"User_{sender.id}"
            
            content = event.message.text or "Сообщение без текста"
            
            # Проверка, является ли это комментарием к посту
            if event.message.reply_to:
                try:
                    # Получение оригинального поста
                    original_msg_id = event.message.reply_to.reply_to_msg_id
                    
                    # Поиск поста в базе данных
                    post_info = find_post_by_message_id(channel_name, original_msg_id)
                    
                    if post_info:
                        # Если пост найден в базе данных, используем его
                        post_id, original_post = post_info
                    else:
                        # Иначе запрашиваем сообщение из Telegram
                        replied_msg = await client.get_messages(event.peer_id, ids=original_msg_id)
                        original_post = replied_msg.text or "Сообщение без текста"
                        post_id = None
                    
                    # Это комментарий
                    add_comment(post_date, channel_name, original_post, content, user_id, username, post_id)
                    print(f"Добавлен комментарий в канале {channel_name} в {post_date}")
                except Exception as e:
                    print(f"Ошибка при обработке комментария: {e}")
            else:
                # Определяем, является ли это постом канала или сообщением группы
                if event.is_channel and not event.is_group:
                    # Это пост канала
                    add_post(post_date, channel_name, content, event.message.id)
                    print(f"Добавлен пост из канала {channel_name} в {post_date}")
                elif event.is_group:
                    # Это сообщение группы
                    add_message(post_date, channel_name, content, user_id, username)
                    print(f"Добавлено сообщение из группы {channel_name} в {post_date}")
        except Exception as e:
            print(f"Ошибка при обработке сообщения: {e}")

# Команда /start
@dp.message(Command("start"))
async def start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Экспорт постов")],
            [KeyboardButton(text="Экспорт комментариев")],
            [KeyboardButton(text="Экспорт сообщений")],
            [KeyboardButton(text="Экспорт всего контента")],
            [KeyboardButton(text="Статистика")],
            [KeyboardButton(text="Помощь")]
        ],
        resize_keyboard=True
    )
    await message.answer("Привет! Выберите действие:", reply_markup=keyboard)

# Получение данных из базы данных по типу и временному диапазону
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

# Функция для получения статистики
def get_statistics():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Общее количество записей в каждой таблице
    cursor.execute("SELECT COUNT(*) FROM posts")
    posts_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM comments")
    comments_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM messages")
    messages_count = cursor.fetchone()[0]
    
    # Топ-5 каналов по количеству постов
    cursor.execute('''
    SELECT channel_name, COUNT(*) as count 
    FROM posts 
    GROUP BY channel_name 
    ORDER BY count DESC 
    LIMIT 5
    ''')
    top_channels = cursor.fetchall()
    
    # Активность по дням недели
    cursor.execute('''
    SELECT strftime('%w', date) as day_of_week, COUNT(*) as count
    FROM (
        SELECT date FROM posts
        UNION ALL
        SELECT date FROM comments
        UNION ALL
        SELECT date FROM messages
    )
    GROUP BY day_of_week
    ORDER BY day_of_week
    ''')
    activity_by_day = cursor.fetchall()
    
    conn.close()
    
    days = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
    activity_formatted = [(days[int(day)], count) for day, count in activity_by_day]
    
    return {
        "total_posts": posts_count,
        "total_comments": comments_count,
        "total_messages": messages_count,
        "top_channels": top_channels,
        "activity_by_day": activity_formatted
    }

# Расчет временного диапазона на основе периода
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
        # По умолчанию одна неделя, если период не распознан
        start_date = (current_date - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    return start_date, end_date

# Функции для создания Excel файлов
def create_excel_file(data_type, data, filename):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    
    if data_type == "posts":
        sheet.title = "Посты"
        headers = ["Дата", "Название канала", "Содержание"]
        sheet.append(headers)
        
        for post in data:
            row_data = [
                post['date'],
                post['channel_name'],
                post['content']
            ]
            sheet.append(row_data)
    
    elif data_type == "comments":
        sheet.title = "Комментарии"
        headers = ["Дата", "Название канала", "Содержание поста", "Комментарий", "ID пользователя", "Имя пользователя"]
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
        sheet.title = "Сообщения"
        headers = ["Дата", "Источник", "Содержание", "ID пользователя", "Имя пользователя"]
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
    
    else:  # Все данные - создание нескольких листов
        for content_type, content_data in data.items():
            if content_type == "posts":
                sheet = workbook.active
                sheet.title = "Посты"
                headers = ["Дата", "Название канала", "Содержание"]
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
                    headers = ["Дата", "Название канала", "Содержание поста", "Комментарий", "ID пользователя", "Имя пользователя"]
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
                    headers = ["Дата", "Источник", "Содержание", "ID пользователя", "Имя пользователя"]
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
    
    # Настройка ширины столбцов
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

# Функции для создания JSON файлов
def create_json_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filename

# Обработчики сообщений для основных кнопок
@dp.message(lambda message: message.text == "Экспорт постов")
async def export_posts_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Последняя неделя", callback_data="posts_week"),
            InlineKeyboardButton(text="Последние две недели", callback_data="posts_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Последний месяц", callback_data="posts_month"),
            InlineKeyboardButton(text="Другой период", callback_data="posts_custom")
        ]
    ])
    await message.answer("Выберите период для экспорта постов:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Экспорт комментариев")
async def export_comments_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Последняя неделя", callback_data="comments_week"),
            InlineKeyboardButton(text="Последние две недели", callback_data="comments_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Последний месяц", callback_data="comments_month"),
            InlineKeyboardButton(text="Другой период", callback_data="comments_custom")
        ]
    ])
    await message.answer("Выберите период для экспорта комментариев:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Экспорт сообщений")
async def export_messages_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Последняя неделя", callback_data="messages_week"),
            InlineKeyboardButton(text="Последние две недели", callback_data="messages_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Последний месяц", callback_data="messages_month"),
            InlineKeyboardButton(text="Другой период", callback_data="messages_custom")
        ]
    ])
    await message.answer("Выберите период для экспорта сообщений:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Экспорт всего контента")
async def export_all_menu(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Последняя неделя", callback_data="all_week"),
            InlineKeyboardButton(text="Последние две недели", callback_data="all_two_weeks")
        ],
        [
            InlineKeyboardButton(text="Последний месяц", callback_data="all_month"),
            InlineKeyboardButton(text="Другой период", callback_data="all_custom")
        ]
    ])
    await message.answer("Выберите период для экспорта всего контента:", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Статистика")
async def show_statistics(message: types.Message):
    stats = get_statistics()
    
    response = "📊 **Статистика базы данных:**\n\n"
    response += f"📝 Всего постов: {stats['total_posts']}\n"
    response += f"💬 Всего комментариев: {stats['total_comments']}\n"
    response += f"✉️ Всего сообщений: {stats['total_messages']}\n\n"
    
    response += "📈 **Топ-5 каналов:**\n"
    for i, (channel, count) in enumerate(stats['top_channels'], 1):
        response += f"{i}. {channel}: {count} постов\n"
    
    response += "\n📅 **Активность по дням недели:**\n"
    for day, count in stats['activity_by_day']:
        response += f"{day}: {count} записей\n"
    
    await message.answer(response, parse_mode="Markdown")

@dp.message(lambda message: message.text == "Помощь")
async def help_command(message: types.Message):
    help_text = (
        "🤖 **Справка по боту**\n\n"
        "Этот бот собирает и экспортирует контент из Telegram-каналов и групп.\n\n"
        "**Основные команды:**\n"
        "• /start - Запустить бота\n\n"
        "**Доступные функции:**\n"
        "• **Экспорт постов** - Экспорт сообщений из каналов\n"
        "• **Экспорт комментариев** - Экспорт комментариев к постам\n"
        "• **Экспорт сообщений** - Экспорт сообщений из групповых чатов\n"
        "• **Экспорт всего контента** - Экспорт всех данных сразу\n"
        "• **Статистика** - Показать аналитику собранных данных\n\n"
        "Для экспорта выберите тип данных, период и формат выгрузки (Excel или JSON)."
    )
    await message.answer(help_text, parse_mode="Markdown")

# Хранение пользовательских предпочтений
user_preferences = {}

# Обработчики запросов обратного вызова для выбора периода
@dp.callback_query(lambda query: query.data.split('_')[1] in ["week", "two_weeks", "month"])
async def handle_period_selection(query: types.CallbackQuery):
    parts = query.data.split('_')
    data_type = parts[0]
    period = parts[1]
    
    # Сохранение предпочтений пользователя по типу данных и периоду
    user_id = query.from_user.id
    if user_id not in user_preferences:
        user_preferences[user_id] = {}
    
    user_preferences[user_id]['data_type'] = data_type
    user_preferences[user_id]['period'] = period
    
    # Показываем кнопки выбора формата после выбора периода
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Excel формат", callback_data=f"{data_type}_format_xlsx"),
            InlineKeyboardButton(text="JSON формат", callback_data=f"{data_type}_format_json")
        ]
    ])
    
    # Отображение выбранного периода на русском
    period_text = {
        "week": "последняя неделя", 
        "two_weeks": "последние две недели", 
        "month": "последний месяц"
    }.get(period, period)
    
    await query.message.answer(f"Выбран период: {period_text}. Теперь выберите формат экспорта:", reply_markup=keyboard)
    await query.answer()

# Обработчики запросов обратного вызова для пользовательского периода
@dp.callback_query(lambda query: query.data.endswith("_custom"))
async def handle_custom_period(query: types.CallbackQuery):
    data_type = query.data.split('_')[0]
    
    # Сохранение предпочтений пользователя по типу данных
    user_id = query.from_user.id
    if user_id not in user_preferences:
        user_preferences[user_id] = {}
    
    user_preferences[user_id]['data_type'] = data_type
    user_preferences[user_id]['waiting_for'] = 'start_date'
    
    await query.message.answer("Введите начальную дату в формате ГГГГ-ММ-ДД:")
    await query.answer()

# Обработчик для получения пользовательской начальной даты
@dp.message(lambda message: message.from_user.id in user_preferences and user_preferences[message.from_user.id].get('waiting_for') == 'start_date')
async def handle_custom_start_date(message: types.Message):
    user_id = message.from_user.id
    
    try:
        # Проверка формата даты
        start_date = f"{message.text} 00:00:00"
        datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        user_preferences[user_id]['start_date'] = start_date
        user_preferences[user_id]['waiting_for'] = 'end_date'
        await message.answer("Введите конечную дату в формате ГГГГ-ММ-ДД:")
    except ValueError:
        await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ГГГГ-ММ-ДД:")

# Обработчик для получения пользовательской конечной даты
@dp.message(lambda message: message.from_user.id in user_preferences and user_preferences[message.from_user.id].get('waiting_for') == 'end_date')
async def handle_custom_end_date(message: types.Message):
    user_id = message.from_user.id
    
    try:
        # Проверка формата даты
        end_date = f"{message.text} 23:59:59"
        datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        user_preferences[user_id]['end_date'] = end_date
        
        # Показываем кнопки выбора формата после выбора периода
        data_type = user_preferences[user_id]['data_type']
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Excel формат", callback_data=f"{data_type}_format_xlsx"),
                InlineKeyboardButton(text="JSON формат", callback_data=f"{data_type}_format_json")
            ]
        ])
        
        await message.answer("Теперь выберите формат экспорта:", reply_markup=keyboard)
    except ValueError:
        await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ГГГГ-ММ-ДД:")

# Обработчики запросов обратного вызова для выбора формата
@dp.callback_query(lambda query: query.data.split('_')[1] == "format")
async def handle_format_selection(query: types.CallbackQuery):
    parts = query.data.split('_')
    data_type = parts[0]
    export_format = parts[2]  # xlsx или json
    
    user_id = query.from_user.id
    if user_id not in user_preferences:
        await query.message.answer("Пожалуйста, сначала выберите тип данных и период.")
        await query.answer()
        return
    
    user_prefs = user_preferences[user_id]
    user_prefs['format'] = export_format
    
    # Обработка экспорта на основе сохраненных предпочтений
    try:
        # Получение диапазона дат из пользовательских дат или предопределенного периода
        if 'start_date' in user_prefs and 'end_date' in user_prefs:
            start_date = user_prefs['start_date']
            end_date = user_prefs['end_date']
        else:
            start_date, end_date = get_date_range(user_prefs.get('period', 'week'))
        
        # Получение данных из базы данных
        data = get_data_by_period(user_prefs['data_type'], start_date, end_date)
        
        # Переводим названия типов для отображения
        data_type_names = {
            "posts": "посты",
            "comments": "комментарии",
            "messages": "сообщения",
            "all": "весь контент"
        }
        
        # Создание экспортного файла
        if export_format == 'xlsx':
            filename = f"{user_prefs['data_type']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            export_file = create_excel_file(user_prefs['data_type'], data, filename)
        else:  # json
            filename = f"{user_prefs['data_type']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            export_file = create_json_file(data, filename)
        
        # Отправка файла
        input_file = FSInputFile(export_file)
        period_str = user_prefs.get('period', 'custom period')
        if period_str == 'custom period':
            period_str = f"с {user_prefs.get('start_date', '').split()[0]} по {user_prefs.get('end_date', '').split()[0]}"
        else:
            period_translations = {
                "week": "последняя неделя",
                "two_weeks": "последние две недели",
                "month": "последний месяц"
            }
            period_str = period_translations.get(period_str, period_str)
        
        data_type_name = data_type_names.get(user_prefs['data_type'], user_prefs['data_type'])
        
        await query.message.answer_document(
            input_file, 
            caption=f"Экспорт: {data_type_name}, период: {period_str}, формат: {export_format.upper()}"
        )
        
        # Очистка файла
        os.remove(export_file)
        
    except Exception as e:
        await query.message.answer(f"
