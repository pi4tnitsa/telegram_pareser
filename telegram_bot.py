from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from datetime import datetime, timedelta
import openpyxl
import matplotlib.pyplot as plt
import pytz
import os
import asyncio
import json
import sqlite3
import logging
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel, PeerUser
from config import api_hash, api_id, BOT_TOKEN

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера с хранилищем состояний
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

# Инициализация клиента Telethon
client = TelegramClient('session_name', api_id, api_hash)

# Путь к базе данных SQLite
DB_FILE = 'telegram_content.db'

# Директория для временных файлов
TEMP_DIR = 'temp'
os.makedirs(TEMP_DIR, exist_ok=True)

# Класс для управления состояниями FSM
class FormStates(StatesGroup):
    waiting_for_start_date = State()
    waiting_for_end_date = State()
    waiting_for_search_query = State()
    waiting_for_channel_name = State()

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
        message_id INTEGER,
        views INTEGER DEFAULT 0,
        forwards INTEGER DEFAULT 0
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
        post_id INTEGER,
        sentiment TEXT DEFAULT 'neutral'
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
        username TEXT,
        media_type TEXT DEFAULT NULL
    )
    ''')
    
    # Создание таблицы для отслеживаемых каналов и групп
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS monitored_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id INTEGER,
        source_name TEXT,
        source_type TEXT,
        added_date TEXT,
        is_active INTEGER DEFAULT 1
    )
    ''')
    
    # Создание таблицы для ключевых слов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT UNIQUE,
        added_date TEXT,
        is_active INTEGER DEFAULT 1
    )
    ''')
    
    # Создание индексов для оптимизации запросов
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_posts_date ON posts(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_posts_channel ON posts(channel_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_comments_date ON comments(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date)')
    
    conn.commit()
    conn.close()
    logger.info("База данных успешно инициализирована")

# Функции для добавления данных в соответствующие таблицы
def add_post(date, channel_name, content, message_id, views=0, forwards=0):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO posts (date, channel_name, content, message_id, views, forwards)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (date, channel_name, content, message_id, views, forwards))
    post_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return post_id

def add_comment(date, channel_name, post_content, comment, user_id, username, post_id=None, sentiment='neutral'):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO comments (date, channel_name, post_content, comment, user_id, username, post_id, sentiment)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (date, channel_name, post_content, comment, user_id, username, post_id, sentiment))
    comment_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return comment_id

def add_message(date, source, content, user_id, username, media_type=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO messages (date, source, content, user_id, username, media_type)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (date, source, content, user_id, username, media_type))
    message_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return message_id

# Функция для добавления отслеживаемого источника
def add_monitored_source(source_id, source_name, source_type):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
        INSERT INTO monitored_sources (source_id, source_name, source_type, added_date)
        VALUES (?, ?, ?, ?)
        ''', (source_id, source_name, source_type, now))
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    finally:
        conn.close()
    return result

# Функция для добавления ключевого слова
def add_keyword(keyword):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
        INSERT INTO keywords (keyword, added_date)
        VALUES (?, ?)
        ''', (keyword.lower(), now))
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    finally:
        conn.close()
    return result

# Функция для получения списка отслеживаемых источников
def get_monitored_sources():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    SELECT source_id, source_name, source_type FROM monitored_sources
    WHERE is_active = 1
    ''')
    sources = cursor.fetchall()
    conn.close()
    return sources

# Функция для получения списка ключевых слов
def get_keywords():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    SELECT keyword FROM keywords
    WHERE is_active = 1
    ''')
    keywords = [row[0] for row in cursor.fetchall()]
    conn.close()
    return keywords

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

# Функция для поиска по контенту
def search_content(query, start_date=None, end_date=None):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    search_query = f"%{query}%"
    
    # Подготовка условий для дат
    date_condition = ""
    params = [search_query]
    
    if start_date and end_date:
        date_condition = "AND date BETWEEN ? AND ?"
        params.extend([start_date, end_date])
    
    # Поиск в постах
    cursor.execute(f'''
    SELECT id, date, channel_name, content, 'post' as type
    FROM posts 
    WHERE content LIKE ? {date_condition}
    ''', params)
    posts = [dict(row) for row in cursor.fetchall()]
    
    # Поиск в комментариях
    cursor.execute(f'''
    SELECT id, date, channel_name, comment as content, 'comment' as type
    FROM comments 
    WHERE comment LIKE ? {date_condition}
    ''', params)
    comments = [dict(row) for row in cursor.fetchall()]
    
    # Поиск в сообщениях
    cursor.execute(f'''
    SELECT id, date, source as channel_name, content, 'message' as type
    FROM messages 
    WHERE content LIKE ? {date_condition}
    ''', params)
    messages = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    # Объединение результатов
    results = posts + comments + messages
    
    # Сортировка по дате
    results.sort(key=lambda x: x['date'], reverse=True)
    
    return results

# Простая функция анализа настроения (можно заменить на более сложную)
def analyze_sentiment(text):
    # Простой анализ на основе ключевых слов
    positive_words = ['хорошо', 'отлично', 'супер', 'класс', 'нравится', 'отличный', 'лучший']
    negative_words = ['плохо', 'ужасно', 'отстой', 'провал', 'недоволен', 'хуже', 'негативный']
    
    text = text.lower()
    
    positive_count = sum(1 for word in positive_words if word in text)
    negative_count = sum(1 for word in negative_words if word in text)
    
    if positive_count > negative_count:
        return 'positive'
    elif negative_count > positive_count:
        return 'negative'
    else:
        return 'neutral'

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
            
            # Определение типа медиа (если есть)
            media_type = None
            if event.message.media:
                if hasattr(event.message.media, 'photo'):
                    media_type = 'photo'
                elif hasattr(event.message.media, 'document'):
                    media_type = 'document'
                elif hasattr(event.message.media, 'video'):
                    media_type = 'video'
                elif hasattr(event.message.media, 'audio'):
                    media_type = 'audio'
            
            # Проверка на наличие ключевых слов
            keywords = get_keywords()
            contains_keyword = any(keyword.lower() in content.lower() for keyword in keywords)
            
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
                    
                    # Анализ настроения комментария
                    sentiment = analyze_sentiment(content)
                    
                    # Это комментарий
                    add_comment(post_date, channel_name, original_post, content, user_id, username, post_id, sentiment)
                    logger.info(f"Добавлен комментарий в канале {channel_name} в {post_date}")
                    
                    # Уведомление, если комментарий содержит ключевое слово
                    if contains_keyword:
                        admin_users = [12345678]  # Список ID администраторов для уведомлений
                        for admin_id in admin_users:
                            try:
                                await bot.send_message(
                                    admin_id,
                                    f"❗️ Обнаружено ключевое слово в комментарии:\n\nКанал: {channel_name}\nДата: {post_date}\nКомментарий: {content[:100]}..."
                                )
                            except Exception as e:
                                logger.error(f"Ошибка при отправке уведомления администратору: {e}")
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке комментария: {e}")
            else:
                # Определяем, является ли это постом канала или сообщением группы
                if event.is_channel and not event.is_group:
                    # Получение количества просмотров и пересылок
                    views = getattr(event.message, 'views', 0)
                    forwards = getattr(event.message, 'forwards', 0)
                    
                    # Это пост канала
                    add_post(post_date, channel_name, content, event.message.id, views, forwards)
                    logger.info(f"Добавлен пост из канала {channel_name} в {post_date}")
                    
                    # Уведомление, если пост содержит ключевое слово
                    if contains_keyword:
                        admin_users = [12345678]  # Список ID администраторов для уведомлений
                        for admin_id in admin_users:
                            try:
                                await bot.send_message(
                                    admin_id,
                                    f"🔔 Обнаружено ключевое слово в посте:\n\nКанал: {channel_name}\nДата: {post_date}\nПост: {content[:100]}..."
                                )
                            except Exception as e:
                                logger.error(f"Ошибка при отправке уведомления администратору: {e}")
                
                elif event.is_group:
                    # Это сообщение группы
                    add_message(post_date, channel_name, content, user_id, username, media_type)
                    logger.info(f"Добавлено сообщение из группы {channel_name} в {post_date}")
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")

# Команда /start
@dp.message(Command("start"))
async def start(message: types.Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Экспорт постов"), KeyboardButton(text="Экспорт комментариев")],
            [KeyboardButton(text="Экспорт сообщений"), KeyboardButton(text="Экспорт всего контента")],
            [KeyboardButton(text="Статистика"), KeyboardButton(text="Поиск контента")],
            [KeyboardButton(text="Управление источниками"), KeyboardButton(text="Ключевые слова")],
            [KeyboardButton(text="Помощь"), KeyboardButton(text="Настройки")]
        ],
        resize_keyboard=True
    )
    await message.answer("👋 Привет! Выберите действие:", reply_markup=keyboard)

# Получение данных из базы данных по типу и временному диапазону
def get_data_by_period(data_type, start_date, end_date):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if data_type == "posts":
        query = '''
        SELECT date, channel_name, content, views, forwards
        FROM posts 
        WHERE date >= ? AND date <= ?
        ORDER BY date
        '''
    elif data_type == "comments":
        query = '''
        SELECT date, channel_name, post_content, comment, user_id, username, sentiment
        FROM comments 
        WHERE date >= ? AND date <= ?
        ORDER BY date
        '''
    elif data_type == "messages":
        query = '''
        SELECT date, source, content, user_id, username, media_type
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
    
    # Статистика по настроению комментариев
    cursor.execute('''
    SELECT sentiment, COUNT(*) as count
    FROM comments
    GROUP BY sentiment
    ORDER BY count DESC
    ''')
    sentiment_stats = cursor.fetchall()
    
    # Статистика по типам медиа
    cursor.execute('''
    SELECT media_type, COUNT(*) as count
    FROM messages
    WHERE media_type IS NOT NULL
    GROUP BY media_type
    ORDER BY count DESC
    ''')
    media_stats = cursor.fetchall()
    
    # Статистика по источникам
    cursor.execute('''
    SELECT source_type, COUNT(*) as count
    FROM monitored_sources
    WHERE is_active = 1
    GROUP BY source_type
    ''')
    source_stats = cursor.fetchall()
    
    conn.close()
    
    days = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
    activity_formatted = [(days[int(day)], count) for day, count in activity_by_day]
    
    sentiment_translation = {
        'positive': 'Положительное',
        'negative': 'Отрицательное',
        'neutral': 'Нейтральное'
    }
    sentiment_formatted = [(sentiment_translation.get(sent, sent), count) for sent, count in sentiment_stats]
    
    media_translation = {
        'photo': 'Фото',
        'video': 'Видео',
        'document': 'Документ',
        'audio': 'Аудио',
        None: 'Текст'
    }
    media_formatted = [(media_translation.get(media, media), count) for media, count in media_stats]
    
    source_translation = {
        'channel': 'Каналы',
        'group': 'Группы',
        'chat': 'Чаты'
    }
    source_formatted = [(source_translation.get(src, src), count) for src, count in source_stats]
    
    return {
        "total_posts": posts_count,
        "total_comments": comments_count,
        "total_messages": messages_count,
        "top_channels": top_channels,
        "activity_by_day": activity_formatted,
        "sentiment_stats": sentiment_formatted,
        "media_stats": media_formatted,
        "source_stats": source_formatted
    }

# Функция для создания графиков
def create_statistics_charts(stats):
    # График активности по дням недели
    plt.figure(figsize=(10, 6))
    days = [day for day, _ in stats['activity_by_day']]
    activity = [count for _, count in stats['activity_by_day']]
    plt.bar(days, activity, color='skyblue')
    plt.title('Активность по дням недели')
    plt.xlabel('День недели')
    plt.ylabel('Количество записей')
    plt.tight_layout()
    activity_chart = f"{TEMP_DIR}/activity_by_day.png"
    plt.savefig(activity_chart)
    plt.close()
    
    # График распределения настроения комментариев
    plt.figure(figsize=(8, 8))
    sentiments = [sent for sent, _ in stats['sentiment_stats']]
    sentiment_counts = [count for _, count in stats['sentiment_stats']]
    plt.pie(sentiment_counts, labels=sentiments, autopct='%1.1f%%', startangle=90, colors=['lightgreen', 'lightcoral', 'lightblue'])
    plt.title('Распределение настроения комментариев')
    plt.axis('equal')
    sentiment_chart = f"{TEMP_DIR}/sentiment_stats.png"
    plt.savefig(sentiment_chart)
    plt.close()
    
    # График типов медиа
    if stats['media_stats']:
        plt.figure(figsize=(10, 6))
        media_types = [media for media, _ in stats['media_stats']]
        media_counts = [count for _, count in stats['media_stats']]
        plt.bar(media_types, media_counts, color='lightgreen')
        plt.title('Типы медиа контента')
        plt.xlabel('Тип')
        plt.ylabel('Количество')
        plt.tight_layout()
        media_chart = f"{TEMP_DIR}/media_stats.png"
        plt.savefig(media_chart)
        plt.close()
    else:
        media_chart = None
    
    return {
        "activity_chart": activity_chart,
        "sentiment_chart": sentiment_chart,
        "media_chart": media_chart
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
    elif period == "three_months":
        start_date = (current_date - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        # По умолчанию одна неделя, если период не распознан
        start_date = (current_date - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    return start_date, end_date

# Continuation of the Excel file creation function
def create_excel_file(data_type, data, filename):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    
    if data_type == "posts":
        sheet.title = "Посты"
        headers = ["Дата", "Название канала", "Содержание", "Просмотры", "Пересылки"]
        sheet.append(headers)
        
        for post in data:
            row_data = [
                post['date'],
                post['channel_name'],
                post['content'],
                post.get('views', 0),
                post.get('forwards', 0)
            ]
            sheet.append(row_data)
    
    elif data_type == "comments":
        sheet.title = "Комментарии"
        headers = ["Дата", "Название канала", "Содержание поста", "Комментарий", "ID пользователя", "Имя пользователя", "Настроение"]
        sheet.append(headers)
        
        for comment in data:
            row_data = [
                comment['date'],
                comment['channel_name'],
                comment['post_content'],
                comment['comment'],
                comment['user_id'],
                comment['username'],
                comment.get('sentiment', 'neutral')
            ]
            sheet.append(row_data)
    
    elif data_type == "messages":
        sheet.title = "Сообщения"
        headers = ["Дата", "Источник", "Содержание", "ID пользователя", "Имя пользователя", "Тип медиа"]
        sheet.append(headers)
        
        for message in data:
            row_data = [
                message['date'],
                message['source'],
                message['content'],
                message['user_id'],
                message['username'],
                message.get('media_type', 'Текст')
            ]
            sheet.append(row_data)
    
    else:  # Все данные - создание нескольких листов
        for content_type, content_data in data.items():
            if content_type == "posts":
                sheet = workbook.active
                sheet.title = "Посты"
                headers = ["Дата", "Название канала", "Содержание", "Просмотры", "Пересылки"]
                sheet.append(headers)
                
                for post in content_data:
                    row_data = [
                        post['date'],
                        post['channel_name'],
                        post['content'],
                        post.get('views', 0),
                        post.get('forwards', 0)
                    ]
                    sheet.append(row_data)
            
            else:
                sheet = workbook.create_sheet(title=content_type.capitalize())
                
                if content_type == "comments":
                    headers = ["Дата", "Название канала", "Содержание поста", "Комментарий", "ID пользователя", "Имя пользователя", "Настроение"]
                    sheet.append(headers)
                    
                    for comment in content_data:
                        row_data = [
                            comment['date'],
                            comment['channel_name'],
                            comment['post_content'],
                            comment['comment'],
                            comment['user_id'],
                            comment['username'],
                            comment.get('sentiment', 'neutral')
                        ]
                        sheet.append(row_data)
                
                elif content_type == "messages":
                    headers = ["Дата", "Источник", "Содержание", "ID пользователя", "Имя пользователя", "Тип медиа"]
                    sheet.append(headers)
                    
                    for message in content_data:
                        row_data = [
                            message['date'],
                            message['source'],
                            message['content'],
                            message['user_id'],
                            message['username'],
                            message.get('media_type', 'Текст')
                        ]
                        sheet.append(row_data)
    
    # Настройка ширины столбцов
    for sheet in workbook.worksheets:
        for col in sheet.columns:
            max_length = 0
            column = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = min(len(str(cell.value)), 100)  # Ограничение ширины столбца
                except:
                    pass
            adjusted_width = (max_length + 2)
            sheet.column_dimensions[column].width = adjusted_width
    
    # Сохранение файла
    workbook.save(filename)
    return filename

# Обработчик для экспорта данных - запрос периода
@dp.message(lambda message: message.text in ["Экспорт постов", "Экспорт комментариев", "Экспорт сообщений", "Экспорт всего контента"])
async def export_request(message: types.Message, state: FSMContext):
    # Сохраняем выбранный тип данных в состоянии
    data_type_mapping = {
        "Экспорт постов": "posts",
        "Экспорт комментариев": "comments",
        "Экспорт сообщений": "messages",
        "Экспорт всего контента": "all"
    }
    data_type = data_type_mapping.get(message.text)
    await state.update_data(data_type=data_type)
    
    # Создаем клавиатуру с выбором периода
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Неделя", callback_data="period_week"),
                InlineKeyboardButton(text="2 недели", callback_data="period_two_weeks")
            ],
            [
                InlineKeyboardButton(text="Месяц", callback_data="period_month"),
                InlineKeyboardButton(text="3 месяца", callback_data="period_three_months")
            ],
            [
                InlineKeyboardButton(text="Указать даты", callback_data="custom_period")
            ]
        ]
    )
    
    await message.answer("Выберите период для экспорта:", reply_markup=keyboard)

# Обработчик выбора периода
@dp.callback_query(lambda c: c.data.startswith('period_'))
async def process_period_selection(callback_query: types.CallbackQuery, state: FSMContext):
    period = callback_query.data.split('_')[1]
    state_data = await state.get_data()
    data_type = state_data.get('data_type')
    
    start_date, end_date = get_date_range(period)
    
    # Получение данных
    data = get_data_by_period(data_type, start_date, end_date)
    
    # Создание имени файла
    file_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"{TEMP_DIR}/{data_type}_{file_suffix}.xlsx"
    
    # Создание Excel файла
    try:
        file_path = create_excel_file(data_type, data, file_name)
        
        # Отправка файла
        excel_file = FSInputFile(file_path)
        await bot.send_document(
            callback_query.from_user.id,
            document=excel_file,
            caption=f"Экспорт данных ({data_type}) за период {start_date} - {end_date}"
        )
        
        # Удаление временного файла
        try:
            os.remove(file_path)
        except:
            pass
        
        await callback_query.answer("Файл успешно экспортирован!")
    except Exception as e:
        logger.error(f"Ошибка при создании файла экспорта: {e}")
        await bot.send_message(
            callback_query.from_user.id,
            f"Произошла ошибка при экспорте данных: {str(e)}"
        )
        await callback_query.answer("Ошибка при экспорте!")

# Обработчик выбора произвольного периода
@dp.callback_query(lambda c: c.data == 'custom_period')
async def process_custom_period(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.send_message(
        callback_query.from_user.id,
        "Введите начальную дату в формате ГГГГ-ММ-ДД:"
    )
    await state.set_state(FormStates.waiting_for_start_date)
    await callback_query.answer()

# Обработчик ввода начальной даты
@dp.message(FormStates.waiting_for_start_date)
async def process_start_date(message: types.Message, state: FSMContext):
    try:
        # Проверка формата даты
        datetime.strptime(message.text, '%Y-%m-%d')
        
        # Сохранение даты
        await state.update_data(start_date=f"{message.text} 00:00:00")
        
        await message.answer("Введите конечную дату в формате ГГГГ-ММ-ДД:")
        await state.set_state(FormStates.waiting_for_end_date)
    except ValueError:
        await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ГГГГ-ММ-ДД (например, 2023-12-31):")

# Обработчик ввода конечной даты
@dp.message(FormStates.waiting_for_end_date)
async def process_end_date(message: types.Message, state: FSMContext):
    try:
        # Проверка формата даты
        datetime.strptime(message.text, '%Y-%m-%d')
        
        # Сохранение даты
        await state.update_data(end_date=f"{message.text} 23:59:59")
        
        # Получение всех данных из состояния
        state_data = await state.get_data()
        data_type = state_data.get('data_type')
        start_date = state_data.get('start_date')
        end_date = state_data.get('end_date')
        
        # Получение данных
        data = get_data_by_period(data_type, start_date, end_date)
        
        # Создание имени файла
        file_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"{TEMP_DIR}/{data_type}_{file_suffix}.xlsx"
        
        # Создание Excel файла
        try:
            file_path = create_excel_file(data_type, data, file_name)
            
            # Отправка файла
            excel_file = FSInputFile(file_path)
            await message.answer_document(
                document=excel_file,
                caption=f"Экспорт данных ({data_type}) за период {start_date} - {end_date}"
            )
            
            # Удаление временного файла
            try:
                os.remove(file_path)
            except:
                pass
            
            # Сброс состояния
            await state.clear()
        except Exception as e:
            logger.error(f"Ошибка при создании файла экспорта: {e}")
            await message.answer(f"Произошла ошибка при экспорте данных: {str(e)}")
    except ValueError:
        await message.answer("Неверный формат даты. Пожалуйста, введите дату в формате ГГГГ-ММ-ДД (например, 2023-12-31):")

# Обработчик для просмотра статистики
@dp.message(lambda message: message.text == "Статистика")
async def show_statistics(message: types.Message):
    try:
        # Получение статистики
        stats = get_statistics()
        
        # Создание графиков
        charts = create_statistics_charts(stats)
        
        # Формирование текста статистики
        stats_text = "📊 **Статистика сбора данных:**\n\n"
        stats_text += f"📝 Всего постов: {stats['total_posts']}\n"
        stats_text += f"💬 Всего комментариев: {stats['total_comments']}\n"
        stats_text += f"📱 Всего сообщений: {stats['total_messages']}\n\n"
        
        stats_text += "📈 **Топ каналов:**\n"
        for i, (channel, count) in enumerate(stats['top_channels'], 1):
            stats_text += f"{i}. {channel}: {count} постов\n"
        
        stats_text += "\n🗣 **Настроение комментариев:**\n"
        for sentiment, count in stats['sentiment_stats']:
            stats_text += f"{sentiment}: {count}\n"
        
        # Отправка текстовой статистики
        await message.answer(stats_text)
        
        # Отправка графиков
        await message.answer_photo(FSInputFile(charts["activity_chart"]), caption="Активность по дням недели")
        await message.answer_photo(FSInputFile(charts["sentiment_chart"]), caption="Распределение настроения комментариев")
        
        if charts.get("media_chart"):
            await message.answer_photo(FSInputFile(charts["media_chart"]), caption="Типы медиа контента")
        
        # Удаление временных файлов
        for chart_path in charts.values():
            if chart_path:
                try:
                    os.remove(chart_path)
                except:
                    pass
    except Exception as e:
        logger.error(f"Ошибка при показе статистики: {e}")
        await message.answer(f"Произошла ошибка при получении статистики: {str(e)}")

# Обработчик для поиска контента - запрос поискового запроса
@dp.message(lambda message: message.text == "Поиск контента")
async def search_request(message: types.Message, state: FSMContext):
    await message.answer("Введите поисковый запрос:")
    await state.set_state(FormStates.waiting_for_search_query)

# Обработчик ввода поискового запроса
@dp.message(FormStates.waiting_for_search_query)
async def process_search_query(message: types.Message, state: FSMContext):
    query = message.text
    await state.update_data(search_query=query)
    
    # Создаем клавиатуру с выбором периода
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Неделя", callback_data="search_week"),
                InlineKeyboardButton(text="2 недели", callback_data="search_two_weeks")
            ],
            [
                InlineKeyboardButton(text="Месяц", callback_data="search_month"),
                InlineKeyboardButton(text="3 месяца", callback_data="search_three_months")
            ],
            [
                InlineKeyboardButton(text="Все время", callback_data="search_all")
            ]
        ]
    )
    
    await message.answer("Выберите период для поиска:", reply_markup=keyboard)

# Обработчик выбора периода для поиска
@dp.callback_query(lambda c: c.data.startswith('search_'))
async def process_search_period(callback_query: types.CallbackQuery, state: FSMContext):
    period = callback_query.data.split('_')[1]
    state_data = await state.get_data()
    query = state_data.get('search_query')
    
    if period == "all":
        # Поиск без ограничения по дате
        results = search_content(query)
    else:
        start_date, end_date = get_date_range(period)
        results = search_content(query, start_date, end_date)
    
    if results:
        # Формирование сообщения с результатами
        result_text = f"🔍 Результаты поиска по запросу '{query}':\n\n"
        
        # Ограничение количества результатов для показа
        show_limit = min(20, len(results))
        for i, result in enumerate(results[:show_limit], 1):
            result_type = result.get('type', 'unknown')
            content = result.get('content', '')[:100] + '...' if len(result.get('content', '')) > 100 else result.get('content', '')
            source = result.get('channel_name', '')
            date = result.get('date', '')
            
            if result_type == 'post':
                emoji = "📝"
            elif result_type == 'comment':
                emoji = "💬"
            elif result_type == 'message':
                emoji = "📱"
            else:
                emoji = "📄"
            
            result_text += f"{i}. {emoji} **{result_type.capitalize()}** от {date}\n"
            result_text += f"Источник: {source}\n"
            result_text += f"Текст: {content}\n\n"
        
        if len(results) > show_limit:
            result_text += f"\n... и еще {len(results) - show_limit} результатов."
            
            # Предложение экспорта результатов
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Экспортировать все результаты", callback_data=f"export_search_{query}")
                    ]
                ]
            )
            await bot.send_message(
                callback_query.from_user.id,
                result_text,
                reply_markup=keyboard
            )
        else:
            await bot.send_message(
                callback_query.from_user.id,
                result_text
            )
    else:
        await bot.send_message(
            callback_query.from_user.id,
            f"По запросу '{query}' ничего не найдено."
        )
    
    # Сброс состояния
    await state.clear()
    await callback_query.answer()

# Обработчик запроса на экспорт результатов поиска
@dp.callback_query(lambda c: c.data.startswith('export_search_'))
async def export_search_results(callback_query: types.CallbackQuery):
    query = callback_query.data.replace('export_search_', '')
    
    # Получение всех результатов
    results = search_content(query)
    
    if results:
        # Подготовка данных для экспорта
        posts_data = [r for r in results if r.get('type') == 'post']
        comments_data = [r for r in results if r.get('type') == 'comment']
        messages_data = [r for r in results if r.get('type') == 'message']
        
        # Структурирование данных для функции экспорта
        export_data = {}
        if posts_data:
            export_data['posts'] = posts_data
        if comments_data:
            export_data['comments'] = comments_data
        if messages_data:
            export_data['messages'] = messages_data
        
        # Создание имени файла
        file_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"{TEMP_DIR}/search_{file_suffix}.xlsx"
        
        # Создание Excel файла
        try:
            file_path = create_excel_file("all", export_data, file_name)
            
            # Отправка файла
            excel_file = FSInputFile(file_path)
            await bot.send_document(
                callback_query.from_user.id,
                document=excel_file,
                caption=f"Результаты поиска по запросу '{query}'"
            )
            
            # Удаление временного файла
            try:
                os.remove(file_path)
            except:
                pass
            
            await callback_query.answer("Файл успешно экспортирован!")
        except Exception as e:
            logger.error(f"Ошибка при создании файла экспорта: {e}")
            await bot.send_message(
                callback_query.from_user.id,
                f"Произошла ошибка при экспорте результатов поиска: {str(e)}"
            )
            await callback_query.answer("Ошибка при экспорте!")
    else:
        await bot.send_message(
            callback_query.from_user.id,
            f"По запросу '{query}' ничего не найдено."
        )
        await callback_query.answer()

# Обработчик для управления источниками
@dp.message(lambda message: message.text == "Управление источниками")
async def manage_sources(message: types.Message):
    # Создаем клавиатуру с действиями
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Добавить источник", callback_data="add_source")
            ],
            [
                InlineKeyboardButton(text="Список источников", callback_data="list_sources")
            ]
        ]
    )
    
    await message.answer("Выберите действие для управления источниками:", reply_markup=keyboard)

# Обработчик запроса на добавление источника
@dp.callback_query(lambda c: c.data == 'add_source')
async def add_source_request(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.send_message(
        callback_query.from_user.id,
        "Введите название канала или группы (без @):"
    )
    await state.set_state(FormStates.waiting_for_channel_name)
    await callback_query.answer()

# Обработчик ввода имени канала
@dp.message(FormStates.waiting_for_channel_name)
async def process_channel_name(message: types.Message, state: FSMContext):
    channel_name = message.text.strip()
    
    try:
        # Попытка получить информацию о канале/группе
        entity = await client.get_entity(channel_name)
        
        # Определение типа источника
        if hasattr(entity, 'megagroup') and entity.megagroup:
            source_type = "group"
        elif hasattr(entity, 'broadcast') and entity.broadcast:
            source_type = "channel"
        else:
            source_type = "chat"
        
        # Добавление источника в базу данных
        success = add_monitored_source(entity.id, entity.title, source_type)
        
        if success:
            await message.answer(f"Источник '{entity.title}' успешно добавлен для мониторинга!")
        else:
            await message.answer(f"Источник '{entity.title}' уже отслеживается.")
        
        # Сброс состояния
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при добавлении источника: {e}")
        await message.answer(f"Не удалось найти или добавить источник. Убедитесь, что имя введено правильно и бот имеет доступ к этому каналу/группе.")

# Обработчик запроса списка источников
@dp.callback_query(lambda c: c.data == 'list_sources')
async def list_sources(callback_query: types.CallbackQuery):
    sources = get_monitored_sources()
    
    if sources:
        source_text = "📋 **Список отслеживаемых источников:**\n\n"
        
        for i, (source_id, source_name, source_type) in enumerate(sources, 1):
            if source_type == "channel":
                emoji = "📢"
            elif source_type == "group":
                emoji = "👥"
            else:
                emoji = "💬"
            
            source_text += f"{i}. {emoji} **{source_name}** (ID: {source_id}, Тип: {source_type})\n"
        
        await bot.send_message(
            callback_query.from_user.id,
            source_text
        )
    else:
        await bot.send_message(
            callback_query.from_user.id,
            "Нет отслеживаемых источников."
        )
    
    await callback_query.answer()

# Обработчик для управления ключевыми словами
@dp.message(lambda message: message.text == "Ключевые слова")
async def manage_keywords(message: types.Message):
    # Создаем клавиатуру с действиями
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Добавить ключевое слово", callback_data="add_keyword")
            ],
            [
                InlineKeyboardButton(text="Список ключевых слов", callback_data="list_keywords")
            ]
        ]
    )
    
    await message.answer("Выберите действие для управления ключевыми словами:", reply_markup=keyboard)

# Обработчик запроса на добавление ключевого слова
@dp.callback_query(lambda c: c.data == 'add_keyword')
async def add_keyword_request(callback_query: types.CallbackQuery):
    await bot.send_message(
        callback_query.from_user.id,
        "Введите ключевое слово для отслеживания:"
    )
    
    # Создание обработчика для следующего сообщения от этого пользователя
    @dp.message(lambda message: message.from_user.id == callback_query.from_user.id)
    async def process_keyword(message: types.Message):
        keyword = message.text.strip()
        
        if keyword:
            success = add_keyword(keyword)
            
            if success:
                await message.answer(f"Ключевое слово '{keyword}' успешно добавлено для отслеживания!")
            else:
                await message.answer(f"Ключевое слово '{keyword}' уже отслеживается.")
        else:
            await message.answer("Ключевое слово не может быть пустым.")
    
    await callback_query.answer()

# Обработчик запроса списка ключевых слов
@dp.callback_query(lambda c: c.data == 'list_keywords')
async def list_keywords(callback_query: types.CallbackQuery):
    keywords = get_keywords()
    
    if keywords:
        keyword_text = "🔑 **Список отслеживаемых ключевых слов:**\n\n"
        
        for i, keyword in enumerate(keywords, 1):
            keyword_text += f"{i}. **{keyword}**\n"
        
        await bot.send_message(
            callback_query.from_user.id,
            keyword_text
        )
    else:
        await bot.send_message(
            callback_query.from_user.id,
            "Нет отслеживаемых ключевых слов."
        )
    
    await callback_query.answer()

# Обработчик для команды "Помощь"
@dp.message(lambda message: message.text == "Помощь")
async def show_help(message: types.Message):
    help_text = """
📚 **Справка по использованию бота:**

**Основные функции:**
- **Экспорт постов** - выгрузка постов из отслеживаемых каналов
- **Экспорт комментариев** - выгрузка комментариев к постам
- **Экспорт сообщений** - выгрузка сообщений из групп
- **Экспорт всего контента** - полный экспорт данных

**Аналитика:**
- **Статистика** - просмотр основных показателей и графиков
- **Поиск контента** - поиск по всем собранным данным

**Управление:**
- **Управление источниками** - добавление и просмотр каналов/групп для мониторинга
- **Ключевые слова** - настройка отслеживаемых ключевых слов

**Как это работает:**
Бот собирает данные из указанных источников, сохраняет их в базу данных и позволяет экспортировать или анализировать информацию. При обнаружении ключевых слов в контенте бот отправляет уведомления администраторам.

**Примечание:** Для корректной работы бот должен быть участником отслеживаемых каналов/групп.
"""
    await message.answer(help_text)

# Обработчик для команды "Настройки"
@dp.message(lambda message: message.text == "Настройки")
async def show_settings(message: types.Message):
    settings_text = """
⚙️ **Настройки бота:**

В данный момент доступны следующие настройки:

1. **Управление источниками** - добавление и удаление отслеживаемых каналов/групп
2. **Ключевые слова** - настройка списка отслеживаемых слов и фраз

Для изменения настроек выберите соответствующий пункт в главном меню.

**Администрирование:**
Для получения расширенных прав администратора, свяжитесь с владельцем бота.
"""
    await message.answer(settings_text)

# Запуск бота
async def main():
    # Инициализация базы данных
    initialize_database()
    
    # Запуск Telethon клиента
    await client.start()
    
    # Запуск aiogram диспетчера
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
