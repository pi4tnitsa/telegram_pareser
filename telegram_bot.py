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

# Функции для создания Excel файлов
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
