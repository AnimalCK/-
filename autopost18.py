import time
import random
import dns.resolver
from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
import asyncio
import os
import re
import logging
import json
from datetime import datetime, timezone, timedelta
import httpx
import sqlite3
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import defaultdict
from deep_translator import GoogleTranslator
import hashlib
import langdetect
from openai import AsyncOpenAI
from prometheus_client import start_http_server, Counter, Gauge

# Настройка DNS резолвера
dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers = ['8.8.8.8', '1.1.1.1']  # Google и Cloudflare DNS

# Настройка парсера HTML
try:
    import lxml
    DEFAULT_PARSER = "lxml"
except ImportError:
    DEFAULT_PARSER = "html.parser"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("autopost.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных из .env
load_dotenv()

# Конфигурация
BOT_TOKEN_COLLECTOR = os.getenv("BOT_TOKEN_COLLECTOR")
BOT_TOKEN_PUBLISHER = os.getenv("BOT_TOKEN_PUBLISHER")
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
CHANNEL = os.getenv("CHANNEL")
PROXY_HTTP = os.getenv("PROXY_HTTP", "").strip()
PROXY_HTTPS = os.getenv("PROXY_HTTPS", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Проверка обязательных переменных
required_vars = [
    "BOT_TOKEN_COLLECTOR", 
    "BOT_TOKEN_PUBLISHER", 
    "API_ID", 
    "API_HASH", 
    "CHANNEL", 
    "ADMIN_CHAT_ID"
]

missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    logger.error(f"Не все обязательные переменные установлены! Отсутствуют: {', '.join(missing)}")
    exit(1)

# Конфигурация источников
SOURCES_CONFIG = [
    {
        "name": "Forbes Россия",
        "url": "https://www.forbes.ru/finansy/",
        "rss_url": "https://www.forbes.ru/newrss.xml",
        "category": "финансы",
        "tags": ["экономика", "бизнес", "рынки"],
        "lang": "ru",
        "trust_score": 9
    },
    {
        "name": "Investing.com (Russian)",
        "url": "https://ru.investing.com/",
        "rss_url": "https://ru.investing.com/rss/news.rss",
        "category": "инвестиции",
        "tags": ["акции", "облигации", "трейдинг"],
        "lang": "ru",
        "trust_score": 8
    },
    {
        "name": "CoinDesk (Russian)",
        "url": "https://www.coindesk.com/ru/",
        "rss_url": "https://www.coindesk.com/ru/arc/outboundfeeds/rss/?outputType=xml",
        "category": "криптовалюты",
        "tags": ["биткоин", "блокчейн", "NFT"],
        "lang": "ru",
        "trust_score": 7
    },
    {
        "name": "РБК",
        "url": "https://www.rbc.ru/",
        "rss_url": "https://rssexport.rbc.ru/rbcnews/news/30/full.rss",
        "category": "экономика",
        "tags": ["бизнес", "рынки", "финансы"],
        "lang": "ru",
        "trust_score": 8
    },
    {
        "name": "Коммерсантъ",
        "url": "https://www.kommersant.ru/",
        "rss_url": "https://www.kommersant.ru/RSS/news.xml",
        "category": "экономика",
        "tags": ["политика", "бизнес", "финансы"],
        "lang": "ru",
        "trust_score": 9
    }
]

# Инициализация базы данных
class NewsDatabase:
    def __init__(self, db_path="news_bot.db"):
        self.db_path = db_path
        self.conn = None
        self.initialize_db()
        
    def initialize_db(self):
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Исправленные SQL-запросы:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posted_links (
                url TEXT PRIMARY KEY,
                timestamp DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS content_hashes (
                hash TEXT PRIMARY KEY,
                timestamp DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS title_hashes (
                hash TEXT PRIMARY KEY,
                timestamp DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                title TEXT,
                category TEXT,
                tags TEXT,
                importance INTEGER,
                source TEXT,
                content_hash TEXT,
                title_hash TEXT,
                timestamp DATETIME,
                summary TEXT,
                trust_score INTEGER,
                engagement REAL DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS translation_cache (
                source_text TEXT PRIMARY KEY,
                translated_text TEXT,
                timestamp DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                source TEXT,
                added_by INTEGER,
                timestamp DATETIME,
                priority INTEGER DEFAULT 1
            )
        ''')
        
        self.conn.commit()
    
    def add_posted_link(self, url):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO posted_links (url, timestamp)
            VALUES (?, ?)
        ''', (url, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
    
    def is_link_posted(self, url):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM posted_links WHERE url = ?", (url,))
        return cursor.fetchone() is not None
    
    def add_content_hash(self, content_hash):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO content_hashes (hash, timestamp)
            VALUES (?, ?)
        ''', (content_hash, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
    
    def is_content_hash_exists(self, content_hash):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM content_hashes WHERE hash = ?", (content_hash,))
        return cursor.fetchone() is not None
    
    def add_title_hash(self, title_hash):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO title_hashes (hash, timestamp)
            VALUES (?, ?)
        ''', (title_hash, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
    
    # Исправленный метод:
    def is_title_hash_recent(self, title_hash, days=3):
        cursor = self.conn.cursor()
        time_threshold = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cursor.execute('''
            SELECT 1 FROM title_hashes 
            WHERE hash = ? AND timestamp > ?
        ''', (title_hash, time_threshold))
        return cursor.fetchone() is not None
    
    def add_analytics(self, analytics_data):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO analytics (
                url, title, category, tags, importance, source, 
                content_hash, title_hash, timestamp, summary, trust_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            analytics_data['url'],
            analytics_data['title'],
            analytics_data['category'],
            json.dumps(analytics_data['tags']),
            analytics_data['importance_level'],
            analytics_data['source'],
            analytics_data['content_hash'],
            analytics_data['title_hash'],
            analytics_data['timestamp'],
            analytics_data.get('summary', ''),
            analytics_data.get('trust_score', 5)
        ))
        self.conn.commit()
    
    def get_recent_analytics(self, hours=12):
        cursor = self.conn.cursor()
        time_threshold = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cursor.execute('''
            SELECT * FROM analytics 
            WHERE timestamp > ?
        ''', (time_threshold,))
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_category_stats(self, hours=12):
        cursor = self.conn.cursor()
        time_threshold = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cursor.execute('''
            SELECT category, COUNT(*) as count 
            FROM analytics 
            WHERE timestamp > ?
            GROUP BY category
        ''', (time_threshold,))
        return {row[0]: row[1] for row in cursor.fetchall()}
    
    def get_keyword_frequency(self, hours=12, limit=5):
        cursor = self.conn.cursor()
        time_threshold = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cursor.execute('''
            SELECT value, COUNT(*) as count
            FROM analytics, json_each(tags)
            WHERE timestamp > ?
            GROUP BY value
            ORDER BY count DESC
            LIMIT ?
        ''', (time_threshold, limit))
        return cursor.fetchall()
    
    def add_pending_link(self, url, source, added_by=None, priority=1):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO pending_links (url, source, added_by, timestamp, priority)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            url, 
            source, 
            added_by, 
            datetime.now(timezone.utc).isoformat(),
            priority
        ))
        self.conn.commit()
        return cursor.lastrowid
    
    def get_pending_links(self, limit=15, priority_threshold=0):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM pending_links 
            WHERE priority >= ?
            ORDER BY priority DESC, timestamp ASC
            LIMIT ?
        ''', (priority_threshold, limit))
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def remove_pending_link(self, link_id):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM pending_links WHERE id = ?", (link_id,))
        self.conn.commit()
    
    def clear_pending_links(self):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM pending_links")
        self.conn.commit()
    
    def get_translation(self, source_text):
        cursor = self.conn.cursor()
        cursor.execute("SELECT translated_text FROM translation_cache WHERE source_text = ?", (source_text,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def add_translation(self, source_text, translated_text):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO translation_cache (source_text, translated_text, timestamp)
            VALUES (?, ?, ?)
        ''', (source_text, translated_text, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()
    
    def update_engagement(self, url, engagement_score):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE analytics 
            SET engagement = engagement + ?
            WHERE url = ?
        ''', (engagement_score, url))
        self.conn.commit()

# Инициализация базы данных
db = NewsDatabase()

# Инициализация Prometheus метрик
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests')
PARSE_SUCCESS = Counter('parse_success_total', 'Successful article parses')
PARSE_FAILURE = Counter('parse_failure_total', 'Failed article parses')
PUBLISH_COUNT = Counter('publish_count_total', 'Total published articles')
QUEUE_SIZE = Gauge('pending_queue_size', 'Current pending links queue size')

# Запуск сервера метрик
start_http_server(8000)

# ================== УТИЛИТЫ И ОБЩИЕ ФУНКЦИИ ==================

def escape_markdown(text):
    """Экранирование специальных символов Markdown"""
    if not text:
        return text
        
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def is_valid_url(url):
    """Проверка валидности URL"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except:
        return False

def validate_proxy_url(proxy_url):
    """Проверка валидности URL прокси"""
    if not proxy_url:
        return False
    
    try:
        parsed = urlparse(proxy_url)
        if not parsed.scheme or not parsed.netloc:
            return False
            
        if parsed.scheme not in ('http', 'https', 'socks5'):
            return False
            
        return True
    except Exception:
        return False

# Настройка прокси
PROXIES = {}
if validate_proxy_url(PROXY_HTTP):
    PROXIES['http://'] = PROXY_HTTP
if validate_proxy_url(PROXY_HTTPS):
    PROXIES['https://'] = PROXY_HTTPS

if PROXIES:
    logger.info(f"Используются прокси: {PROXIES}")
else:
    logger.info("Прокси не настроены или невалидны, работаем без них")

# Список User-Agent для ротации
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
]

# Список рефереров
REFERERS = [
    "https://www.google.com/",
    "https://yandex.ru/",
    "https://duckduckgo.com/",
    "https://www.bing.com/",
    "https://www.facebook.com/",
    "https://twitter.com/",
]

async def create_async_session():
    """Создание асинхронной сессии с настройками для обхода блокировок"""
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    timeout = httpx.Timeout(30.0, connect=10.0)
    
    # Создаем базовую конфигурацию клиента
    client_params = {
        'limits': limits,
        'timeout': timeout,
        'follow_redirects': True,
        'http2': True,
        'headers': {
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
    }
    
    # Добавляем прокси если они настроены
    if PROXIES:
        client_params['proxies'] = PROXIES
    
    return httpx.AsyncClient(**client_params)

def detect_language(text):
    """Определение языка текста"""
    try:
        return langdetect.detect(text) if text else 'en'
    except:
        return 'en'

async def translate_to_russian(text, source_lang='auto'):
    """Перевод текста на русский язык с кэшированием"""
    if not text or len(text) < 10:
        return text
    
    # Проверка наличия перевода в кэше
    cached = db.get_translation(text)
    if cached:
        return cached
    
    try:
        # Автоопределение языка
        if source_lang == 'auto':
            source_lang = detect_language(text)
            
        if source_lang == 'ru':
            return text
            
        text = text[:3000]
        
        translator = GoogleTranslator(source=source_lang, target='ru')
        translated = translator.translate(text)
        
        # Сохранение в кэш
        db.add_translation(text, translated)
        return translated
    except Exception as e:
        logger.error(f"Ошибка перевода: {e}")
        return text

def extract_keywords(text, num_keywords=5):
    """Извлечение ключевых слов из текста"""
    try:
        stop_words = set(stopwords.words('russian') + stopwords.words('english'))
        words = word_tokenize(text.lower())
        words = [word for word in words if word.isalnum() and word not in stop_words]
        
        freq_dist = nltk.FreqDist(words)
        return [word for word, _ in freq_dist.most_common(num_keywords)]
    except Exception as e:
        logger.error(f"Ошибка извлечения ключевых слов: {e}")
        return []

def analyze_importance(title, content):
    """Анализ важности контента"""
    try:
        urgent_keywords = ["срочно", "срочная", "кризис", "падение", "рост", "авария", "чп", "urgent", "crisis", "crash"]
        important_keywords = ["важно", "изменение", "закон", "решение", "итоги", "important", "decision", "law"]
        
        text = (title + " " + content).lower()
        
        if any(keyword in text for keyword in urgent_keywords):
            return "🔥 СРОЧНО", 3
        elif any(keyword in text for keyword in important_keywords):
            return "❗ ВАЖНО", 2
        else:
            return "📌 ИНФОРМАЦИЯ", 1
    except Exception as e:
        logger.error(f"Ошибка анализа важности: {e}")
        return "📌 ИНФОРМАЦИЯ", 1

def clean_title(title):
    """Очистка заголовка от мусорных элементов"""
    if not title:
        return title
    
    patterns = [
        r'\s*-\s*Forbes\s*Russia$',
        r'\s*-\s*Инвестиции$',
        r'\s*-\s*CoinDesk$',
        r'\s*:\s*Главные новости$',
        r'\s*\|.*$',
        r'\s*—.*$',
        r'\s*–.*$'
    ]
    
    for pattern in patterns:
        title = re.sub(pattern, "", title, flags=re.IGNORECASE)
    
    return escape_markdown(title.strip())

def clean_content(content, source_name):
    """Очистка контента от мусорных фраз и ненужных элементов"""
    if not content:
        return content
    
    # Удаление крипто-котировок
    crypto_pattern = r'\b(?:[A-Z]{2,5}\s*\$\s*[\d,]+\.\d{2}\s*[+-]\s*\d+\.\d{2}\s*%\b\s*)+'
    content = re.sub(crypto_pattern, "", content, flags=re.IGNORECASE)
    
    # Удаление временных меток
    time_patterns = [
        r'\d{1,2}\s*(час[а]?|ч)\s*назад',
        r'\d{1,2}\s*(минут[ы]?|мин)\s*назад',
        r'\d{1,2}\s*(день|дня|дней)\s*назад',
        r'вчера в \d{1,2}:\d{2}',
        r'сегодня в \d{1,2}:\d{2}',
        r'\d{1,2}\s*[а-я]+\s*назад'
    ]
    
    # Удаление мусорных фраз
    junk_phrases = [
        "Режим для слабовидящих",
        "Запустить прослушивание",
        "Теперь Forbes можно слушать",
        "Попробовать",
        "Поделиться",
        "В избранное",
        "Карьера и свой бизнес",
        "Читать далее",
        "Комментарии",
        "Источник:",
        "Фото:",
        "Видео:",
        "Следите за новостями",
        "Подпишитесь на",
        "Реклама",
        "Партнерский материал"
    ]
    
    # Удаление рекламных блоков
    ads_patterns = [
        r"ADVERTISEMENT",
        r"Реклам[аы]",
        r"Партнерский материал",
        r"Спонсор проекта",
        r"Advertise with us"
    ]
    
    # Удаление соц. сетей
    social_patterns = [
        r"Поделиться в [А-Яа-яA-Za-z]+",
        r"Share on [A-Za-z]+",
        r"Like us on [A-Za-z]+",
        r"Следите за нами в [А-Яа-я]+"
    ]
    
    # Специфичные для источников паттерны
    source_specific = {
        "Forbes Россия": [
            r"Теги:.*",
            r"Автор:.*",
            r"Фото:.*"
        ],
        "Investing.com": [
            r"Читать дальше",
            r"По теме:.*",
            r"Связанные рынки.*"
        ],
        "CoinDesk": [
            r"Subscribe to.*",
            r"Follow us on.*",
            r"Disclaimer:.*"
        ],
        "РБК": [
            r"Читайте также",
            r"Автор текста:",
            r"Фото:"
        ],
        "Коммерсантъ": [
            r"Подробнее читайте",
            r"Фото:",
            r"Источник изображения:"
        ]
    }
    
    # Применение всех фильтров
    for pattern in time_patterns + ads_patterns + social_patterns:
        content = re.sub(pattern, "", content, flags=re.IGNORECASE)
    
    for phrase in junk_phrases:
        content = content.replace(phrase, "")
    
    for source, patterns in source_specific.items():
        if source_name in source:
            for pattern in patterns:
                content = re.sub(pattern, "", content, flags=re.IGNORECASE)
    
    # Финальная очистка
    content = re.sub(r"^\s*[\dА-Яа-я]+\s*назад\W*", "", content)
    content = re.sub(r'(?:\s*\n\s*){2,}', '\n\n', content)
    content = re.sub(r'\s+', ' ', content).strip()
    
    return escape_markdown(content)

def extract_main_content(soup):
    """Улучшенное извлечение основного контента статьи"""
    content_selectors = [
        {'selector': 'article', 'attribute': 'text'},
        {'selector': 'div.post-content', 'attribute': 'text'},
        {'selector': 'div.article-content', 'attribute': 'text'},
        {'selector': 'div.content', 'attribute': 'text'},
        {'selector': 'div.entry-content', 'attribute': 'text'},
        {'selector': 'div.story-block', 'attribute': 'text'},
        {'selector': 'main', 'attribute': 'text'},
        {'selector': '[itemprop="articleBody"]', 'attribute': 'text'},
        {'selector': '.article-body', 'attribute': 'text'},
        {'selector': '.post-body', 'attribute': 'text'},
        {'selector': '.text', 'attribute': 'text'},
        {'selector': '.content-area', 'attribute': 'text'},
        {'selector': 'div.article__text', 'attribute': 'text'},  # РБК
        {'selector': 'div.article-text', 'attribute': 'text'},   # Коммерсант
    ]
    
    for selector in content_selectors:
        element = soup.select_one(selector['selector'])
        if element:
            if selector['attribute'] == 'text':
                content = element.get_text(separator='\n', strip=True)
            else:
                content = element.get(selector['attribute'], '')
            if content and len(content) > 300:
                return content
    
    return extract_content_algorithmically(soup)

def extract_content_algorithmically(soup):
    """Алгоритмическое извлечение контента"""
    # Удаляем ненужные элементы
    for element in soup(['script', 'style', 'header', 'footer', 'aside', 'nav']):
        element.decompose()
    
    # Ищем элемент с наибольшим количеством текста
    body = soup.body
    if body:
        # Вычисляем плотность текста для каждого элемента
        elements = body.find_all(True)
        best_element = None
        best_density = 0
        
        for element in elements:
            text = element.get_text()
            text_length = len(text)
            html_length = len(str(element))
            
            if html_length > 0:
                density = text_length / html_length
                if density > best_density and text_length > 200:
                    best_density = density
                    best_element = element
        
        if best_element:
            return best_element.get_text(separator='\n', strip=True)
    
    # Фолбэк: собираем все параграфы
    paragraphs = soup.find_all('p')
    texts = []
    for p in paragraphs:
        text = p.get_text(strip=True)
        if text:
            texts.append(text)
    return '\n\n'.join(texts)

async def generate_summary(content):
    """Генерация краткого содержания с помощью OpenAI"""
    if not OPENAI_API_KEY or not content:
        return ""
    
    try:
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[
                {
                    "role": "system",
                    "content": "Ты опытный редактор финансовых новостей. Сделай краткую выжимку из текста, выделив самое важное. Ответ должен быть на русском языке."
                },
                {
                    "role": "user",
                    "content": f"Сгенерируй краткое содержание длиной не более 200 символов:\n\n{content[:10000]}"
                }
            ],
            max_tokens=150,
            temperature=0.5
        )
        
        summary = response.choices[0].message.content.strip()
        return f"\n\n📌 **Кратко:** {summary}"
    except Exception as e:
        logger.error(f"Ошибка генерации краткого содержания: {e}")
        return ""

async def parse_article(url, source_config):
    """Парсинг контента статьи с переводом на русский"""
    try:
        REQUEST_COUNT.inc()
        
        # Проверка URL статьи
        if not is_valid_url(url):
            logger.error(f"Неверный URL статьи: {url}")
            return None
            
        async with await create_async_session() as session:
            # Случайные заголовки для каждого запроса
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': random.choice(REFERERS),
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Случайная задержка для имитации поведения человека
            delay = random.uniform(1.0, 3.0)
            await asyncio.sleep(delay)
            
            try:
                response = await session.get(url, headers=headers)
                response.raise_for_status()
            except Exception as e:
                logger.warning(f"Ошибка при запросе ({url}): {e}")
                return None
            
            # Парсинг контента
            soup = BeautifulSoup(response.content, DEFAULT_PARSER)
            
            # Извлечение заголовка с очисткой
            title = soup.title.string.strip() if soup.title else "Без заголовка"
            title = clean_title(title)
            
            # Извлечение основного контента
            article_content = extract_main_content(soup)
            
            # Если контент слишком короткий, пробуем альтернативные методы
            if len(article_content or "") < 100:
                logger.warning(f"Контент слишком короткий: {url}")
                PARSE_FAILURE.inc()
                return None
                
            # Применяем очистку контента
            article_content = clean_content(article_content, source_config['name'])
            
            # Перевод на русский если источник не русскоязычный
            if source_config.get('lang', 'en') != 'ru':
                title = await translate_to_russian(title)
                article_content = await translate_to_russian(article_content)
            
            # Извлечение ключевых слов
            keywords = extract_keywords(f"{title} {article_content}")
            
            # Определение важности
            importance, importance_level = analyze_importance(title, article_content)
            
            # Формирование тегов
            tags = list(set(source_config['tags'] + keywords))[:5]
            
            # Хеши для предотвращения повторов
            title_hash = hashlib.md5(title.encode('utf-8')).hexdigest()
            content_hash = hashlib.md5(article_content[:3000].encode('utf-8')).hexdigest()
            
            # Генерация краткого содержания
            summary = await generate_summary(article_content) if OPENAI_API_KEY else ""
            
            PARSE_SUCCESS.inc()
            return {
                'title': title,
                'content': article_content,
                'url': url,
                'importance': importance,
                'importance_level': importance_level,
                'category': source_config['category'],
                'tags': tags,
                'source': source_config['name'],
                'content_hash': content_hash,
                'title_hash': title_hash,
                'trust_score': source_config.get('trust_score', 5),
                'publish_date': datetime.now(timezone.utc),
                'summary': summary
            }
    except Exception as e:
        logger.error(f"Ошибка парсинга статьи {url}: {type(e).__name__} - {str(e)}", exc_info=True)
        PARSE_FAILURE.inc()
        return None

def format_post(article):
    """Форматирование поста для Telegram"""
    try:
        max_length = 3000
        if len(article['content']) > max_length:
            content = article['content'][:max_length] + "..."
        else:
            content = article['content']
        
        tags_str = " ".join([f"#{tag.replace(' ', '_')}" for tag in article['tags']])
        
        # Добавление оценки доверия
        trust_emoji = "🔹"
        if article['trust_score'] >= 9:
            trust_emoji = "🔶"
        elif article['trust_score'] >= 7:
            trust_emoji = "🔷"
        
        message = (
            f"{article['importance']} **{article['title']}**\n\n"
            f"ℹ️ **Категория:** {article['category']}\n"
            f"📰 **Источник:** {article['source']} {trust_emoji}\n\n"
            f"{content}{article.get('summary', '')}\n\n"
            f"🔗 [Оригинал статьи]({article['url']})\n\n"
            f"{tags_str}"
        )
        
        return message
    except Exception as e:
        logger.error(f"Ошибка форматирования поста: {e}")
        return None

async def fetch_articles(source_config):
    """Получение статей из источника с улучшенной обработкой ошибок"""
    try:
        async with await create_async_session() as session:
            url = source_config.get('rss_url', source_config['url'])
            
            if not url:
                logger.error(f"URL не указан для источника: {source_config['name']}")
                return []
                
            logger.info(f"Запрос к источнику [{source_config['name']}]: {url}")
            
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'application/xml, text/xml, */*;q=0.9',
                'Referer': random.choice(REFERERS),
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive'
            }
            
            try:
                response = await session.get(url, headers=headers)
                response.raise_for_status()
            except httpx.RequestError as e:
                logger.error(f"Ошибка запроса к {url}: {e}")
                return []
            
            # Парсинг RSS
            soup = BeautifulSoup(response.content, 'xml')
            
            items = []
            if soup.find('rss'):
                items = soup.find_all('item')
            elif soup.find('feed'):
                items = soup.find_all('entry')
            
            logger.info(f"Найдено {len(items)} элементов в RSS {source_config['name']}")
            
            articles = []
            for item in items[:10]:
                link = None
                
                if item.find('link'):
                    link_elem = item.find('link')
                    if link_elem.get('href'):
                        link = link_elem.get('href').strip()
                    elif link_elem.text:
                        link = link_elem.text.strip()
                elif item.find('guid'):
                    link = item.find('guid').text.strip()
                
                if not link or not is_valid_url(link):
                    continue
                    
                articles.append(link)
            
            return articles
    
    except Exception as e:
        logger.error(f"Ошибка получения статей: {e}", exc_info=True)
        return []

async def generate_analytics_report():
    """Генерация аналитического отчета за последние 12 часов"""
    try:
        recent_data = db.get_recent_analytics(12)
        
        if not recent_data:
            return "За последние 12 часов новостей не найдено"
        
        category_stats = db.get_category_stats(12)
        keyword_freq = db.get_keyword_frequency(12, 5)
        
        importance_count = {1: 0, 2: 0, 3: 0}
        for item in recent_data:
            importance_count[item['importance']] += 1
        
        report = "📊 **Аналитический отчет за последние 12 часов**\n\n"
        report += f"📰 Всего новостей: {len(recent_data)}\n"
        report += f"🔥 Срочных: {importance_count[3]}\n"
        report += f"❗ Важных: {importance_count[2]}\n"
        report += f"📌 Информационных: {importance_count[1]}\n\n"
        
        report += "**Распределение по категориям:**\n"
        for category, count in category_stats.items():
            report += f"- {category}: {count} новостей\n"
        
        report += "\n**Топ ключевых слов:**\n"
        for keyword, count in keyword_freq:
            report += f"#{keyword.replace(' ', '_')} ({count}), "
        
        report = report.rstrip(", ") + "\n\n"
        
        report += "**Аналитический вывод:**\n"
        if importance_count[3] > 5:
            report += "Высокая концентрация срочных новостей указывает на напряженную ситуацию в финансовом секторе. "
        elif importance_count[3] > 2:
            report += "Наличие нескольких срочных новостей сигнализирует о повышенной волатильности рынков. "
        else:
            report += "Отсутствие значительного количества срочных новостей говорит об относительной стабильности. "
        
        report += "Рекомендуется обратить особое внимание на отчеты крупных корпораций и данные по инфляции."
        
        return report
        
    except Exception as e:
        logger.error(f"Ошибка генерации отчета: {e}")
        return "Не удалось сгенерировать аналитический отчет"

async def send_error_notification(message):
    """Отправка уведомления об ошибке в Telegram"""
    try:
        client = TelegramClient('error_notifier', API_ID, API_HASH)
        await client.start(bot_token=BOT_TOKEN_PUBLISHER)
        await client.send_message(ADMIN_CHAT_ID, f"🚨 Ошибка: {message}")
        await client.disconnect()
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление об ошибке: {e}")

async def send_morning_briefing(publisher):
    """Формирование и отправка утреннего брифинга"""
    try:
        logger.info("Формирование утреннего брифинга...")
        report = await generate_analytics_report()
        await publisher.send_message(CHANNEL, report, parse_mode='markdown')
        logger.info("Утренний брифинг отправлен")
    except Exception as e:
        logger.error(f"Ошибка отправки брифинга: {e}")

# ================== КОД БОТА-СБОРЩИКА ==================

async def run_collector():
    """Запуск бота-сборщика"""
    logger.info("Запуск бота-сборщика...")
    
    try:
        collector = TelegramClient('collector_session', API_ID, API_HASH)
        await collector.start(bot_token=BOT_TOKEN_COLLECTOR)
        logger.info("Бот-сборщик успешно запущен")
        await collector.send_message(ADMIN_CHAT_ID, "🔄 Бот-сборщик запущен и готов к работе!")
        
        # Обработчики событий
        @collector.on(events.NewMessage(pattern=r'https?://\S+'))
        async def handle_link(event):
            try:
                url = event.message.text.strip()
                if not is_valid_url(url):
                    await event.reply("❌ Неверный формат URL")
                    return
                    
                logger.info(f"Получена ссылка от пользователя: {url}")
                
                db.add_pending_link(
                    url=url,
                    source='user_submission',
                    added_by=event.sender_id,
                    priority=3  # Высокий приоритет
                )
                
                await event.reply("✅ Ссылка добавлена в очередь на обработку!")
            except Exception as e:
                logger.error(f"Ошибка обработки ссылки: {e}")
        
        @collector.on(events.NewMessage(from_users=ADMIN_CHAT_ID, pattern='/stats'))
        async def handle_stats(event):
            try:
                report = await generate_analytics_report()
                await event.reply(report, parse_mode='markdown')
            except Exception as e:
                logger.error(f"Ошибка генерации отчета: {e}")
        
        @collector.on(events.NewMessage(from_users=ADMIN_CHAT_ID, pattern='/collect'))
        async def handle_collect(event):
            try:
                await collect_links()
                await event.reply("✅ Сбор ссылок завершен!")
            except Exception as e:
                logger.error(f"Ошибка сбора ссылок: {e}")
        
        async def collect_links():
            logger.info("Начало сбора ссылок")
            pending_urls = {item['url'] for item in db.get_pending_links(limit=1000)}
            
            for source in SOURCES_CONFIG:
                try:
                    articles = await fetch_articles(source)
                    new_count = 0
                    for url in articles:
                        if url in pending_urls:
                            continue
                        db.add_pending_link(
                            url=url,
                            source=source['name'],
                            priority=2  # Средний приоритет
                        )
                        pending_urls.add(url)
                        new_count += 1
                    logger.info(f"Для источника {source['name']} добавлено {new_count} новых ссылок")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Ошибка сбора ссылок из {source['name']}: {e}")
            
            logger.info(f"Всего ссылок в очереди: {len(pending_urls)}")
            await collector.send_message(ADMIN_CHAT_ID, f"✅ Сбор ссылок завершен! В очереди: {len(pending_urls)} ссылок")
        
        async def scheduled_collection():
            logger.info("Запуск периодической обработки очереди")
            while True:
                try:
                    await collect_links()
                    QUEUE_SIZE.set(len(db.get_pending_links(limit=1000)))
                    await asyncio.sleep(1800)  # 30 минут
                except Exception as e:
                    logger.error(f"Ошибка в scheduled_collection: {e}")
                    await asyncio.sleep(60)
        
        asyncio.create_task(scheduled_collection())
        
        logger.info("Бот-сборщик готов к работе")
        await collector.run_until_disconnected()
        
    except errors.RPCError as e:
        logger.error(f"Ошибка подключения Telegram: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка в сборщике: {e}", exc_info=True)

# ================== КОД БОТА-ПУБЛИКАТОРА ==================

async def run_publisher():
    """Запуск бота-публикатора"""
    logger.info("Запуск бота-публикатора...")
    
    try:
        publisher = TelegramClient('publisher_session', API_ID, API_HASH)
        await publisher.start(bot_token=BOT_TOKEN_PUBLISHER)
        logger.info("Бот-публикатор успешно запущен")
        await publisher.send_message(ADMIN_CHAT_ID, "🚀 Бот-публикатор запущен!")
        
        # Обработчики событий
        @publisher.on(events.NewMessage)
        async def track_engagement(event):
            """Отслеживание вовлеченности"""
            if event.chat_id == int(CHANNEL):
                if event.reply_to_msg_id:
                    # Реакции на сообщения
                    db.update_engagement(event.reply_to_msg_id, 0.5)
                elif event.views:
                    # Просмотры
                    db.update_engagement(event.id, event.views / 1000)
        
        @publisher.on(events.NewMessage(from_users=ADMIN_CHAT_ID, pattern='/publish'))
        async def handle_publish(event):
            try:
                await event.reply("⏳ Начинаю обработку очереди...")
                await process_pending_links(publisher)
            except Exception as e:
                logger.error(f"Ошибка ручной публикации: {e}")
        
        @publisher.on(events.NewMessage(from_users=ADMIN_CHAT_ID, pattern='/clean'))
        async def handle_clean(event):
            try:
                db.clear_pending_links()
                await event.reply("✅ Очередь очищена!")
            except Exception as e:
                logger.error(f"Ошибка очистки очереди: {e}")
        
        @publisher.on(events.NewMessage(from_users=ADMIN_CHAT_ID, pattern=r'/post\s+(https?://\S+)'))
        async def handle_immediate_post(event):
            try:
                url = event.pattern_match.group(1).strip()
                if not is_valid_url(url):
                    await event.reply("❌ Неверный URL")
                    return
                    
                # Добавляем с высоким приоритетом
                db.add_pending_link(
                    url=url,
                    source='immediate_command',
                    added_by=event.sender_id,
                    priority=5  # Наивысший приоритет
                )
                await event.reply("✅ Ссылка добавлена в очередь с высоким приоритетом!")
            except Exception as e:
                logger.error(f"Ошибка добавления ссылки: {e}")
        
        async def process_pending_links(client):
            try:
                pending = db.get_pending_links(limit=15)
                if not pending:
                    logger.info("Нет ссылок для обработки")
                    await client.send_message(ADMIN_CHAT_ID, "ℹ️ Нет ссылок для обработки")
                    return
                
                logger.info(f"Начало обработки {len(pending)} ссылок")
                await client.send_message(ADMIN_CHAT_ID, f"🔍 Начинаю обработку {len(pending)} ссылок...")
                
                processed = []
                errors = []
                success_count = 0
                
                for item in pending:
                    try:
                        logger.info(f"Обработка ссылки: {item['url']}")
                        
                        if item['source'] == 'user_submission':
                            source_config = {
                                'name': 'Пользовательская ссылка',
                                'category': 'разное',
                                'tags': ['новости'],
                                'lang': 'ru',
                                'trust_score': 5
                            }
                        else:
                            source_config = next(
                                (s for s in SOURCES_CONFIG if s['name'] == item['source']), 
                                {
                                    'name': item['source'],
                                    'category': 'разное',
                                    'tags': ['новости'],
                                    'lang': 'ru',
                                    'trust_score': 5
                                }
                            )
                        
                        article = await parse_article(item['url'], source_config)
                        if not article:
                            logger.warning(f"Не удалось распарсить статью: {item['url']}")
                            errors.append(item['url'])
                            continue
                        
                        is_duplicate = False
                        
                        if db.is_link_posted(item['url']):
                            logger.info(f"Пропущен дубликат по URL: {item['url']}")
                            is_duplicate = True
                        
                        elif db.is_content_hash_exists(article['content_hash']):
                            logger.info(f"Пропущен дубликат по контенту: {article['title'][:50]}...")
                            is_duplicate = True
                        
                        elif db.is_title_hash_recent(article['title_hash']):
                            logger.info(f"Пропущен возможный дубликат по заголовку: {article['title']}")
                            is_duplicate = True
                        
                        if is_duplicate:
                            processed.append(item['id'])
                            continue
                        
                        message = format_post(article)
                        if not message:
                            logger.warning(f"Не удалось отформатировать пост для: {item['url']}")
                            errors.append(item['url'])
                            continue
                        
                        try:
                            await client.send_message(
                                entity=CHANNEL,
                                message=message,
                                link_preview=False,
                                parse_mode="markdown"
                            )
                            PUBLISH_COUNT.inc()
                            success_count += 1
                            
                            # Сохраняем данные для аналитики
                            analytics_entry = {
                                "url": item['url'],
                                "title": article['title'],
                                "category": article['category'],
                                "tags": article['tags'],
                                "importance_level": article['importance_level'],
                                "source": article['source'],
                                "content_hash": article['content_hash'],
                                "title_hash": article['title_hash'],
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "summary": article.get('summary', ''),
                                "trust_score": article['trust_score']
                            }
                            db.add_analytics(analytics_entry)
                            
                            # Помечаем как опубликованное
                            db.add_posted_link(item['url'])
                            db.add_content_hash(article['content_hash'])
                            db.add_title_hash(article['title_hash'])
                            
                        except errors.RPCError as e:
                            logger.error(f"Ошибка Telegram: {e}")
                            errors.append(item['url'])
                            continue
                        except Exception as e:
                            logger.error(f"Ошибка публикации: {e}")
                            errors.append(item['url'])
                            continue
                        
                        processed.append(item['id'])
                        logger.info(f"Опубликовано: {article['title'][:50]}...")
                        
                        await asyncio.sleep(random.uniform(3, 8))
                    
                    except Exception as e:
                        logger.error(f"Ошибка обработки ссылки {item['url']}: {e}")
                        errors.append(item['url'])
                
                # Удаляем обработанные ссылки
                for item_id in processed:
                    db.remove_pending_link(item_id)
                
                report = (
                    f"✅ Обработка завершена!\n"
                    f"• Успешно опубликовано: {success_count}\n"
                    f"• Ошибок обработки: {len(errors)}\n"
                    f"• Дубликатов/пропущено: {len(processed) - success_count}\n"
                    f"• Осталось в очереди: {len(db.get_pending_links(limit=1000))}"
                )
                await client.send_message(ADMIN_CHAT_ID, report)
                logger.info(report)
                
                if errors:
                    error_list = "\n".join(f"- {url}" for url in errors[:10])
                    if len(errors) > 10:
                        error_list += f"\n... и ещё {len(errors) - 10} ошибок"
                    await client.send_message(
                        ADMIN_CHAT_ID, 
                        f"❌ Ссылки с ошибками:\n{error_list}",
                        parse_mode=None
                    )
            except Exception as e:
                logger.error(f"Критическая ошибка в process_pending_links: {e}", exc_info=True)
        
        async def scheduled_processing():
            logger.info("Запуск периодической обработки очереди")
            while True:
                try:
                    await process_pending_links(publisher)
                    QUEUE_SIZE.set(len(db.get_pending_links(limit=1000)))
                    
                    # Утренний брифинг в 8:00 по МСК (UTC+3)
                    now_utc = datetime.now(timezone.utc)
                    moscow_tz = timezone(timedelta(hours=3))
                    moscow_time = now_utc.astimezone(moscow_tz)
                    
                    if moscow_time.hour == 8 and moscow_time.minute == 0:
                        await send_morning_briefing(publisher)
                    
                    await asyncio.sleep(300)  # 5 минут
                except Exception as e:
                    logger.error(f"Ошибка в scheduled_processing: {e}")
                    await asyncio.sleep(60)
        
        asyncio.create_task(scheduled_processing())
        
        logger.info("Бот-публикатор готов к работе")
        await publisher.run_until_disconnected()
        
    except errors.RPCError as e:
        logger.error(f"Ошибка подключения Telegram: {e}")
    except Exception as e:
        logger.error(f"Критическая ошибка в публикаторе: {e}", exc_info=True)

# ================== ОСНОВНАЯ ФУНКЦИЯ ==================

async def main():
    """Основная функция для запуска ботов"""
    tasks = [
        asyncio.create_task(run_collector()),
        asyncio.create_task(run_publisher())
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        logger.info("Запуск системы сбора и публикации новостей")
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Остановка системы по запросу пользователя")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Завершение работы системы")