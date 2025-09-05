import os
import logging
import sqlite3
from datetime import datetime, date
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramForbiddenError
import asyncio
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Константы
MAX_DURATION = 60
MAX_CONCURRENT_CONVERSIONS = 3
QUEUE_LIMIT = 10
SEMAPHORE_TIMEOUT = 300.0
CONVERSION_TIMEOUT = 600.0
STUCK_THRESHOLD = 600

# ПРАВИЛЬНАЯ ИНИЦИАЛИЗАЦИЯ БОТА И ДИСПЕТЧЕРА
API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

if not API_TOKEN:
    raise ValueError("Необходимо указать BOT_TOKEN в .env файле")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Глобальные переменные
processing_queue = set()
active_conversions = 0
stuck_tasks = {}
queue_lock = asyncio.Lock()
shutdown_flag = False
semaphore = None
executor = None

# Инициализация базы данных
def init_db():
    logger.info("Инициализация базы данных...")
    try:
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registration_date TIMESTAMP,
                last_activity TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                conversion_date TIMESTAMP,
                video_duration REAL,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных успешно инициализирована")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")

# Функции работы с БД
def save_user(user: types.User):
    logger.info(f"Сохранение пользователя в БД: ID={user.id}, username={user.username}")
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, username, first_name, last_name, registration_date, last_activity)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            datetime.now(),
            datetime.now()
        ))
        conn.commit()
        logger.info(f"Пользователь {user.id} успешно сохранен в БД")
    except Exception as e:
        logger.error(f"Ошибка при сохранении пользователя {user.id}: {e}")
    finally:
        conn.close()

def update_user_activity(user_id: int):
    logger.info(f"Обновление активности пользователя: {user_id}")
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE users 
            SET last_activity = ?
            WHERE user_id = ?
        ''', (datetime.now(), user_id))
        conn.commit()
        logger.info(f"Активность пользователя {user_id} обновлена")
    except Exception as e:
        logger.error(f"Ошибка при обновлении активности пользователя {user_id}: {e}")
    finally:
        conn.close()

def save_conversion(user_id: int, duration: float):
    logger.info(f"Сохранение конвертации: user_id={user_id}, duration={duration}")
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO conversions 
            (user_id, conversion_date, video_duration)
            VALUES (?, ?, ?)
        ''', (user_id, datetime.now(), duration))
        conn.commit()
        logger.info(f"Конвертация для пользователя {user_id} успешно сохранена")
    except Exception as e:
        logger.error(f"Ошибка при сохранении конвертации для пользователя {user_id}: {e}")
    finally:
        conn.close()

def get_stats():
    logger.info("Получение статистики...")
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        today = date.today()
        cursor.execute('''
            SELECT COUNT(*) FROM users 
            WHERE DATE(registration_date) = ?
        ''', (today,))
        today_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM conversions')
        total_conversions = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) FROM conversions 
            WHERE DATE(conversion_date) = ?
        ''', (today,))
        today_conversions = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT AVG(video_duration) FROM conversions
        ''')
        avg_duration = cursor.fetchone()[0] or 0
        
        stats = {
            'total_users': total_users,
            'today_users': today_users,
            'total_conversions': total_conversions,
            'today_conversions': today_conversions,
            'avg_duration': round(avg_duration, 2)
        }
        
        logger.info(f"Статистика получена: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return {}
    finally:
        conn.close()

# Функции бота
async def check_subscription(user_id: int) -> bool:
    logger.info(f"Проверка подписки пользователя: {user_id}")
    try:
        member = await bot.get_chat_member(chat_id="@ai_genom", user_id=user_id)
        is_subscribed = member.status in ['member', 'administrator', 'creator']
        logger.info(f"Пользователь {user_id} {'подписан' if is_subscribed else 'не подписан'} на канал")
        return is_subscribed
    except TelegramForbiddenError:
        logger.warning(f"Бот не имеет доступа к информации о пользователе {user_id}")
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке подписки пользователя {user_id}: {e}")
        return False

async def subscription_required(message: types.Message) -> bool:
    user_id = message.from_user.id
    logger.info(f"Проверка обязательной подписки для пользователя: {user_id}")
    
    if not await check_subscription(user_id):
        logger.warning(f"Пользователь {user_id} не подписан на канал")
        keyboard = types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/ai_genom")],
                [types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription")]
            ]
        )
        await message.answer(
            "⚠️ Для использования бота необходимо подписаться на наш канал @ai_genom",
            reply_markup=keyboard
        )
        return False
    logger.info(f"Пользователь {user_id} прошел проверку подписки")
    return True

# Синхронная функция для конвертации видео
def convert_video_process(video_path: str, gif_path: str) -> bool:
    try:
        from moviepy.editor import VideoFileClip
        clip = VideoFileClip(video_path)
        clip.write_gif(gif_path, fps=15)
        clip.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка в процессе конвертации: {e}")
        return False

# Асинхронная задача для конвертации
async def process_video_task(video_local_path: str, gif_local_path: str):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, convert_video_process, video_local_path, gif_local_path)

# Асинхронная функция обработки видео
async def process_video_with_semaphore(message: types.Message, video_local_path: str, gif_local_path: str, file_id: str):
    global active_conversions, shutdown_flag
    
    user_id = message.from_user.id
    task_id = f"{user_id}_{file_id}"
    
    if shutdown_flag:
        await message.answer("❌ Бот находится в режиме остановки. Попробуйте позже.")
        return
    
    logger.info(f"Видео пользователя {user_id} ожидает освобождения семафора")
    
    async with queue_lock:
        processing_queue.add(user_id)
        queue_position = len(processing_queue)
        stuck_tasks[task_id] = asyncio.get_event_loop().time()
        logger.info(f"Пользователь {user_id} добавлен в очередь. Позиция: {queue_position}")
    
    if queue_position > QUEUE_LIMIT:
        async with queue_lock:
            processing_queue.discard(user_id)
            stuck_tasks.pop(task_id, None)
        await message.answer("❌ Очередь переполнена. Пожалуйста, попробуйте позже.")
        return
    
    if queue_position > MAX_CONCURRENT_CONVERSIONS:
        queue_wait = queue_position - MAX_CONCURRENT_CONVERSIONS
        await message.answer(f"⏳ Ваше видео добавлено в очередь. Ожидайте: {queue_wait} видео перед вами")
    
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=SEMAPHORE_TIMEOUT)
        logger.info(f"Пользователь {user_id} получил доступ к семафору")
        
        async with queue_lock:
            processing_queue.discard(user_id)
            stuck_tasks.pop(task_id, None)
            active_conversions += 1
        
        if shutdown_flag:
            semaphore.release()
            await message.answer("❌ Бот находится в режиме остановки. Попробуйте позже.")
            return
            
        await message.answer("🎬 Начинаю обработку вашего видео...")
        
        try:
            success = await asyncio.wait_for(
                process_video_task(video_local_path, gif_local_path),
                timeout=CONVERSION_TIMEOUT
            )
            
            if success and not shutdown_flag:
                try:
                    from moviepy.editor import VideoFileClip
                    clip = VideoFileClip(video_local_path)
                    duration = clip.duration
                    clip.close()
                    
                    save_conversion(user_id, duration)
                    
                    await message.answer_document(types.FSInputFile(gif_local_path))
                    await message.answer("✅ Готово! Вот ваш GIF файл.")
                    logger.info(f"Видео пользователя {user_id} успешно обработано")
                except Exception as e:
                    logger.error(f"Ошибка при отправке результата пользователю {user_id}: {e}")
                    if not shutdown_flag:
                        await message.answer("❌ Ошибка при отправке результата.")
            elif not success:
                if not shutdown_flag:
                    await message.answer("❌ Ошибка при конвертации видео.")
                    
        except asyncio.TimeoutError:
            logger.error(f"Таймаут конвертации для пользователя {user_id}")
            if not shutdown_flag:
                await message.answer("⏰ Конвертация заняла слишком много времени. Попробуйте уменьшить видео.")
                
    except asyncio.TimeoutError:
        logger.error(f"Таймаут ожидания семафора для пользователя {user_id}")
        async with queue_lock:
            processing_queue.discard(user_id)
            stuck_tasks.pop(task_id, None)
        if not shutdown_flag:
            await message.answer("⏰ Превышено время ожидания. Попробуйте позже.")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке видео для пользователя {user_id}: {e}")
        if not shutdown_flag:
            await message.answer("❌ Произошла ошибка при обработке видео.")
    finally:
        try:
            semaphore.release()
            logger.info(f"Семафор освобожден для пользователя {user_id}")
        except:
            pass
            
        async with queue_lock:
            active_conversions -= 1
        
        try:
            if os.path.exists(video_local_path):
                os.remove(video_local_path)
            if os.path.exists(gif_local_path):
                os.remove(gif_local_path)
        except Exception as e:
            logger.error(f"Ошибка при удалении временных файлов для пользователя {user_id}: {e}")

# Команды бота
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    global shutdown_flag
    
    if shutdown_flag:
        await message.answer("❌ Бот остановлен. Администратор может запустить его командой /start_bot")
        return
        
    user = message.from_user
    user_id = user.id
    logger.info(f"Получена команда /start от пользователя: {user_id}")
    
    save_user(user)
    update_user_activity(user_id)
    
    if not await check_subscription(user_id):
        logger.info(f"Пользователь {user_id} не подписан, отправляем кнопки подписки")
        keyboard = types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/ai_genom")],
                [types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription")]
            ]
        )
        await message.answer(
            "👋 Привет! Для использования бота необходимо подписаться на наш канал @ai_genom\n\n"
            "После подписки нажмите кнопку 'Проверить подписку'",
            reply_markup=keyboard
        )
        return
    
    await message.answer("🎉 Добро пожаловать! Отправь мне короткое видео (до 1 минуты), и я сделаю из него GIF!")

@dp.message(Command("stats"))
async def show_stats(message: types.Message):
    user = message.from_user
    user_id = user.id
    logger.info(f"Получена команда /stats от пользователя: {user_id}")
    
    if str(user_id) != str(ADMIN_ID):
        logger.warning(f"Пользователь {user_id} попытался получить статистику без прав администратора")
        await message.answer("❌ У вас нет прав для просмотра статистики")
        return
    
    logger.info(f"Администратор {user_id} запрашивает статистику")
    stats = get_stats()
    
    if not stats:
        logger.error("Ошибка при получении статистики")
        await message.answer("❌ Ошибка при получении статистики")
        return
    
    async with queue_lock:
        queue_info = f"""
👥 В очереди: {len(processing_queue)}
🔄 Активных конвертаций: {active_conversions}
🚦 Состояние: {'Остановка' if shutdown_flag else 'Работает'}
        """
    
    stats_text = f"""
📊 <b>Статистика бота</b>

{queue_info}

👥 <b>Пользователи:</b>
├ Всего: {stats['total_users']}
└ Сегодня: {stats['today_users']}

🎬 <b>Конвертации:</b>
├ Всего: {stats['total_conversions']}
└ Сегодня: {stats['today_conversions']}

⏱ <b>Средняя длительность видео:</b> {stats['avg_duration']} сек
    """
    
    logger.info(f"Отправка статистики администратору {user_id}")
    await message.answer(stats_text, parse_mode="HTML")

@dp.message(Command("queue_status"))
async def queue_status(message: types.Message):
    user = message.from_user
    user_id = user.id
    logger.info(f"Получена команда /queue_status от пользователя: {user_id}")
    
    if str(user_id) != str(ADMIN_ID):
        logger.warning(f"Пользователь {user_id} попытался получить статус очереди без прав администратора")
        await message.answer("❌ У вас нет прав для просмотра статуса очереди")
        return
    
    async with queue_lock:
        queue_size = len(processing_queue)
        active_count = active_conversions
        is_shutdown = shutdown_flag
        stuck_count = len(stuck_tasks)
    
    status_text = f"""
📊 <b>Статус очереди обработки</b>

🚦 <b>Состояние системы:</b> {'🛑 Остановлена' if is_shutdown else '🟢 Работает'}

📋 <b>Очередь:</b>
├ В очереди: {queue_size}
├ Активных конвертаций: {active_count}
└ Зависших задач: {stuck_count}

⚙️ <b>Настройки:</b>
├ Максимум одновременно: {MAX_CONCURRENT_CONVERSIONS}
└ Лимит очереди: {QUEUE_LIMIT}
    """
    
    logger.info(f"Отправка статуса очереди администратору {user_id}")
    await message.answer(status_text, parse_mode="HTML")

@dp.message(Command("clear_queue"))
async def clear_queue(message: types.Message):
    user_id = message.from_user.id
    if str(user_id) != str(ADMIN_ID):
        await message.answer("❌ У вас нет прав для этой команды")
        return
    
    async with queue_lock:
        queue_size = len(processing_queue)
        processing_queue.clear()
        stuck_tasks.clear()
        while semaphore._value < MAX_CONCURRENT_CONVERSIONS:
            try:
                semaphore.release()
            except RuntimeError:
                break
        
    logger.warning(f"Администратор {user_id} очистил очередь. Было {queue_size} задач")
    await message.answer(f"✅ Очередь очищена. Было {queue_size} задач в очереди.")

@dp.message(Command("shutdown"))
async def shutdown_bot(message: types.Message):
    global shutdown_flag
    user_id = message.from_user.id
    if str(user_id) != str(ADMIN_ID):
        await message.answer("❌ У вас нет прав для этой команды")
        return
    
    shutdown_flag = True
    logger.warning(f"Администратор {user_id} инициировал остановку бота")
    await message.answer("🔄 Бот переходит в режим остановки. Обрабатываются текущие задачи...")
    
    start_time = asyncio.get_event_loop().time()
    while active_conversions > 0 and (asyncio.get_event_loop().time() - start_time) < 300:
        await message.answer(f"⏳ Ожидание завершения {active_conversions} активных конвертаций...")
        await asyncio.sleep(5)
        logger.info(f"Ожидание завершения {active_conversions} активных конвертаций...")
    
    await message.answer("✅ Бот остановлен. Используйте /start_bot для запуска.")

@dp.message(Command("start_bot"))
async def start_bot_command(message: types.Message):
    global shutdown_flag
    user_id = message.from_user.id
    if str(user_id) != str(ADMIN_ID):
        await message.answer("❌ У вас нет прав для этой команды")
        return
    
    if not shutdown_flag:
        await message.answer("✅ Бот уже запущен!")
        return
    
    shutdown_flag = False
    logger.info(f"Администратор {user_id} запустил бота командой /start_bot")
    
    await message.answer("🚀 Бот успешно запущен! Теперь он снова принимает видео.")

@dp.callback_query(lambda callback: callback.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery):
    global shutdown_flag
    
    if shutdown_flag:
        await callback.message.edit_text("❌ Бот остановлен. Администратор может запустить его командой /start_bot")
        await callback.answer()
        return
        
    user = callback.from_user
    user_id = user.id
    logger.info(f"Получен callback проверки подписки от пользователя: {user_id}")
    
    save_user(user)
    
    if await check_subscription(user_id):
        logger.info(f"Пользователь {user_id} подписан, подтверждаем подписку")
        await callback.message.edit_text("✅ Отлично! Теперь вы можете отправлять видео для конвертации в GIF.")
        await callback.answer("Подписка подтверждена! ✅")
    else:
        logger.warning(f"Пользователь {user_id} не подписан, отправляем уведомление")
        await callback.answer("❌ Вы не подписались на канал. Пожалуйста, подпишитесь и попробуйте снова.", show_alert=True)

@dp.message(lambda message: message.video)
async def handle_video(message: types.Message):
    global shutdown_flag
    
    if shutdown_flag:
        await message.answer("❌ Бот остановлен. Администратор может запустить его командой /start_bot")
        return
        
    user = message.from_user
    user_id = user.id
    video = message.video
    logger.info(f"Получено видео от пользователя {user_id}, file_id: {video.file_id}")
    
    save_user(user)
    update_user_activity(user_id)
    
    if not await subscription_required(message):
        logger.warning(f"Пользователь {user_id} не прошел проверку подписки при отправке видео")
        return

    file_id = video.file_id
    try:
        file_info = await bot.get_file(file_id)
        file_path = file_info.file_path
        logger.info(f"Получен путь к файлу: {file_path}")
    except Exception as e:
        logger.error(f"Ошибка при получении информации о файле от пользователя {user_id}: {e}")
        await message.answer("❌ Ошибка при обработке видео. Попробуйте отправить видео заново.")
        return

    video_local_path = f"temp_{file_id}.mp4"
    gif_local_path = f"temp_{file_id}.gif"
    logger.info(f"Временные пути: video={video_local_path}, gif={gif_local_path}")

    try:
        logger.info(f"Начало скачивания видео для пользователя {user_id}")
        await bot.download_file(file_path, video_local_path)
        logger.info(f"Видео успешно скачано для пользователя {user_id}")
    except Exception as e:
        logger.error(f"Ошибка при скачивании видео для пользователя {user_id}: {e}")
        await message.answer("❌ Ошибка при скачивании видео. Попробуйте отправить видео заново.")
        return

    try:
        logger.info(f"Проверка длительности видео для пользователя {user_id}")
        from moviepy.editor import VideoFileClip
        clip = VideoFileClip(video_local_path)
        if clip.duration > MAX_DURATION:
            logger.warning(f"Видео пользователя {user_id} слишком длинное: {clip.duration} сек")
            await message.answer("❌ Видео слишком длинное. Максимальная длина — 1 минута.")
            clip.close()
            if os.path.exists(video_local_path):
                os.remove(video_local_path)
            return
        clip.close()
        
        asyncio.create_task(process_video_with_semaphore(message, video_local_path, gif_local_path, file_id))
        
    except Exception as e:
        logger.error(f"Ошибка при предварительной обработке видео для пользователя {user_id}: {e}")
        await message.answer("❌ Ошибка при обработке видео. Попробуйте ещё раз.")
        try:
            if os.path.exists(video_local_path):
                os.remove(video_local_path)
            if os.path.exists(gif_local_path):
                os.remove(gif_local_path)
        except:
            pass

@dp.message()
async def handle_other_messages(message: types.Message):
    global shutdown_flag
    
    if shutdown_flag:
        return
        
    user = message.from_user
    user_id = user.id
    logger.info(f"Получено другое сообщение от пользователя {user_id}: {message.text}")
    
    save_user(user)
    update_user_activity(user_id)
    
    if not await subscription_required(message):
        logger.warning(f"Пользователь {user_id} не прошел проверку подписки при отправке сообщения")
        return
    
    logger.info(f"Пользователь {user_id} прошел проверку, отправляем инструкцию")
    await message.answer("📥 Пожалуйста, отправьте видео файл (до 1 минуты) для конвертации в GIF.")

# Основная функция
async def main():
    global semaphore, executor
    
    logger.info("Инициализация бота...")
    
    # Инициализация семафора и пула процессов
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS)
    executor = ProcessPoolExecutor(max_workers=MAX_CONCURRENT_CONVERSIONS)
    
    init_db()
    
    logger.info("Бот успешно запущен")
    await dp.start_polling(bot)

if __name__ == '__main__':
    logger.info("Запуск бота...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")
