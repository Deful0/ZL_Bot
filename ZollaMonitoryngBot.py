import psycopg2
from psycopg2 import Error
import telebot
from telebot import types
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(sql_update, sql_check):
    load_dotenv()

    BOT_TOKEN = os.getenv('BOT_TOKEN')
    CHAT_ID = os.getenv('CHAT_ID')

    DB_CONFIG = {
        "user": os.getenv('USER'),
        "password": os.getenv('PASSWORD'),
        "host": os.getenv('HOST'),
        "port": os.getenv('PORT'),
        "database": os.getenv('DATABASE'),
    }

    send_telegram_message(BOT_TOKEN, CHAT_ID, DB_CONFIG, sql_update, sql_check)



def send_telegram_message(BOT_TOKEN, CHAT_ID, DB_CONFIG, sql_update, sql_check):
    """Основная функция для отправки сообщения"""
    try:
        if not BOT_TOKEN or not CHAT_ID:
            logger.error("Не указаны BOT_TOKEN или CHAT_ID в .env файле")
            return False

        # Обновляем таблицу
        affected_rows, update_success = update_table(DB_CONFIG, sql_update)

        # Проверяем обновление таблиц
        tables_status, problematic_tables, check_success = check_table_updates(DB_CONFIG, sql_check)

        # Инициализация бота
        bot = telebot.TeleBot(BOT_TOKEN)

        # Отправляем сообщение о проверке
        check_message = format_check_message(tables_status, problematic_tables, check_success)
        if check_message:
            bot.send_message(CHAT_ID, check_message)

        logger.info(f"Сообщения отправлены в чат {CHAT_ID}")
        return update_success and check_success

    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")

        # Попытка отправить сообщение об ошибке
        try:
            bot = telebot.TeleBot(BOT_TOKEN)
            error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            bot.send_message(CHAT_ID, f"❌ Критическая ошибка при выполнении скрипта: {e}\nВремя: {error_time}")
        except:
            logger.error("Не удалось отправить сообщение об ошибке в Telegram")

        return False

def update_table(DB_CONFIG, sql_update):
    """Обновление данных в таблице"""
    connection = None
    cursor = None
    try:
        # Подключение к БД
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Выполнение SQL запроса
        query = open(sql_update, 'r', encoding='utf-8').read()
        logger.info(f"Выполняю sql файл обновления: {sql_update}")

        cursor.execute(query)
        affected_rows = cursor.rowcount
        connection.commit()

        logger.info(f"Обновлено {affected_rows} записей в таблице monitoring_mart")
        return affected_rows, True

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Ошибка при выполнении запроса обновления: {error}")
        if connection:
            connection.rollback()
        return 0, False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def check_table_updates(DB_CONFIG, sql_check):
    """Проверка обновления таблиц"""
    connection = None
    cursor = None
    try:
        # Подключение к БД
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Выполнение SQL запроса проверки
        query = open(sql_check, 'r', encoding='utf-8').read()
        logger.info(f"Выполняю sql файл проверки: {sql_check}")

        cursor.execute(query)
        results = cursor.fetchall()

        # Формируем список таблиц с флагами
        tables_status = []
        problematic_tables = []

        for table_name, flag_update in results:
            status = "✅ Обновлена сегодня" if flag_update == 1 else "❌ НЕ обновлена сегодня"
            tables_status.append(f"{table_name}: {status}")

            if flag_update == 0:
                problematic_tables.append(table_name)

        logger.info(f"Проверено {len(results)} таблиц. Проблемных: {len(problematic_tables)}")
        return tables_status, problematic_tables, True

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Ошибка при выполнении запроса проверки: {error}")
        return [], [], False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def format_check_message(tables_status, problematic_tables, success=True):
    """Форматирование сообщения о результатах проверки"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not success:
        return f"❌ Ошибка при проверке обновления таблиц на {current_time}"

    if not tables_status:
        return f"📭 Нет данных для проверки на {current_time}"

    if problematic_tables:
        message = f"📊 Отчет по обновлению таблиц на {current_time}\n\n"
        message += "\n".join(tables_status)
        message += f"\n\n📈 Итого: проверено {len(tables_status)} таблиц"
        message += f"\n❌ Проблемные таблицы ({len(problematic_tables)}):\n"
        message += "\n".join([f"• {table}" for table in problematic_tables])
        message += "\n\n⚠️ Требуется внимание!"
    else:
        message = None

    return message





if __name__ == '__main__':
    main('sql_update.sql', 'sql_check.sql')