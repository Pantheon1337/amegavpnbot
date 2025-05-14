import sqlite3
from datetime import datetime
import os

def get_db_connection():
    """Создает и возвращает соединение с базой данных"""
    return sqlite3.connect('vpn_bot.db')

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем существование базы данных
    if not os.path.exists('vpn_bot.db'):
        # Создаем таблицу клиентов
        cursor.execute('''
            CREATE TABLE clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                payment_day INTEGER NOT NULL,
                status TEXT NOT NULL,
                telegram_username TEXT,
                chat_id INTEGER,
                last_notified TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Создаем таблицу истории платежей
        cursor.execute('''
            CREATE TABLE payment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                payment_date TEXT,
                amount REAL,
                status TEXT,
                FOREIGN KEY (client_id) REFERENCES clients (id)
            )
        ''')
        
        conn.commit()
        print("База данных создана")
    else:
        # Проверяем наличие поля chat_id
        cursor.execute("PRAGMA table_info(clients)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'chat_id' not in columns:
            cursor.execute('ALTER TABLE clients ADD COLUMN chat_id INTEGER')
            conn.commit()
            print("Добавлено поле chat_id")
    
    conn.close()

def import_existing_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Пример данных для импорта (замените на свои)
    existing_clients = [
        # Формат: (name, payment_day, status)
        ("Иван Петров", 5, "paid"),
        ("Алексей Сидоров", 10, "unpaid"),
        ("Мария Иванова", 15, "paid"),
        ("Дмитрий Кузнецов", 20, "unpaid"),
    ]
    
    try:
        cursor.executemany('''INSERT INTO clients (name, payment_day, status)
                              VALUES (?, ?, ?)''', existing_clients)
        conn.commit()
        print(f"Успешно импортировано {len(existing_clients)} записей")
    except sqlite3.IntegrityError as e:
        print("Ошибка импорта:", e)
    finally:
        conn.close()

if __name__ == '__main__':
    init_db()
    
    # Раскомментируйте для импорта данных
    # import_existing_data()