import os
import json
import shutil
import firebase_admin
from firebase_admin import credentials, db

FIREBASE_URL = "https://lab2-dd3c8-default-rtdb.europe-west1.firebasedatabase.app"
FIREBASE_CREDENTIALS_PATH = "firebase_key.json"


def initialize_firebase():
    """Ініціалізує Firebase App, якщо він ще не ініціалізований.
    Використовує змінну середовища для хостингу або локальний файл для розробки.
    """
    if not firebase_admin._apps:
        try:
            if os.getenv('FIREBASE_CREDENTIALS_JSON'):
                creds_dict = json.loads(os.getenv('FIREBASE_CREDENTIALS_JSON'))
                cred = credentials.Certificate(creds_dict)
            else:
                cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
                
            firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_URL})
        except Exception as e:
            print(f"Помилка ініціалізації Firebase: {e}")
            return False
    return True

def run_extraction_job(date: str, feature: str, raw_dir: str):
    """
    Витягує дані за конкретну дату і фічу з Firebase та ідемпотентно зберігає їх.
    """
    
    target_feature = feature.upper()
    target_dir = os.path.join(raw_dir, target_feature, date)
    target_file = os.path.join(target_dir, f"{date}.json")
    
    if os.path.exists(target_dir):
        print(f"Очищення існуючої директорії: {target_dir}")
        shutil.rmtree(target_dir)
        
    os.makedirs(target_dir, exist_ok=True)
    
    if not initialize_firebase():
        return None
        
    try:
        ref = db.reference(f'metals_data/historical_prices/{date}')
        data = ref.get()

        if data is None:
            print(f"Дані за {date} не знайдено.")
            with open(target_file, 'w') as f:
                json.dump({"error": "No data found for this date in Firebase."}, f)
            return target_file

        filtered_data = {
            target_feature: data.get(target_feature)
        }
        
        if not filtered_data.get(target_feature):
            print(f"Дані для фічі '{target_feature}' не знайдено.")
            with open(target_file, 'w') as f:
                json.dump({"error": f"No data found for feature '{target_feature}' on {date}."}, f)
            return target_file

        with open(target_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=4)
        
        print(f"Дані успішно збережено у {target_file}")
        return target_file
        
    except Exception as e:
        print(f"Помилка під час вивантаження даних з Firebase: {e}")
        return None

def run_extraction_job_all(raw_dir: str):
    """
    Витягує ВСІ історичні дані з Firebase та зберігає їх, розділяючи за металом і датою.
    
    :param raw_dir: Шлях до кореневої директорії 'raw/'.
    :return: Список збережених файлів або None.
    """
  
    target_dir = os.path.join(raw_dir, "ALL_HISTORICAL_DATA")
    saved_files = []
    
    if os.path.exists(target_dir):
        print(f"Очищення існуючої директорії: {target_dir}")
        shutil.rmtree(target_dir)
        
    os.makedirs(target_dir, exist_ok=True)
    
    if not initialize_firebase():
        return None
        
    try:
        ref = db.reference('metals_data/historical_prices')
        all_data_by_date = ref.get()

        if all_data_by_date is None:
            print("Не знайдено історичних даних у Firebase.")
            return []
            
        for date, metals_data in all_data_by_date.items():
            
            for feature, prices in metals_data.items():
                
                feature_dir = os.path.join(target_dir, feature.upper())
                os.makedirs(feature_dir, exist_ok=True)
                
                file_name = f"{date}.json"
                target_file = os.path.join(feature_dir, file_name)
                
                data_to_save = {feature: prices}

                with open(target_file, 'w', encoding='utf-8') as f:
                    json.dump(data_to_save, f, ensure_ascii=False, indent=4)
                
                saved_files.append(target_file)
        
        print(f"Успішно збережено {len(saved_files)} файлів.")
        return saved_files
        
    except Exception as e:
        print(f"Помилка під час вивантаження всіх даних: {e}")
        return None
