import unittest
import os
import json
import shutil
from unittest.mock import patch, MagicMock
from job import run_extraction_job, initialize_firebase 

TEST_RAW_DIR = "test_raw"


class TestJob(unittest.TestCase):

    def setUp(self):
        """Налаштування: створення тестової директорії перед кожним тестом."""
        if os.path.exists(TEST_RAW_DIR):
            shutil.rmtree(TEST_RAW_DIR)
        os.makedirs(TEST_RAW_DIR)

    def tearDown(self):
        """Очищення: видалення тестової директорії після кожного тесту."""
        if os.path.exists(TEST_RAW_DIR):
            shutil.rmtree(TEST_RAW_DIR)

    @patch('job.initialize_firebase', return_value=True)
    @patch('job.db')
    def test_job_extraction_and_idempotency(self, mock_db, mock_init_firebase):
        """Тест успішного вивантаження та ідемпотентності."""
        
        TEST_DATE = "2025-10-14"
        TEST_FEATURE = "GC"
        FAKE_FIREBASE_DATA = {
            "GC": {"Close": 2000.0, "Open": 1990.0},
            "SI": {"Close": 24.0, "Open": 23.5}
        }
        
        mock_db.reference.return_value.get.return_value = FAKE_FIREBASE_DATA
        
        target_dir = os.path.join(TEST_RAW_DIR, TEST_FEATURE, TEST_DATE)
        os.makedirs(target_dir, exist_ok=True)
        with open(os.path.join(target_dir, "old_file.txt"), 'w') as f:
            f.write("old data")
            
        output_path = run_extraction_job(
            date=TEST_DATE, 
            feature=TEST_FEATURE, 
            raw_dir=TEST_RAW_DIR
        )
        
        self.assertIsNotNone(output_path)
        self.assertTrue(os.path.exists(output_path))
        
        self.assertFalse(os.path.exists(os.path.join(target_dir, "old_file.txt")))
        
        with open(output_path, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
            
        self.assertIn(TEST_FEATURE, saved_data)
        self.assertEqual(saved_data[TEST_FEATURE]['Close'], 2000.0)

    @patch('job.initialize_firebase', return_value=True)
    @patch('job.db')
    def test_job_no_data_for_date(self, mock_db, mock_init_firebase):
        TEST_DATE = "2025-01-01"
        TEST_FEATURE = "GC"
        
        mock_db.reference.return_value.get.return_value = None
        
        output_path = run_extraction_job(
            date=TEST_DATE, 
            feature=TEST_FEATURE, 
            raw_dir=TEST_RAW_DIR
        )
        
        self.assertIsNotNone(output_path)
        self.assertTrue(os.path.exists(output_path))
        
        with open(output_path, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
            
        self.assertIn("error", saved_data)
        self.assertIn("No data found for this date", saved_data['error'])


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
