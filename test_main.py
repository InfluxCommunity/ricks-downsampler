import unittest
from main import parse_interval, get_next_run_time_minutes, get_next_run_time_hours
from datetime import datetime

class TestParseInterval(unittest.TestCase):
    def test_positive_case(self):
        self.assertEqual(parse_interval('10m'), (10, 'm'))
        self.assertEqual(parse_interval('5h'), (5, 'h'))
        self.assertEqual(parse_interval('3d'), (3, 'd'))

    def test_negative_case(self):
        with self.assertRaises(ValueError):
            parse_interval('10')
        with self.assertRaises(ValueError):
            parse_interval('m')
        with self.assertRaises(ValueError):
            parse_interval('10w')

class TestGetNextRunTime(unittest.TestCase):
    def test_hours(self):
        test_cases = [{"clock_minute":1, "clock_hour":1, "expected_hour":2}]
        for tc in test_cases:
            test_time = datetime(2023,7,7, tc["clock_hour"], tc["clock_minute"])
            next_run_time = get_next_run_time_hours(1, now=test_time)
            self.assert_(next_run_time, datetime(2023, 7, tc["expected_hour"], 0))
            
    def test_minutes_basic(self):
        test_cases = [{"clock_minutes":1, "run_interval":1, "expected_minute":2},
                      {"clock_minutes":15, "run_interval":10, "expected_minute":20},
                      {"clock_minutes":4, "run_interval":45, "expected_minute":45}]
        for tc in test_cases:
            test_time = datetime(2023, 7, 7, 14, tc["clock_minutes"])
            next_run_time = get_next_run_time_minutes(tc["run_interval"], now=test_time)
            self.assertEqual(next_run_time, datetime(2023, 7, 7, 14, tc["expected_minute"]))
        
    def test_minutes_midnight(self):
        # run close to midnight, expect it to run the next day
        test_cases = [{"clock_minutes":59, "run_interval":1, "expected_minute":0, "expected_date": 8, "expected_hour":0}]
        for tc in test_cases:
            test_time = datetime(2023, 7, 7, 23, tc["clock_minutes"])
            next_run_time = get_next_run_time_minutes(tc["run_interval"], now=test_time)
            self.assertEqual(next_run_time, datetime(2023, 7, tc["expected_date"], tc["expected_hour"], tc["expected_minute"]))

    def test_minutes_greater_than_60(self):
        # before 11pm
        now = datetime(2023, 7, 7, 22, 30) 
        next_run_time = get_next_run_time_minutes(120, now=now)
        self.assertEqual(next_run_time, datetime(2023, 7, 7, 23, 0))

        #after 11pm
        now = datetime(2023, 7, 7, 23, 30) 
        next_run_time = get_next_run_time_minutes(120, now=now)
        self.assertEqual(next_run_time, datetime(2023, 7, 8, 0, 0))

if __name__ == "__main__":
    unittest.main()