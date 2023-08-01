import unittest
from unittest.mock import MagicMock, Mock, patch
from main import parse_interval, write_downsampled_data
from influxql_generator import field_is_num, generate_fields_string, generate_group_by_string
from schedule_calculator import get_next_run_time_minutes, get_next_run_time_hours, get_next_run_time, get_then
from datetime import datetime
import traceback

class SQLGeneration(unittest.TestCase):
    def test_field_is_num(self):
        fields = {'req_bytes': 'integer', 'resp_bytes': 'float', 'status': 'string'}
        
        self.assertTrue(field_is_num('req_bytes', fields))
        self.assertTrue(field_is_num('resp_bytes', fields))
        self.assertFalse(field_is_num('status', fields))
        self.assertFalse(field_is_num('non_existent_field', fields))

    def test_generate_fields_string(self):
        fields = {'req_bytes': 'integer', 'resp_bytes': 'float', 'status': 'string'}

        expected_result = '\tmean("req_bytes") as "req_bytes",\n\tmean("resp_bytes") as "resp_bytes"'
        self.assertEqual(generate_fields_string(fields), expected_result)

        fields = {'status': 'string'}
        expected_result = ''
        self.assertEqual(generate_fields_string(fields), expected_result)

        fields = {}
        expected_result = ''
        self.assertEqual(generate_fields_string(fields), expected_result)

    def test_generate_group_by_clause(self):
        tags = ['tag1', 'tag2', 'tag3']
        interval = '5m'
        expected_result = 'time(5m), tag1, tag2, tag3'
        self.assertEqual(generate_group_by_string(tags, interval), expected_result)

        tags = []
        expected_result = 'time(5m)'
        self.assertEqual(generate_group_by_string(tags, interval), expected_result)

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
        with self.assertRaises(ValueError):
            parse_interval('0m')

class TestNowANdThen(unittest.TestCase):
    def test_get_then_boundaries(self):
        test_time = datetime(2023,7,7,12,0)
        then = get_then(1,"m",test_time)
        self.assertEqual(then, datetime(2023,7,7,11,59))

        test_time = datetime(2023,7,7,0,0)
        then = get_then(1,"m",test_time)
        self.assertEqual(then, datetime(2023,7,6,23,59))

        test_time = datetime(2023,7,7,12,0)
        then = get_then(1,"m",test_time)
        self.assertEqual(then, datetime(2023,7,7,11,59))

class TestGetNextRunTime(unittest.TestCase):
    def test_get_with_previous_hour(self):
        test_cases = [{"interval_val":1, "interval_type":"h","run_prev":True,"expected_hour":8,
                       "interval_val":1, "interval_type":"h","run_prev":True,"expected_hour":7,
                       }]
        
        for tc in test_cases:
            test_time = datetime(2023,7,7,7,7)
            t = get_next_run_time(tc["interval_val"], tc["interval_type"], now=test_time, run_previous=tc["run_prev"] )
            self.assertEqual(t.hour, tc["expected_hour"])

    def test_get_with_previous_minutes(self):
        test_cases = [{"interval_val":10, "interval_type":"m","run_prev":True,"expected_minute":20
                       }]
        
        for tc in test_cases:
            test_time = datetime(2023,7,7,7,29)
            t = get_next_run_time(tc["interval_val"], tc["interval_type"], now=test_time, run_previous=tc["run_prev"] )
            self.assertEqual(t.minute, tc["expected_minute"])

    def test_hours(self):
        test_cases = [{"clock_minute":1, "clock_hour":1, "expected_hour":2}]
        for tc in test_cases:
            test_time = datetime(2023,7,7, tc["clock_hour"], tc["clock_minute"])
            next_run_time = get_next_run_time_hours(1, now=test_time)
            self.assertTrue(next_run_time, datetime(2023, 7, tc["expected_hour"], 0))

    def test_days(self):
        test_cases = [{"clock_day": 7, "expected_day":8}]
        for tc in test_cases:
            test_time = datetime(2023,7,tc["clock_day"], 7)
            next_run_time = get_next_run_time_hours(1, now=test_time)
            self.assertTrue(next_run_time, datetime(2023, 7, 8, 0, 0))

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

class TestWriteData(unittest.TestCase):
    def test_write_downsampled_data(self):
        # create a mock write client
        mock_target_client = Mock()
        mock_target_client.write = MagicMock()

        # Create a mock dataframe
        mock_df = MagicMock()
        mock_df.columns = []
        mock_df.shape = (0, 0)
        mock_df.__getitem__.return_value = False
        mock_df.drop.return_value = mock_df

        # Create a mock batch
        mock_batch = Mock()
        mock_batch.to_pandas.return_value = mock_df

        # Setup the mock batch to return a mock dataframe
        mock_batch.to_pandas.return_value = mock_df

        # Create a mock reader
        mock_reader = Mock()
        mock_reader.read_chunk.side_effect = [
            # Return a mock batch and buffer on the first call
            (mock_batch, None),
            # Raise StopIteration on the second call to end the loop
            StopIteration()
        ]

        with patch('main.target_client', new=mock_target_client):
            success, error, row_count, retries = write_downsampled_data(mock_reader)

        # Verify the result
        assert success
        assert row_count == 0
        assert retries == 0
        assert error is None

        # Verify the mocks were called as expected
        mock_reader.read_chunk.assert_called()
        mock_batch.to_pandas.assert_called()
        mock_target_client.write.assert_called()
        
if __name__ == "__main__":
    unittest.main()