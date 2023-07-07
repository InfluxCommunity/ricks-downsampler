import unittest
from main import parse_interval

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

if __name__ == "__main__":
    unittest.main()