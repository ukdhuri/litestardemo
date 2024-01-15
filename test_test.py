import pandas as pd
import unittest

from test import compare_hash_values

class TestCompareHashValues(unittest.TestCase):
    def test_compare_hash_values(self):
        # Create sample dataframes
        df1 = pd.DataFrame({'ConcatenatedKeys': ['A', 'B', 'C', 'D'],
                            'HashValue': [1, 2, 3, 4]})
        df2 = pd.DataFrame({'ConcatenatedKeys': ['A', 'B', 'C', 'D'],
                            'HashValue': [1, 7, 3, 4]})
        
        # Call the function
        result = compare_hash_values(df1, df2)

        # Expected output
        expected_output = pd.DataFrame({'ConcatenatedKeys': ['A', 'E'],
                                        'Value1': [1, None],
                                        'Value2': [None, 5]})

    

        # Assert the result
        self.assertTrue(result.equals(expected_output))

if __name__ == '__main__':
    unittest.main()