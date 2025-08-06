import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from ParquetMerger import parquet_merger


class TestMerger(unittest.TestCase):

    def test_parameter(self):
        # Case 1: Empty list
        self.assertRaises(ValueError,
                          parquet_merger,
                          input_paths=[],
                          output_path='../data/output/output.parquet')

        # Case 2: Empty output
        self.assertRaises(ValueError,
                          parquet_merger,
                          input_paths=['../data/input/client.parquet'],
                          output_path='')

    def test_schema(self):
        root = '../data/input/'

        list_ = [root + 'client.parquet', root + 'client_2.parquet']
        output = '../data/output/output.parquet'

        # Case 1: Match between files
        self.assertEqual(parquet_merger(list_, output),
                         None)

        # Case 2: Non-match between files
        list_.append(root + 'client_info.parquet')
        self.assertRaises(ValueError,
                          parquet_merger,
                          input_paths=list_,
                          output_path=output)

    def test_result(self):
        root = '../data/input/'

        list_ = [root + 'client.parquet',
                 root + 'client_2.parquet']
        output = '../data/output/output.parquet'

        parquet_merger(list_, output)

        df = pd.read_parquet(output)

        data = {
            'Client':   ['A', 'B', 'C', 'D', 'E', 'F'],
            'Product':  ['Notebook', 'Pencil', 'Paper',
                         'Monitor', 'Chair', 'Pencil'],
            'Quantity': [1, 40, 30, 2, 5, 300]
        }
        expected = pd.DataFrame(data)

        try:
            assert_frame_equal(expected, df)
        except AssertionError as e:
            print(f'Result error: {e}')


if __name__ == '__main__':
    unittest.main()
