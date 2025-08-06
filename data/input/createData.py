import pandas as pd


def create_data():
    """
    Function used to create sample data used for testing
    parquet_merger
    """

    data = {
        'Client':   ['A', 'B', 'C'],
        'Product':  ['Notebook', 'Pencil', 'Paper'],
        'Quantity': [1, 40, 30]
    }

    df = pd.DataFrame(data)
    df.to_parquet('client.parquet')

    data = {
        'Client':   ['D', 'E', 'F'],
        'Product':  ['Monitor', 'Chair', 'Pencil'],
        'Quantity': [2, 5, 300]
    }

    df = pd.DataFrame(data)
    df.to_parquet('client_2.parquet')

    data = {
        'Client_id':     [1, 4, 5],
        'Spent':         [400, 200, 100],
        'Last_purchase': ['10/04/2020', '04/12/2021', '30/06/2022']
    }

    df = pd.DataFrame(data)
    df.to_parquet('client_info.parquet')


if __name__ == '__main__':
    create_data()
