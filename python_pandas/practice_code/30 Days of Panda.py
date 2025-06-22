# https://leetcode.com/studyplan/30-days-of-pandas/

# Category 1: Data Filtering

# Day 1: Big Countries
import pandas as pd

def big_countries(world: pd.DataFrame) -> pd.DataFrame:
    return world[(world['area'] >= 3000000) | (world['population'] >= 25000000)][['name','population','area']]

# Day 2: Recyclable and Low Fat Products
import pandas as pd

def find_products(products: pd.DataFrame) -> pd.DataFrame:
    return products[(products['low_fats'] == 'Y') & (products['recyclable'] == 'Y')][['product_id']]

# Day 3: Customers Who Never Order
import pandas as pd

def find_customers(customers: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(customers,orders, left_on='id',right_on='customerId', how='left')
    merged_name = merged[merged['id_y'].isna()][['name']].rename(columns={'name':'Customers'})
    return merged_name


# Day 4: Article Views I
import pandas as pd

def article_views(views: pd.DataFrame) -> pd.DataFrame:
    same = views[views['author_id'] == views['viewer_id']]['author_id'].unique()
    new = pd.DataFrame(data=same, columns=['id']).sort_values(by=['id'])
    return new

    # OR

import pandas as pd
def article_views(views: pd.DataFrame) -> pd.DataFrame:
    return (
        views.loc[views['author_id'] == views['viewer_id'], 'author_id']
        .drop_duplicates()
        .to_frame(name='id')
        .sort_values('id')
        .reset_index(drop=True)
    )

# Category 2: 