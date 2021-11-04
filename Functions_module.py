import pandas as pd
from functools import wraps
import datetime as dt

# Function that makes logs in the pipeline
def log_step(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = dt.datetime.now()
        result = func(*args, **kwargs)
        time_taken = str(dt.datetime.now() - tic)
        print(f"just ran step {func.__name__} shape={result.shape} took {time_taken}s")
        return result
    return wrapper

# Function that returns copy of DataFrame
@log_step
def start_pipeline(df):
    return df.copy()

# Function that drops columns from a list of columns
@log_step
def drop_columns(df, columns):
    return df.drop(columns,axis=1)

#Function that returns number of missing values
def check_missing_values(data): 
    print('Missing values:' + '\n' + str(data.isna().sum()))

#Function that returns number of duplicated rows
def check_duplicates(data): 
    print('Duplicated rows: ', data.duplicated().sum())

def check_table(df):
    check_missing_values(df)
    check_duplicates(df)
    df.info()
    
@log_step
#Function that returns rows with notNaN values in 'column'
def select_notNan_rows(df,columns):
    for col in columns:
        df = df[df[col].notna()]
    return df

@log_step
#Function that drops duplicates
def drop_duplicates_custom(df,columns=[]):
    if(len(columns) > 0):
        return df.drop_duplicates(columns)
    else:
        return df.drop_duplicates()

#Function for cleaning price and promo_price columns
@log_step
def clean_prices(p,columns):
    for col in columns:
        p[col] = p[col].apply(lambda x : x +'.00' if x.count('.') == 0 else x)
        p[col] = p[col].apply(lambda x: x  + '0' if x[-2]=='.' else x)
        p[col] = p[col].apply(lambda x: str(float(x.replace('.',''))/1000) if ( (x[-4]=='.') & (x.count('.')==2)) else x)
        p[col] = p[col].apply(lambda x: str(float(x.replace('.',''))/10000) if ( (x[-4]=='.') & (x.count('.')==1)) else x)
        p[col] = p[col].astype(float)
    return p






# @log_step
# def clean_price(p):
#     p.price = p.price.apply(lambda x : x +'.00' if x.count('.') == 0 else x)
#     p.price = p.price.apply(lambda x: x  + '0' if x[-2]=='.' else x)
#     p.price = p.price.apply(lambda x: str(float(x.replace('.',''))/1000) if ( (x[-4]=='.') & (x.count('.')==2)) else x)
#     p.price = p.price.apply(lambda x: str(float(x.replace('.',''))/10000) if ( (x[-4]=='.') & (x.count('.')==1)) else x)
#     p.price = p.price.astype(float)
#     return p

# @log_step
# def clean_promo_price(p):
#     p.promo_price = p.promo_price.apply(lambda x : x +'.00' if x.count('.') == 0 else x)
#     p.promo_price = p.promo_price.apply(lambda x: x  + '0' if x[-2]=='.' else x)
#     p.promo_price = p.promo_price.apply(lambda x: str(float(x.replace('.',''))/1000) if ( (x[-4]=='.') & (x.count('.')==2)) else x)
#     p.promo_price = p.promo_price.apply(lambda x: str(float(x.replace('.',''))/10000) if ( (x[-4]=='.') & (x.count('.')==1)) else x)
#     p.promo_price = p.promo_price.astype(float)
#     return p
