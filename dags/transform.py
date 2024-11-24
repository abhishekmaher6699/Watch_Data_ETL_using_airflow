import pandas as pd
import sys
import re

def transform_data(data):
    try:
        df = pd.DataFrame(data)
        df['price'] = df['price'].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
        df['Case Size'] = df['Case Size'].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
        df['Limited Edition'] = df['Limited Edition'].apply(lambda x: 1 if x == 'Yes' else 0)
        df['Frequency (bph)'] = df['Frequency'].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
        df['Lug Width'] = df['Lug Width'].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
        df['Power Reserve (hours)'] = df['Power Reserve'].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
        df['Case Thickness'] = df['Case Thickness'].apply(lambda x: re.sub(r'[^\d.]', '', str(x)))
        df['Interchangeable Strap'] = df['Interchangeable Strap'].apply(lambda x: 1 if x == 'Yes' else 0)

        csv_data = df.to_csv(index=False)
        if not csv_data:
            raise ValueError("CSV conversion resulted in empty data")
        
        return csv_data
    
    except Exception as e:
        raise