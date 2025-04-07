import pandas as pd
import re

def main():
    # Read the CSV file
    df = pd.read_csv('data/raw_data.csv')
    df.to_csv('1_bronze/bronze.csv', index=False)

if __name__ == "__main__":
    main()