import pandas as pd
import re

def main():
    # Read the CSV file
    df = pd.read_csv('data/raw_data.csv')
    df = data_cleaning(df)
    df.to_csv('data/bronze/raw_data_bronze.csv', index=False)

def data_cleaning(df):

    # Columns treatment
    df['email'] = df['email'].apply(email_preprocess)
    df['name'] = df['name'].apply(name_preprocess)
    df['date_of_birth'] = df['date_of_birth'].apply(date_preprocess)
    df['signup_date'] = df['signup_date'].apply(date_preprocess)

    # Merge duplicate emails
    df = merge_duplicate_emails(df)

    # Drop rows with invalid emails
    df = df.dropna(subset=['email'])

    # Order df by 'signup_date'
    df = df.sort_values(by='signup_date')

    return df

def name_preprocess(name):
    """
    Cleans the name by removing leading and trailing spaces and converting to lowercase.
    """
    if isinstance(name, str):
        name = name.strip().lower()
        return name
    return None  # Handle non-string inputs

def date_preprocess(date):
    """
    Converts date strings to datetime objects.
    """
    if isinstance(date, pd.Timestamp):
        return date
    
    if isinstance(date, str):
        try:
            return pd.to_datetime(date, errors='coerce')
        except ValueError:
            return None  # Handle invalid date formats
        
    return None  # Handle non-string inputs

def email_preprocess(email):
    email = email.strip().lower()
    email =  email_at_fill(email)
    email = email_dotcom_fill(email)
    return email

def email_at_fill(email):
    """
    Fixes emails by adding an "@" before "example", "gmail", "hotmail", "outlook", "yahoo" if it's missing.
    """
    if isinstance(email, str):
        domains = ["example", "gmail", "hotmail", "outlook", "yahoo"]
        for domain in domains:
            if domain in email and "@" not in email:
                email = email.replace(domain, "@" + domain)
        # Basic email validation using regex
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            return None  # Or handle invalid emails as needed
        return email
    return None  # Handle non-string inputs

def email_dotcom_fill(email):
    """
    Appends '.com' to emails if they are missing it.
    """
    if isinstance(email, str):
        if ".com" not in email:
            # Basic check to avoid adding .com to already valid emails
            if re.match(r"[^@]+@[^@]+\.[^@]+", email):
                return email
            elif "@" in email:
                email = email.strip()
                email = email + ".com"
        return email
    return None

def merge_duplicate_emails(df):
    """
    Merges records with duplicate emails, filling missing data from duplicates, and removes duplicates.
    """
    # Find duplicate emails
    duplicate_emails = df[df['email'].duplicated(keep=False)]

    # Iterate through duplicate emails
    for email in duplicate_emails['email'].unique():
        # Get all rows with the same email
        email_group = df[df['email'] == email]

        if len(email_group) > 1:
            # Get the index of the first row (the one we'll merge into)
            first_index = email_group.index[0]

            # Iterate through the other rows and fill NaN values in the first row
            for index in email_group.index[1:]:
                for col in df.columns:
                    if pd.isna(df.loc[first_index, col]) and not pd.isna(df.loc[index, col]):
                        df.loc[first_index, col] = df.loc[index, col]

    # Remove duplicate rows based on email
    df = df.drop_duplicates(subset=['email'], keep='first')

    return df

if __name__ == "__main__":
    main()