
import pandas as pd
import json
import ast

df = pd.read_csv('./resources/commits.csv')


# Define a function to extract values from the "author" column
def extract_author_info(row):
    try:
        author_dict = json.loads(row['author'])
        return pd.Series({
            'author_name': author_dict['author']['name'],
            'author_email': author_dict['author']['email'],
            'author_time_sec': author_dict['author']['time_sec'],
            'tz_offset': author_dict['author'].get('tz_offset', None),
            'date_seconds': author_dict['author']['date'].get('seconds', None),
            'date_nanos': author_dict['author']['date'].get('nanos', None)
        })
    except (json.JSONDecodeError, KeyError):
        # Handle errors if the JSON structure is not as expected
        return pd.Series({
            'author_name': None,
            'author_email': None,
            'author_time_sec': None,
            'tz_offset': None,
            'date_seconds': None,
            'date_nanos': None
        })


# Apply the function to create new columns
author_info_df = df.apply(extract_author_info, axis=1)

# Concatenate the original DataFrame with the new columns
df = pd.concat([df, author_info_df], axis=1)

# Drop the original "author" column if needed
df = df.drop('author', axis=1)


# Define a function to extract values from the "committer" column
def extract_committer_info(row):
    try:
        committer_dict = json.loads(row['committer'])
        return pd.Series({
            'committer_name': committer_dict['committer']['name'],
            'committer_email': committer_dict['committer']['email'],
            'committer_time_sec': committer_dict['committer']['time_sec'],
            'committer_tz_offset': committer_dict['committer'].get('tz_offset', None),
            'committer_date_seconds': committer_dict['committer']['date'].get('seconds', None),
            'committer_date_nanos': committer_dict['committer']['date'].get('nanos', None)
        })
    except (json.JSONDecodeError, KeyError):
        # Handle errors if the JSON structure is not as expected
        return pd.Series({
            'committer_name': None,
            'committer_email': None,
            'committer_time_sec': None,
            'committer_tz_offset': None,
            'committer_date_seconds': None,
            'committer_date_nanos': None
        })

# Apply the function to create new columns for committer information
committer_info_df = df.apply(extract_committer_info, axis=1)

# Concatenate the original DataFrame with the new committer columns
df = pd.concat([df, committer_info_df], axis=1)

# Drop the original "committer" column if needed
df = df.drop('committer', axis=1)

# Save the updated DataFrame to a new CSV file
df.to_csv('./resources/commits.csv', index=False)

print(df)