import pandas as pd
import ast

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv('../resources/languages.csv')

# Parse the JSON-encoded strings in the language column
df['language'] = df['language'].apply(ast.literal_eval)

# Expand the language column into separate columns
df_expanded = pd.json_normalize(df['language'])

# Concatenate the original DataFrame with the normalized DataFrame
result_df = pd.concat([df['repo_name'], df_expanded], axis=1)

# Parse the data of language name and language bytes
result_df['language_name'] = result_df['language'].apply(lambda x: [item['name'] for item in x] if x else [])
result_df['language_bytes'] = result_df['language'].apply(lambda x: [item['bytes'] for item in x] if x else [])

# Drop the language column
final = result_df.drop(columns=['language'], axis=1)

# Store clean data into a CSV file in resources folder
final.to_csv('../resources/languages.csv', index=False, sep=',')

print(final)
