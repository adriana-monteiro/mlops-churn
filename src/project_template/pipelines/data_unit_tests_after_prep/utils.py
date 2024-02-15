# Function to check if one hot encoding is done correctly.
def one_hot_check(df_eng, df_enc, col):
    value_counts = df_eng[col].value_counts()
    value_counts_dict = value_counts.to_dict()
    encoded_counts = {}

    for key, value in value_counts_dict.items():
        if isinstance(key, str):
            encoded_key = f'{col}_{key.strip()}'
        else:
            encoded_key = f'{col}_{key}'
        encoded_counts[encoded_key] = value


    for key, value in encoded_counts.items():
        if key in df_enc.columns:
            assert value == df_enc[key].sum()

    return 0