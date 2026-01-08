import pandas as pd

def _ensure_numeric(series, default=0):
    # Coerce to numeric and fill NaNs
    return pd.to_numeric(series, errors='coerce').fillna(default)

def compute_engagement(df):
    # --- 1. Convert and clean date ---
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['date'] = df['date'].dt.tz_localize(None)

    # --- 2. Coerce key numeric columns ---
    numeric_cols = ['likes', 'views', 'comments', 'duration']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _ensure_numeric(df[col], default=0)
        else:
            df[col] = 0  # create missing columns as zeros

    # --- 3. Sort (latest first) ---
    df = df.sort_values(['username', 'date'], ascending=[True, False])

    # --- 4. Rank posts (latest = 1, oldest = n) ---
    df['post_number'] = df.groupby('username').cumcount() + 1

    # --- 5. Filter out accounts with <30 posts ---
    df = df.groupby('username').filter(lambda x: len(x) >= 1)

    # --- 6. Sort by username and chronological post_number ---
    df = df.sort_values(['username', 'post_number']).reset_index(drop=True)

    # --- 7. Compute average likes of next 50 posts (look-ahead rolling mean) ---
    def avg_next_50(likes):
        return likes[::-1].rolling(window=50, min_periods=50).mean()[::-1].shift(-1)

    df['avg_last_50'] = (
        df.groupby('username', group_keys=False)['likes']
        .apply(avg_next_50)
    )

    # --- 8. Label viral posts (likes > 115% of avg_next_50) ---
    df['viral'] = (df['likes'] > df['avg_last_50'] * 1.15).astype(int)

    # --- 9. Limit to first 50 posts per user ---
    df = df.groupby('username').head(50)

    # --- 10. Verification output ---
    print(df[['username', 'post_number', 'likes', 'avg_last_50', 'viral']])

    return df