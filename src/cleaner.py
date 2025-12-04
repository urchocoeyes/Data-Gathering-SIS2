import pandas as pd
import os

def clean_matches():
    
    if not os.path.exists("data/raw_matches.csv"):
        print("Raw data not found. Running scraper...")
        from scraper import scrape_matches
        df = scrape_matches()
    else:
        df = pd.read_csv("data/raw_matches.csv")
    
    print(f"Raw matches: {len(df)}")
    
    df = df.drop_duplicates()
    print(f"   After removing duplicates: {len(df)}")
    
    df = df.dropna(subset=['home_team', 'away_team', 'score'])
    df = df[df['home_team'] != '']
    df = df[df['away_team'] != '']
    
    def parse_score(score):
        try:
            if '-' in score:
                home, away = score.split('-')
            elif ':' in score:
                home, away = score.split(':')
            else:
                return 0, 0
            return int(home.strip()), int(away.strip())
        except:
            return 0, 0
    
    scores = df['score'].apply(parse_score)
    df['home_goals'] = scores.apply(lambda x: x[0])
    df['away_goals'] = scores.apply(lambda x: x[1])
    
    df['result'] = 'D'  # Draw
    df.loc[df['home_goals'] > df['away_goals'], 'result'] = 'H'  # Home win
    df.loc[df['away_goals'] > df['home_goals'], 'result'] = 'A'  # Away win
    
    df['total_goals'] = df['home_goals'] + df['away_goals']
    df['goal_difference'] = df['home_goals'] - df['away_goals']
    
    try:
        df['date_parsed'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    except:
        from datetime import datetime
        df['date_parsed'] = datetime.now()
    
    if 'match_id' not in df.columns:
        df['match_id'] = df.apply(
            lambda row: f"MATCH_{row.name:04d}",
            axis=1
        )
    
    df['home_team'] = df['home_team'].str.strip()
    df['away_team'] = df['away_team'].str.strip()
    
    df['competition'] = df['competition'].fillna('Premier League')
    
    final_columns = [
        'match_id', 'date', 'date_parsed', 'home_team', 'away_team',
        'home_goals', 'away_goals', 'score', 'total_goals', 'goal_difference',
        'result', 'competition', 'scraped_at'
    ]
    
    df = df[final_columns]
    
    df.to_csv("data/clean_matches.csv", index=False)
    
    print("\nCleaning Summary:")
    print(f"   Total matches: {len(df)}")
    print(f"   Home wins: {(df['result'] == 'H').sum()}")
    print(f"   Away wins: {(df['result'] == 'A').sum()}")
    print(f"   Draws: {(df['result'] == 'D').sum()}")
    print(f"   Average goals: {df['total_goals'].mean():.2f}")
    
    return df

if __name__ == "__main__":
    df = clean_matches()
    print("\nCleaning complete!")
    print("\nSample data:")
    print(df[['date', 'home_team', 'score', 'away_team', 'result']].head())