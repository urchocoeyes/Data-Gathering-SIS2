import pandas as pd
import sqlite3
import os

def create_database():
    
    conn = sqlite3.connect('data/football.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS matches (
        match_id TEXT PRIMARY KEY,
        match_date TEXT,
        date_parsed TEXT,
        home_team TEXT NOT NULL,
        away_team TEXT NOT NULL,
        home_goals INTEGER NOT NULL,
        away_goals INTEGER NOT NULL,
        score TEXT NOT NULL,
        total_goals INTEGER,
        goal_difference INTEGER,
        result TEXT CHECK(result IN ('H', 'A', 'D')),
        competition TEXT,
        scraped_at TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON matches(date_parsed)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_home_team ON matches(home_team)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_away_team ON matches(away_team)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_competition ON matches(competition)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_result ON matches(result)')
    
    conn.commit()
    conn.close()
    
    print("Database schema created")

def load_data():
    
    if not os.path.exists("data/clean_matches.csv"):
        print("Clean data not found. Running cleaner...")
        from cleaner import clean_matches
        df = clean_matches()
    else:
        df = pd.read_csv("data/clean_matches.csv")
    
    conn = sqlite3.connect('data/football.db')
    
    df.to_sql('matches', conn, if_exists='replace', index=False)
    
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM matches')
    total_matches = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT 
            result,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM matches), 1) as percentage
        FROM matches
        GROUP BY result
        ORDER BY count DESC
    ''')
    results = cursor.fetchall()
    
    cursor.execute('''
        SELECT 
            competition,
            COUNT(*) as match_count
        FROM matches
        GROUP BY competition
        ORDER BY match_count DESC
    ''')
    competitions = cursor.fetchall()
    
    cursor.execute('SELECT MIN(date_parsed), MAX(date_parsed) FROM matches')
    date_range = cursor.fetchone()
    
    conn.close()
    
    print(f"\nDATABASE LOADING REPORT")
    print(f"   Total matches loaded: {total_matches}")
    
    print(f"\n   Result distribution:")
    for result, count, percentage in results:
        result_name = {'H': 'Home Win', 'A': 'Away Win', 'D': 'Draw'}.get(result, result)
        print(f"     {result_name}: {count} ({percentage}%)")
    
    print(f"\n   Competition distribution:")
    for competition, count in competitions[:5]:  # Top 5
        print(f"     {competition}: {count} matches")
    
    print(f"\n   Date range: {date_range[0]} to {date_range[1]}")
    
    return total_matches

if __name__ == "__main__":
    create_database()
    count = load_data()
    
    if count >= 100:
        print(f"\nSUCCESS! {count} matches loaded into database")
        print(f"Database file: data/football.db")
        print(f"\nVerify with: sqlite3 data/football.db")
        print(f"   Commands: SELECT COUNT(*) FROM matches;")
        print(f"             SELECT * FROM matches LIMIT 3;")
    else:
        print(f"\n FAILED: Only {count} matches loaded (minimum 100 required)")