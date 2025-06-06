import pandas as pd
from sqlalchemy import create_engine, text

CSV_PATH = r'C:\Users\rahel\Downloads\archive\top_anime_dataset.csv'  
DB_CONNECTION = 'mysql+mysqlconnector://root:root@localhost/anime_db'

# Učitaj podatke
df = pd.read_csv(CSV_PATH)
df = df.drop_duplicates(subset=['anime_id'])
print("CSV učitan i očišćen")

# Spoji se na bazu
engine = create_engine(DB_CONNECTION)
print("Spojeno na bazu")

# RESETIRAJ CIJELU BAZU
with engine.connect() as conn:
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
    conn.execute(text("DROP DATABASE IF EXISTS anime_db;"))
    conn.execute(text("CREATE DATABASE anime_db;"))
    conn.execute(text("USE anime_db;"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

# 1. GLAVNA TABLICA - SVI GLAVNI ATRIBUTI
main_cols = [
    'anime_id', 'name', 'english_name', 'japanese_names', 
    'score', 'type', 'episodes', 'premiered', 'source', 
    'duration', 'rating', 'rank', 'popularity', 'favorites', 
    'scored_by', 'members', 'synopsis'
]
df[main_cols].to_sql('anime', engine, if_exists='replace', index=False)

with engine.connect() as conn:
    conn.execute(text("ALTER TABLE anime ADD PRIMARY KEY (anime_id);"))
print("✓ Glavna anime tablica kreirana sa svim atributima")

# 2. URL TABLICE (dodatne informacije)
df[['anime_id', 'anime_url', 'image_url']].to_sql(
    'anime_urls', 
    engine, 
    if_exists='replace', 
    index=False
)
print("✓ URL tablica kreirana")

# 3. ŽANROVI
genres_data = []
for _, row in df.iterrows():
    if pd.notna(row['genres']):
        for genre in str(row['genres']).split(','):
            genres_data.append({
                'anime_id': row['anime_id'],
                'genre': genre.strip()
            })

pd.DataFrame(genres_data).to_sql('anime_genre', engine, if_exists='replace', index=False)
print("✓ Žanrovi kreirani")

# 4. PRODUCENTI
producers_data = []
for _, row in df.iterrows():
    if pd.notna(row['producers']):
        for producer in str(row['producers']).split(','):
            producers_data.append({
                'anime_id': row['anime_id'],
                'producer': producer.strip()
            })

pd.DataFrame(producers_data).to_sql('anime_producer', engine, if_exists='replace', index=False)
print("✓ Producenti kreirani")

# 5. STUDIJI
studios_data = []
for _, row in df.iterrows():
    if pd.notna(row['studios']):
        for studio in str(row['studios']).split(','):
            studios_data.append({
                'anime_id': row['anime_id'],
                'studio': studio.strip()
            })

pd.DataFrame(studios_data).to_sql('anime_studio', engine, if_exists='replace', index=False)
print("✓ Studiji kreirani")

# 6. TIPOVI (detaljnije)
df[['anime_id', 'type', 'source']].to_sql('anime_type', engine, if_exists='replace', index=False)
print("✓ Tipovi kreirani")

# 7. STATISTIKE (ocjene, popularnost, favoriti)
stats_cols = ['anime_id', 'score', 'rank', 'popularity', 'favorites', 'scored_by', 'members']
df[stats_cols].to_sql('anime_stats', engine, if_exists='replace', index=False)
print("✓ Statistike kreirane")

# 8. POSTAVI FOREIGN KEYEVE
with engine.connect() as conn:
    # URLs
    conn.execute(text("""
        ALTER TABLE anime_urls 
        ADD CONSTRAINT fk_urls_anime 
        FOREIGN KEY (anime_id) REFERENCES anime(anime_id) 
        ON DELETE CASCADE
    """))
    
    # Žanrovi
    conn.execute(text("""
        ALTER TABLE anime_genre 
        ADD CONSTRAINT fk_genre_anime 
        FOREIGN KEY (anime_id) REFERENCES anime(anime_id) 
        ON DELETE CASCADE
    """))
    
    # Producenti
    conn.execute(text("""
        ALTER TABLE anime_producer 
        ADD CONSTRAINT fk_producer_anime 
        FOREIGN KEY (anime_id) REFERENCES anime(anime_id) 
        ON DELETE CASCADE
    """))
    
    # Studiji
    conn.execute(text("""
        ALTER TABLE anime_studio 
        ADD CONSTRAINT fk_studio_anime 
        FOREIGN KEY (anime_id) REFERENCES anime(anime_id) 
        ON DELETE CASCADE
    """))
    
    # Tipovi
    conn.execute(text("""
        ALTER TABLE anime_type 
        ADD CONSTRAINT fk_type_anime 
        FOREIGN KEY (anime_id) REFERENCES anime(anime_id) 
        ON DELETE CASCADE
    """))
    
    # Statistike
    conn.execute(text("""
        ALTER TABLE anime_stats 
        ADD CONSTRAINT fk_stats_anime 
        FOREIGN KEY (anime_id) REFERENCES anime(anime_id) 
        ON DELETE CASCADE
    """))

print("✅ SVI FOREIGN KEYEVI POSTAVLJENI!")

# PROVJERA VEZA
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT 
            TABLE_NAME,
            CONSTRAINT_NAME,
            REFERENCED_TABLE_NAME
        FROM 
            information_schema.KEY_COLUMN_USAGE 
        WHERE 
            REFERENCED_TABLE_SCHEMA = 'anime_db'
            AND REFERENCED_TABLE_NAME IS NOT NULL;
    """))
    
    print("\n🔗 AKTIVNE VEZE:")
    for row in result:
        print(f"   {row[0]} → {row[2]} ({row[1]})")

print(f"\n✅ BAZA KREIRANA S {len(df.columns)} STUPACA PODIJELJENIH U 7 TABLICA!")
