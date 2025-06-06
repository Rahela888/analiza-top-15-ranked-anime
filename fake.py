import subprocess
import sys

# Instaliraj potrebne pakete
packages = ['sqlalchemy', 'pymysql', 'pandas']
for package in packages:
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, ForeignKey, Float, Boolean

# --- Database setup ---
engine = create_engine('mysql+pymysql://root:root@localhost:3306/anime_warehouse')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# ---------------------
# POPRAVLJENE DEFINICIJE KLASA SA FOREIGN KEY KOLONAMA
# ---------------------
class DimGenre(Base):
    __tablename__ = 'dim_genre'
    genre_id = Column(Integer, primary_key=True, autoincrement=True)
    genre_name = Column(String(100), unique=True)
    category = Column(String(50))

class DimStudio(Base):
    __tablename__ = 'dim_studio'
    studio_id = Column(Integer, primary_key=True, autoincrement=True)
    studio_name = Column(String(200), unique=True)
    country = Column(String(50), default='Japan')

class DimProducer(Base):
    __tablename__ = 'dim_producer'
    producer_id = Column(Integer, primary_key=True, autoincrement=True)
    producer_name = Column(String(200), unique=True)
    type = Column(String(50), default='Production')

class DimTime(Base):
    __tablename__ = 'dim_time'
    time_id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, unique=True)
    decade = Column(String(10))
    era = Column(String(20))

class DimRating(Base):
    __tablename__ = 'dim_rating'
    rating_id = Column(Integer, primary_key=True)
    score_range = Column(String(20))
    category = Column(String(30))

# POPRAVLJENO: DimAnime SA FOREIGN KEY KOLONAMA
class DimAnime(Base):
    __tablename__ = 'dim_anime'
    anime_id = Column(BigInteger, primary_key=True)
    name = Column(String(500))
    type = Column(String(50))
    episodes = Column(Integer)
    status = Column(String(50))
    aired = Column(String(100))
    source = Column(String(50))
    duration = Column(String(50))
    rating = Column(String(50))
    score = Column(Float)
    members = Column(Integer)
    
    # DODANO: FOREIGN KEY KOLONE U DIM_ANIME
    genre_id = Column(Integer, ForeignKey('dim_genre.genre_id'), nullable=True)
    studio_id = Column(Integer, ForeignKey('dim_studio.studio_id'), nullable=True)
    producer_id = Column(Integer, ForeignKey('dim_producer.producer_id'), nullable=True)
    time_id = Column(Integer, ForeignKey('dim_time.time_id'), nullable=True)
    rating_id = Column(Integer, ForeignKey('dim_rating.rating_id'), nullable=True)

# FACT TABELA
class FactAnimeStats(Base):
    __tablename__ = 'fact_anime_stats'
    fact_id = Column(BigInteger, primary_key=True, autoincrement=True)
    anime_id = Column(BigInteger, ForeignKey('dim_anime.anime_id'), nullable=False)
    score = Column(Float)
    scored_by = Column(Integer)
    favorites = Column(Integer)
    popularity = Column(Integer)
    rank = Column(Integer)
    load_date = Column(DateTime, default=datetime.now)

# OBRIŠI I KREIRAJ TABELE
print("Brišem postojeće tabele...")
try:
    session.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
    session.execute(text("DROP TABLE IF EXISTS fact_anime_stats"))
    session.execute(text("DROP TABLE IF EXISTS dim_anime"))
    session.execute(text("DROP TABLE IF EXISTS dim_rating"))
    session.execute(text("DROP TABLE IF EXISTS dim_time"))
    session.execute(text("DROP TABLE IF EXISTS dim_producer"))
    session.execute(text("DROP TABLE IF EXISTS dim_studio"))
    session.execute(text("DROP TABLE IF EXISTS dim_genre"))
    session.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
    session.commit()
    print("✓ Stare tabele obrisane")
except Exception as e:
    print(f"Greška pri brisanju: {e}")
    session.rollback()

# KREIRAJ NOVE TABELE
print("Kreiram nove tabele sa foreign key vezama...")
Base.metadata.create_all(engine)
session.commit()
print("✓ Nove tabele kreirane sa foreign key vezama!")

# Učitaj CSV ili kreiraj test podatke
try:
    df = pd.read_csv(r'C:\Users\rahel\Downloads\archive\top_anime_dataset.csv')
    print(f"✓ CSV učitan - {len(df)} redova")
    print("Dostupne kolone:", df.columns.tolist())
except FileNotFoundError:
    print("❌ CSV fajl nije pronađen! Kreiram test podatke...")
    df = pd.DataFrame({
        'anime_id': range(1, 101),
        'name': [f'Test Anime {i}' for i in range(1, 101)],
        'type': ['TV', 'Movie', 'OVA'] * 34,
        'episodes': [12, 24, 1] * 34,
        'score': [7.5 + (i % 30) * 0.1 for i in range(100)],
        'scored_by': [1000 + i * 100 for i in range(100)],
        'members': [10000 + i * 1000 for i in range(100)],
        'favorites': [100 + i * 50 for i in range(100)],
        'popularity': list(range(1, 101)),
        'rank': list(range(1, 101)),
        'genres': ['Action,Drama', 'Romance,Comedy', 'Adventure,Fantasy'] * 34,
        'studios': ['Studio A', 'Studio B', 'Studio C'] * 34,
        'producers': ['Producer X', 'Producer Y', 'Producer Z'] * 34
    })

# Helper funkcije
def safe_int(value, default=0):
    try:
        if pd.isna(value) or value == '':
            return default
        return int(float(str(value).replace(',', '')))
    except:
        return default

def safe_float(value, default=0.0):
    try:
        if pd.isna(value) or value == '':
            return default
        return float(str(value).replace(',', ''))
    except:
        return default

def get_year_from_data(row):
    for col in ['year', 'aired', 'premiered']:
        if col in row and pd.notna(row[col]):
            try:
                year_str = str(row[col])
                import re
                year_match = re.search(r'\b(19|20)\d{2}\b', year_str)
                if year_match:
                    return int(year_match.group())
            except:
                continue
    return 2020

def get_rating_category(score):
    if score >= 9.0: return 'Masterpiece'
    elif score >= 8.0: return 'Excellent'
    elif score >= 7.0: return 'Good'
    elif score >= 6.0: return 'Average'
    return 'Poor'

def get_or_create_genre(name):
    if not name or str(name).strip() == '':
        return None
    name = str(name).strip()[:100]
    try:
        obj = session.query(DimGenre).filter_by(genre_name=name).first()
        if not obj:
            obj = DimGenre(genre_name=name, category='General')
            session.add(obj)
            session.flush()
        return obj.genre_id
    except Exception as e:
        print(f"Greška pri kreiranju žanra {name}: {e}")
        session.rollback()
        return None

def get_or_create_studio(name):
    if not name or str(name).strip() == '':
        return None
    name = str(name).strip()[:200]
    try:
        obj = session.query(DimStudio).filter_by(studio_name=name).first()
        if not obj:
            obj = DimStudio(studio_name=name, country='Japan')
            session.add(obj)
            session.flush()
        return obj.studio_id
    except Exception as e:
        print(f"Greška pri kreiranju studija {name}: {e}")
        session.rollback()
        return None

def get_or_create_producer(name):
    if not name or str(name).strip() == '':
        return None
    name = str(name).strip()[:200]
    try:
        obj = session.query(DimProducer).filter_by(producer_name=name).first()
        if not obj:
            obj = DimProducer(producer_name=name, type='Production')
            session.add(obj)
            session.flush()
        return obj.producer_id
    except Exception as e:
        print(f"Greška pri kreiranju producenta {name}: {e}")
        session.rollback()
        return None

def get_or_create_time(year):
    try:
        obj = session.query(DimTime).filter_by(year=year).first()
        if not obj:
            obj = DimTime(
                year=year,
                decade=f"{year//10*10}s",
                era='Modern' if year >= 2000 else 'Classic'
            )
            session.add(obj)
            session.flush()
        return obj.time_id
    except Exception as e:
        print(f"Greška pri kreiranju vremena {year}: {e}")
        session.rollback()
        return None

def get_or_create_rating(score):
    rating_id = int(score) if score > 0 else 1
    try:
        obj = session.query(DimRating).filter_by(rating_id=rating_id).first()
        if not obj:
            obj = DimRating(
                rating_id=rating_id,
                score_range=f"{rating_id}.0-{rating_id+1}.0",
                category=get_rating_category(score)
            )
            session.add(obj)
            session.flush()
        return obj.rating_id
    except Exception as e:
        print(f"Greška pri kreiranju rating-a {rating_id}: {e}")
        session.rollback()
        return None

# ETL proces
print("Počinje ETL proces...")
anime_count = 0
fact_count = 0

for idx, row in df.iterrows():
    try:
        # 1. KREIRAJ DIMENZIJE PRVO
        year = get_year_from_data(row)
        time_id = get_or_create_time(year)
        
        score = safe_float(row.get('score', 7.0))
        rating_id = get_or_create_rating(score)
        
        # Genre
        genre_id = None
        if 'genres' in row and pd.notna(row['genres']):
            genres = str(row['genres']).split(',')
            if genres:
                genre_id = get_or_create_genre(genres[0].strip())
        
        # Studio
        studio_id = None
        if 'studios' in row and pd.notna(row['studios']):
            studios = str(row['studios']).split(',')
            if studios:
                studio_id = get_or_create_studio(studios[0].strip())
        
        # Producer
        producer_id = None
        if 'producers' in row and pd.notna(row['producers']):
            producers = str(row['producers']).split(',')
            if producers:
                producer_id = get_or_create_producer(producers[0].strip())

        # 2. KREIRAJ ANIME SA FOREIGN KEY VEZAMA
        anime_id = safe_int(row.get('anime_id', idx + 1))
        if anime_id == 0:
            anime_id = idx + 1
            
        existing_anime = session.query(DimAnime).filter_by(anime_id=anime_id).first()
        if not existing_anime:
            new_anime = DimAnime(
                anime_id=anime_id,
                name=str(row.get('name', f'Anime {anime_id}'))[:500],
                type=str(row.get('type', 'TV'))[:50],
                episodes=safe_int(row.get('episodes', 12)),
                status=str(row.get('status', 'Finished'))[:50],
                aired=str(row.get('aired', ''))[:100],
                source=str(row.get('source', 'Manga'))[:50],
                duration=str(row.get('duration', '24 min'))[:50],
                rating=str(row.get('rating', 'PG-13'))[:50],
                score=score,
                members=safe_int(row.get('members', 10000)),
                # FOREIGN KEY VEZE
                genre_id=genre_id,
                studio_id=studio_id,
                producer_id=producer_id,
                time_id=time_id,
                rating_id=rating_id
            )
            session.add(new_anime)
            anime_count += 1

        # 3. KREIRAJ FACT ZAPIS
        fact_record = FactAnimeStats(
            anime_id=anime_id,
            score=score,
            scored_by=safe_int(row.get('scored_by', 1000)),
            favorites=safe_int(row.get('favorites', 100)),
            popularity=safe_int(row.get('popularity', 1000)),
            rank=safe_int(row.get('rank', 1000))
        )
        session.add(fact_record)
        fact_count += 1

        # Commit svakih 50 redova
        if idx % 50 == 0:
            session.commit()
            print(f"[INFO] Obrađeno {idx} redova")

    except Exception as e:
        print(f"[ERROR] Greška na redu {idx}: {e}")
        session.rollback()
        continue

# Final commit
session.commit()
print("✅ ETL završen uspešno!")

# Proveri rezultate
print(f"\n📊 FINALNI REZULTATI:")
try:
    anime_total = session.query(DimAnime).count()
    genre_total = session.query(DimGenre).count()
    studio_total = session.query(DimStudio).count()
    producer_total = session.query(DimProducer).count()
    time_total = session.query(DimTime).count()
    rating_total = session.query(DimRating).count()
    fact_total = session.query(FactAnimeStats).count()
    
    print(f"Anime tabela: {anime_total} zapisa")
    print(f"Genre tabela: {genre_total} zapisa")
    print(f"Studio tabela: {studio_total} zapisa")
    print(f"Producer tabela: {producer_total} zapisa")
    print(f"Time tabela: {time_total} zapisa")
    print(f"Rating tabela: {rating_total} zapisa")
    print(f"FACT tabela: {fact_total} zapisa")
    
    print("\n✅ USPEH! Foreign key veze su kreirane!")
        
except Exception as e:
    print(f"Greška pri proveri rezultata: {e}")

session.close()
print("Sesija zatvorena.")

