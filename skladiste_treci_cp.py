from sqlalchemy import create_engine, Column, Integer, BigInteger, String, ForeignKey, Float, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Kreiranje baze podataka
temp_engine = create_engine('mysql+pymysql://root:root@localhost:3306/', echo=True)
with temp_engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS anime_warehouse"))
    conn.commit()

# Spajanje na bazu
engine = create_engine('mysql+pymysql://root:root@localhost:3306/anime_warehouse', echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# DIMENZIJSKE TABLICE
class DimAnime(Base):
    __tablename__ = 'dim_anime'
    anime_id = Column(BigInteger, primary_key=True)  
    name = Column(String(255))
    type = Column(String(50))  
    episodes = Column(Integer)
    score = Column(Float)
    members = Column(Integer)

class DimGenre(Base):
    __tablename__ = 'dim_genre'
    genre_id = Column(Integer, primary_key=True, autoincrement=True)
    genre_name = Column(String(100), unique=True)

class DimStudio(Base):
    __tablename__ = 'dim_studio'
    studio_id = Column(Integer, primary_key=True, autoincrement=True)
    studio_name = Column(String(255), unique=True)

class DimProducer(Base):
    __tablename__ = 'dim_producer'
    producer_id = Column(Integer, primary_key=True, autoincrement=True)
    producer_name = Column(String(255), unique=True)

class DimRating(Base):
    __tablename__ = 'dim_rating'
    rating_id = Column(Integer, primary_key=True)
    score_range = Column(String(20))
    category = Column(String(30))

class DimTime(Base):
    __tablename__ = 'dim_time'
    time_id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer)
    decade = Column(String(10))
    era = Column(String(20))

# BRIDGE TABLICE (many-to-many veze)
class AnimeGenre(Base):
    __tablename__ = 'anime_genre'
    anime_id = Column(BigInteger, ForeignKey('dim_anime.anime_id'), primary_key=True)
    genre_id = Column(Integer, ForeignKey('dim_genre.genre_id'), primary_key=True)

class AnimeStudio(Base):
    __tablename__ = 'anime_studio'
    anime_id = Column(BigInteger, ForeignKey('dim_anime.anime_id'), primary_key=True)
    studio_id = Column(Integer, ForeignKey('dim_studio.studio_id'), primary_key=True)

class AnimeProducer(Base):
    __tablename__ = 'anime_producer'
    anime_id = Column(BigInteger, ForeignKey('dim_anime.anime_id'), primary_key=True)
    producer_id = Column(Integer, ForeignKey('dim_producer.producer_id'), primary_key=True)

# FACT TABLICA - CENTRALNA TABLICA STAR SHEME
class FactAnimeStats(Base):
    __tablename__ = 'fact_anime_stats'
    fact_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # FOREIGN KEY VEZE PREMA SVIM DIMENZIJAMA
    anime_id = Column(BigInteger, ForeignKey('dim_anime.anime_id'), nullable=False)
    genre_id = Column(Integer, ForeignKey('dim_genre.genre_id'), nullable=True)
    studio_id = Column(Integer, ForeignKey('dim_studio.studio_id'), nullable=True)
    producer_id = Column(Integer, ForeignKey('dim_producer.producer_id'), nullable=True)
    rating_id = Column(Integer, ForeignKey('dim_rating.rating_id'), nullable=True)
    time_id = Column(Integer, ForeignKey('dim_time.time_id'), nullable=True)
    
    # MJERE/METRICS
    score = Column(Float)
    members = Column(Integer)
    popularity_rank = Column(Integer)
    favorites = Column(Integer)
    scored_by = Column(Integer)
    rank = Column(Integer)

# Kreiranje svih tablica
print("🔨 Kreiram star schema model...")
Base.metadata.create_all(engine)
print("✅ Svih 10 tablica kreirano prema star schema modelu!")

# Provjera strukture
with engine.connect() as conn:
    result = conn.execute(text("SHOW TABLES"))
    print("\n📋 KREIRANE TABLICE:")
    tables = []
    for row in result:
        tables.append(row[0])
        print(f"   • {row[0]}")
    
    print(f"\n📊 UKUPNO: {len(tables)} tablica")
    
    # Provjera foreign key veza
    result = conn.execute(text("""
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
        FROM 
            information_schema.KEY_COLUMN_USAGE 
        WHERE 
            REFERENCED_TABLE_SCHEMA = 'anime_warehouse'
            AND REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY TABLE_NAME;
    """))
    
    print("\n🔗 FOREIGN KEY VEZE:")
    fk_count = 0
    for row in result:
        print(f"   {row[0]}.{row[1]} → {row[2]}.{row[3]}")
        fk_count += 1
    
    print(f"\n📈 UKUPNO: {fk_count} foreign key veza")

print("\n🌟 STAR SCHEMA MODEL USPJEŠNO KREIRAN!")
print("   • 6 dimenzijskih tablica")
print("   • 1 fact tablica") 
print("   • 3 bridge tablice")
print("   • Sve foreign key veze implementirane")

session.close()
