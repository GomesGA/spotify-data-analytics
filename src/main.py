import os
import time
import json
import requests
import pandas as pd
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
from collections import Counter
from datetime import datetime

# ==========================================
# 1. CONFIGURAÇÕES INICIAIS
# ==========================================
load_dotenv()
os.makedirs('data', exist_ok=True)
os.makedirs('cache', exist_ok=True)

LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
CACHE_FILE = 'cache/spotify_cache.json'
EXTRACTION_DATE = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ==========================================
# 2. REGRAS DE NEGÓCIO E NORMALIZAÇÃO
# ==========================================
GENRE_MAPPING = {
    # Música Asiática / Trilhas Sonoras (Doramas, Animes, K-Pop)
    'kpop': 'k-pop',
    'korean pop': 'k-pop',
    'k-pop girls': 'k-pop',
    'k-pop boys': 'k-pop',
    'korean drama': 'soundtrack',
    'jpop': 'j-pop',
    'japanese pop': 'j-pop',
    'anime': 'j-pop',
    'jrock': 'j-rock',
    'japanese rock': 'j-rock',
    'ost': 'soundtrack',
    'score': 'soundtrack',
    'video game music': 'soundtrack',

    # Hip-Hop e R&B
    'hip hop': 'hip-hop',
    'hiphop': 'hip-hop',
    'underground hip-hop': 'hip-hop',
    'rnb': 'r&b',
    'r and b': 'r&b',
    'rhythm and blues': 'r&b',
    'contemporary r&b': 'r&b',

    # Rock e Alternativo
    'rock and roll': 'rock',
    'rock n roll': 'rock',
    'classic rock': 'rock',
    'hard rock': 'rock',
    'alt rock': 'alternative',
    'alternative rock': 'alternative',
    'indie pop': 'indie',
    'indie rock': 'indie',
    'indie folk': 'indie',

    # Eletrônica e Pop
    'edm': 'electronic',
    'electronica': 'electronic',
    'dance': 'electronic',
    'electro': 'electronic',
    'pop music': 'pop',
    'dance-pop': 'pop',

    # Música Brasileira
    'brasil': 'brazilian',
    'brazilian pop': 'mpb',
    'brazilian rock': 'rock nacional',
    'funk carioca': 'funk brasileiro',
    'brazilian funk': 'funk brasileiro',
    'sertanejo universitario': 'sertanejo'
}

BAD_TAGS = {
    'seen live', 'favorites', 'favorite', 'british', 'american', 
    'female vocalists', 'male vocalists', 'singer-songwriter', 'under 2000 listeners'
}

def normalizar_generos(lista_generos):
    """Aplica a limpeza, normalização e resolve conflitos de hierarquia (Macro vs Micro)"""
    clean_genres = set()
    
    # Limpeza e Tradução (De -> Para)
    for g in lista_generos:
        raw_name = g.lower().strip()
        if raw_name not in BAD_TAGS:
            norm_name = GENRE_MAPPING.get(raw_name, raw_name)
            clean_genres.add(norm_name)
            
    # Poda Hierárquica (Evitar pontuação dupla em géneros genéricos)
    if 'k-pop' in clean_genres or 'j-pop' in clean_genres:
        clean_genres.discard('pop')
        clean_genres.discard('dance')
        clean_genres.discard('electronic')
        
    if 'rock nacional' in clean_genres or 'indie' in clean_genres:
        clean_genres.discard('rock')
        clean_genres.discard('pop')

    if 'mpb' in clean_genres or 'sertanejo' in clean_genres or 'funk brasileiro' in clean_genres:
        clean_genres.discard('brazilian')
        
    return list(clean_genres)

# ==========================================
# 3. SISTEMA DE CACHE
# ==========================================
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)

cache_data = load_cache()

# ==========================================
# 4. INTEGRAÇÕES (APIs)
# ==========================================
def conectar_spotify():
    return Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv('SPOTIPY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIPY_CLIENT_SECRET'),
        redirect_uri=os.getenv('SPOTIPY_REDIRECT_URI'),
        scope="user-top-read"
    ))

def get_lastfm_genres(artist_name, retries=3):
    """Busca géneros de um ARTISTA no Last.fm com cache"""
    if artist_name in cache_data:
        return cache_data[artist_name]
    
    if not LASTFM_API_KEY:
        return []
    
    params = {
        'method': 'artist.getTopTags',
        'artist': artist_name,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'autocorrect': 1
    }
    
    for attempt in range(retries):
        try:
            time.sleep(0.3)
            resp = requests.get('https://ws.audioscrobbler.com/2.0/', 
                              params=params, timeout=15,
                              headers={'User-Agent': 'SpotifyPowerBI/1.0'})
            
            if resp.status_code == 200:
                tags = resp.json().get('toptags', {}).get('tag', [])
                raw_genres = [t['name'] for t in tags if int(t.get('count', 0)) > 10]
                clean_genres = normalizar_generos(raw_genres)[:5] 
                
                cache_data[artist_name] = clean_genres
                save_cache(cache_data)
                return clean_genres
            time.sleep(2)
        except:
            time.sleep(2 * (attempt + 1))
    
    cache_data[artist_name] = []
    save_cache(cache_data)
    return []

def get_lastfm_track_genres(artist_name, track_name, retries=3):
    """Busca géneros específicos de uma MÚSICA no Last.fm com cache"""
    cache_key = f"track_{artist_name}_{track_name}"
    
    if cache_key in cache_data:
        return cache_data[cache_key]
    
    if not LASTFM_API_KEY:
        return []
    
    params = {
        'method': 'track.getTopTags',
        'artist': artist_name,
        'track': track_name,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'autocorrect': 1
    }
    
    for attempt in range(retries):
        try:
            time.sleep(0.3)
            resp = requests.get('https://ws.audioscrobbler.com/2.0/', 
                              params=params, timeout=15,
                              headers={'User-Agent': 'SpotifyPowerBI/1.0'})
            
            if resp.status_code == 200:
                tags = resp.json().get('toptags', {}).get('tag', [])
                raw_genres = [t['name'] for t in tags if int(t.get('count', 0)) > 10]
                clean_genres = normalizar_generos(raw_genres)[:5] 
                
                cache_data[cache_key] = clean_genres
                save_cache(cache_data)
                return clean_genres
            time.sleep(2)
        except:
            time.sleep(2 * (attempt + 1))
    
    cache_data[cache_key] = []
    save_cache(cache_data)
    return []

# ==========================================
# 5. EXTRAÇÃO DE DADOS
# ==========================================
def extract_all_data(sp):
    """Extrai os dados do Spotify e cruza com o Last.fm"""
    periods = [
        ('short_term', '1_mes', 1),
        ('medium_term', '6_meses', 2),
        ('long_term', '12_meses', 3)
    ]
    
    all_artists_rows = []
    all_tracks_rows = []
    artista_genero_rows = [] 
    musica_genero_rows = [] 
    unique_artists = {}
    
    print("📊 A extrair dados do Spotify...")
    
    for api_term, label, period_order in periods:
        print(f"\n🎵 Período: {label}")
        
        # --- PROCESSAMENTO DE ARTISTAS ---
        artists = sp.current_user_top_artists(limit=50, time_range=api_term)['items']
        
        for rank, item in enumerate(artists, 1):
            name = item['name']
            spotify_id = item.get('id', '')
            spotify_genres = item.get('genres', [])
            source = 'Spotify'
            
            if not spotify_genres:
                spotify_genres = get_lastfm_genres(name)
                source = 'Last.fm' if spotify_genres else 'Não classificado'
            
            spotify_genres = normalizar_generos(spotify_genres)
            
            for g in spotify_genres:
                if not any(row['Artista'] == name and row['Genero'] == g for row in artista_genero_rows):
                    artista_genero_rows.append({
                        'Spotify_ID': spotify_id,
                        'Artista': name,
                        'Genero': g
                    })

            genres_str = ', '.join(spotify_genres) if spotify_genres else 'N/A'
            
            all_artists_rows.append({
                'Artista': name, 'Periodo': label, 'Periodo_Ordem': period_order,
                'Ranking': rank, 'Popularidade': item.get('popularity', 0),
                'Generos': genres_str, 'Fonte_Dados': source,
                'Data_Extracao': EXTRACTION_DATE, 'Spotify_ID': spotify_id
            })
            
            if name not in unique_artists:
                unique_artists[name] = {
                    'Artista': name, 'Generos_Totais': set(spotify_genres),
                    'Popularidade_Maxima': item.get('popularity', 0),
                    'Spotify_ID': spotify_id, 'URL_Spotify': item.get('external_urls', {}).get('spotify', ''),
                    'Imagem': item.get('images', [{}])[0].get('url', '') if item.get('images') else ''
                }
            else:
                unique_artists[name]['Generos_Totais'].update(spotify_genres)
                unique_artists[name]['Popularidade_Maxima'] = max(unique_artists[name]['Popularidade_Maxima'], item.get('popularity', 0))
        
        # --- PROCESSAMENTO DE MÚSICAS ---
        tracks = sp.current_user_top_tracks(limit=50, time_range=api_term)['items']
        
        for rank, item in enumerate(tracks, 1):
            track_name = item['name']
            artist_names = ', '.join([a['name'] for a in item['artists']])
            main_artist = item['artists'][0]['name'] if item['artists'] else 'Desconhecido'
            track_spotify_id = item['id']
            
            track_genres = get_lastfm_track_genres(main_artist, track_name)
            
            for g in track_genres:
                if not any(row['Spotify_ID'] == track_spotify_id and row['Genero'] == g for row in musica_genero_rows):
                    musica_genero_rows.append({
                        'Spotify_ID': track_spotify_id,
                        'Musica': track_name,
                        'Artista': main_artist,
                        'Genero': g
                    })
            
            track_genres_str = ', '.join(track_genres) if track_genres else 'N/A'
            
            all_tracks_rows.append({
                'Musica': track_name,
                'Artista_Principal': main_artist,
                'Todos_Artistas': artist_names,
                'Periodo': label,
                'Periodo_Ordem': period_order,
                'Ranking': rank,
                'Album': item['album']['name'],
                'Popularidade_Musica': item.get('popularity', 0),
                'Generos_Musica': track_genres_str, 
                'Data_Extracao': EXTRACTION_DATE,
                'Preview_URL': item.get('preview_url', ''),
                'Spotify_ID': track_spotify_id
            })
        
        print(f"   ✅ {len(artists)} artistas, {len(tracks)} músicas processadas")
    
    return all_artists_rows, all_tracks_rows, unique_artists, artista_genero_rows, musica_genero_rows

# ==========================================
# 6. GERAÇÃO DE FICHEIROS PARA POWER BI
# ==========================================
def generate_powerbi_tables(artists_rows, tracks_rows, unique_artists, artista_genero_rows, musica_genero_rows):
    print("\n📁 A gerar ficheiros CSV para o Power BI...")
    
    df_artists = pd.DataFrame(artists_rows)
    df_artists.to_csv('data/fato_artistas.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ fato_artistas.csv")
    
    df_tracks = pd.DataFrame(tracks_rows)
    df_tracks.to_csv('data/fato_musicas.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ fato_musicas.csv")
    
    dim_data = []
    for name, data in unique_artists.items():
        dim_data.append({
            'Artista': name,
            'Generos_Consolidados': ', '.join(sorted(data['Generos_Totais'])) if data['Generos_Totais'] else 'N/A',
            'Total_Generos_Unicos': len(data['Generos_Totais']),
            'Popularidade_Maxima': data['Popularidade_Maxima'],
            'Spotify_ID': data['Spotify_ID'],
            'URL_Spotify': data['URL_Spotify'],
            'Imagem_URL': data['Imagem']
        })
    df_dim = pd.DataFrame(dim_data)
    df_dim.to_csv('data/dim_artistas.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ dim_artistas.csv")
    
    # Novas tabelas de Gêneros
    pd.DataFrame(artista_genero_rows).to_csv('data/dim_artista_genero.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ dim_artista_genero.csv (Para gráficos de Variedade)")
    
    pd.DataFrame(musica_genero_rows).to_csv('data/dim_musica_genero.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ dim_musica_genero.csv (Para gráficos de Volume/Repetição)")

    periods_df = pd.DataFrame([
        {'Periodo': '1_mes', 'Periodo_Nome': 'Último Mês', 'Ordem': 1, 'Dias': 30},
        {'Periodo': '6_meses', 'Periodo_Nome': 'Últimos 6 Meses', 'Ordem': 2, 'Dias': 180},
        {'Periodo': '12_meses', 'Periodo_Nome': 'Último Ano', 'Ordem': 3, 'Dias': 365}
    ])
    periods_df.to_csv('data/dim_periodos.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ dim_periodos.csv")
    
    summary = []
    for period in ['1_mes', '6_meses', '12_meses']:
        artists_count = len(df_artists[df_artists['Periodo'] == period])
        tracks_count = len(df_tracks[df_tracks['Periodo'] == period])
        
        period_genres = df_artists[df_artists['Periodo'] == period]['Generos']
        all_genres = []
        for g_list in period_genres:
            if g_list != 'N/A':
                all_genres.extend([g.strip() for g in g_list.split(',')])
        top_genre = Counter(all_genres).most_common(1)[0][0] if all_genres else 'N/A'
        
        summary.append({
            'Periodo': period,
            'Total_Artistas': artists_count,
            'Total_Musicas': tracks_count,
            'Genero_Predominante': top_genre,
            'Data_Atualizacao': EXTRACTION_DATE
        })
    pd.DataFrame(summary).to_csv('data/resumo_kpis.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ resumo_kpis.csv")
    
    artist_periods = df_artists.groupby('Artista')['Periodo'].apply(list).reset_index()
    artist_periods['Quantidade_Periodos'] = artist_periods['Periodo'].apply(len)
    artist_periods['Aparece_1mes'] = artist_periods['Periodo'].apply(lambda x: '1_mes' in x)
    artist_periods['Aparece_6meses'] = artist_periods['Periodo'].apply(lambda x: '6_meses' in x)
    artist_periods['Aparece_12meses'] = artist_periods['Periodo'].apply(lambda x: '12_meses' in x)
    
    rankings = df_artists.pivot(index='Artista', columns='Periodo', values='Ranking').reset_index()
    evolucao = pd.merge(artist_periods, rankings, on='Artista', how='left')
    evolucao.to_csv('data/matriz_evolucao.csv', index=False, encoding='utf-8-sig', sep=';')
    print("   ✅ matriz_evolucao.csv")

# ==========================================
# 7. EXECUÇÃO PRINCIPAL
# ==========================================
def main():
    if not LASTFM_API_KEY:
        print("❌ LASTFM_API_KEY não configurada no ficheiro .env!")
        return
    
    try:
        sp = conectar_spotify()
        artists_rows, tracks_rows, unique_artists, artista_genero_rows, musica_genero_rows = extract_all_data(sp)
        generate_powerbi_tables(artists_rows, tracks_rows, unique_artists, artista_genero_rows, musica_genero_rows)
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()