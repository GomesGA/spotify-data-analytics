# 🎧 Spotify & Last.fm Analytics: Pipeline de Dados End-to-End

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Spotify API](https://img.shields.io/badge/Spotify_API-1DB954?style=for-the-badge&logo=spotify&logoColor=white)
![Looker Studio](https://img.shields.io/badge/Looker_Studio-4285F4?style=for-the-badge&logo=google&logoColor=white)

## 📌 Visão Geral
Este projeto é um pipeline completo de Engenharia de Dados e Business Intelligence (BI). O objetivo é extrair o histórico de consumo musical do Spotify, enriquecer esses dados com metadados de géneros do Last.fm e modelar tudo num *Star Schema* (Esquema em Estrela). 

A visualização final é feita num Dashboard interativo no Looker Studio, projetado com uma interface moderna ("Dark Mode") inspirada no próprio Spotify, permitindo análises profundas sobre **Variedade Musical** (artistas) vs. **Volume de Escuta** (repetição de faixas).

---

## 🏗️ Arquitetura do Pipeline (ETL)

1. **Extract (Extração):** Conexão via OAuth 2.0 à API do Spotify para obter o Top 50 de artistas e músicas em janelas de 1 mês, 6 meses e 1 ano. Requisições à API do Last.fm para suprir a falta de géneros musicais no Spotify.
2. **Transform (Transformação):** - Normalização global de nomenclatura de géneros (Entity Resolution).
   - *Poda Hierárquica* para resolver conflitos de categorias macro vs. micro (ex: impedir que "Pop" ofusque "K-pop").
   - Geração de tabelas dimensionais e tabelas ponte (Bridge Tables) para evitar duplicidade de dados relacionais.
3. **Load (Carregamento):** Exportação dos dados limpos para ficheiros `.csv` estruturados e otimizados para ferramentas de BI.

---

## 📂 Estrutura de Dados (Modelagem Star Schema)

O projeto gera uma pasta `/data` contendo o modelo de dados pronto a ser consumido:

* **Tabelas Fato:**
  * `fato_artistas.csv`: Histórico de ranking e popularidade de artistas por período.
  * `fato_musicas.csv`: Histórico de ranking e popularidade de faixas por período.
* **Tabelas Dimensão:**
  * `dim_artistas.csv`: Cadastro único com metadados e URLs de imagens de perfil.
  * `dim_periodos.csv`: Tabela calendário para ordenação cronológica dos filtros.
* **Tabelas Ponte (Bridge Tables):**
  * `dim_artista_genero.csv`: Resolve relações N:N para análise de Variedade.
  * `dim_musica_genero.csv`: Resolve relações N:N para análise de Volume.

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
* Python 3.8+
* Chaves de API do [Spotify for Developers](https://developer.spotify.com/)
* Chave de API do [Last.fm API](https://www.last.fm/api)

### Instalação

1. Clone este repositório:
```bash
git clone https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git
cd SEU_REPOSITORIO
```

2. Crie um ambiente virtual e instale as dependências:
```bash
python -m venv .venv
source .venv/bin/activate  # No Windows use: .venv\Scripts\activate
pip install pandas spotipy python-dotenv requests
```

3. Configure as variáveis de ambiente:
Crie um ficheiro `.env` na raiz do projeto com as suas credenciais:
```env
SPOTIPY_CLIENT_ID='sua_chave_aqui'
SPOTIPY_CLIENT_SECRET='sua_chave_aqui'
SPOTIPY_REDIRECT_URI='http://localhost:8888/callback'
LASTFM_API_KEY='sua_chave_aqui'
```

4. Execute o pipeline de dados:
```bash
python script.py
```

---

## 📊 Dashboard Interativo (Looker Studio)

Os dados gerados pelo pipeline alimentam um painel gerencial focado em UI/UX.

🔗 **[Clique aqui para aceder ao Dashboard Interativo](#)** *(https://lookerstudio.google.com/reporting/b3cc6264-c035-4614-8c98-e24c68ddeaee)*

### Principais Análises Visuais:
* **Filtro Dinâmico:** Segmentação unificada (Cross-filtering) para 1 mês, 6 meses ou 1 ano.
* **Top Artistas:** Grelha visual de classificação renderizando imagens dinâmicas.
* **Variedade vs. Volume:** Comparação entre *Donut Chart* (estilos escutados por artista único) e *Treemap* (estilos consumidos em volume absoluto de reproduções).

---
*Desenvolvido como projeto de portfólio aplicando conceitos de Engenharia de Dados e Business Intelligence.*
