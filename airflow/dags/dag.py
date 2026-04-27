import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ============================================================
# KONFIGURASI GLOBAL
# ============================================================
START_DATE_DATA  = '2015-01-01'
END_DATE_DATA    = '2025-12-31'
GROQ_API_KEY     = os.getenv("GROQ_API_KEY")
OIL_OUTPUT_FILE  = '/tmp/oil_price_20year.csv'
NEWS_OUTPUT_FILE = '/tmp/ev_news.csv'
TRENDS_OUTPUT_FILE = '/tmp/ev_trends.csv'

# ============================================================
# TASK 1 — Scrape Indeks Google Trends (dari indeks_serach_ev.py)
# ============================================================
def scrape_indeks_ev(**context):
    from pytrends.request import TrendReq
    import pandas as pd

    print(f"[TASK 1] Mengambil data Google Trends untuk 'electric vehicle'...")
    print(f"  Rentang: {START_DATE_DATA} s/d {END_DATE_DATA}")

    # timeout=30 agar tidak hang selamanya di dalam Docker
    pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 30))
    kw_list  = ["electric vehicle"]
    pytrends.build_payload(kw_list, timeframe=f"{START_DATE_DATA} {END_DATE_DATA}")
    df = pytrends.interest_over_time()

    # interest_over_time() bisa return None di beberapa versi pytrends
    if df is None or df.empty:
        print("[TASK 1] Tidak ada data trends yang ditemukan.")
        return

    # Hapus kolom 'isPartial' jika ada
    if 'isPartial' in df.columns:
        df = df.drop(columns=['isPartial'])

    df = df.reset_index()
    df.to_csv(TRENDS_OUTPUT_FILE, index=False)

    print(f"[TASK 1] Berhasil! {len(df)} baris data trends.")
    print(f"[TASK 1] Data disimpan ke: {TRENDS_OUTPUT_FILE}")
    print(df.head().to_string())


# ============================================================
# TASK 2 — Scrape Harga Minyak Mentah (dari harga_minyak.py)
# ============================================================
def scrape_harga_minyak(**context):
    import requests
    import pandas as pd

    # Endpoint M = data bulanan ~80 tahun (~964 baris)
    API_URL = "https://www.macrotrends.net/economic-data/1369/M"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.macrotrends.net/1369/crude-oil-price-history-chart",
        "Accept":  "application/json",
    }

    print("[TASK 2] Mengambil data harga minyak bulanan dari macrotrends.net...")
    response = requests.get(API_URL, headers=headers, timeout=15)
    response.raise_for_status()

    json_data = response.json()

    if json_data is None or "data" not in json_data or json_data["data"] is None:
        raise ValueError(
            f"[TASK 2] API tidak mengembalikan data. URL: {API_URL} | "
            f"Response: {response.text[:300]}"
        )

    raw_data   = json_data["data"]
    timestamps = [row[0] for row in raw_data]
    prices     = [row[1] for row in raw_data]

    df = pd.DataFrame({
        "date":      pd.to_datetime(timestamps, unit="ms", utc=True).tz_convert(None),
        "price_usd": prices,
    })
    df = df.sort_values("date").reset_index(drop=True)

    print(f"[TASK 2] Total keseluruhan: {len(df)} baris "
          f"({df['date'].min().year} - {df['date'].max().year})")

    # Filter 20 tahun terakhir
    cutoff_year = df["date"].max().year - 20
    df_20y = df[df["date"].dt.year > cutoff_year].reset_index(drop=True)

    print(f"[TASK 2] Setelah filter 20 tahun terakhir: {len(df_20y)} baris")
    print(f"[TASK 2] Rentang: {df_20y['date'].min().date()} s/d {df_20y['date'].max().date()}")
    print(df_20y.tail(5).to_string(index=False))

    df_20y.to_csv(OIL_OUTPUT_FILE, index=False)
    print(f"[TASK 2] Data disimpan ke: {OIL_OUTPUT_FILE}")


# ============================================================
# TASK 3 — Scrape Berita EV dari Google News (dari get_berita.py)
# ============================================================
def scrape_berita_ev(**context):
    from GoogleNews import GoogleNews
    import pandas as pd

    print("[TASK 3] Mengambil berita 'electric vehicle' dari Google News...")

    # Format tanggal: MM/DD/YYYY (wajib untuk GoogleNews)
    formatted_start = datetime.strptime(START_DATE_DATA, '%Y-%m-%d').strftime('%m/%d/%Y')
    formatted_end   = datetime.strptime(END_DATE_DATA,   '%Y-%m-%d').strftime('%m/%d/%Y')

    googlenews = GoogleNews(lang='en', region='US')
    googlenews.set_time_range(formatted_start, formatted_end)

    keyword = "electric vehicle"
    print(f"[TASK 3] Mencari: '{keyword}' dari {formatted_start} hingga {formatted_end}...")
    googlenews.search(keyword)

    results = googlenews.results()

    # Filter berita yang tidak memiliki judul
    valid_results = [news for news in results if news.get('title')]

    if not valid_results:
        print("[TASK 3] Tidak ditemukan berita valid atau koneksi terblokir.")
        return

    print(f"[TASK 3] Ditemukan {len(valid_results)} berita valid:\n")
    for i, news in enumerate(valid_results, 1):
        scrapID = f"SCRAP-{i:03d}"
        print(f"  scrapID : {scrapID}")
        print(f"  Judul   : {news.get('title')}")
        print(f"  Tanggal : {news.get('date')}")
        print(f"  Media   : {news.get('media')}")
        print(f"  Tautan  : {news.get('link')}")
        print("  " + "-" * 48)

    df = pd.DataFrame(valid_results)
    df.insert(0, 'scrapID', [f"SCRAP-{i:03d}" for i in range(1, len(df) + 1)])
    df.to_csv(NEWS_OUTPUT_FILE, index=False)
    print(f"[TASK 3] Data disimpan ke: {NEWS_OUTPUT_FILE}")


# ============================================================
# DEFINISI DAG
# ============================================================
default_args = {
    'owner':            'kelompok-5',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id='ev_data_pipeline',
    description='Pipeline ETL: Google Trends EV + Harga Minyak + Berita EV',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@weekly',          # Jalankan setiap minggu
    catchup=False,
    tags=['ev', 'scraping', 'kelompok-5'],
) as dag:

    task_indeks_ev = PythonOperator(
        task_id='scrape_indeks_ev',
        python_callable=scrape_indeks_ev,
    )

    task_harga_minyak = PythonOperator(
        task_id='scrape_harga_minyak',
        python_callable=scrape_harga_minyak,
    )

    task_berita_ev = PythonOperator(
        task_id='scrape_berita_ev',
        python_callable=scrape_berita_ev,
    )

    # ── Alur Eksekusi ──────────────────────────────────────
    # task_indeks_ev dan task_harga_minyak jalan paralel,
    # lalu keduanya selesai baru task_berita_ev jalan.
    # ──────────────────────────────────────────────────────
    [task_indeks_ev, task_harga_minyak] >> task_berita_ev
