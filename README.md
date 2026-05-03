# IPBD Kelompok 5 - Case Method
<p align="center">
  Muhammad Rasyid Haunan | L0224007 | Kelas A<br> 
  Prayuda Afifan Handoyo | L0224008 | Kelas A<br>
  Infrastruktur dan Platform Big Data
</p>

## Overview
Pipeline pemrosesan data End-to-End berbasis Apache Airflow untuk mengolah tren kendaraan listrik (EV) dan harga minyak. Data mentah disimpan ke data lake (Garage), diproses ke data warehouse (PostgreSQL), lalu diringkas ke tabel analitik untuk visualisasi di Metabase.

## Gambaran Arsitektur
Alur utama mengikuti pola arsitektur medallion: bronze (raw) → silver (cleaned) → gold (facts/insights).

![Arsitektur pipeline](assets/architecture-diagram.gif)

## Sumber Data
- **Google Trends**: tren pencarian terkait EV.
- **Wikipedia Pageviews**: ketertarikan publik melalui jumlah view periodik laman Wikipedia EV.
- **Harga Minyak (Brent)**: data historis dari sumber finansial (Yahoo Finance).

## Orkestrasi DAG Airflow
Seluruh tahapan ingestion dan transformasi dijalankan oleh DAG Airflow yang mengatur urutan bronze → silver → gold.
Tampilan DAG bisa dilihat di https://airflow.ipbd.prayudahlah.dev.

![DAG Airflow](assets/dag.png)

## Lapisan Data
Data disusun dalam tiga lapisan utama:
- **Bronze**: data mentah dari sumber eksternal disimpan di data lake (Garage).
- **Silver**: data dibersihkan dan distandarkan ke PostgreSQL.
- **Gold**: tabel fakta untuk analitik.

### Bronze Layer
![Bronze layer](assets/garage-bronze-layer.png)

### Silver Layer
![Silver layer](assets/postgres-silver-layer.png)

### Gold Layer
![Gold layer](assets/postgres-gold-layer.png)

## Output Analitik
Hasil akhir digunakan untuk analisis tren bulanan, dampak event, serta korelasi EV vs minyak.
Dashboard publik dapat diakses di https://metabase.ipbd.prayudahlah.dev/public/dashboard/77dd3122-85c8-4a68-869e-04803bc28853.

![Dashboard Metabase](assets/dashboard.png)

## Tech Stack
- Apache Airflow
- Python (pandas, polars, pytrends, yfinance)
- Garage (S3-compatible)
- PostgreSQL
- Metabase

## Setup Environment
1. Clone repository
   ```bash
   git clone https://github.com/prayudahlah/ipbd-kelompok-5
   ```
2. Salin file env contoh:
   ```bash
   cp .env.example .env
   ```
3. Isi variabel pada `.env` sesuai environment kamu.
4. Untuk `GARAGE_WEBUI_HASHED_PASSWORD`, gunakan bcrypt:
   - Utama (htpasswd):
     ```bash
     htpasswd -nbBC 10 <user> <password> | cut -d: -f2
     ```
   - Fallback (Python + bcrypt) jika metode utama tidak berhasil:
     ```bash
     python - <<'PY'
     import bcrypt
     pwd = b"<password>"
     print(bcrypt.hashpw(pwd, bcrypt.gensalt(rounds=10)).decode())
     PY
     ```
     Jika modul belum ada: `pip install bcrypt`.
5. Buat file kosong untuk Garage (biarkan script mengisi):
   ```bash
   touch garage/env.garage
   ```
6. Jalankan docker compose
   - Development
      ```bash
      docker compose -f compose.dev.yaml up --watch --build
      ```
   - Deployment
      ```bash
      docker compose -f compose.prod.yaml up -d --build
      ```
