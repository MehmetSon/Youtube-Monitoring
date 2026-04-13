# Social Listener Prototype

Web tabanli bir sosyal dinleme prototipi.

Bu surum:

- Flask tabanli bir web arayuzu sunar
- SQLite veya PostgreSQL ile bulunan icerikleri cache eder
- Tekrarlanan aramalarda daha once yakalanan sonuclari DB'den gosterir
- YouTube icin gercek API ile video ve yorum toplamayi destekler
- Facebook, Instagram ve LinkedIn icin adaptor/fallback yapisi sunar
- Demo veri moduyla arayuz ve veri akisini calistirir

## Kurulum

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/python run.py
```

Ardindan tarayicida `http://127.0.0.1:5000` adresini acin.

## Ortam degiskenleri

- `APP_SECRET_KEY`: Flask secret key
- `APP_DEBUG`: local gelistirme icin debug modu
- `APP_HOST`: local calistirmada bind edilecek host
- `APP_PORT`: `PORT` yoksa local calistirmada kullanilacak port
- `APP_ENABLE_DEMO_DATA`: `true` ise gercek adaptor yoksa demo veri kullanilir
- `APP_DATABASE_URL`: verilirse uygulama otomatik PostgreSQL moduna gecer
- `APP_DB_PATH`: SQLite dosya yolu
- `APP_OWNED_YOUTUBE_CHANNELS_PATH`: owned YouTube kanallarinin JSON dosya yolu
- `APP_TARGET_LANGUAGE`: hedef dil kodu, varsayilan `tr`
- `APP_TARGET_REGION`: hedef ulke kodu, varsayilan `TR`
- `APP_STRICT_LANGUAGE_FILTER`: `true` ise uygulama Turkce olmayan icerikleri ikinci bir filtreyle eler
- `APIFY_TOKEN`: Apify API token'i; resmi Facebook sayfasi icin posts + reels toplayici bunu kullanir
- `APIFY_FACEBOOK_SEARCH_ACTOR_ID`: Facebook mention/keyword aramasi icin actor kimligi
- `APIFY_FACEBOOK_POSTS_ACTOR_ID`: varsayilan Facebook posts actor kimligi
- `APIFY_FACEBOOK_REELS_ACTOR_ID`: varsayilan Facebook reels actor kimligi
- `APIFY_FACEBOOK_SEARCH_LIMIT`: mention aramasinda cekilecek maksimum sonuc sayisi
- `APIFY_FACEBOOK_POSTS_LIMIT`: resmi Facebook sayfasindan cekilecek maksimum post sayisi
- `APIFY_FACEBOOK_REELS_LIMIT`: resmi Facebook sayfasindan cekilecek maksimum reel sayisi
- `WEB_CONCURRENCY`: Gunicorn worker sayisi
- `GUNICORN_BIND`: Gunicorn bind adresi, varsayilan `0.0.0.0:$PORT`
- `GUNICORN_THREADS`: Gunicorn thread sayisi
- `GUNICORN_TIMEOUT`: Gunicorn request timeout saniyesi
- `YOUTUBE_API_KEY`: resmi YouTube API anahtari
- `YOUTUBE_MAX_RESULTS`: arama basina maksimum video sonucu
- `YOUTUBE_MAX_PAGES`: arama icin gezilecek sayfa sayisi
- `YOUTUBE_FETCH_COMMENTS`: `true` ise bulunan videolarin son yorum thread'lerini de toplar
- `YOUTUBE_COMMENT_THREADS_PER_VIDEO`: video basina cekilecek en yeni yorum thread sayisi

## Simdiki sinirlar

- YouTube adaptoru anahtar varsa gercek arama yapar ve yorum toplar
- Facebook tarafinda Apify ile hem mention/keyword aramasi hem de resmi sayfa URL'si girildiyse posts + reels tek listede toplanabilir
- Instagram ve LinkedIn adaptorleri halen iskelet durumundadir
- Yorum toplama ilk surumde YouTube ile baglidir; diger platformlarda henuz gercek connector yoktur

## YouTube'u gercek veriye acmak

1. Google Cloud Console uzerinden bir proje acin.
2. `YouTube Data API v3` servisini etkinlestirin.
3. Bir API key olusturun.
4. `.env` dosyasina su alanlari yazin:

```bash
APP_ENABLE_DEMO_DATA=false
APP_TARGET_LANGUAGE=tr
APP_TARGET_REGION=TR
APP_STRICT_LANGUAGE_FILTER=true
APIFY_TOKEN=your-apify-token
APIFY_FACEBOOK_SEARCH_ACTOR_ID=scraper_one/facebook-posts-search
APIFY_FACEBOOK_POSTS_ACTOR_ID=apify/facebook-posts-scraper
APIFY_FACEBOOK_REELS_ACTOR_ID=apify/facebook-reels-scraper
APIFY_FACEBOOK_SEARCH_LIMIT=50
APIFY_FACEBOOK_POSTS_LIMIT=25
APIFY_FACEBOOK_REELS_LIMIT=25
YOUTUBE_API_KEY=your-key
YOUTUBE_MAX_RESULTS=50
YOUTUBE_MAX_PAGES=3
YOUTUBE_FETCH_COMMENTS=true
YOUTUBE_COMMENT_THREADS_PER_VIDEO=5
```

5. Uygulamayi yeniden baslatin.

Bu durumda arama sonuclari demo yerine gercek YouTube video ve yorum verisiyle dolacaktir.
Varsayilan kurulum, bir sorguda sayfa basina 50 video olacak sekilde 3 sayfaya kadar ilerler ve yorumlarda arama terimlerini kullanir.
Ek olarak YouTube aramasina `regionCode=TR` ve `relevanceLanguage=tr` sinyalleri gonderilir; uygulama da title, description ve yorum metinlerinde dil tespiti + Turkce heuristik ile ikinci bir filtre uygular.

## Owned channel mantigi

Sadece keyword aramasi yeterli olmadigi icin uygulama owned YouTube kanallarini da destekler.

Ornek:

- `carrefoursa` aratilirsa resmi `CarrefourSA` kanali da taranir
- `trendyol` aratilirsa resmi `Trendyol` kanali da taranir

Bu sayede video basliginda veya aciklamasinda marka adi gecmese bile, resmi kanal yuklemesi oldugu icin uygulamaya dahil edilir.

Varsayilan kanal listesi su dosyada tutulur:

- [data/owned_youtube_channels.json](/Users/mehmetkabak/Documents/New%20project/data/owned_youtube_channels.json)

Bu dosyaya yeni marka ve kanal ekleyebilirsiniz.

## Ucretsiz canliya alma

Projeyi Render uzerinde tek tikta ayaga kaldiracak dosyalar hazir:

- [render.yaml](/Users/mehmetkabak/Documents/New%20project/render.yaml)
- [gunicorn.conf.py](/Users/mehmetkabak/Documents/New%20project/gunicorn.conf.py)
- [Procfile](/Users/mehmetkabak/Documents/New%20project/Procfile)
- [wsgi.py](/Users/mehmetkabak/Documents/New%20project/wsgi.py)

Bu proje ucretsiz canli kullanim icin su mimariyla hazir:

- Render Free Web Service
- Supabase Free PostgreSQL
- Gunicorn ile servis etme

### 1. Supabase ucretsiz veritabani olusturma

1. [Supabase](https://supabase.com/) hesabinda yeni bir proje acin.
2. Proje acildiktan sonra `Connect` butonuna basin.
3. Connection string olarak pooler adresini alin.
4. Bu adresi bir yere kaydedin; Render'a `APP_DATABASE_URL` olarak girecegiz.

Not: Supabase dokumani, baglanti bilgisini `Connect` ekranindan almanizi ve pooler modlarini kullanmanizi oneriyor. Ayrica transaction mode prepared statements desteklemez; uygulama bu uyumsuzlugu onlemek icin `prepare_threshold=None` ile baglanir. Kaynaklar:

- [Supabase connection strings](https://supabase.com/docs/guides/database/connecting-to-postgres/serverless-drivers)
- [Supabase disabling prepared statements](https://supabase.com/docs/guides/troubleshooting/disabling-prepared-statements-qL8lEL)

### 2. Render ile yayinlama

1. Projeyi GitHub repo'suna push edin.
2. Render Dashboard'da `New +` > `Blueprint` secin.
3. Repository'i baglayin; Render kokteki `render.yaml` dosyasini otomatik okuyacak.
4. Kurulum sirasinda su alanlari girin:
   - `APP_DATABASE_URL`: Supabase connection string
   - `YOUTUBE_API_KEY`: YouTube anahtariniz
5. Deploy tamamlaninca uygulama `.onrender.com` adresinden acilacak.

### Render'da ayarlanacak kritik degerler

- `APP_SECRET_KEY`: Render blueprint bunu otomatik uretir
- `APP_ENABLE_DEMO_DATA=false`
- `APP_DATABASE_URL=postgresql://...`
- `WEB_CONCURRENCY=1`
- `GUNICORN_THREADS=4`

Render'in resmi ucretsiz web service dokumanina gore free servisler 15 dakika idle kalinca uyur ve local filesystem kalici degildir. Bu nedenle SQLite yerine Supabase Postgres kullaniyoruz. Kaynak: [Render Deploy for Free](https://render.com/docs/free)

### Yerelde production benzeri calistirma

```bash
GUNICORN_BIND=127.0.0.1:5000 .venv/bin/gunicorn --config gunicorn.conf.py wsgi:app
```

Ardindan `http://127.0.0.1:5000/health` ile servis kontrol edilebilir.

## Dizin yapisi

```text
social_listener/
  app.py
  config.py
  db.py
  repository.py
  services/
    adapters.py
    collection.py
  static/
    app.css
    app.js
  templates/
    index.html
run.py
wsgi.py
gunicorn.conf.py
Procfile
render.yaml
```
