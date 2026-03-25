# Sosyal Dinleme Uygulamasi Mimari Tasarimi

## Hedef

Arama kutusuna girilen anahtar kelimeler icin Facebook, Instagram, YouTube ve LinkedIn kaynaklarindan olabildigince yuksek coverage ile paylasimlari, yorumlari ve ilgili icerikleri toplamak; bulunan verileri veritabanina kaydetmek; daha sonra ayni veri kaynaktan tekrar gelmese bile daha once yakalanan icerigi gosterebilmek.

## Gercekci urun hedefi

Bu sistem "tum platformlardaki tum public icerigi eksiksiz getirir" vaadiyle kurulmamali. Bunun yerine su hedef konulmali:

- resmi API ile alinabilen verileri almak
- lisansli vendor baglantisi varsa onu sisteme eklemek
- bulunan icerigi normalize edip kaydetmek
- tekrar yakalanamayan icerikleri daha once kaydedildiyse gostermek
- coverage seviyesini platform bazli olarak raporlamak

## Onerilen teknoloji secimi

Web uzerinden gelistirelim.

- Frontend: Next.js 15 + TypeScript
- Backend API: Next.js Route Handlers veya ayrik NestJS/Fastify servis
- Worker: Node.js worker + BullMQ
- Queue: Redis
- Veritabani: PostgreSQL
- Arama: ilk surumde PostgreSQL full-text + trigram
- Dosya saklama: opsiyonel S3 uyumlu obje depolama
- Gozlemleme: OpenTelemetry + Sentry + structured logs

## Neden bu yapi?

- Tek bir web uygulamasi ile hizli baslangic yapariz
- API anahtarlarini backend tarafinda gizleriz
- Veri toplama ve UI akisini ayiririz
- DB sayesinde daha once yakalanan icerigi saklariz
- Ileride vendor entegrasyonu eklemek kolay olur
- Ilk surumde Elasticsearch zorunlu olmaz

## Yuksek seviye mimari

```text
Kullanici
  |
  v
Next.js Web UI
  |
  v
API/BFF
  |------------------------------|
  |                              |
  v                              v
PostgreSQL                   Redis + Worker Queue
  ^                              |
  |                              v
  |                       Collector Workers
  |                              |
  |            -----------------------------------------
  |            |            |            |             |
  |            v            v            v             v
  |        YouTube      Facebook     Instagram     LinkedIn
  |        Adapter      Adapter      Adapter       Adapter
  |            \            |            |             /
  |             \-----------|------------|------------/
  |                         v
  |                    Normalizer
  |                         |
  |                         v
  -------------------- Upsert / Dedupe
```

## Uygulama davranisi

Kullanici arama yaptiginda sistem yalnizca canli API cevabi beklememeli. En dogru model hibrit modeldir:

1. Kullanici `carrefoursa, karfur, carrefur` gibi kelimeler girer.
2. Sistem once DB'deki mevcut eslesmeleri hizli sekilde getirir.
3. Ayni anda worker'lara "bu sorguyu kaynaklardan yeniden tara" gorevi atilir.
4. Taze veri geldikce DB'ye upsert edilir.
5. UI yeni bulunan sonuclari canli olarak yeniler.

Bu model sayesinde:

- ilk ekran hizli acilir
- daha once yakalanan icerik kaybolmaz
- yeni bulunan icerikler sonradan eklenir
- kullanici her aramada sifirdan beklemez

## Ana moduller

### 1. Search UI

- arama kutusu
- kaynak filtresi
- zaman filtresi
- sirlama: en yeni -> en eski
- icerik tipi: post, comment, video, page post, hashtag post
- coverage etiketi: `live`, `cached`, `vendor`, `owned-source`

### 2. Query Manager

Kullanici sorgularini normalize eder:

- kucuk harfe cevirme
- Turkce karakter varyasyonlari
- typo varyantlari
- es anlamli / markaya ozel varyantlar

Ornek:

- carrefoursa
- carrefour sa
- karfur
- carrefur

### 3. Source Adapters

Her platform icin ayri adaptor yazilir.

Adaptor sorumluluklari:

- kaynak kimlik bilgilerini yonetmek
- resmi API veya vendor API cagrisi yapmak
- cevabi ortak formata cevirmek
- rate limit ve hata yonetimi yapmak

### 4. Normalizer

Tum platformlardan gelen veriyi ortak bir veri modeline indirger:

- kaynak
- icerik tipi
- ana metin
- medya linkleri
- yazar
- yayin tarihi
- kaynak URL
- platforma ozel ham payload

### 5. Collector Worker

Arka planda calisir:

- kayitli sorgulari periyodik tarar
- manuel arama tetiklemelerinde canli fetch yapar
- duplicate kontrolu yapar
- guncellenmis engagement sayilarini tekrar cekebilir

### 6. Search Engine

Ilk surumde PostgreSQL ile baslayalim:

- `tsvector` full-text search
- trigram similarity
- exact phrase search
- keyword hit table

Ileride ihtiyac olursa OpenSearch eklenir.

## Onerilen veri akisi

### Arama zamani

1. UI `GET /api/search?q=carrefoursa&from=...&to=...`
2. API once PostgreSQL'den sonuclari getirir
3. Eger sorgu stale ise worker job uretir
4. Worker adapter'lari calistirir
5. Yeni veriler `content_items` tablosuna yazilir
6. UI polling veya websocket ile taze sonuclari alir

### Periyodik toplama

1. Kayitli sorgular scheduler tarafindan secilir
2. Her sorgu ilgili kaynak adaptorlari icin queue'ya atilir
3. Gelen veriler normalize edilir
4. Duplicate veriler merge edilir
5. Keyword hit ve mention tablolarina islenir

## Veritabani tasarimi

### `search_queries`

- id
- name
- raw_query
- normalized_terms jsonb
- platforms jsonb
- is_active
- created_at
- updated_at

### `sources`

- id
- platform
- source_type
- source_external_id
- source_name
- is_owned
- metadata jsonb
- created_at

### `content_items`

- id
- platform
- source_id
- external_content_id
- content_type
- author_name
- author_external_id
- title
- body_text
- body_text_normalized
- content_url
- permalink
- language
- published_at
- collected_at
- updated_at
- raw_payload jsonb
- checksum
- is_deleted

### `content_metrics`

- id
- content_item_id
- likes_count
- comments_count
- shares_count
- views_count
- captured_at

### `keyword_hits`

- id
- query_id
- content_item_id
- matched_terms jsonb
- match_score
- first_seen_at
- last_seen_at

### `collection_runs`

- id
- query_id
- platform
- status
- started_at
- finished_at
- result_count
- error_message
- metadata jsonb

## Duplicate engelleme stratejisi

Her platform icin benzersiz kimlik gerekir:

- platform + external_content_id

Ek olarak:

- URL checksum
- normalized text fingerprint
- yayin tarihi yakinligi

Bu sayede ayni icerik tekrar geldiyse yeni satir acmak yerine guncelleme yapariz.

## Coverage arttirma stratejisi

En kritik kisim burasi.

### 1. Resmi API + cache + tekrar tarama

- bulunani kaydet
- ayni sorguyu belirli araliklarla yeniden tara
- engagement metriklerini ayri guncelle

### 2. Owned source baglantilari

Ozellikle Facebook, Instagram ve LinkedIn icin en temiz coverage artisi buradan gelir:

- baglanan page/account/company page verileri
- yorumlar
- owned metrikler

### 3. Vendor adapter katmani

Mimari en bastan vendor baglamaya hazir olmali.

Desteklenecek adaptor tipi:

- official-api
- vendor-api
- owned-source-api

Boylece ileride Brandwatch, Sprinklr veya Meltwater eklenirse cekirdegi bozmadan baglariz.

### 4. Query enrichment

Kullanicinin girdigi tek kelimeyi zenginlestiririz:

- typo varyantlari
- marka yazim varyantlari
- hashtag varyantlari
- quote / phrase varyantlari

Bu coverage'i API degistirmeden artirir.

### 5. Source watchlist

Genel aramanin zayif oldugu platformlarda belirli kaynaklari aktif izlemek daha yuksek coverage verir.

Ornek:

- belirli Facebook page listesi
- belirli Instagram creator/business hesaplari
- belirli LinkedIn company page'ler
- belirli YouTube channel listeleri

## Platform bazli beklenti

### YouTube

- genel keyword arama en guclu platform
- video ve yorum coverage'i iyi
- ilk surumde ana kaynak olabilir

### Facebook

- owned page ve belirli public page odakli dusunulmeli
- genel public post coverage'i dusuk kalabilir
- vendor varsa adaptor ile desteklenmeli

### Instagram

- hashtag ve tracked hesaplar daha guclu
- genel serbest metin coverage'i sinirli olabilir

### LinkedIn

- owned company page coverage'i uygun
- genel public post coverage'i sinirli olabilir

## Kullaniciya gosterilecek durumlar

UI seffaf olmali:

- `Cached result`: daha once yakalanmis veri
- `Fresh result`: yeni cekilen veri
- `Coverage limited`: bu platformda resmi erisim sinirli
- `Owned source`: yetkili bagli kaynaktan geldi
- `Vendor source`: lisansli veri saglayicisindan geldi

Bu etiketler urunu durust hale getirir.

## Hukuki ve operasyonel notlar

- public veri toplamak da kişisel veri isleme anlamina gelebilir
- aydinlatma, saklama suresi ve silme politikasi tanimlanmali
- platform bazli terms'e uygun adaptor ayrimi yapilmali
- silinen iceriklerin yeniden gosterimi icin retention politikasina karar verilmeli

## Ilk surum kapsam onerisi

### Faz 1

- Next.js web uygulamasi
- PostgreSQL
- Redis + BullMQ
- YouTube adaptor
- Facebook owned/public page adaptor
- temel arama ekrani
- DB cache mantigi

### Faz 2

- Instagram adaptor
- LinkedIn owned page adaptor
- query enrichment
- source watchlist
- canli yenileme

### Faz 3

- vendor adaptor katmani
- gelismis coverage raporlama
- alarm sistemi
- raporlama ve dashboard

## Baslangic icin net tavsiye

Bu urunu yapabiliriz.

Ama dogru hedef su olmali:

- web tabanli uygulama
- once DB merkezli hibrit arama
- resmi API ile cekebildigimizi cekmek
- buldugumuzu kalici saklamak
- vendor entegrasyonuna acik altyapi kurmak

Ilk teknik secim:

- Next.js
- Node worker
- PostgreSQL
- Redis

Bu kombinasyon hizli baslangic, dusuk karmasiklik ve buyume esnekligi acisindan en mantikli secimdir.
