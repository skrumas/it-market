import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def parse_condition(condition_url):
    """Schema.org URL'sini okunabilir hale çevir"""
    mapping = {
        'https://schema.org/NewCondition': 'Neu',
        'https://schema.org/RefurbishedCondition': 'Generalüberholt',
        'https://schema.org/UsedCondition': 'Gebraucht',
        'https://schema.org/DamagedCondition': 'Beschädigt'
    }
    return mapping.get(condition_url, condition_url)

def detect_availability_from_url(url):
    """URL'den availability'yi tespit et (E2 kodu = ab Lager)"""
    if '-E2-' in url or url.endswith('-E2'):
        return 'ab Lager'
    else:
        return 'mit Lieferzeit'

def get_product_name(soup, json_data_list):
    """Ürün adını birden fazla yöntemle bulmaya çalış"""
    
    # YÖNTEM 1: JSON-LD'den al (en çok offer'i olan Product'ı al)
    max_offers = 0
    best_name = ""
    
    for data in json_data_list:
        try:
            if isinstance(data, list):
                for item in data:
                    if item.get('@type') == 'Product' and 'name' in item and 'offers' in item:
                        offer_count = len(item.get('offers', []))
                        if offer_count > max_offers:
                            max_offers = offer_count
                            best_name = item['name']
            elif data.get('@type') == 'Product' and 'name' in data and 'offers' in data:
                offer_count = len(data.get('offers', []))
                if offer_count > max_offers:
                    max_offers = offer_count
                    best_name = data['name']
        except:
            continue
    
    if best_name:
        return best_name
    
    # YÖNTEM 2: H1 page-title
    h1 = soup.find('h1', class_='page-title')
    if h1:
        span = h1.find('span', class_='base')
        if span:
            return span.get_text(strip=True)
        else:
            return h1.get_text(strip=True)
    
    # YÖNTEM 3: Herhangi bir H1
    h1 = soup.find('h1')
    if h1:
        return h1.get_text(strip=True)
    
    # YÖNTEM 4: Title tag
    title = soup.find('title')
    if title:
        return title.get_text(strip=True).split('|')[0].strip()
    
    # YÖNTEM 5: Meta property og:title
    meta = soup.find('meta', property='og:title')
    if meta and meta.get('content'):
        return meta['content']
    
    return "UNKNOWN"

def scrape_product_variants(url, max_retries=3):
    """Ürün varyantlarını scrape et — 503'te retry yapar"""
    
    logger.info(f"Scraping: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 503:
                wait = attempt * 10  # 10s, 20s, 30s
                logger.warning(f"  HTTP 503 — {attempt}/{max_retries} deneme, {wait}s bekleniyor...")
                time.sleep(wait)
                continue
            
            if response.status_code != 200:
                logger.warning(f"  HTTP {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # JSON-LD script'lerini bul
            scripts = soup.find_all('script', type='application/ld+json')
            
            json_data_list = []
            all_variants = []
            
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    json_data_list.append(data)
                    
                    # Liste formatında
                    if isinstance(data, list):
                        for item in data:
                            if item.get('@type') == 'Product' and 'offers' in item:
                                offer_count = len(item['offers'])
                                for offer in item['offers']:
                                    condition = parse_condition(offer['itemCondition'])
                                    offer_url = offer.get('url', '')
                                    availability = detect_availability_from_url(offer_url)
                                    price = offer['price']
                                    all_variants.append({
                                        'condition': condition,
                                        'availability': availability,
                                        'price': price,
                                        'url': offer_url,
                                        'offer_count': offer_count
                                    })
                    
                    # Tek obje formatında
                    elif data.get('@type') == 'Product' and 'offers' in data:
                        offer_count = len(data['offers'])
                        for offer in data['offers']:
                            condition = parse_condition(offer['itemCondition'])
                            offer_url = offer.get('url', '')
                            availability = detect_availability_from_url(offer_url)
                            price = offer['price']
                            all_variants.append({
                                'condition': condition,
                                'availability': availability,
                                'price': price,
                                'url': offer_url,
                                'offer_count': offer_count
                            })
                            
                except Exception as e:
                    logger.debug(f"  JSON parse error: {e}")
                    continue
            
            # En çok offer'i olan Product'tan gelen varyantları seç
            if all_variants:
                max_offer_count = max(v['offer_count'] for v in all_variants)
                variants = [v for v in all_variants if v['offer_count'] == max_offer_count]
            else:
                variants = []
            
            product_name = get_product_name(soup, json_data_list)
            
            return {
                'product_name': product_name,
                'variants': variants
            }
            
        except Exception as e:
            logger.error(f"  Error (deneme {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(5)
            else:
                return None

    return None

def process_urls(input_excel, output_excel, max_workers=1):
    """Excel'den URL'leri oku ve işle"""
    
    logger.info(f"Excel okunuyor: {input_excel}")
    df_input = pd.read_excel(input_excel)
    
    url_column = df_input.columns[0]
    urls = df_input[url_column].tolist()
    
    urls = [str(url).strip() for url in urls if pd.notna(url)]
    urls = [url if url.startswith('http') else 'https://' + url for url in urls]
    
    logger.info(f"Toplam {len(urls)} URL bulundu\n")
    
    results = []
    results_lock = Lock()
    
    def process_single_url(index_url):
        """Tek bir URL'yi işle"""
        index, url = index_url
        try:
            data = scrape_product_variants(url)
            
            if data and data['variants']:
                variant_dict = {
                    'Generalüberholt - ab Lager': '',
                    'Generalüberholt - mit Lieferzeit': '',
                    'Neu - ab Lager': '',
                    'Neu - mit Lieferzeit': ''
                }
                
                for variant in data['variants']:
                    key = f"{variant['condition']} - {variant['availability']}"
                    if key in variant_dict:
                        variant_dict[key] = variant['price']
                
                result = {
                    'Url': url,
                    'Product Name': data['product_name'],
                    'Generalüberholt - ab Lager': variant_dict['Generalüberholt - ab Lager'],
                    'Generalüberholt - mit Lieferzeit': variant_dict['Generalüberholt - mit Lieferzeit'],
                    'Neu - ab Lager': variant_dict['Neu - ab Lager'],
                    'Neu - mit Lieferzeit': variant_dict['Neu - mit Lieferzeit']
                }
            else:
                result = {
                    'Url': url,
                    'Product Name': data['product_name'] if data else 'ERROR',
                    'Generalüberholt - ab Lager': '',
                    'Generalüberholt - mit Lieferzeit': '',
                    'Neu - ab Lager': '',
                    'Neu - mit Lieferzeit': ''
                }
            
            with results_lock:
                results.append(result)
                logger.info(f"[{len(results)}/{len(urls)}] ✓ {result['Product Name']}")
            
            # Rate limiting — 3 saniye bekle
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"[{index}] Exception: {e}")
            with results_lock:
                results.append({
                    'Url': url,
                    'Product Name': 'EXCEPTION',
                    'Generalüberholt - ab Lager': '',
                    'Generalüberholt - mit Lieferzeit': '',
                    'Neu - ab Lager': '',
                    'Neu - mit Lieferzeit': ''
                })
    
    logger.info(f"Scraping basliyor ({max_workers} worker)...\n")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_url, item): item for item in enumerate(urls)}
        completed = 0
        
        for future in as_completed(futures):
            completed += 1
            if completed % 100 == 0:
                logger.info(f"İlerleme: {completed}/{len(urls)} ({completed*100//len(urls)}%)")
    
    df_output = pd.DataFrame(results)
    
    logger.info(f"\nExcel kaydediliyor: {output_excel}")
    df_output.to_excel(output_excel, index=False, engine='openpyxl')
    
    logger.info(f"\n✓ TAMAMLANDI! {len(results)} urun islendi.")
    logger.info(f"Dosya: {output_excel}")
    
    return df_output

# KULLANIM
if __name__ == "__main__":
    
    input_file = "/Users/sukru/Downloads/input_urls.xlsx"
    output_file = f"/Users/sukru/Downloads/itmarket_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    max_workers = int(__import__('os').getenv('MAX_WORKERS', '1'))
    
    try:
        df = process_urls(input_file, output_file, max_workers=max_workers)
        
        logger.info("\n" + "="*80)
        logger.info("OZET:")
        logger.info("="*80)
        logger.info(df.to_string(index=False))
        
    except FileNotFoundError:
        logger.error(f"\nHATA: Input dosyasi bulunamadi: {input_file}")
        logger.error("Ilk sutuna URL'leri yazin (baslik: 'Url' veya 'URL')")
        
    except Exception as e:
        logger.error(f"\nHATA: {e}")
        import traceback
        traceback.print_exc()
