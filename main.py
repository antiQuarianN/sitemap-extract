import xml.etree.ElementTree as ET
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
from datetime import datetime

# Функция для загрузки и парсинга Sitemap
def fetch_and_parse_sitemap(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return ET.fromstring(response.content)
        else:
            print(f"Ошибка при загрузке: {url} (Код: {response.status_code})")
            return None
    except Exception as e:
        print(f"Ошибка при подключении к {url}: {e}")
        return None

# Функция для проверки кода ответа страницы
def check_url_status(url):
    try:
        response = requests.get(url)
        return url, response.status_code
    except Exception as e:
        print(f"Ошибка при проверке {url}: {e}")
        return url, 'error'

### Обработка для urlset
def process_urlset(sitemap_content, parent_sitemap=None, path_filter=None):
    urls_data = []
    
    for url in sitemap_content.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
        loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
        lastmod = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
        lastmod = lastmod.text if lastmod is not None else 'none'
        
        changefreq = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}changefreq')
        changefreq = changefreq.text if changefreq is not None else 'none'
        
        priority = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}priority')
        priority = priority.text if priority is not None else 'none'
        
        # Применяем фильтрацию по пути
        if path_filter is None or path_filter in loc:
            urls_data.append([loc, lastmod, changefreq, priority, 'none', parent_sitemap])
    
    return urls_data

### Обработка для sitemapindex - извлекаем ссылки из вложенных sitemaps
def process_sitemapindex(sitemap_content, path_filter=None):
    urls_data = []
    
    sitemaps = sitemap_content.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap')
    total_sitemaps = len(sitemaps)
    
    for i, sitemap in enumerate(sitemaps):
        loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
        print(f"Обработка вложенного sitemap: {i+1}/{total_sitemaps}")
        
        nested_sitemap_content = fetch_and_parse_sitemap(loc)
        if nested_sitemap_content is not None:
            urls_data.extend(process_urlset(nested_sitemap_content, parent_sitemap=loc, path_filter=path_filter))
    
    print("Сканирование sitemapindex завершено. Начинаю обработку ссылок...")
    
    return urls_data

### Проверка URL для urlset
def check_urls_for_urlset(urls):
    url_status_data = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(check_url_status, url[0]): url for url in urls}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Проверка URL"):
            url, status_code = future.result()
            original_url_data = futures[future]
            url_status_data.append([original_url_data[5], original_url_data[0], status_code, original_url_data[1], original_url_data[2], original_url_data[3]])
    
    return url_status_data

# Функция для создания уникального имени файла
def create_unique_filename(base_name):
    now = datetime.now()
    time_str = now.strftime("%H-%M")
    
    counter = 1
    while True:
        filename = f"{base_name}_{time_str}_{counter}.xlsx"
        if not os.path.exists(filename):
            break
        counter += 1
    
    return filename

# Сохранение данных для urlset
def save_urlset_to_excel(data, base_output_file, chunk_size=1000000):
    total_rows = len(data)
    num_chunks = (total_rows // chunk_size) + 1
    columns = ['source_sitemap', 'loc', 'status_code', 'lastmod', 'changefreq', 'priority']
    
    for i in range(num_chunks):
        chunk_data = data[i * chunk_size:(i + 1) * chunk_size]
        df = pd.DataFrame(chunk_data, columns=columns)
        filename = create_unique_filename(base_output_file)
        df.to_excel(filename, index=False)
        print(f"Данные успешно сохранены в {filename}")

# Основная функция
def main():
    sitemap_url = input("Введите URL sitemap: ")
    path_filter = input("Введите фильтр по пути (например, /industries) или оставьте пустым для всех ссылок: ")
    path_filter = path_filter.strip() if path_filter else None
    
    sitemap_content = fetch_and_parse_sitemap(sitemap_url)
    if sitemap_content is not None:
        if sitemap_content.tag.endswith('urlset'):
            urls_data = process_urlset(sitemap_content, path_filter=path_filter)
            url_status_data = check_urls_for_urlset(urls_data)
            save_urlset_to_excel(url_status_data, 'urlset')
        elif sitemap_content.tag.endswith('sitemapindex'):
            urls_data = process_sitemapindex(sitemap_content, path_filter=path_filter)
            url_status_data = check_urls_for_urlset(urls_data)
            save_urlset_to_excel(url_status_data, 'sitemapindex')
    else:
        print("Не удалось загрузить основной sitemap.")

if __name__ == "__main__":
    main()
