import xml.etree.ElementTree as ET
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

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

# Функция для извлечения URL из sitemap с фильтрацией по заданному пути
def extract_urls_from_sitemap(sitemap_content, parent_sitemap=None, path_filter=None):
    urls_data = []
    
    if sitemap_content.tag.endswith('sitemapindex'):
        sitemaps = sitemap_content.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap')
        total_sitemaps = len(sitemaps)
        for i, sitemap in enumerate(sitemaps):
            loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
            print(f"Обработка вложенного Sitemap: {i+1}/{total_sitemaps}")
            nested_sitemap_content = fetch_and_parse_sitemap(loc)
            if nested_sitemap_content is not None:
                urls_data.extend(extract_urls_from_sitemap(nested_sitemap_content, parent_sitemap=loc, path_filter=path_filter))
        print("Сканирование sitemapindex завершено. Начинаю обработку ссылок...")
    
    elif sitemap_content.tag.endswith('urlset'):
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
                urls_data.append([loc, lastmod, changefreq, priority, parent_sitemap])
    
    return urls_data

# Функция для многопоточной проверки всех URL с прогресс-баром
def check_urls_in_parallel(urls):
    url_status_data = []
    
    # Используем ThreadPoolExecutor для многопоточности с прогресс-баром
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(check_url_status, url[0]): url for url in urls}
        
        # Добавляем прогресс-бар для отслеживания
        for future in tqdm(as_completed(futures), total=len(futures), desc="Проверка URL"):
            url, status_code = future.result()
            original_url_data = futures[future]
            url_status_data.append([original_url_data[0], original_url_data[1], original_url_data[2], original_url_data[3], original_url_data[4], status_code])
    
    return url_status_data

# Функция для сохранения данных в xlsx с разбиением на файлы
def save_data_to_excel_in_chunks(data, base_output_file, chunk_size=1000000):
    total_rows = len(data)
    num_chunks = (total_rows // chunk_size) + 1
    
    for i in range(num_chunks):
        chunk_data = data[i * chunk_size:(i + 1) * chunk_size]
        df = pd.DataFrame(chunk_data, columns=['source_sitemap', 'loc', 'status_code', 'lastmod', 'changefreq', 'priority'])
        output_file = f"{os.path.splitext(base_output_file)[0]}_{i+1}.xlsx"
        df.to_excel(output_file, index=False)
        print(f"Данные успешно сохранены в {output_file}")

def main():
    sitemap_url = input("Введите URL sitemap: ")
    path_filter = input("Введите фильтр по пути (например, /industries) или оставьте пустым для всех ссылок: ")
    path_filter = path_filter.strip() if path_filter else None
    
    # Загружаем и парсим основной sitemap
    sitemap_content = fetch_and_parse_sitemap(sitemap_url)
    if sitemap_content is not None:
        # Извлекаем данные из sitemap с фильтрацией по пути
        urls_data = extract_urls_from_sitemap(sitemap_content, path_filter=path_filter)
        
        # Многопоточная проверка URL и их статусов с прогресс-баром
        url_status_data = check_urls_in_parallel(urls_data)
        
        # Сохраняем данные в xlsx с разбиением на файлы
        base_output_file = 'sitemap_done.xlsx'
        save_data_to_excel_in_chunks(url_status_data, base_output_file)
    else:
        print("Не удалось загрузить основной sitemap.")

if __name__ == "__main__":
    main()
