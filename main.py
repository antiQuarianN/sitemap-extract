import xml.etree.ElementTree as ET

def extract_urls_from_sitemap(file_path):
    # Читаем содержимое файла
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Парсим содержимое как XML
    root = ET.fromstring(content)

    # Извлекаем все URL
    urls = []
    for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
        loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
        urls.append(loc)

    return urls

def save_urls_to_file(urls, output_file):
    with open(output_file, 'w', encoding='utf-8') as file:
        for url in urls:
            file.write(url + '\n')

# Названия файлов
sitemap_path = 'sitemap.xml'
output_file = 'links.txt'

urls = extract_urls_from_sitemap(sitemap_path)
save_urls_to_file(urls, output_file)