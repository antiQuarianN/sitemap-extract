import xml.etree.ElementTree as ET
import pandas as pd

def extract_urls_from_sitemap(file_path):
    # Читаем содержимое файла
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Парсим содержимое как XML
    root = ET.fromstring(content)

    # Извлекаем все URL и дополнительные данные
    urls = []
    data = []
    for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
        loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
        urls.append(loc)
        
        lastmod = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
        lastmod = lastmod.text if lastmod is not None else 'none'
        
        changefreq = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}changefreq')
        changefreq = changefreq.text if changefreq is not None else 'none'
        
        priority = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}priority')
        priority = priority.text if priority is not None else 'none'
        
        data.append([loc, lastmod, changefreq, priority])

    return urls, data

def save_urls_to_file(urls, output_file):
    with open(output_file, 'w', encoding='utf-8') as file:
        for url in urls:
            file.write(url + '\n')

def save_data_to_excel(data, output_file):
    df = pd.DataFrame(data, columns=['loc', 'lastmod', 'changefreq', 'priority'])
    df.to_excel(output_file, index=False)

def main():
    sitemap_path = 'sitemap.xml'
    
    choice = input("Выберите вариант выгрузки (1: только URL, 2: все данные в Excel): ")
    
    urls, data = extract_urls_from_sitemap(sitemap_path)
    
    if choice == '1':
        output_file = 'links.txt'
        save_urls_to_file(urls, output_file)
        print(f"URL успешно сохранены в {output_file}")
    elif choice == '2':
        output_file = 'sitemap_data.xlsx'
        save_data_to_excel(data, output_file)
        print(f"Данные успешно сохранены в {output_file}")
    else:
        print("Неверный выбор. Попробуйте снова.")

if __name__ == "__main__":
    main()