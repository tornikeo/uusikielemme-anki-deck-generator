import warnings
warnings.filterwarnings('ignore')
import dotenv 
from openai import OpenAI
from tqdm import tqdm
import requests
import re
from bs4 import BeautifulSoup
from joblib import Parallel, delayed, Memory
import backoff
from markdownify import markdownify as md
from IPython.display import display, Markdown, Latex
from tqdm import tqdm
import pandas as pd



@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=60)
def fetch_sitemap(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def fetch_urls_from_sitemap_with_pattern(sitemap_url, pattern):
    sitemap_content = fetch_sitemap(sitemap_url)
    if sitemap_content:
        soup = BeautifulSoup(sitemap_content, 'xml')
        urls = [loc.text for loc in soup.find_all('loc') if re.match(pattern, loc.text)]
        return urls
    else:
        print(f"Failed to fetch sitemap from {sitemap_url}")
        return []
    
def extract_urls_from_category_page(category_url):
    response = requests.get(category_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', class_='entry_title')
        title = soup.find('h1', class_='archive_title').text.strip()
        urls = [link.get('href') for link in links]
        return title, urls
    else:
        print(f"Failed to fetch category page from {category_url}")
        return '', []

def extract_post_content(post_url):
    response = requests.get(post_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title = soup.find('h1', class_='post_title').text.strip()
        
        # Extract content
        content = soup.find('article', id='post_body')
        markdown_content = f'# {title} \n\n'
        text = f'# {title} \n\n'
        tables = []
        for element in content.children:
            if element.name == 'p':
                markdown_content += md(str(element)) + '\n\n'
                text += md(str(element)) + '\n\n'
            elif element.name == 'table':
                markdown_content += md(str(element)) + '\n\n'
                df = pd.read_html(str(element), header=0)[0]  # Assume only one table per element
                tables.append(df)
            elif element.name == 'div':
                pass
            elif element.name == 'ul':
                # Ignore unordered lists
                pass
        
        return {'title': title, 
                'text': text,
                'tables': tables,
                'content': markdown_content.strip()}
    else:
        print(f"Failed to fetch post page from {post_url}")
        return {'title': '', 'content': '', 'text':'', 'tables': []}
    
# Instead of lru_cache, we have to use joblib.Memory, as per https://github.com/joblib/joblib/issues/226
mem = Memory('.joblib_cache')
fetch_sitemap = mem.cache(fetch_sitemap, verbose=False)
extract_post_content = mem.cache(extract_post_content, verbose=False)

def scrape():
    sitemap_url = "https://uusikielemme.fi/category-sitemap.xml"
    pattern = r'https://uusikielemme.fi/category/finnish-.*'  # Example regex pattern

    urls = fetch_urls_from_sitemap_with_pattern(sitemap_url, pattern)

    # Print the matched URLs
    jobs = []
    for url in urls:
        def fun(url):
            # print(url)
            # print("Extracted URLs from category page")
            category_title, post_urls = extract_urls_from_category_page(url)
            post_data = []
            for post_url in post_urls:
                page_data = extract_post_content(post_url)
                post_data.append(page_data)
            return dict(category=category_title, posts=post_data)
                # print("Title:", post_data['title'])
                # print("Content:", post_data['content'])
                # display(post_data['title'])
                # display(Markdown(post_data['content']))
                # for table in post_data['tables']:
                    # display(table)
        jobs.append(delayed(fun)(url))
    data = Parallel(64)(tqdm(jobs)) # Bottleneck isn't CPU but waiting for requests.get, so this should be reasonable
    return data