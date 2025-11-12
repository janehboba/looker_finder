"""
Complete Website Crawler - All-in-One Script
Crawls entire websites and saves data locally for manual upload to Databricks

Usage:
    1. Edit the CONFIGURATION section below
    2. Run: python website_crawler.py
    3. Find output in ./crawled_data/ directory
    4. Upload JSON file to Databricks manually
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import json
import time
import os
from datetime import datetime
from collections import deque
import re
from typing import Set, Dict, List, Tuple
import logging
from pathlib import Path


# ============================================================================
# CONFIGURATION - EDIT THESE SETTINGS
# ============================================================================

CONFIG = {
    # Target website
    'base_url': 'https://example.com',  # CHANGE THIS!
    
    # Crawling limits
    'max_pages': 100,                    # Maximum pages to crawl
    'max_depth': 0,                      # 0 = unlimited, 1 = homepage only, 2 = homepage + 1 level
    'delay_between_requests': 2.0,       # Seconds between requests (be respectful!)
    
    # Content settings
    'max_content_length': 50000,         # Max characters per page
    'min_content_length': 100,           # Minimum content to save a page
    'request_timeout': 15,               # Request timeout in seconds
    
    # Behavior
    'respect_robots_txt': True,          # Follow robots.txt rules
    'follow_external_links': False,      # Crawl external domains
    'remove_query_params': False,        # Remove ?query=params from URLs
    
    # Output
    'output_directory': './crawled_data',  # Where to save files
    'save_logs_to_file': True,            # Save logs to crawler.log
    'log_level': 'INFO',                  # DEBUG, INFO, WARNING, ERROR
    
    # URL filtering
    'excluded_extensions': [
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.tar', '.gz', '.jpg', '.jpeg', '.png', 
        '.gif', '.bmp', '.svg', '.webp', '.mp4', '.avi', '.mov',
        '.mp3', '.wav', '.css', '.js', '.xml', '.json', '.ico'
    ],
    'excluded_patterns': [
        '/login', '/logout', '/signin', '/signup', '/register',
        '/cart', '/checkout', '/admin', '/wp-admin',
        '/feed', '/rss', '/api/', '/search?'
    ],
    'included_patterns': [],  # Only crawl URLs with these patterns (empty = all)
    
    # Content extraction
    'content_selectors': [
        'main', 'article', '[role="main"]', '.main-content',
        '.content', '#content', '.post-content', '.article-content'
    ],
    'remove_elements': [
        'script', 'style', 'nav', 'footer', 'header', 'aside',
        '.advertisement', '.sidebar', '.comments', '#comments'
    ],
}


# ============================================================================
# CRAWLER IMPLEMENTATION
# ============================================================================

class WebsiteCrawler:
    """Complete website crawler with all features"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.base_url = config['base_url']
        self.domain = urlparse(self.base_url).netloc
        self.max_pages = config['max_pages']
        self.delay = config['delay_between_requests']
        self.max_depth = config['max_depth']
        
        self.visited_urls: Set[str] = set()
        self.to_visit: deque = deque([(self.base_url, 0)])
        self.scraped_data: List[Dict] = []
        
        self._setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.robot_parser = None
        if config['respect_robots_txt']:
            self._setup_robots_txt()
    
    def _setup_logging(self):
        """Configure logging"""
        log_level = getattr(logging, self.config['log_level'])
        handlers = [logging.StreamHandler()]
        
        if self.config['save_logs_to_file']:
            handlers.append(logging.FileHandler('crawler.log'))
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=handlers,
            force=True
        )
        self.logger = logging.getLogger(__name__)
    
    def _setup_robots_txt(self):
        """Parse robots.txt"""
        try:
            robots_url = urljoin(self.base_url, '/robots.txt')
            self.robot_parser = RobotFileParser()
            self.robot_parser.set_url(robots_url)
            self.robot_parser.read()
            self.logger.info("✓ Parsed robots.txt")
        except:
            self.robot_parser = None
    
    def can_fetch(self, url: str) -> bool:
        """Check robots.txt"""
        if not self.robot_parser:
            return True
        try:
            return self.robot_parser.can_fetch('*', url)
        except:
            return True
    
    def is_valid_url(self, url: str) -> bool:
        """Check if URL should be crawled"""
        try:
            parsed = urlparse(url)
            
            # Check domain
            if not self.config['follow_external_links']:
                if parsed.netloc and parsed.netloc != self.domain:
                    return False
            
            # Check excluded extensions
            if any(url.lower().endswith(ext) for ext in self.config['excluded_extensions']):
                return False
            
            # Check excluded patterns
            if any(pattern in url.lower() for pattern in self.config['excluded_patterns']):
                return False
            
            # Check included patterns
            if self.config['included_patterns']:
                if not any(pattern in url.lower() for pattern in self.config['included_patterns']):
                    return False
            
            # Check robots.txt
            if not self.can_fetch(url):
                return False
            
            return True
        except:
            return False
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL"""
        url = url.split('#')[0]
        if self.config['remove_query_params']:
            url = url.split('?')[0]
        return url
    
    def extract_links(self, soup: BeautifulSoup, current_url: str, current_depth: int) -> List[Tuple[str, int]]:
        """Extract valid links"""
        links = []
        
        if self.max_depth > 0 and current_depth >= self.max_depth:
            return links
        
        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            absolute_url = urljoin(current_url, href)
            absolute_url = self.normalize_url(absolute_url)
            
            if self.is_valid_url(absolute_url) and absolute_url not in self.visited_urls:
                links.append((absolute_url, current_depth + 1))
        
        return links
    
    def clean_text(self, text: str) -> str:
        """Clean extracted text"""
        return re.sub(r'\s+', ' ', text).strip()
    
    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        """Extract page metadata"""
        metadata = {}
        
        # Meta tags
        for name in ['description', 'keywords', 'author']:
            tag = soup.find('meta', attrs={'name': name})
            if tag and 'content' in tag.attrs:
                metadata[name] = tag['content']
        
        # Open Graph
        for prop in ['og:title', 'og:description', 'og:image']:
            tag = soup.find('meta', attrs={'property': prop})
            if tag and 'content' in tag.attrs:
                metadata[prop] = tag['content']
        
        return metadata
    
    def extract_content(self, soup: BeautifulSoup, url: str, depth: int) -> Dict:
        """Extract all content from page"""
        
        # Title
        title = soup.find('title')
        title_text = self.clean_text(title.text) if title else ''
        
        # Metadata
        metadata = self.extract_metadata(soup)
        
        # Main content
        main_content = None
        for selector in self.config['content_selectors']:
            main_content = soup.select_one(selector)
            if main_content:
                break
        
        if not main_content:
            main_content = soup.find('body')
        
        # Remove unwanted elements
        if main_content:
            for selector in self.config['remove_elements']:
                for element in main_content.select(selector):
                    element.decompose()
            content_text = self.clean_text(main_content.get_text())
        else:
            content_text = ''
        
        # Limit length
        max_length = self.config['max_content_length']
        if len(content_text) > max_length:
            content_text = content_text[:max_length] + '... [truncated]'
        
        # Headings
        headings = []
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            headings.append({
                'level': heading.name,
                'text': self.clean_text(heading.get_text())
            })
        
        # Links
        links = []
        for link in soup.find_all('a', href=True)[:20]:
            links.append({
                'text': self.clean_text(link.get_text()),
                'href': urljoin(url, link['href'])
            })
        
        # Images
        images = []
        for img in soup.find_all('img', alt=True)[:10]:
            if img.get('src'):
                images.append({
                    'src': urljoin(url, img['src']),
                    'alt': img['alt']
                })
        
        return {
            'url': url,
            'depth': depth,
            'title': title_text,
            'metadata': metadata,
            'content': content_text,
            'headings': headings,
            'links': links,
            'images': images,
            'content_length': len(content_text),
            'word_count': len(content_text.split()),
            'timestamp': datetime.now().isoformat()
        }
    
    def crawl_page(self, url: str, depth: int) -> bool:
        """Crawl a single page"""
        try:
            self.logger.info(f"Crawling (depth {depth}): {url}")
            
            timeout = self.config['request_timeout']
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '')
            if 'text/html' not in content_type.lower():
                self.logger.info(f"Skipping non-HTML: {url}")
                return False
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_data = self.extract_content(soup, url, depth)
            
            # Check minimum length
            if page_data['content_length'] >= self.config['min_content_length']:
                self.scraped_data.append(page_data)
                self.logger.info(
                    f"✓ {page_data['content_length']} chars, "
                    f"{page_data['word_count']} words"
                )
            else:
                self.logger.info(f"✗ Skipping (minimal content)")
            
            # Find new links
            new_links = self.extract_links(soup, url, depth)
            for link_url, link_depth in new_links:
                if link_url not in self.visited_urls:
                    if not any(link_url == u for u, _ in self.to_visit):
                        self.to_visit.append((link_url, link_depth))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error: {e}")
            return False
    
    def crawl(self) -> List[Dict]:
        """Crawl entire website"""
        self.logger.info("="*60)
        self.logger.info(f"Starting crawl: {self.base_url}")
        self.logger.info(f"Max pages: {self.max_pages}")
        self.logger.info(f"Max depth: {self.max_depth if self.max_depth > 0 else 'unlimited'}")
        self.logger.info(f"Delay: {self.delay}s")
        self.logger.info("="*60)
        
        start_time = time.time()
        
        while self.to_visit and len(self.visited_urls) < self.max_pages:
            url, depth = self.to_visit.popleft()
            
            if url in self.visited_urls:
                continue
            
            self.visited_urls.add(url)
            self.crawl_page(url, depth)
            time.sleep(self.delay)
            
            # Progress
            if len(self.visited_urls) % 10 == 0:
                elapsed = time.time() - start_time
                rate = len(self.visited_urls) / elapsed if elapsed > 0 else 0
                self.logger.info(
                    f"Progress: {len(self.visited_urls)} pages, "
                    f"{len(self.to_visit)} queued, "
                    f"{rate:.2f} pages/sec"
                )
        
        elapsed = time.time() - start_time
        self.logger.info("="*60)
        self.logger.info(f"✓ Crawl complete!")
        self.logger.info(f"Total pages: {len(self.visited_urls)}")
        self.logger.info(f"Pages saved: {len(self.scraped_data)}")
        self.logger.info(f"Time: {elapsed:.1f}s")
        self.logger.info("="*60)
        
        return self.scraped_data


def save_results(data: List[Dict], config: Dict):
    """Save crawled data locally"""
    output_dir = config['output_directory']
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save main data
    data_file = f"scraped_data_{timestamp}.json"
    data_path = os.path.join(output_dir, data_file)
    
    with open(data_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Data saved: {data_path}")
    
    # Save summary
    summary = {
        'timestamp': timestamp,
        'base_url': config['base_url'],
        'total_pages': len(data),
        'total_words': sum(p.get('word_count', 0) for p in data),
        'total_characters': sum(p['content_length'] for p in data),
        'urls': [p['url'] for p in data],
        'depths': [p.get('depth', 0) for p in data],
        'data_file': data_file,
        'statistics': {
            'avg_content_length': sum(p['content_length'] for p in data) / len(data) if data else 0,
            'avg_word_count': sum(p.get('word_count', 0) for p in data) / len(data) if data else 0,
            'max_depth': max(p.get('depth', 0) for p in data) if data else 0
        }
    }
    
    summary_file = f"summary_{timestamp}.json"
    summary_path = os.path.join(output_dir, summary_file)
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Summary saved: {summary_path}")
    
    return data_path, summary_path


def main():
    """Main execution"""
    print("\n" + "="*60)
    print("Website Crawler for Databricks")
    print("="*60)
    
    # Validate config
    if CONFIG['base_url'] == 'https://example.com':
        print("\n⚠️  WARNING: You need to edit the CONFIG section!")
        print("Change 'base_url' to your target website.\n")
        return
    
    # Crawl
    crawler = WebsiteCrawler(CONFIG)
    
    try:
        data = crawler.crawl()
    except KeyboardInterrupt:
        print("\n\n⚠️  Crawl interrupted by user")
        data = crawler.scraped_data
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return
    
    if not data:
        print("\n❌ No data scraped. Check your configuration.")
        return
    
    # Save results
    data_path, summary_path = save_results(data, CONFIG)
    
    # Final summary
    print("\n" + "="*60)
    print("SUCCESS!")
    print("="*60)
    print(f"Pages crawled: {len(data)}")
    print(f"Total words: {sum(p.get('word_count', 0) for p in data):,}")
    print(f"Data file: {data_path}")
    print(f"\nNext step: Upload {os.path.basename(data_path)} to Databricks")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
