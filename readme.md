# Website Crawler for Databricks Deep Research Agent

A comprehensive Python-based website crawler that systematically crawls entire websites and saves data locally for you to manually upload to Databricks.

## Features

- ✅ **Full Website Crawling**: Automatically discovers and crawls all pages on a website
- ✅ **Robots.txt Compliant**: Respects website crawling rules
- ✅ **Depth Control**: Limit crawling by link depth
- ✅ **Content Extraction**: Intelligently extracts main content, metadata, headings, and images
- ✅ **Configurable Filtering**: Exclude specific URLs, file types, or paths
- ✅ **Rate Limiting**: Respectful delays between requests
- ✅ **Retry Logic**: Handles temporary failures gracefully
- ✅ **Progress Tracking**: Real-time logging of crawl progress
- ✅ **Local Storage**: Saves data as JSON files for manual upload

## Installation

### Install Dependencies

```bash
pip install -r requirements.txt
```

That's it! No Databricks authentication needed for the crawler.

## Quick Start

### Basic Usage

1. **Edit the configuration file** (`crawler_config.py`):

```python
CRAWLER_CONFIG = {
    'base_url': 'https://your-target-website.com',  # Change this!
    'max_pages': 100,
    'delay_between_requests': 2.0,
}
```

2. **Run the crawler**:

```bash
python enhanced_crawler.py
```

3. **Find your data**: The crawler saves files to `./crawled_data/` directory:
   - `scraped_data_TIMESTAMP.json` - All crawled content
   - `summary_TIMESTAMP.json` - Crawl statistics

4. **Upload to Databricks**: Manually upload the JSON file through the Databricks UI:
   - Go to your Databricks workspace
   - Navigate to Data > DBFS or Workspace
   - Upload `scraped_data_TIMESTAMP.json`
   - Note the path for use in notebooks

## Configuration Guide

### Crawler Settings (`CRAWLER_CONFIG`)

```python
CRAWLER_CONFIG = {
    # Starting URL
    'base_url': 'https://example.com',
    
    # Max pages to crawl (increase for larger sites)
    'max_pages': 100,
    
    # Delay between requests (seconds) - be respectful!
    'delay_between_requests': 2.0,
    
    # Max content per page (characters)
    'max_content_length': 50000,
    
    # Request timeout
    'request_timeout': 15,
    
    # Follow external links?
    'follow_external_links': False,
    
    # Respect robots.txt?
    'respect_robots_txt': True,
}
```

### Content Extraction (`CONTENT_CONFIG`)

```python
CONTENT_CONFIG = {
    # Where to look for main content
    'content_selectors': [
        'main', 'article', '[role="main"]',
        '.content', '#content', '.post-content'
    ],
    
    # Elements to remove
    'remove_elements': [
        'script', 'style', 'nav', 'footer',
        'header', '.advertisement', '.sidebar'
    ],
    
    # Minimum content length to save
    'min_content_length': 100,
}
```

### URL Filtering (`URL_FILTER_CONFIG`)

```python
URL_FILTER_CONFIG = {
    # Exclude these file types
    'excluded_extensions': [
        '.pdf', '.jpg', '.png', '.zip', '.mp4'
    ],
    
    # Exclude these URL patterns
    'excluded_patterns': [
        '/login', '/admin', '/cart', '/search?'
    ],
    
    # Only include URLs with these patterns (optional)
    'included_patterns': [
        # '/blog/', '/docs/'  # Uncomment to restrict crawling
    ],
}
```

### Advanced Settings

```python
ADVANCED_CONFIG = {
    # Number of retries for failed requests
    'max_retries': 3,
    
    # Crawl depth limit (0 = unlimited)
    'max_depth': 0,  # Set to 2 for: homepage + 1 level deep
}
```

## Usage Examples

### Example 1: Crawl a Blog

```python
# crawler_config.py
CRAWLER_CONFIG = {
    'base_url': 'https://techblog.com',
    'max_pages': 200,
    'delay_between_requests': 1.5,
}

URL_FILTER_CONFIG = {
    'included_patterns': ['/blog/', '/articles/'],  # Only blog posts
}
```

### Example 2: Crawl Documentation Site

```python
# crawler_config.py
CRAWLER_CONFIG = {
    'base_url': 'https://docs.example.com',
    'max_pages': 500,
    'delay_between_requests': 1.0,
}

URL_FILTER_CONFIG = {
    'included_patterns': ['/docs/'],
    'excluded_patterns': ['/api-reference/'],  # Skip API docs
}
```

### Example 3: Shallow Crawl (Homepage + Direct Links Only)

```python
# crawler_config.py
CRAWLER_CONFIG = {
    'base_url': 'https://news-site.com',
    'max_pages': 50,
}

ADVANCED_CONFIG = {
    'max_depth': 1,  # Only follow links from homepage
}
```

### Example 4: Fast Crawl (for testing)

```python
# crawler_config.py
CRAWLER_CONFIG = {
    'base_url': 'https://example.com',
    'max_pages': 10,  # Small number for testing
    'delay_between_requests': 0.5,  # Faster (use carefully!)
}
```

## Output Structure

### Data Format

Each crawled page produces a JSON object:

```json
{
  "url": "https://example.com/page",
  "depth": 1,
  "title": "Page Title",
  "metadata": {
    "description": "Meta description",
    "keywords": "keyword1, keyword2",
    "author": "John Doe"
  },
  "content": "Main page content text...",
  "headings": [
    {"level": "h1", "text": "Main Heading"},
    {"level": "h2", "text": "Subheading"}
  ],
  "links": [
    {"text": "Link text", "href": "https://..."}
  ],
  "images": [
    {"src": "https://...", "alt": "Image description"}
  ],
  "content_length": 5234,
  "word_count": 892,
  "timestamp": "2025-01-15T10:30:45"
}
```

### Files Created Locally

After running the crawler, you'll find in `./crawled_data/`:

1. **Main Data File**: `scraped_data_TIMESTAMP.json`
   - Contains all crawled pages
   - This is the file you'll upload to Databricks

2. **Summary File**: `summary_TIMESTAMP.json`
   - Metadata about the crawl
   - Statistics and URLs list

## Uploading to Databricks

### Method 1: Databricks UI (Recommended)

1. Open your Databricks workspace
2. Click **Data** in the left sidebar
3. Navigate to where you want to store the file (e.g., DBFS or Unity Catalog)
4. Click **Upload** and select your `scraped_data_TIMESTAMP.json` file
5. Note the path (e.g., `/FileStore/research_data/scraped_data.json`)

### Method 2: Databricks CLI

If you have Databricks CLI installed:

```bash
databricks fs cp ./crawled_data/scraped_data_20250115_103045.json dbfs:/research_data/
```

### Method 3: Via Notebook

Upload the file through the Databricks UI, then access it in a notebook:

```python
# The file will be available at /dbfs/FileStore/... or wherever you uploaded it
```

## Using with Databricks Notebooks

Once you've uploaded the data to Databricks, use it in a notebook:

```python
import json

# Read the uploaded data (adjust path to where you uploaded it)
with open("/dbfs/FileStore/research_data/scraped_data_20250115_103045.json", "r") as f:
    pages = json.load(f)

print(f"Loaded {len(pages)} pages")

# Example: Get all page titles
titles = [page['title'] for page in pages]

# Example: Find pages with specific keywords
keyword_pages = [
    page for page in pages 
    if 'machine learning' in page['content'].lower()
]

print(f"Found {len(keyword_pages)} pages about machine learning")
```

### Processing with Your Llama 80B Model

```python
import requests

# Configure your Model Serving endpoint
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
MODEL_ENDPOINT = "your-llama-80b-endpoint-name"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def analyze_with_llama(content, title):
    """Send content to Llama for analysis"""
    url = f"https://{DATABRICKS_HOST}/serving-endpoints/{MODEL_ENDPOINT}/invocations"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    prompt = f"""Analyze this webpage and extract key insights:

Title: {title}
Content: {content[:3000]}  # Limit to first 3000 chars

Provide:
1. Main topics (3-5 bullet points)
2. Key findings or claims
3. Important statistics or data points
"""
    
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1000,
        "temperature": 0.1
    }
    
    response = requests.post(url, headers=headers, json=payload)
    return response.json()['choices'][0]['message']['content']

# Process each page
results = []
for page in pages[:5]:  # Start with first 5 pages
    print(f"Analyzing: {page['title']}")
    
    analysis = analyze_with_llama(page['content'], page['title'])
    
    results.append({
        'url': page['url'],
        'title': page['title'],
        'analysis': analysis
    })
    
    print(f"✓ Completed\n")

# Save results
spark.createDataFrame(results).write.mode("overwrite").saveAsTable("research.page_analysis")
print("Results saved to research.page_analysis table")
```

### Building a Deep Research Pipeline

```python
# 1. Load all pages
with open("/dbfs/FileStore/research_data/scraped_data.json", "r") as f:
    pages = json.load(f)

# 2. Filter to most relevant pages
relevant_pages = [
    page for page in pages 
    if page['word_count'] > 500  # Only substantial content
    and any(keyword in page['content'].lower() 
            for keyword in ['research', 'study', 'analysis'])
]

# 3. Chunk long content
def chunk_content(content, chunk_size=2000):
    """Split content into manageable chunks"""
    words = content.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(' '.join(words[i:i+chunk_size]))
    return chunks

# 4. Process with Llama
all_insights = []
for page in relevant_pages:
    chunks = chunk_content(page['content'])
    
    for i, chunk in enumerate(chunks):
        insight = analyze_with_llama(chunk, f"{page['title']} - Part {i+1}")
        all_insights.append({
            'url': page['url'],
            'chunk': i+1,
            'insight': insight
        })

# 5. Aggregate results
df = spark.createDataFrame(all_insights)
df.write.mode("overwrite").saveAsTable("research.detailed_insights")
```

## Troubleshooting

### Issue: "No data was scraped"

**Solutions**:
1. Check if the website blocks scrapers (try accessing in browser)
2. Lower `delay_between_requests` (but be respectful)
3. Check if `included_patterns` is too restrictive
4. Verify the `base_url` is correct
5. Look at the log file (`crawler.log`) for detailed errors

### Issue: "Can't find the output files"

**Solutions**:
1. Check the `./crawled_data/` directory
2. Look at the console output for the exact file path
3. The directory is created in the same folder where you run the script

### Issue: "File is too large to upload to Databricks"

**Solutions**:
1. Reduce `max_pages` in the config
2. Use `included_patterns` to focus on specific sections
3. Split the crawl into multiple runs
4. Upload via Databricks CLI instead of UI

### Issue: "Crawl is too slow"

**Solutions**:
1. Reduce `delay_between_requests` (carefully!)
2. Reduce `max_pages`
3. Use `included_patterns` to focus on specific sections
4. Set `max_depth` to limit crawl depth

### Issue: "Getting blocked by website"

**Solutions**:
1. Increase `delay_between_requests` (2-5 seconds)
2. Check if you're respecting robots.txt
3. Some sites require JavaScript - consider using Selenium
4. Contact site owner for API access if available

## Advanced: Using with Selenium (for JavaScript sites)

For JavaScript-heavy websites, install Selenium:

```bash
pip install selenium webdriver-manager
```

Then modify the crawler to use Selenium instead of requests (I can provide this code if needed).

## Best Practices

1. **Be Respectful**: Use appropriate delays (2+ seconds)
2. **Start Small**: Test with `max_pages=10` first
3. **Check robots.txt**: Always respect website rules
4. **Monitor Progress**: Watch logs for errors
5. **Backup Data**: Enable local backups
6. **Rate Limiting**: Don't overwhelm servers
7. **Legal Compliance**: Ensure you have permission to scrape

## License & Disclaimer

This tool is for research and educational purposes. Always:
- Check website Terms of Service
- Respect robots.txt
- Use appropriate rate limiting
- Consider legal implications of web scraping in your jurisdiction

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review configuration settings
3. Check log files for detailed error messages
4. Ensure all dependencies are installed

## Next Steps

After crawling, you can:
1. Use the data with your Llama 80B model in Databricks
2. Build a vector search index for semantic search
3. Create a dashboard to visualize insights
4. Set up scheduled crawls with Databricks Jobs
