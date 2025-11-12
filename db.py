# Databricks notebook source
# MAGIC %md
# MAGIC # Deep Research Agent - Web Data Analysis
# MAGIC 
# MAGIC This notebook processes web data crawled by the enhanced_crawler.py script
# MAGIC and uses your Llama 80B model to extract insights.
# MAGIC 
# MAGIC ## Setup
# MAGIC 1. Upload your `scraped_data_TIMESTAMP.json` file to Databricks
# MAGIC 2. Update the `DATA_PATH` variable below
# MAGIC 3. Update the `MODEL_ENDPOINT` variable with your Llama endpoint name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Update these variables
DATA_PATH = "/dbfs/FileStore/research_data/scraped_data.json"  # Change to your file path
MODEL_ENDPOINT = "your-llama-80b-endpoint-name"  # Change to your endpoint name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Explore Data

# COMMAND ----------

import json
import pandas as pd

# Load the crawled data
with open(DATA_PATH, "r") as f:
    pages = json.load(f)

print(f"✓ Loaded {len(pages)} pages")
print(f"✓ Total words: {sum(page.get('word_count', 0) for page in pages):,}")
print(f"✓ Average content length: {sum(page['content_length'] for page in pages) / len(pages):.0f} characters")

# COMMAND ----------

# Show sample data structure
print("Sample page structure:")
print(json.dumps(pages[0], indent=2)[:1000], "...")

# COMMAND ----------

# Create summary DataFrame
df_summary = pd.DataFrame([
    {
        'url': page['url'],
        'title': page['title'],
        'word_count': page.get('word_count', 0),
        'content_length': page['content_length'],
        'num_headings': len(page.get('headings', [])),
        'num_images': len(page.get('images', [])),
        'depth': page.get('depth', 0)
    }
    for page in pages
])

display(df_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call Llama Model for Analysis

# COMMAND ----------

import requests

# Get Databricks configuration
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def analyze_with_llama(content, title, max_tokens=1000):
    """
    Send content to Llama 80B for analysis
    """
    url = f"https://{DATABRICKS_HOST}/serving-endpoints/{MODEL_ENDPOINT}/invocations"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Construct prompt
    prompt = f"""Analyze the following webpage content and provide a structured analysis:

Title: {title}
Content: {content[:3000]}  # Limit to 3000 characters

Please provide:
1. Main Topic (1-2 sentences)
2. Key Points (3-5 bullet points)
3. Important Facts or Statistics (if any)
4. Overall Sentiment (positive/neutral/negative)
5. Target Audience (who is this content for?)

Format your response clearly with headers."""
    
    payload = {
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content']
    except Exception as e:
        return f"Error: {str(e)}"

# Test with one page
print("Testing with first page...")
test_result = analyze_with_llama(pages[0]['content'], pages[0]['title'])
print(f"\nAnalysis of: {pages[0]['title']}")
print("="*60)
print(test_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process All Pages

# COMMAND ----------

import time

# Process all pages (with progress tracking)
results = []

for i, page in enumerate(pages):
    print(f"Processing {i+1}/{len(pages)}: {page['title'][:50]}...")
    
    analysis = analyze_with_llama(page['content'], page['title'])
    
    results.append({
        'url': page['url'],
        'title': page['title'],
        'word_count': page.get('word_count', 0),
        'analysis': analysis,
        'timestamp': page['timestamp']
    })
    
    # Add small delay to avoid overwhelming the endpoint
    if i < len(pages) - 1:
        time.sleep(0.5)

print(f"\n✓ Completed analysis of {len(results)} pages")

# COMMAND ----------

# Convert to Spark DataFrame and save
df_results = spark.createDataFrame(results)

# Save to Delta table
df_results.write.mode("overwrite").saveAsTable("research.page_analysis")

print("✓ Results saved to 'research.page_analysis' table")
display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Summary Report

# COMMAND ----------

def generate_summary_report(all_analyses):
    """
    Use Llama to generate an executive summary from all analyses
    """
    # Combine all analyses (limit length)
    combined = "\n\n".join([
        f"Page: {r['title']}\nAnalysis: {r['analysis'][:500]}"
        for r in all_analyses[:10]  # Use first 10 pages for summary
    ])
    
    prompt = f"""Based on the following webpage analyses, create an executive summary report:

{combined}

Please provide:
1. Overall Theme (what are these pages about?)
2. Key Insights (5-7 main takeaways)
3. Patterns or Trends (what's common across pages?)
4. Notable Findings (anything particularly interesting or important?)
5. Recommendations (what should someone do with this information?)

Write in a professional, concise style suitable for an executive report."""
    
    url = f"https://{DATABRICKS_HOST}/serving-endpoints/{MODEL_ENDPOINT}/invocations"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 2000,
        "temperature": 0.2
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content']
    except Exception as e:
        return f"Error generating summary: {str(e)}"

# Generate summary
print("Generating executive summary...")
summary_report = generate_summary_report(results)

print("\n" + "="*80)
print("EXECUTIVE SUMMARY REPORT")
print("="*80)
print(summary_report)
print("="*80)

# COMMAND ----------

# Save summary report
summary_data = [{
    'report': summary_report,
    'num_pages_analyzed': len(results),
    'timestamp': pd.Timestamp.now().isoformat()
}]

df_summary = spark.createDataFrame(summary_data)
df_summary.write.mode("overwrite").saveAsTable("research.executive_summary")

print("\n✓ Executive summary saved to 'research.executive_summary' table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced: Multi-Turn Deep Dive

# COMMAND ----------

def deep_dive_analysis(page_content, page_title):
    """
    Multi-turn conversation for deeper analysis
    """
    # First turn: Initial analysis
    messages = [
        {"role": "user", "content": f"""Analyze this content and identify the 3 most important aspects:

Title: {page_title}
Content: {page_content[:2000]}"""}
    ]
    
    url = f"https://{DATABRICKS_HOST}/serving-endpoints/{MODEL_ENDPOINT}/invocations"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Turn 1: Get key aspects
    response = requests.post(
        url,
        headers=headers,
        json={"messages": messages, "max_tokens": 500, "temperature": 0.1}
    )
    
    initial_analysis = response.json()['choices'][0]['message']['content']
    
    # Turn 2: Deep dive on first aspect
    messages.append({"role": "assistant", "content": initial_analysis})
    messages.append({"role": "user", "content": "Focus on the first aspect you mentioned. Provide a detailed explanation with examples from the content."})
    
    response = requests.post(
        url,
        headers=headers,
        json={"messages": messages, "max_tokens": 800, "temperature": 0.1}
    )
    
    deep_analysis = response.json()['choices'][0]['message']['content']
    
    return {
        'initial_analysis': initial_analysis,
        'deep_analysis': deep_analysis
    }

# Example: Deep dive on first page
print("Performing deep dive analysis on first page...")
deep_result = deep_dive_analysis(pages[0]['content'], pages[0]['title'])

print(f"\n{'='*60}")
print("INITIAL ANALYSIS:")
print(deep_result['initial_analysis'])
print(f"\n{'='*60}")
print("DEEP DIVE:")
print(deep_result['deep_analysis'])
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results

# COMMAND ----------

# Export to CSV for further analysis
results_pdf = pd.DataFrame(results)
results_pdf.to_csv("/dbfs/FileStore/research_data/analysis_results.csv", index=False)

print("✓ Results exported to /dbfs/FileStore/research_data/analysis_results.csv")
print("\nYou can download this file from the Databricks UI or use it in other notebooks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Vector Search**: Create embeddings and use Databricks Vector Search for semantic queries
# MAGIC 2. **Scheduled Updates**: Set up a Databricks Job to run this notebook regularly
# MAGIC 3. **Dashboard**: Create a visualization dashboard using the results
# MAGIC 4. **API Integration**: Expose insights through a Model Serving endpoint
# MAGIC 5. **Multi-Agent**: Use multiple specialized prompts for different analysis types
