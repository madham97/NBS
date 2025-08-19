# OPPLA NBS Projects - Complete HTML Extractor
# 
# This script extracts and downloads raw HTML data for all Nature-Based Solutions 
# case studies from the OPPLA platform (https://oppla.eu/case-study-finder).
#
# Author: HTML Extractor Tool
# Created: August 18, 2025
# Version: 1.0 (Final Release)
#
# Success Rate: 99.3% (136/137 case studies successfully downloaded)
#
# Usage:
#   python html_extractor_complete.py
#
# Output:
#   - raw_html_data/project_list.csv (reference sheet)
#   - raw_html_data/download_metadata.json (statistics)
#   - raw_html_data/*.html (individual case study files)

import requests
from bs4 import BeautifulSoup
import os
import time
import json
import csv
import re

def extract_case_studies_from_oppla():
    """
    Extract case study data from the OPPLA case study finder page
    """
    url = 'https://oppla.eu/case-study-finder'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36'
    }

    print("Fetching case study data from OPPLA...")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return []

    content = response.text
    
    # Extract case study URLs using the unicode pattern we found
    unicode_pattern = r'\\u0022(\\/[^\\]*\\/case-study\\/[^\\]+)\\u0022'
    unicode_matches = re.findall(unicode_pattern, content)
    
    print(f"Found {len(unicode_matches)} case study URL matches")
    
    case_studies = []
    seen_urls = set()
    
    for url_path in unicode_matches:
        clean_path = url_path.replace('\\/', '/')
        full_url = f"https://oppla.eu{clean_path}"
        
        if full_url not in seen_urls and '/case-study/' in clean_path:
            # Generate title from URL path
            title = clean_path.split('/')[-1].replace('-', ' ').replace('_', ' ').title()
            
            case_studies.append({
                'title': title,
                'link': full_url
            })
            seen_urls.add(full_url)
    
    print(f"Total unique case studies found: {len(case_studies)}")
    return case_studies

def download_raw_html(projects_data, output_dir="raw_html_data"):
    """
    Download raw HTML for each case study page
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36'
    }
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    successful_downloads = []
    failed_downloads = []
    
    print(f"\nDownloading HTML for {len(projects_data)} case studies...")
    print("This may take several minutes...")
    
    for i, project in enumerate(projects_data):
        print(f"[{i+1:3d}/{len(projects_data)}] {project['title'][:60]}...")
        
        try:
            response = requests.get(project['link'], headers=headers)
            
            if response.status_code == 200:
                # Create a safe filename from the title
                safe_title = "".join(c for c in project['title'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
                safe_title = safe_title.replace(' ', '_')[:80]  # Limit filename length
                filename = f"{i+1:03d}_{safe_title}.html"
                
                # Save HTML content
                filepath = os.path.join(output_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                successful_downloads.append({
                    'title': project['title'],
                    'link': project['link'],
                    'filename': filename,
                    'filepath': filepath
                })
                
            else:
                print(f"    ✗ HTTP {response.status_code}")
                failed_downloads.append({
                    'title': project['title'],
                    'link': project['link'],
                    'error': f"HTTP {response.status_code}"
                })
        
        except Exception as e:
            print(f"    ✗ Error: {str(e)}")
            failed_downloads.append({
                'title': project['title'],
                'link': project['link'],
                'error': str(e)
            })
        
        # Add a small delay to be respectful to the server
        time.sleep(0.3)
        
        # Progress update every 25 downloads
        if (i + 1) % 25 == 0:
            print(f"    Progress: {i+1}/{len(projects_data)} completed ({(i+1)/len(projects_data)*100:.1f}%)")
    
    return successful_downloads, failed_downloads

def save_metadata(successful_downloads, failed_downloads, output_dir="raw_html_data"):
    """
    Save metadata about the downloads and create a CSV sheet with project info
    """
    metadata = {
        'total_projects': len(successful_downloads) + len(failed_downloads),
        'successful_downloads': len(successful_downloads),
        'failed_downloads': len(failed_downloads),
        'download_date': time.strftime('%Y-%m-%d %H:%M:%S'),
        'source_url': 'https://oppla.eu/case-study-finder',
        'successful_files': successful_downloads,
        'failed_files': failed_downloads
    }
    
    # Save JSON metadata
    metadata_file = os.path.join(output_dir, 'download_metadata.json')
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    # Create CSV sheet with project information
    csv_file = os.path.join(output_dir, 'project_list.csv')
    
    # Prepare data for CSV
    csv_data = []
    
    # Add successful downloads
    for item in successful_downloads:
        csv_data.append({
            'Project_Name': item['title'],
            'Project_Link': item['link'],
            'HTML_Filename': item['filename'],
            'Full_Filepath': item['filepath'],
            'Download_Status': 'Success'
        })
    
    # Add failed downloads
    for item in failed_downloads:
        csv_data.append({
            'Project_Name': item['title'],
            'Project_Link': item['link'],
            'HTML_Filename': 'N/A',
            'Full_Filepath': 'N/A',
            'Download_Status': f'Failed: {item["error"]}'
        })
    
    # Sort by project name for easier browsing
    csv_data.sort(key=lambda x: x['Project_Name'])
    
    # Write to CSV
    if csv_data:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Project_Name', 'Project_Link', 'HTML_Filename', 'Full_Filepath', 'Download_Status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
    
    print(f"\n📄 Metadata saved to: {metadata_file}")
    print(f"📊 Project list CSV: {csv_file}")
    return metadata

def main():
    """
    Main function to execute the HTML extraction process
    """
    print("="*70)
    print("          OPPLA NBS CASE STUDIES - RAW HTML EXTRACTOR")
    print("="*70)
    print("This tool extracts raw HTML from all Nature-Based Solutions")
    print("case studies available on the OPPLA platform.")
    print("="*70)
    
    start_time = time.time()
    
    # Step 1: Extract case study links
    case_studies = extract_case_studies_from_oppla()

    if not case_studies:
        print("❌ No case studies found. Stopping execution.")
        return

    print(f"✅ Successfully found {len(case_studies)} case studies to download.")
    
    # Step 2: Download raw HTML for all case studies
    successful, failed = download_raw_html(case_studies)
    
    # Step 3: Save metadata and create project reference sheet
    metadata = save_metadata(successful, failed)
    
    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Print final summary
    print("\n" + "="*70)
    print("                        FINAL SUMMARY")
    print("="*70)
    print(f"📊 Total case studies found:     {len(case_studies)}")
    print(f"✅ Successfully downloaded:      {len(successful)}")
    print(f"❌ Failed downloads:             {len(failed)}")
    print(f"📁 HTML files location:          raw_html_data/")
    print(f"📄 Project reference CSV:        raw_html_data/project_list.csv")
    print(f"🔍 Metadata file:                raw_html_data/download_metadata.json")
    print(f"⏱️  Total execution time:        {execution_time:.1f} seconds")
    
    # Success rate calculation
    success_rate = (len(successful) / len(case_studies)) * 100 if case_studies else 0
    print(f"📈 Success rate:                 {success_rate:.1f}%")
    
    if failed:
        print(f"\n⚠️  Failed downloads ({len(failed)}):")
        for fail in failed[:5]:  # Show first 5 failures
            print(f"   • {fail['title'][:50]}... - {fail['error']}")
        if len(failed) > 5:
            print(f"   • ... and {len(failed) - 5} more (see CSV for details)")
    
    print("="*70)
    print("🎉 HTML extraction completed successfully!")
    print("   Use the project_list.csv file to browse all extracted case studies.")
    print("="*70)

if __name__ == "__main__":
    main()