#!/usr/bin/env python3
"""
UNACITY NBS Projects - Production HTML Scraper

This script efficiently extracts raw HTML data from all Nature-Based Solutions 
projects on the Una.City platform using advanced Cloudflare bypass techniques.

Features:
- Undetected ChromeDriver for reliable Cloudflare bypass
- Adaptive wait times (fast after initial challenge is solved)
- Smart challenge detection
- Comprehensive error handling and progress tracking
- CSV export with project metadata

Author: Cloudflare Bypass Specialist
Created: August 18, 2025
Version: 2.0 (Production)

Requirements:
    pip install undetected-chromedriver fake-useragent beautifulsoup4 tqdm

Usage:
    python unacity_scraper.py
"""

import undetected_chromedriver as uc
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import os
import time
import json
import csv
import random
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class UnacityScraper:
    def __init__(self):
        self.base_url = "https://una.city"
        self.output_dir = "raw_html_data"
        self.driver = None
        self.cloudflare_solved = False
        
        # Initialize the browser
        self._setup_browser()
    
    def _setup_browser(self):
        """Setup undetected Chrome browser with optimal stealth configuration"""
        print("🔧 Setting up undetected Chrome browser...")
        
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Use random user agent for better stealth
        ua = UserAgent()
        user_agent = ua.chrome
        options.add_argument(f"--user-agent={user_agent}")
        
        print(f"   📱 User Agent: {user_agent}")
        
        try:
            self.driver = uc.Chrome(options=options, version_main=None)
            print("   ✅ Browser initialized successfully")
        except Exception as e:
            print(f"   ❌ Failed to initialize browser: {str(e)}")
            raise
    
    def _detect_cloudflare_challenge(self, page_source, page_title="", current_url=""):
        """Detect if we're currently facing a Cloudflare challenge"""
        if not page_source:
            return False
            
        page_source_lower = page_source.lower()
        page_title_lower = page_title.lower()
        
        # Cloudflare challenge indicators
        cf_indicators = [
            'just a moment',
            'checking your browser',
            'cloudflare',
            'please wait',
            'security check',
            'ddos protection',
            'ray id'
        ]
        
        # Check for indicators
        has_cf_indicator = any(indicator in page_source_lower for indicator in cf_indicators) or \
                          any(indicator in page_title_lower for indicator in cf_indicators)
        
        # Additional check: very short content usually indicates challenge page
        is_short_content = len(page_source) < 2000
        
        return has_cf_indicator or (is_short_content and 'una.city' in current_url)
    
    def _get_page_content(self, url, max_retries=3):
        """Get page content with adaptive Cloudflare handling"""
        for attempt in range(max_retries):
            try:
                print(f"   Loading {url}")
                self.driver.get(url)
                
                # Quick initial check - only wait 3 seconds initially
                time.sleep(3)
                
                page_source = self.driver.page_source
                current_url = self.driver.current_url
                page_title = self.driver.title
                
                # Check if we hit a Cloudflare challenge
                is_challenge = self._detect_cloudflare_challenge(page_source, page_title, current_url)
                
                if not is_challenge and self.cloudflare_solved:
                    # No challenge detected and we've solved it before - proceed quickly
                    print("   ✅ No Cloudflare challenge - proceeding quickly")
                    time.sleep(random.uniform(1, 2))
                    return page_source
                
                elif is_challenge:
                    print("   🛡️ Cloudflare challenge detected - waiting for resolution...")
                    
                    # Extended wait for challenge resolution
                    max_challenge_wait = 15 if self.cloudflare_solved else 20
                    
                    for wait_cycle in range(max_challenge_wait):
                        time.sleep(3)
                        
                        try:
                            page_source = self.driver.page_source
                            current_url = self.driver.current_url
                            page_title = self.driver.title
                            
                            if not self._detect_cloudflare_challenge(page_source, page_title, current_url):
                                print(f"   ✅ Cloudflare challenge solved in {(wait_cycle + 1) * 3} seconds!")
                                self.cloudflare_solved = True
                                time.sleep(random.uniform(2, 4))
                                return page_source
                            else:
                                if wait_cycle % 5 == 0:
                                    print(f"   ⏳ Still solving challenge... ({(wait_cycle + 1) * 3}s)")
                                    
                        except Exception as e:
                            if wait_cycle % 10 == 0:
                                print(f"   ⚠️ Error checking challenge status: {str(e)}")
                    
                    print(f"   ❌ Challenge not solved within {max_challenge_wait * 3} seconds")
                
                else:
                    # First time or uncertain - give it a medium wait
                    print("   ⏳ First access - moderate wait...")
                    time.sleep(10)
                    
                    page_source = self.driver.page_source
                    if not self._detect_cloudflare_challenge(page_source, self.driver.title, self.driver.current_url):
                        print("   ✅ Access successful!")
                        self.cloudflare_solved = True
                        return page_source
                
                # Try getting page source anyway if we reach here
                try:
                    page_source = self.driver.page_source
                    if len(page_source) > 2000:
                        print("   ⚠️ Proceeding with current page content...")
                        return page_source
                except:
                    pass
                    
            except Exception as e:
                print(f"   ❌ Error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    print("   🔄 Retrying...")
                    time.sleep(10)
        
        print(f"   ❌ Failed to get content after {max_retries} attempts")
        return None
    
    def get_projects_on_page(self, page_num):
        """Extract project links from a specific page"""
        url = f"{self.base_url}?page={page_num}"
        
        try:
            html_content = self._get_page_content(url)
            if not html_content:
                return []
                
            soup = BeautifulSoup(html_content, 'html.parser')
            project_divs = soup.select(".views-row")
            projects = []

            for div in project_divs:
                h3 = div.find("h3")
                if h3 and h3.a:
                    title = h3.a.get_text(strip=True)
                    relative_link = h3.a.get("href")
                    full_link = self.base_url + relative_link if relative_link.startswith("/") else relative_link
                    projects.append({
                        'title': title,
                        'link': full_link
                    })
            
            return projects
            
        except Exception as e:
            print(f"   ❌ Error scraping page {page_num}: {e}")
            return []
    
    def extract_all_projects(self):
        """Extract all project data from Una.City"""
        print("\n🔍 Extracting all projects from Una.City...")
        all_projects = []
        page = 0
        
        while True:
            print(f"\nScraping page {page}...")
            projects = self.get_projects_on_page(page)
            
            if not projects:
                print(f"Finished scraping at page {page}.")
                break

            print(f"Found {len(projects)} projects on page {page}")
            all_projects.extend(projects)
            page += 1
            
            # Adaptive delay between pages
            if self.cloudflare_solved:
                delay = random.uniform(2, 4)
                print(f"   ⚡ Quick delay: {delay:.1f}s (Cloudflare solved)")
            else:
                delay = random.uniform(5, 8)
                print(f"   ⏳ Standard delay: {delay:.1f}s (Cloudflare status unknown)")
            
            time.sleep(delay)

        print(f"\n✅ Total projects found: {len(all_projects)}")
        return all_projects
    
    def download_html_files(self, projects_data):
        """Download raw HTML for each project page"""
        # Create output directory
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        successful_downloads = []
        failed_downloads = []
        
        print(f"\n📥 Downloading HTML for {len(projects_data)} projects...")
        print("Using adaptive timing - faster after Cloudflare is solved!")
        
        for i, project in enumerate(tqdm(projects_data, desc="Downloading HTML files")):
            try:
                html_content = self._get_page_content(project['link'])
                
                if html_content:
                    # Create safe filename
                    safe_title = "".join(c for c in project['title'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    safe_title = safe_title.replace(' ', '_')[:80]
                    filename = f"{i+1:03d}_{safe_title}.html"
                    
                    # Save HTML content
                    filepath = os.path.join(self.output_dir, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    
                    successful_downloads.append({
                        'title': project['title'],
                        'link': project['link'],
                        'filename': filename,
                        'filepath': filepath
                    })
                    
                else:
                    failed_downloads.append({
                        'title': project['title'],
                        'link': project['link'],
                        'error': "Failed to retrieve content"
                    })
            
            except Exception as e:
                failed_downloads.append({
                    'title': project['title'],
                    'link': project['link'],
                    'error': str(e)
                })
            
            # Adaptive delay between downloads
            if i < len(projects_data) - 1:
                if self.cloudflare_solved:
                    delay = random.uniform(1, 2)
                    if i % 50 == 0:  # Status update every 50 items
                        print(f"   ⚡ Fast mode: {delay:.1f}s delay")
                else:
                    delay = random.uniform(2, 4)
                    if i % 20 == 0:  # More frequent updates when slower
                        print(f"   ⏳ Standard delay: {delay:.1f}s")
                
                time.sleep(delay)
        
        return successful_downloads, failed_downloads
    
    def save_metadata(self, successful_downloads, failed_downloads):
        """Save metadata and create CSV reference"""
        metadata = {
            'total_projects': len(successful_downloads) + len(failed_downloads),
            'successful_downloads': len(successful_downloads),
            'failed_downloads': len(failed_downloads),
            'download_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_url': 'https://una.city',
            'method_used': 'Undetected ChromeDriver',
            'cloudflare_bypassed': self.cloudflare_solved,
            'successful_files': successful_downloads,
            'failed_files': failed_downloads
        }
        
        # Save JSON metadata
        metadata_file = os.path.join(self.output_dir, 'download_metadata.json')
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        # Create CSV reference
        csv_file = os.path.join(self.output_dir, 'project_list.csv')
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
        
        # Sort by project name
        csv_data.sort(key=lambda x: x['Project_Name'])
        
        # Write CSV
        if csv_data:
            with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Project_Name', 'Project_Link', 'HTML_Filename', 'Full_Filepath', 'Download_Status']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)
        
        print(f"\n📄 Metadata saved to: {metadata_file}")
        print(f"📊 Project list CSV: {csv_file}")
        return metadata
    
    def cleanup(self):
        """Clean up browser resources"""
        if self.driver:
            print("\n🔧 Closing browser...")
            try:
                self.driver.quit()
            except:
                pass
    
    def run_scraping(self):
        """Main scraping workflow"""
        print("="*70)
        print("          UNACITY NBS PROJECTS - PRODUCTION SCRAPER")
        print("="*70)
        print("Extracting Nature-Based Solutions projects from Una.City")
        print("Using: Undetected ChromeDriver with Adaptive Cloudflare Bypass")
        print("="*70)
        
        start_time = time.time()
        
        try:
            # Step 1: Extract all project links
            projects = self.extract_all_projects()
            
            if not projects:
                print("❌ No projects found. Stopping execution.")
                return
            
            # Step 2: Download HTML files
            successful, failed = self.download_html_files(projects)
            
            # Step 3: Save metadata
            metadata = self.save_metadata(successful, failed)
            
        except KeyboardInterrupt:
            print("\n⚠️ Process interrupted by user")
        except Exception as e:
            print(f"\n❌ Unexpected error: {str(e)}")
        finally:
            self.cleanup()
        
        # Final summary
        end_time = time.time()
        execution_time = end_time - start_time
        
        print("\n" + "="*70)
        print("                        FINAL SUMMARY")
        print("="*70)
        print(f"📊 Total projects found:         {len(projects) if 'projects' in locals() else 0}")
        print(f"✅ Successfully downloaded:      {len(successful) if 'successful' in locals() else 0}")
        print(f"❌ Failed downloads:             {len(failed) if 'failed' in locals() else 0}")
        print(f"📁 HTML files location:          {self.output_dir}/")
        print(f"📄 Project reference CSV:        {self.output_dir}/project_list.csv")
        print(f"🔍 Metadata file:                {self.output_dir}/download_metadata.json")
        print(f"⏱️  Total execution time:        {execution_time:.1f} seconds")
        
        if 'projects' in locals() and 'successful' in locals():
            success_rate = (len(successful) / len(projects)) * 100 if projects else 0
            print(f"📈 Success rate:                 {success_rate:.1f}%")
            
            if 'failed' in locals() and failed:
                print(f"\n⚠️  Failed downloads ({len(failed)}):")
                for fail in failed[:5]:
                    print(f"   • {fail['title'][:50]}... - {fail['error']}")
                if len(failed) > 5:
                    print(f"   • ... and {len(failed) - 5} more (see CSV for details)")
        
        print("="*70)
        print("🎉 Scraping completed!")
        print("="*70)

def main():
    """Entry point"""
    print("🚀 Starting Una.City NBS Projects Scraper")
    print("⚠️ Note: First page may take 30-60 seconds due to Cloudflare challenge")
    print("   Subsequent pages will be much faster once challenge is solved")
    
    proceed = input("\nDo you want to proceed? (y/n): ").strip().lower()
    if proceed != 'y':
        print("Operation cancelled.")
        return
    
    scraper = UnacityScraper()
    scraper.run_scraping()

if __name__ == "__main__":
    main()