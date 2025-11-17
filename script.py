"""
SEC and FINRA Compliance Data Scraper
Extracts enforcement actions, regulatory notices, and guidance for compliance training data
"""

import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import List, Dict, Optional
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SECScraper:
    """Scraper for SEC enforcement actions and litigation releases"""

    def __init__(self, output_dir: str = "data/sec"):
        self.base_url = "https://www.sec.gov"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # SEC requests require User-Agent
        self.headers = {
            'User-Agent': 'Research Project compliance-research@example.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        self.rate_limit_delay = 0.15  # SEC rate limit: max 10 requests per second

    def scrape_enforcement_actions(self,
                                   start_year: int = 2020,
                                   end_year: Optional[int] = None,
                                   max_items_per_year: int = 100) -> List[Dict]:
        """
        Scrape SEC enforcement actions (litigation releases and administrative proceedings)

        Args:
            start_year: Starting year for data collection
            end_year: Ending year (defaults to current year)
            max_items_per_year: Maximum number of items to scrape per year

        Returns:
            List of enforcement action dictionaries
        """
        if end_year is None:
            end_year = datetime.now().year

        enforcement_data = []

        # SEC has structured pages for litigation releases and admin proceedings
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping SEC enforcement actions for {year}")

            # Litigation releases - using the new URL structure
            litig_data = self._scrape_litigation_releases_new(year, max_items_per_year)
            enforcement_data.extend(litig_data)

            # Administrative proceedings
            admin_data = self._scrape_admin_proceedings_new(year, max_items_per_year)
            enforcement_data.extend(admin_data)

        logger.info(f"Scraped {len(enforcement_data)} total SEC enforcement actions")
        self._save_data(enforcement_data, "sec_enforcement_actions.json")
        return enforcement_data

    def _scrape_litigation_releases_new(self, year: int, max_items: int) -> List[Dict]:
        """Scrape SEC litigation releases using the structured query page"""
        data = []

        # SEC's new structured litigation releases page
        # URL format: /enforcement-litigation/litigation-releases?year=YYYY&month=All
        url = f"{self.base_url}/enforcement-litigation/litigation-releases"
        params = {
            'year': year,
            'month': 'All',
            'populate': ''  # This param appears in their URL structure
        }

        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find litigation release links
            # They typically have a specific pattern in the href or are in a results table
            # Common patterns: /litigation/litreleases/YYYY/lr-XXXXX.htm or similar
            links = soup.find_all('a', href=re.compile(r'/litigation/litreleases/|/litrelease/'))

            # Also look for links in tables or divs with specific classes
            # SEC often uses views or tables for their results
            result_containers = soup.find_all(['div', 'tr'], class_=re.compile(r'views-row|result|item'))
            for container in result_containers:
                container_links = container.find_all('a', href=True)
                links.extend(container_links)

            # Remove duplicates
            unique_links = {}
            for link in links:
                href = link.get('href', '')
                if 'litreleases' in href.lower() or 'litrelease' in href.lower():
                    unique_links[href] = link

            logger.info(f"Found {len(unique_links)} potential litigation release links for {year}")

            for i, (href, link) in enumerate(unique_links.items()):
                if i >= max_items:
                    break

                try:
                    release_url = self._normalize_url(href)
                    release_data = self._scrape_release_detail(release_url, 'litigation')
                    if release_data:
                        data.append(release_data)
                    time.sleep(self.rate_limit_delay)
                except Exception as e:
                    logger.warning(f"Error scraping litigation release {href}: {e}")
                    continue

        except requests.RequestException as e:
            logger.error(f"Error fetching litigation releases for {year}: {e}")

        return data

    def _scrape_admin_proceedings_new(self, year: int, max_items: int) -> List[Dict]:
        """Scrape SEC administrative proceedings using structured query page"""
        data = []

        # SEC administrative proceedings page with year filter
        # URL pattern similar to litigation releases
        url = f"{self.base_url}/enforcement-litigation/administrative-proceedings"
        params = {
            'year': year,
            'month': 'All',
            'populate': ''
        }

        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find administrative proceeding links
            # Pattern: /litigation/admin/ or /litigation/opinions/ or similar
            links = soup.find_all('a', href=re.compile(r'/litigation/admin|/litigation/opinions|/alj/'))

            # Also check for results in containers
            result_containers = soup.find_all(['div', 'tr'], class_=re.compile(r'views-row|result|item'))
            for container in result_containers:
                container_links = container.find_all('a', href=True)
                links.extend(container_links)

            # Remove duplicates and filter
            unique_links = {}
            for link in links:
                href = link.get('href', '')
                # Skip PDFs initially (can add PDF processing later if needed)
                if ('admin' in href.lower() or 'alj' in href.lower() or 'opinion' in href.lower()) and '.pdf' not in href.lower():
                    unique_links[href] = link

            logger.info(f"Found {len(unique_links)} potential admin proceeding links for {year}")

            for i, (href, link) in enumerate(unique_links.items()):
                if i >= max_items:
                    break

                try:
                    release_url = self._normalize_url(href)
                    release_data = self._scrape_release_detail(release_url, 'administrative')
                    if release_data:
                        data.append(release_data)
                    time.sleep(self.rate_limit_delay)
                except Exception as e:
                    logger.warning(f"Error scraping admin proceeding {href}: {e}")
                    continue

        except requests.RequestException as e:
            logger.error(f"Error fetching admin proceedings for {year}: {e}")

        return data

    def _scrape_release_detail(self, url: str, release_type: str) -> Optional[Dict]:
        """Scrape detailed content from an individual release page"""
        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract text content (removing scripts, styles, etc.)
            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()

            # Extract title
            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "No Title"

            # Extract main content
            content = soup.get_text(separator='\n', strip=True)

            # Extract date (various formats used by SEC)
            date_match = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})|(\w+ \d{1,2}, \d{4})', content[:500])
            date_str = date_match.group() if date_match else "Date unknown"

            # Extract release number
            release_num_match = re.search(r'(LR|Release No\.)[-\s]*(\d+[-\d]*)', title_text)
            release_number = release_num_match.group(2) if release_num_match else "Unknown"

            return {
                'source': 'SEC',
                'type': release_type,
                'release_number': release_number,
                'title': title_text,
                'url': url,
                'date': date_str,
                'content': content[:10000],  # Limit content length
                'full_content': content,
                'scraped_at': datetime.now().isoformat()
            }

        except Exception as e:
            logger.warning(f"Error scraping release detail from {url}: {e}")
            return None

    def _normalize_url(self, url: str) -> str:
        """Normalize relative URLs to absolute URLs"""
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            return f"{self.base_url}{url}"
        else:
            return f"{self.base_url}/{url}"

    def _save_data(self, data: List[Dict], filename: str):
        """Save scraped data to JSON file"""
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved data to {output_path}")


class FINRAScraper:
    """Scraper for FINRA disciplinary actions and regulatory notices"""

    def __init__(self, output_dir: str = "data/finra"):
        self.base_url = "https://www.finra.org"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.rate_limit_delay = 1.0  # Be conservative with FINRA

    def scrape_disciplinary_actions(self,
                                    start_date: Optional[str] = None,
                                    max_items: int = 500) -> List[Dict]:
        """
        Scrape FINRA disciplinary actions

        Args:
            start_date: Start date in YYYY-MM-DD format
            max_items: Maximum number of items to scrape

        Returns:
            List of disciplinary action dictionaries
        """
        if start_date is None:
            # Default to 3 years ago
            start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')

        disciplinary_data = []

        # FINRA Disciplinary Actions search
        # Note: FINRA's site structure may require using their search API or form submission
        logger.info(f"Scraping FINRA disciplinary actions from {start_date}")

        # FINRA has a disciplinary actions online database
        # The actual implementation would need to handle their search interface
        search_url = f"{self.base_url}/rules-guidance/oversight-enforcement/finra-disciplinary-actions"

        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(search_url, headers=self.headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find links to disciplinary actions
            # This will vary based on FINRA's current page structure
            action_links = soup.find_all('a', href=re.compile(r'case-detail|disciplinary'))

            for link in action_links[:max_items]:
                try:
                    action_url = self._normalize_url(link['href'])
                    action_data = self._scrape_disciplinary_detail(action_url)
                    if action_data:
                        disciplinary_data.append(action_data)
                    time.sleep(self.rate_limit_delay)
                except Exception as e:
                    logger.warning(f"Error scraping disciplinary action: {e}")
                    continue

        except requests.RequestException as e:
            logger.error(f"Error fetching FINRA disciplinary actions: {e}")

        logger.info(f"Scraped {len(disciplinary_data)} FINRA disciplinary actions")
        self._save_data(disciplinary_data, "finra_disciplinary_actions.json")
        return disciplinary_data

    def scrape_regulatory_notices(self,
                                  start_year: int = 2020,
                                  max_items: int = 200) -> List[Dict]:
        """Scrape FINRA regulatory notices"""
        notices_data = []

        logger.info(f"Scraping FINRA regulatory notices from {start_year}")

        # FINRA Regulatory Notices page
        url = f"{self.base_url}/rules-guidance/notices"

        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find notice links
            notice_links = soup.find_all('a', href=re.compile(r'/\d{2}-\d{2}|regulatory-notice'))

            for link in notice_links[:max_items]:
                try:
                    notice_url = self._normalize_url(link['href'])
                    notice_data = self._scrape_notice_detail(notice_url)
                    if notice_data:
                        notices_data.append(notice_data)
                    time.sleep(self.rate_limit_delay)
                except Exception as e:
                    logger.warning(f"Error scraping regulatory notice: {e}")
                    continue

        except requests.RequestException as e:
            logger.error(f"Error fetching FINRA regulatory notices: {e}")

        logger.info(f"Scraped {len(notices_data)} FINRA regulatory notices")
        self._save_data(notices_data, "finra_regulatory_notices.json")
        return notices_data

    def _scrape_disciplinary_detail(self, url: str) -> Optional[Dict]:
        """Scrape detailed content from a disciplinary action"""
        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Clean up
            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()

            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "No Title"

            content = soup.get_text(separator='\n', strip=True)

            # Extract case number
            case_match = re.search(r'Case #?\s*(\d+)', content[:1000])
            case_number = case_match.group(1) if case_match else "Unknown"

            # Extract violations mentioned
            violations = []
            violation_patterns = [
                r'Rule \d+',
                r'NASD Rule \d+',
                r'FINRA Rule \d+',
                r'Section \d+\([a-z]\)'
            ]
            for pattern in violation_patterns:
                matches = re.findall(pattern, content)
                violations.extend(matches)

            return {
                'source': 'FINRA',
                'type': 'disciplinary_action',
                'case_number': case_number,
                'title': title_text,
                'url': url,
                'violations_cited': list(set(violations)),
                'content': content[:10000],
                'full_content': content,
                'scraped_at': datetime.now().isoformat()
            }

        except Exception as e:
            logger.warning(f"Error scraping disciplinary detail from {url}: {e}")
            return None

    def _scrape_notice_detail(self, url: str) -> Optional[Dict]:
        """Scrape detailed content from a regulatory notice"""
        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()

            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "No Title"

            content = soup.get_text(separator='\n', strip=True)

            # Extract notice number
            notice_match = re.search(r'(\d{2}-\d{2})', title_text)
            notice_number = notice_match.group(1) if notice_match else "Unknown"

            return {
                'source': 'FINRA',
                'type': 'regulatory_notice',
                'notice_number': notice_number,
                'title': title_text,
                'url': url,
                'content': content[:10000],
                'full_content': content,
                'scraped_at': datetime.now().isoformat()
            }

        except Exception as e:
            logger.warning(f"Error scraping notice detail from {url}: {e}")
            return None

    def _normalize_url(self, url: str) -> str:
        """Normalize relative URLs to absolute URLs"""
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            return f"{self.base_url}{url}"
        else:
            return f"{self.base_url}/{url}"

    def _save_data(self, data: List[Dict], filename: str):
        """Save scraped data to JSON file"""
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved data to {output_path}")


def main():
    """Main execution function"""

    # Initialize scrapers
    sec_scraper = SECScraper(output_dir="C:/Users/lvdp0/sec")
    finra_scraper = FINRAScraper(output_dir="C:/Users/lvdp0/finra")

    # Scrape SEC data
    logger.info("Starting SEC data collection...")
    sec_data = sec_scraper.scrape_enforcement_actions(
        start_year=2020,
        max_items_per_year=100  # 100 items per year
    )

    # Scrape FINRA data
    logger.info("Starting FINRA data collection...")
    finra_disciplinary = finra_scraper.scrape_disciplinary_actions(max_items=200)
    finra_notices = finra_scraper.scrape_regulatory_notices(start_year=2020, max_items=100)

    # Combine and create summary
    total_items = len(sec_data) + len(finra_disciplinary) + len(finra_notices)

    summary = {
        'total_items': total_items,
        'sec_enforcement_actions': len(sec_data),
        'finra_disciplinary_actions': len(finra_disciplinary),
        'finra_regulatory_notices': len(finra_notices),
        'collection_date': datetime.now().isoformat()
    }

    # Save summary
    with open('C:/Users/lvdp0/collection_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Data collection complete. Total items: {total_items}")
    logger.info(f"Summary saved to data/collection_summary.json")


if __name__ == "__main__":
    main()