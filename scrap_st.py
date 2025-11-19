"""
Vanguard Website Compliance Content Scraper
Finds all publicly available compliance-related pages and content
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import time
from typing import Set, List, Dict
from collections import deque
import re
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VanguardComplianceScraper:
    """Scraper to find all compliance-related content on Vanguard's website"""

    # Keywords that indicate compliance-related content
    COMPLIANCE_KEYWORDS = [
        'compliance',
        'regulatory',
        'regulation',
        'disclosure',
        'legal',
        'terms',
        'privacy',
        'security',
        'fraud',
        'protection',
        'rights',
        'responsibilities',
        'complaint',
        'dispute',
        'arbitration',
        'finra',
        'sec',
        'cfpb',
        'fiduciary',
        'best interest',
        'suitability',
        'risk disclosure',
        'prospectus',
        'form adv',
        'form crs',
        'customer relationship summary',
        'conflicts of interest',
        'code of ethics',
        'business continuity',
        'cybersecurity',
        'data protection',
        'anti-money laundering',
        'aml',
        'kyc',
        'know your customer',
        'sanctions',
        'ofac',
        'regulation best interest',
        'reg bi'
    ]

    def __init__(self, base_url: str = "https://investor.vanguard.com"):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.visited_urls: Set[str] = set()
        self.compliance_urls: List[Dict] = []
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.rate_limit_delay = 1.0  # Be respectful

    def is_compliance_related(self, text: str, url: str) -> bool:
        """Check if content is compliance-related"""
        text_lower = text.lower()
        url_lower = url.lower()

        # Check URL for keywords
        for keyword in self.COMPLIANCE_KEYWORDS:
            if keyword.replace(' ', '-') in url_lower or keyword.replace(' ', '') in url_lower:
                return True

        # Check content for keywords (need multiple matches to avoid false positives)
        keyword_count = sum(1 for keyword in self.COMPLIANCE_KEYWORDS if keyword in text_lower)

        # If URL suggests compliance or content has multiple compliance keywords
        return keyword_count >= 3

    def normalize_url(self, url: str) -> str:
        """Normalize URL to absolute form"""
        if not url:
            return ""

        # Remove fragments
        url = url.split('#')[0]

        # Convert to absolute URL
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            return urljoin(self.base_url, url)
        else:
            return urljoin(self.base_url, '/' + url)

    def should_crawl(self, url: str) -> bool:
        """Determine if URL should be crawled"""
        if not url or url in self.visited_urls:
            return False

        parsed = urlparse(url)

        # Stay on Vanguard domains
        if not any(domain in parsed.netloc for domain in ['vanguard.com', 'vanguard.co.uk']):
            return False

        # Skip certain file types
        skip_extensions = ['.pdf', '.jpg', '.png', '.gif', '.css', '.js', '.zip', '.doc', '.docx']
        if any(url.lower().endswith(ext) for ext in skip_extensions):
            return False

        # Skip common non-content URLs
        skip_patterns = [
            '/login',
            '/signin',
            '/signout',
            '/logout',
            '/account',
            '/my-account',
            '/secure',
            'javascript:',
            'mailto:',
            'tel:'
        ]
        if any(pattern in url.lower() for pattern in skip_patterns):
            return False

        return True

    def extract_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """Extract all valid links from page"""
        links = []

        for anchor in soup.find_all('a', href=True):
            url = self.normalize_url(anchor['href'])
            if url and self.should_crawl(url):
                links.append(url)

        return list(set(links))  # Remove duplicates

    def scrape_page(self, url: str) -> Dict:
        """Scrape a single page and analyze for compliance content"""
        try:
            time.sleep(self.rate_limit_delay)

            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Remove script and style elements
            for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()

            # Extract text content
            text_content = soup.get_text(separator=' ', strip=True)

            # Extract title
            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else "No Title"

            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            description = meta_desc.get('content', '') if meta_desc else ''

            # Check if compliance-related
            is_compliance = self.is_compliance_related(text_content, url)

            # Extract keywords found
            found_keywords = [
                keyword for keyword in self.COMPLIANCE_KEYWORDS
                if keyword in text_content.lower()
            ]

            # Extract all links for crawling
            links = self.extract_links(soup, url)

            page_data = {
                'url': url,
                'title': title_text,
                'description': description,
                'is_compliance_related': is_compliance,
                'compliance_keywords_found': found_keywords,
                'keyword_count': len(found_keywords),
                'outbound_links': links,
                'content_preview': text_content[:500],
                'content_length': len(text_content)
            }

            return page_data

        except Exception as e:
            logger.warning(f"Error scraping {url}: {e}")
            return None

    def crawl(self, start_urls: List[str], max_pages: int = 500) -> List[Dict]:
        """
        Crawl Vanguard website starting from given URLs

        Args:
            start_urls: List of starting URLs to crawl from
            max_pages: Maximum number of pages to crawl
        """
        queue = deque(start_urls)
        pages_crawled = 0

        logger.info(f"Starting crawl from {len(start_urls)} URLs")
        logger.info(f"Maximum pages to crawl: {max_pages}")

        while queue and pages_crawled < max_pages:
            url = queue.popleft()

            if url in self.visited_urls:
                continue

            logger.info(f"Crawling [{pages_crawled + 1}/{max_pages}]: {url}")

            self.visited_urls.add(url)
            page_data = self.scrape_page(url)

            if page_data:
                pages_crawled += 1

                # If compliance-related, save it
                if page_data['is_compliance_related']:
                    logger.info(f"✓ Found compliance content: {page_data['title']}")
                    self.compliance_urls.append(page_data)

                # Add new links to queue
                for link in page_data.get('outbound_links', []):
                    if link not in self.visited_urls and self.should_crawl(link):
                        queue.append(link)

            # Log progress every 50 pages
            if pages_crawled % 50 == 0:
                logger.info(
                    f"Progress: {pages_crawled} pages crawled, {len(self.compliance_urls)} compliance pages found")

        logger.info(f"\nCrawl complete!")
        logger.info(f"Total pages crawled: {pages_crawled}")
        logger.info(f"Compliance-related pages found: {len(self.compliance_urls)}")

        return self.compliance_urls

    def save_results(self, filename: str = "vanguard_compliance_urls.json"):
        """Save compliance URLs to JSON file"""
        # Sort by keyword count (most relevant first)
        sorted_results = sorted(
            self.compliance_urls,
            key=lambda x: x['keyword_count'],
            reverse=True
        )

        output = {
            'total_compliance_pages': len(sorted_results),
            'crawl_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'pages': sorted_results
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        logger.info(f"Results saved to {filename}")

        # Also create a simple URL list
        url_list_file = filename.replace('.json', '_urls.txt')
        with open(url_list_file, 'w') as f:
            for page in sorted_results:
                f.write(f"{page['url']}\n")
        logger.info(f"URL list saved to {url_list_file}")

        # Create a summary report
        self._create_summary_report(sorted_results, filename.replace('.json', '_summary.txt'))

    def _create_summary_report(self, results: List[Dict], filename: str):
        """Create a human-readable summary report"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("VANGUARD COMPLIANCE CONTENT SUMMARY\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Total Compliance-Related Pages Found: {len(results)}\n")
            f.write(f"Crawl Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Top keywords
            all_keywords = {}
            for page in results:
                for keyword in page['compliance_keywords_found']:
                    all_keywords[keyword] = all_keywords.get(keyword, 0) + 1

            f.write("Top Compliance Keywords Found:\n")
            for keyword, count in sorted(all_keywords.items(), key=lambda x: x[1], reverse=True)[:20]:
                f.write(f"  - {keyword}: {count} pages\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP COMPLIANCE PAGES (by relevance)\n")
            f.write("=" * 80 + "\n\n")

            for i, page in enumerate(results[:50], 1):
                f.write(f"{i}. {page['title']}\n")
                f.write(f"   URL: {page['url']}\n")
                f.write(f"   Keywords: {', '.join(page['compliance_keywords_found'][:5])}\n")
                if page['description']:
                    f.write(f"   Description: {page['description'][:150]}...\n")
                f.write("\n")

        logger.info(f"Summary report saved to {filename}")


def main():
    """Main execution"""

    # Starting points for crawl
    start_urls = [
        "https://investor.vanguard.com",
        "https://investor.vanguard.com/corporate-portal",
        "https://investor.vanguard.com/investor-resources-education",
        "https://investor.vanguard.com/investor-resources-education/privacy",
        "https://investor.vanguard.com/investor-resources-education/privacy/privacy-policy",
        "https://corporate.vanguard.com",
        # Add any other known compliance-related starting points
    ]

    scraper = VanguardComplianceScraper(base_url="https://investor.vanguard.com")

    logger.info("Starting Vanguard compliance content scraper...")
    logger.info("This will crawl public Vanguard websites to find compliance-related content")

    # Crawl the website
    compliance_pages = scraper.crawl(
        start_urls=start_urls,
        max_pages=500  # Adjust based on how thorough you want to be
    )

    # Save results
    scraper.save_results(filename="data/vanguard_compliance_urls.json")

    # Print summary
    print("\n" + "=" * 80)
    print("SCRAPING COMPLETE")
    print("=" * 80)
    print(f"\nFound {len(compliance_pages)} compliance-related pages")
    print("\nTop 10 most relevant pages:")
    for i, page in enumerate(sorted(compliance_pages, key=lambda x: x['keyword_count'], reverse=True)[:10], 1):
        print(f"\n{i}. {page['title']}")
        print(f"   URL: {page['url']}")
        print(f"   Keywords found: {page['keyword_count']}")
        print(f"   Top keywords: {', '.join(page['compliance_keywords_found'][:5])}")

    print("\n" + "=" * 80)
    print("Files created:")
    print("  - data/vanguard_compliance_urls.json (full data)")
    print("  - data/vanguard_compliance_urls_urls.txt (URL list)")
    print("  - data/vanguard_compliance_urls_summary.txt (summary report)")


if __name__ == "__main__":
    main()