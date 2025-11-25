"""
SEC and FINRA Compliance Violation Scraper

Scrapes news releases, litigation files, and enforcement actions
related to compliance violations from SEC and FINRA websites.

Author: Compliance Monitoring System
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import re
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, Generator
from pathlib import Path
from urllib.parse import urljoin
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class EnforcementAction:
    """Data class representing an enforcement action or compliance violation."""
    source: str  # SEC or FINRA
    action_type: str  # litigation_release, press_release, admin_proceeding, etc.
    release_number: Optional[str]
    title: str
    date: Optional[str]
    url: str
    summary: Optional[str] = None
    violations: list = field(default_factory=list)
    penalties: Optional[str] = None
    respondents: list = field(default_factory=list)
    raw_text: Optional[str] = None
    
    @property
    def unique_id(self) -> str:
        """Generate unique ID for deduplication."""
        content = f"{self.source}:{self.url}"
        return hashlib.md5(content.encode()).hexdigest()[:12]


class RateLimiter:
    """Simple rate limiter to be respectful to servers."""
    
    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0
    
    def wait(self):
        """Wait if necessary to maintain rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()


class BaseScraper:
    """Base scraper with common functionality."""
    
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (compatible; ComplianceMonitor/1.0; Research purposes)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    # Keywords indicating compliance violations
    COMPLIANCE_KEYWORDS = [
        'compliance', 'violation', 'enforcement', 'fraud', 'misleading',
        'failure to supervise', 'books and records', 'recordkeeping',
        'anti-money laundering', 'aml', 'kyc', 'know your customer',
        'suitability', 'best interest', 'fiduciary', 'disclosure',
        'insider trading', 'market manipulation', 'reg sho',
        'whistleblower', 'retaliation', 'custody rule', 'safeguarding',
        'advertising', 'marketing rule', 'off-channel', 'communications',
        'cybersecurity', 'data breach', 'controls', 'supervisory',
        'fcpa', 'bribery', 'corruption', 'sanctions', 'ofac'
    ]
    
    def __init__(self, rate_limit: float = 1.0):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.rate_limiter = RateLimiter(rate_limit)
    
    def fetch_page(self, url: str, timeout: int = 30) -> Optional[BeautifulSoup]:
        """Fetch and parse a web page."""
        self.rate_limiter.wait()
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def fetch_text(self, url: str, timeout: int = 30) -> Optional[str]:
        """Fetch raw text content from a URL."""
        self.rate_limiter.wait()
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def extract_violations(self, text: str) -> list:
        """Extract compliance violation types from text."""
        if not text:
            return []
        text_lower = text.lower()
        found = []
        for keyword in self.COMPLIANCE_KEYWORDS:
            if keyword in text_lower:
                found.append(keyword)
        return list(set(found))
    
    def is_compliance_related(self, text: str) -> bool:
        """Check if content is related to compliance violations."""
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in self.COMPLIANCE_KEYWORDS)


class SECScraper(BaseScraper):
    """Scraper for SEC enforcement and litigation data."""
    
    BASE_URL = "https://www.sec.gov"
    
    # SEC endpoint patterns
    ENDPOINTS = {
        'litigation_releases': '/enforcement-litigation/litigation-releases',
        'admin_proceedings': '/enforcement-litigation/administrative-proceedings',
        'press_releases': '/newsroom/press-releases',
    }
    
    def scrape_litigation_releases(self, max_pages: int = 5) -> Generator[EnforcementAction, None, None]:
        """Scrape SEC litigation releases."""
        logger.info("Scraping SEC litigation releases...")
        
        base_url = f"{self.BASE_URL}{self.ENDPOINTS['litigation_releases']}"
        
        for page in range(max_pages):
            url = f"{base_url}?page={page}" if page > 0 else base_url
            soup = self.fetch_page(url)
            
            if not soup:
                break
            
            # Find litigation release links
            releases = soup.find_all('a', href=re.compile(r'/litigation-releases/lr-\d+'))
            
            if not releases:
                # Try alternative pattern
                releases = soup.find_all('a', href=re.compile(r'/litigation/litreleases/'))
            
            if not releases:
                logger.info(f"No more releases found on page {page}")
                break
            
            for release in releases:
                href = release.get('href', '')
                if not href:
                    continue
                
                full_url = urljoin(self.BASE_URL, href)
                title = release.get_text(strip=True)
                
                # Extract release number from URL or title
                release_num_match = re.search(r'LR[- ]?(\d+)', title + href, re.IGNORECASE)
                release_num = f"LR-{release_num_match.group(1)}" if release_num_match else None
                
                # Fetch full details
                detail_soup = self.fetch_page(full_url)
                summary = None
                date = None
                raw_text = None
                
                if detail_soup:
                    # Extract content
                    content_div = detail_soup.find('div', class_='article-content') or detail_soup.find('main')
                    if content_div:
                        raw_text = content_div.get_text(separator=' ', strip=True)
                        summary = raw_text[:500] + '...' if len(raw_text) > 500 else raw_text
                    
                    # Extract date
                    date_elem = detail_soup.find('time') or detail_soup.find(class_=re.compile(r'date'))
                    if date_elem:
                        date = date_elem.get_text(strip=True)
                
                violations = self.extract_violations(raw_text or title)
                
                action = EnforcementAction(
                    source='SEC',
                    action_type='litigation_release',
                    release_number=release_num,
                    title=title,
                    date=date,
                    url=full_url,
                    summary=summary,
                    violations=violations,
                    raw_text=raw_text
                )
                
                yield action
            
            logger.info(f"Processed SEC litigation releases page {page + 1}")
    
    def scrape_press_releases(self, max_pages: int = 5, 
                              filter_compliance: bool = True) -> Generator[EnforcementAction, None, None]:
        """Scrape SEC press releases, optionally filtering for compliance-related content."""
        logger.info("Scraping SEC press releases...")
        
        base_url = f"{self.BASE_URL}{self.ENDPOINTS['press_releases']}"
        
        for page in range(max_pages):
            url = f"{base_url}?page={page}" if page > 0 else base_url
            soup = self.fetch_page(url)
            
            if not soup:
                break
            
            # Find press release links
            releases = soup.find_all('a', href=re.compile(r'/press-releases?/\d{4}-\d+'))
            
            if not releases:
                logger.info(f"No more press releases found on page {page}")
                break
            
            for release in releases:
                href = release.get('href', '')
                if not href:
                    continue
                
                full_url = urljoin(self.BASE_URL, href)
                title = release.get_text(strip=True)
                
                # Extract release number
                release_num_match = re.search(r'(\d{4}-\d+)', href)
                release_num = release_num_match.group(1) if release_num_match else None
                
                # Quick filter on title
                if filter_compliance and not self.is_compliance_related(title):
                    continue
                
                # Fetch full details
                detail_soup = self.fetch_page(full_url)
                summary = None
                date = None
                raw_text = None
                penalties = None
                respondents = []
                
                if detail_soup:
                    content_div = detail_soup.find('div', class_='article-content') or detail_soup.find('main')
                    if content_div:
                        raw_text = content_div.get_text(separator=' ', strip=True)
                        summary = raw_text[:500] + '...' if len(raw_text) > 500 else raw_text
                        
                        # Extract penalty amounts
                        penalty_match = re.search(
                            r'\$[\d,]+(?:\.\d+)?\s*(?:million|billion)?(?:\s+(?:penalty|fine|settlement))?',
                            raw_text, re.IGNORECASE
                        )
                        if penalty_match:
                            penalties = penalty_match.group(0)
                    
                    date_elem = detail_soup.find('time') or detail_soup.find(class_=re.compile(r'date'))
                    if date_elem:
                        date = date_elem.get_text(strip=True)
                
                # Final compliance filter on full text
                if filter_compliance and raw_text and not self.is_compliance_related(raw_text):
                    continue
                
                violations = self.extract_violations(raw_text or title)
                
                action = EnforcementAction(
                    source='SEC',
                    action_type='press_release',
                    release_number=release_num,
                    title=title,
                    date=date,
                    url=full_url,
                    summary=summary,
                    violations=violations,
                    penalties=penalties,
                    respondents=respondents,
                    raw_text=raw_text
                )
                
                yield action
            
            logger.info(f"Processed SEC press releases page {page + 1}")
    
    def scrape_admin_proceedings(self, max_pages: int = 5) -> Generator[EnforcementAction, None, None]:
        """Scrape SEC administrative proceedings."""
        logger.info("Scraping SEC administrative proceedings...")
        
        base_url = f"{self.BASE_URL}{self.ENDPOINTS['admin_proceedings']}"
        
        for page in range(max_pages):
            url = f"{base_url}?page={page}" if page > 0 else base_url
            soup = self.fetch_page(url)
            
            if not soup:
                break
            
            # Find admin proceeding links
            releases = soup.find_all('a', href=re.compile(r'/administrative-proceedings/'))
            
            if not releases:
                logger.info(f"No more admin proceedings found on page {page}")
                break
            
            for release in releases:
                href = release.get('href', '')
                if not href or '/administrative-proceedings' not in href:
                    continue
                if href == self.ENDPOINTS['admin_proceedings']:
                    continue
                
                full_url = urljoin(self.BASE_URL, href)
                title = release.get_text(strip=True)
                
                # Extract release number
                release_num_match = re.search(r'(ia-\d+|33-\d+|34-\d+)', href, re.IGNORECASE)
                release_num = release_num_match.group(1).upper() if release_num_match else None
                
                # Fetch full details
                detail_soup = self.fetch_page(full_url)
                summary = None
                date = None
                raw_text = None
                
                if detail_soup:
                    content_div = detail_soup.find('div', class_='article-content') or detail_soup.find('main')
                    if content_div:
                        raw_text = content_div.get_text(separator=' ', strip=True)
                        summary = raw_text[:500] + '...' if len(raw_text) > 500 else raw_text
                    
                    date_elem = detail_soup.find('time') or detail_soup.find(class_=re.compile(r'date'))
                    if date_elem:
                        date = date_elem.get_text(strip=True)
                
                violations = self.extract_violations(raw_text or title)
                
                action = EnforcementAction(
                    source='SEC',
                    action_type='administrative_proceeding',
                    release_number=release_num,
                    title=title,
                    date=date,
                    url=full_url,
                    summary=summary,
                    violations=violations,
                    raw_text=raw_text
                )
                
                yield action
            
            logger.info(f"Processed SEC admin proceedings page {page + 1}")


class FINRAScraper(BaseScraper):
    """Scraper for FINRA enforcement and disciplinary action data."""
    
    BASE_URL = "https://www.finra.org"
    
    # FINRA endpoints
    ENDPOINTS = {
        'disciplinary_actions': '/rules-guidance/oversight-enforcement/disciplinary-actions',
        'enforcement': '/rules-guidance/enforcement',
        'adjudications': '/rules-guidance/adjudication-decisions',
    }
    
    # Monthly disciplinary action URLs follow this pattern
    MONTHLY_ACTION_PATTERN = '/rules-guidance/rulebooks/monthly-disciplinary-actions-{month}-{year}'
    
    def scrape_monthly_actions(self, months_back: int = 6) -> Generator[EnforcementAction, None, None]:
        """Scrape FINRA monthly disciplinary action summaries."""
        logger.info(f"Scraping FINRA monthly disciplinary actions for last {months_back} months...")
        
        current_date = datetime.now()
        
        for i in range(months_back):
            target_date = current_date - timedelta(days=30 * i)
            month_name = target_date.strftime('%B').lower()
            year = target_date.year
            
            url = f"{self.BASE_URL}{self.MONTHLY_ACTION_PATTERN.format(month=month_name, year=year)}"
            
            soup = self.fetch_page(url)
            if not soup:
                logger.warning(f"Could not fetch FINRA monthly actions for {month_name} {year}")
                continue
            
            # Try to find PDF link for detailed disciplinary actions
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
            
            # Also extract summary content from the page
            content = soup.find('div', class_='field-body') or soup.find('main')
            
            if content:
                raw_text = content.get_text(separator=' ', strip=True)
                
                # Parse individual actions from the text
                # FINRA typically lists firms/individuals with their violations
                action = EnforcementAction(
                    source='FINRA',
                    action_type='monthly_disciplinary_summary',
                    release_number=f"{month_name.capitalize()}-{year}",
                    title=f"FINRA Monthly Disciplinary Actions - {month_name.capitalize()} {year}",
                    date=f"{month_name.capitalize()} {year}",
                    url=url,
                    summary=raw_text[:500] + '...' if len(raw_text) > 500 else raw_text,
                    violations=self.extract_violations(raw_text),
                    raw_text=raw_text
                )
                
                yield action
            
            # Also yield links to PDF detailed reports
            for pdf_link in pdf_links:
                pdf_href = pdf_link.get('href', '')
                if 'disciplinary' in pdf_href.lower():
                    pdf_url = urljoin(self.BASE_URL, pdf_href)
                    
                    action = EnforcementAction(
                        source='FINRA',
                        action_type='monthly_disciplinary_pdf',
                        release_number=f"PDF-{month_name.capitalize()}-{year}",
                        title=f"FINRA Disciplinary Actions PDF - {month_name.capitalize()} {year}",
                        date=f"{month_name.capitalize()} {year}",
                        url=pdf_url,
                        summary="Detailed disciplinary action document (PDF)",
                        violations=[]
                    )
                    
                    yield action
            
            logger.info(f"Processed FINRA actions for {month_name} {year}")
    
    def scrape_disciplinary_search(self, search_terms: list = None) -> Generator[EnforcementAction, None, None]:
        """
        Scrape FINRA disciplinary actions using their search interface.
        Note: FINRA uses a JavaScript-heavy interface, so we'll parse what's available.
        """
        logger.info("Scraping FINRA disciplinary actions database info...")
        
        # FINRA's online search requires JavaScript, but we can get the main page info
        url = f"{self.BASE_URL}/rules-guidance/oversight-enforcement/finra-disciplinary-actions-online"
        
        soup = self.fetch_page(url)
        if not soup:
            return
        
        # Extract any available summary information
        content = soup.find('div', class_='field-body') or soup.find('main')
        
        if content:
            action = EnforcementAction(
                source='FINRA',
                action_type='disciplinary_database_info',
                release_number=None,
                title="FINRA Disciplinary Actions Online Database",
                date=datetime.now().strftime('%Y-%m-%d'),
                url=url,
                summary="Access FINRA's searchable database of disciplinary actions from 2005 onwards.",
                violations=[]
            )
            
            yield action
    
    def scrape_enforcement_news(self, max_pages: int = 3) -> Generator[EnforcementAction, None, None]:
        """Scrape FINRA enforcement news and announcements."""
        logger.info("Scraping FINRA enforcement news...")
        
        url = f"{self.BASE_URL}{self.ENDPOINTS['enforcement']}"
        
        soup = self.fetch_page(url)
        if not soup:
            return
        
        # Find news/announcement links
        links = soup.find_all('a', href=re.compile(r'/media-center/|/newsroom/'))
        
        for link in links[:20]:  # Limit to first 20 relevant links
            href = link.get('href', '')
            if not href:
                continue
            
            full_url = urljoin(self.BASE_URL, href)
            title = link.get_text(strip=True)
            
            if not title or len(title) < 10:
                continue
            
            # Only process compliance-related items
            if not self.is_compliance_related(title):
                continue
            
            detail_soup = self.fetch_page(full_url)
            summary = None
            raw_text = None
            date = None
            
            if detail_soup:
                content = detail_soup.find('div', class_='field-body') or detail_soup.find('main')
                if content:
                    raw_text = content.get_text(separator=' ', strip=True)
                    summary = raw_text[:500] + '...' if len(raw_text) > 500 else raw_text
                
                date_elem = detail_soup.find('time') or detail_soup.find(class_=re.compile(r'date'))
                if date_elem:
                    date = date_elem.get_text(strip=True)
            
            action = EnforcementAction(
                source='FINRA',
                action_type='enforcement_news',
                release_number=None,
                title=title,
                date=date,
                url=full_url,
                summary=summary,
                violations=self.extract_violations(raw_text or title),
                raw_text=raw_text
            )
            
            yield action


class ComplianceDataExporter:
    """Export scraped compliance data to various formats."""
    
    def __init__(self, output_dir: str = 'output'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def to_json(self, actions: list, filename: str = 'compliance_actions.json') -> Path:
        """Export actions to JSON format."""
        filepath = self.output_dir / filename
        
        data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_actions': len(actions),
                'sources': list(set(a.source for a in actions))
            },
            'actions': [asdict(a) for a in actions]
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(actions)} actions to {filepath}")
        return filepath
    
    def to_csv(self, actions: list, filename: str = 'compliance_actions.csv') -> Path:
        """Export actions to CSV format."""
        filepath = self.output_dir / filename
        
        fieldnames = [
            'unique_id', 'source', 'action_type', 'release_number', 
            'title', 'date', 'url', 'summary', 'violations', 'penalties'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for action in actions:
                row = {
                    'unique_id': action.unique_id,
                    'source': action.source,
                    'action_type': action.action_type,
                    'release_number': action.release_number or '',
                    'title': action.title,
                    'date': action.date or '',
                    'url': action.url,
                    'summary': (action.summary or '')[:300],  # Truncate for CSV
                    'violations': ', '.join(action.violations),
                    'penalties': action.penalties or ''
                }
                writer.writerow(row)
        
        logger.info(f"Exported {len(actions)} actions to {filepath}")
        return filepath
    
    def generate_summary_report(self, actions: list, filename: str = 'summary_report.md') -> Path:
        """Generate a markdown summary report."""
        filepath = self.output_dir / filename
        
        # Aggregate statistics
        by_source = {}
        by_type = {}
        all_violations = []
        
        for action in actions:
            by_source[action.source] = by_source.get(action.source, 0) + 1
            by_type[action.action_type] = by_type.get(action.action_type, 0) + 1
            all_violations.extend(action.violations)
        
        # Count violation types
        violation_counts = {}
        for v in all_violations:
            violation_counts[v] = violation_counts.get(v, 0) + 1
        
        top_violations = sorted(violation_counts.items(), key=lambda x: -x[1])[:15]
        
        report = f"""# Compliance Violations Summary Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total Actions Collected:** {len(actions)}

## Actions by Source

| Source | Count |
|--------|-------|
"""
        for source, count in sorted(by_source.items()):
            report += f"| {source} | {count} |\n"
        
        report += """
## Actions by Type

| Type | Count |
|------|-------|
"""
        for atype, count in sorted(by_type.items(), key=lambda x: -x[1]):
            report += f"| {atype} | {count} |\n"
        
        report += """
## Top Violation Keywords

| Violation Type | Occurrences |
|---------------|-------------|
"""
        for violation, count in top_violations:
            report += f"| {violation} | {count} |\n"
        
        report += """
## Recent Actions

"""
        # List most recent actions
        for action in actions[:20]:
            report += f"### {action.title[:80]}{'...' if len(action.title) > 80 else ''}\n\n"
            report += f"- **Source:** {action.source}\n"
            report += f"- **Type:** {action.action_type}\n"
            if action.date:
                report += f"- **Date:** {action.date}\n"
            if action.violations:
                report += f"- **Violations:** {', '.join(action.violations[:5])}\n"
            if action.penalties:
                report += f"- **Penalties:** {action.penalties}\n"
            report += f"- **URL:** {action.url}\n\n"
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"Generated summary report at {filepath}")
        return filepath


def main():
    """Main execution function."""
    print("=" * 60)
    print("SEC/FINRA Compliance Violations Scraper")
    print("=" * 60)
    
    # Initialize scrapers
    sec_scraper = SECScraper(rate_limit=0.5)  # 2 requests per second max
    finra_scraper = FINRAScraper(rate_limit=0.5)
    exporter = ComplianceDataExporter(output_dir='compliance_data')
    
    all_actions = []
    
    # Scrape SEC data
    print("\n[1/5] Scraping SEC Litigation Releases...")
    for action in sec_scraper.scrape_litigation_releases(max_pages=2):
        all_actions.append(action)
        print(f"  - {action.title[:60]}...")
    
    print(f"\n[2/5] Scraping SEC Press Releases (compliance-related)...")
    for action in sec_scraper.scrape_press_releases(max_pages=2, filter_compliance=True):
        all_actions.append(action)
        print(f"  - {action.title[:60]}...")
    
    print(f"\n[3/5] Scraping SEC Administrative Proceedings...")
    for action in sec_scraper.scrape_admin_proceedings(max_pages=2):
        all_actions.append(action)
        print(f"  - {action.title[:60]}...")
    
    # Scrape FINRA data
    print(f"\n[4/5] Scraping FINRA Monthly Disciplinary Actions...")
    for action in finra_scraper.scrape_monthly_actions(months_back=3):
        all_actions.append(action)
        print(f"  - {action.title[:60]}...")
    
    print(f"\n[5/5] Scraping FINRA Enforcement News...")
    for action in finra_scraper.scrape_enforcement_news():
        all_actions.append(action)
        print(f"  - {action.title[:60]}...")
    
    # Deduplicate by unique_id
    seen_ids = set()
    unique_actions = []
    for action in all_actions:
        if action.unique_id not in seen_ids:
            seen_ids.add(action.unique_id)
            unique_actions.append(action)
    
    print(f"\n{'=' * 60}")
    print(f"Total unique actions collected: {len(unique_actions)}")
    
    # Export data
    print("\nExporting data...")
    json_path = exporter.to_json(unique_actions)
    csv_path = exporter.to_csv(unique_actions)
    report_path = exporter.generate_summary_report(unique_actions)
    
    print(f"\nOutput files:")
    print(f"  - JSON: {json_path}")
    print(f"  - CSV: {csv_path}")
    print(f"  - Report: {report_path}")
    print("\nDone!")
    
    return unique_actions


if __name__ == '__main__':
    actions = main()
