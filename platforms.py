"""
Job scraping functions for various platforms
Each function returns a list of job dictionaries with standardized format
"""

import logging
import time
import random
from typing import List, Dict, Any
from urllib.parse import urljoin, quote_plus
import requests
from bs4 import BeautifulSoup

from helpers import fetch_html, fetch_with_playwright, load_config

logger = logging.getLogger(__name__)

def add_random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """Add random delay between requests to avoid rate limiting"""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def scrape_naukri(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Naukri.com"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:2]:  # Limit to avoid rate limiting
            search_url = f"https://www.naukri.com/{role.replace(' ', '-').lower()}-jobs"
            
            try:
                response = fetch_html(search_url, session)
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Find job cards (Naukri structure may vary)
                job_cards = soup.find_all('article', class_='jobTuple') or soup.find_all('div', class_='jobTuple')
                
                for card in job_cards[:20]:  # Limit per search
                    try:
                        title_elem = card.find('a', class_='title') or card.find('h3')
                        if not title_elem:
                            continue
                            
                        title = title_elem.get_text(strip=True)
                        link = urljoin('https://www.naukri.com', title_elem.get('href', ''))
                        
                        company_elem = card.find('a', class_='subTitle') or card.find('div', class_='companyInfo')
                        company = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                        
                        location_elem = card.find('span', class_='locationsContainer') or card.find('li', class_='location')
                        location = location_elem.get_text(strip=True) if location_elem else 'India'
                        
                        # Try to get job description
                        jd = ""
                        try:
                            jd_response = fetch_html(link, session)
                            jd_soup = BeautifulSoup(jd_response.text, 'lxml')
                            jd_elem = jd_soup.find('div', class_='jobDescription') or jd_soup.find('section', class_='job-description')
                            if jd_elem:
                                jd = jd_elem.get_text(separator=' ', strip=True)
                        except:
                            pass
                        
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': location,
                            'link': link,
                            'jd': jd,
                            'source': 'Naukri'
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error parsing Naukri job card: {e}")
                        continue
                
                add_random_delay()
                
            except Exception as e:
                logger.error(f"Error scraping Naukri for role '{role}': {e}")
                continue
    
    except Exception as e:
        logger.error(f"General error in Naukri scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Naukri")
    return jobs

def scrape_linkedin(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from LinkedIn (best effort - may require authentication)"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:2]:
            for location in ['India', 'Remote']:
                search_url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(role)}&location={quote_plus(location)}&f_TPR=r86400"  # Last 24 hours
                
                try:
                    # LinkedIn often blocks automated requests, so this is best-effort
                    response = fetch_html(search_url, session)
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    # LinkedIn job cards
                    job_cards = soup.find_all('div', class_='job-search-card') or soup.find_all('li', class_='result-card')
                    
                    for card in job_cards[:15]:
                        try:
                            link_elem = card.find('a', class_='base-card__full-link') or card.find('h3').find('a')
                            if not link_elem:
                                continue
                                
                            title = link_elem.get_text(strip=True)
                            link = link_elem.get('href', '')
                            
                            company_elem = card.find('h4', class_='base-search-card__subtitle') or card.find('a', {'data-tracking-control-name': 'public_jobs_jserp-result_job-search-card-subtitle'})
                            company = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                            
                            location_elem = card.find('span', class_='job-search-card__location')
                            location_text = location_elem.get_text(strip=True) if location_elem else location
                            
                            jobs.append({
                                'title': title,
                                'company': company,
                                'location': location_text,
                                'link': link,
                                'jd': '',  # LinkedIn JD requires additional request which often fails
                                'source': 'LinkedIn'
                            })
                            
                        except Exception as e:
                            logger.debug(f"Error parsing LinkedIn job card: {e}")
                            continue
                    
                    add_random_delay(2, 4)  # Longer delay for LinkedIn
                    
                except Exception as e:
                    logger.error(f"Error scraping LinkedIn for role '{role}' in '{location}': {e}")
                    continue
    
    except Exception as e:
        logger.error(f"General error in LinkedIn scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from LinkedIn")
    return jobs

def scrape_indeed(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Indeed India"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:2]:
            for location in ['Bangalore', 'Hyderabad', 'Pune', 'India']:
                search_url = f"https://in.indeed.com/jobs?q={quote_plus(role)}&l={quote_plus(location)}&fromage=1"  # Last day
                
                try:
                    response = fetch_html(search_url, session)
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    # Indeed job cards
                    job_cards = soup.find_all('div', class_='job_seen_beacon') or soup.find_all('div', {'data-jk': True})
                    
                    for card in job_cards[:15]:
                        try:
                            title_elem = card.find('h2', class_='jobTitle') or card.find('a', {'data-jk': True})
                            if not title_elem:
                                continue
                            
                            title_link = title_elem.find('a') if title_elem.name != 'a' else title_elem
                            title = title_link.get('title') or title_link.get_text(strip=True)
                            link = urljoin('https://in.indeed.com', title_link.get('href', ''))
                            
                            company_elem = card.find('span', class_='companyName') or card.find('a', {'data-testid': 'company-name'})
                            company = company_elem.get_text(strip=True) if company_elem else 'Not specified'
                            
                            location_elem = card.find('div', class_='companyLocation') or card.find('div', {'data-testid': 'job-location'})
                            location_text = location_elem.get_text(strip=True) if location_elem else location
                            
                            jobs.append({
                                'title': title,
                                'company': company,
                                'location': location_text,
                                'link': link,
                                'jd': '',  # Indeed JD would require additional request
                                'source': 'Indeed'
                            })
                            
                        except Exception as e:
                            logger.debug(f"Error parsing Indeed job card: {e}")
                            continue
                    
                    add_random_delay()
                    
                except Exception as e:
                    logger.error(f"Error scraping Indeed for role '{role}' in '{location}': {e}")
                    continue
    
    except Exception as e:
        logger.error(f"General error in Indeed scraper: {e}")