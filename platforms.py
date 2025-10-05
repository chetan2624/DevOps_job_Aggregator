"""
Job scraping functions for various platforms
Each function returns a list of job dictionaries with standardized format
FULLY FIXED: All syntax errors resolved
"""

import logging
import time
import random
from typing import List, Dict, Any
from urllib.parse import urljoin, quote_plus
import requests
from bs4 import BeautifulSoup

from helpers import fetch_html

logger = logging.getLogger(__name__)

def add_random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """Add random delay between requests to avoid rate limiting"""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def scrape_naukri(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape FRESHER jobs from Naukri.com"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:4]:
            search_role = role.lower().replace(' ', '-')
            search_urls = [
                f"https://www.naukri.com/{search_role}-jobs",
                f"https://www.naukri.com/fresher-{search_role}-jobs",
            ]
            
            for search_url in search_urls:
                try:
                    response = fetch_html(search_url, session)
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    job_cards = soup.find_all('article', class_='jobTuple') or soup.find_all('div', class_='jobTuple')
                    
                    for card in job_cards[:15]:
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
                            
                            exp_elem = card.find('span', class_='expwdth') or card.find('li', class_='experience')
                            experience = exp_elem.get_text(strip=True) if exp_elem else ''
                            
                            jd = ""
                            try:
                                jd_response = fetch_html(link, session)
                                jd_soup = BeautifulSoup(jd_response.text, 'lxml')
                                jd_elem = jd_soup.find('div', class_='jobDescription') or jd_soup.find('section', class_='job-description')
                                if jd_elem:
                                    jd = jd_elem.get_text(separator=' ', strip=True)
                                
                                if experience:
                                    jd = f"{experience} | {jd}"
                            except Exception as e:
                                logger.debug(f"Could not fetch JD from Naukri: {e}")
                                if experience:
                                    jd = f"{title} {experience}"
                            
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
                    
                    add_random_delay(2, 4)
                    
                except Exception as e:
                    logger.debug(f"Error with Naukri URL '{search_url}': {e}")
                    continue
                
                if jobs:
                    break
    
    except Exception as e:
        logger.error(f"General error in Naukri scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Naukri")
    return jobs

def scrape_linkedin(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape FRESHER jobs from LinkedIn"""
    jobs = []
    session = requests.Session()
    
    try:
        india_locations = ['India', 'Bangalore', 'Bengaluru', 'Hyderabad', 'Pune', 'Delhi NCR']
        
        for role in roles[:3]:
            for location in india_locations[:2]:
                search_url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(role)}&location={quote_plus(location)}&f_E=1,2&f_TPR=r604800&sortBy=DD"
                
                try:
                    response = fetch_html(search_url, session)
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    job_cards = (
                        soup.find_all('div', class_='job-search-card') or 
                        soup.find_all('li', class_='result-card') or
                        soup.find_all('div', class_='base-card')
                    )
                    
                    for card in job_cards[:10]:
                        try:
                            link_elem = (
                                card.find('a', class_='base-card__full-link') or 
                                card.find('a', class_='result-card__full-card-link') or
                                card.find('h3', class_='base-search-card__title')
                            )
                            
                            if not link_elem:
                                title_h3 = card.find('h3')
                                if title_h3:
                                    link_elem = title_h3.find('a')
                            
                            if not link_elem:
                                continue
                            
                            title = link_elem.get_text(strip=True)
                            link = link_elem.get('href', '')
                            
                            if link and not link.startswith('http'):
                                link = f"https://www.linkedin.com{link}"
                            
                            company_elem = (
                                card.find('h4', class_='base-search-card__subtitle') or 
                                card.find('a', class_='hidden-nested-link') or
                                card.find('span', class_='job-card-container__company-name')
                            )
                            company = company_elem.get_text(strip=True) if company_elem else 'Company'
                            
                            location_elem = (
                                card.find('span', class_='job-search-card__location') or
                                card.find('span', class_='job-card-container__metadata-item')
                            )
                            location_text = location_elem.get_text(strip=True) if location_elem else location
                            
                            seniority_elem = card.find('span', class_='job-card-container__job-insight')
                            jd_snippet = seniority_elem.get_text(strip=True) if seniority_elem else ""
                            
                            jobs.append({
                                'title': title,
                                'company': company,
                                'location': location_text,
                                'link': link,
                                'jd': jd_snippet,
                                'source': 'LinkedIn'
                            })
                            
                        except Exception as e:
                            logger.debug(f"Error parsing LinkedIn job card: {e}")
                            continue
                    
                    add_random_delay(3, 5)
                    
                except Exception as e:
                    logger.debug(f"Error scraping LinkedIn for '{role}' in '{location}': {e}")
                    continue
    
    except Exception as e:
        logger.error(f"General error in LinkedIn scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from LinkedIn")
    return jobs

def scrape_indeed(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape FRESHER jobs from Indeed India - FIXED"""
    jobs = []
    session = requests.Session()
    
    try:
        india_locations = ['Bangalore', 'Bengaluru', 'Hyderabad', 'Pune']
        
        for role in roles[:2]:
            for location in india_locations[:2]:
                search_url = f"https://in.indeed.com/jobs?q={quote_plus(role)}&l={quote_plus(location)}&sort=date"
                
                try:
                    response = fetch_html(search_url, session)
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    job_cards = (
                        soup.find_all('div', class_='job_seen_beacon') or 
                        soup.find_all('div', {'data-jk': True}) or
                        soup.find_all('td', class_='resultContent')
                    )
                    
                    if not job_cards:
                        logger.debug(f"No job cards found on Indeed for '{role}' in '{location}'")
                        continue
                    
                    for card in job_cards[:10]:
                        try:
                            title_elem = (
                                card.find('h2', class_='jobTitle') or 
                                card.find('a', {'data-jk': True}) or
                                card.find('span', {'title': True})
                            )
                            
                            if not title_elem:
                                continue
                            
                            if title_elem.name == 'h2':
                                title_link = title_elem.find('a')
                                if not title_link:
                                    title_link = title_elem.find('span')
                            else:
                                title_link = title_elem
                            
                            if not title_link:
                                continue
                                
                            title = title_link.get('title') or title_link.get_text(strip=True)
                            link_href = title_link.get('href', '')
                            link = urljoin('https://in.indeed.com', link_href) if link_href else ''
                            
                            company_elem = (
                                card.find('span', class_='companyName') or 
                                card.find('a', {'data-testid': 'company-name'}) or
                                card.find('span', {'data-testid': 'company-name'})
                            )
                            company = company_elem.get_text(strip=True) if company_elem else 'Company'
                            
                            location_elem = (
                                card.find('div', class_='companyLocation') or 
                                card.find('div', {'data-testid': 'job-location'}) or
                                card.find('div', {'data-testid': 'text-location'})
                            )
                            location_text = location_elem.get_text(strip=True) if location_elem else location
                            
                            snippet_elem = card.find('div', class_='job-snippet') or card.find('td', class_='resultContent')
                            jd_snippet = snippet_elem.get_text(separator=' ', strip=True) if snippet_elem else ""
                            
                            jobs.append({
                                'title': title,
                                'company': company,
                                'location': location_text,
                                'link': link,
                                'jd': jd_snippet,
                                'source': 'Indeed'
                            })
                            
                        except Exception as e:
                            logger.debug(f"Error parsing Indeed job card: {e}")
                            continue
                    
                    add_random_delay(3, 5)
                    
                except Exception as e:
                    logger.warning(f"Indeed may be blocking requests for '{role}' in '{location}': {e}")
                    continue
    
    except Exception as e:
        logger.error(f"General error in Indeed scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Indeed")
    return jobs

def scrape_wellfound(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Wellfound"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:2]:
            search_url = f"https://wellfound.com/jobs?search={quote_plus(role)}"
            
            try:
                response = fetch_html(search_url, session)
                soup = BeautifulSoup(response.text, 'lxml')
                
                job_cards = soup.find_all('div', class_='job-listing') or soup.find_all('a', {'data-test': 'job-link'})
                
                for card in job_cards[:10]:
                    try:
                        if card.name == 'a':
                            link = urljoin('https://wellfound.com', card.get('href', ''))
                            title = card.find('div', class_='job-title') or card.find('h3')
                            title = title.get_text(strip=True) if title else 'DevOps Role'
                        else:
                            link_elem = card.find('a')
                            if not link_elem:
                                continue
                            link = urljoin('https://wellfound.com', link_elem.get('href', ''))
                            title = link_elem.get_text(strip=True)
                        
                        company_elem = card.find('div', class_='company') or card.find('span', class_='company-name')
                        company = company_elem.get_text(strip=True) if company_elem else 'Startup'
                        
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': 'India/Remote',
                            'link': link,
                            'jd': '',
                            'source': 'Wellfound'
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error parsing Wellfound job card: {e}")
                        continue
                
                add_random_delay()
                
            except Exception as e:
                logger.debug(f"Error scraping Wellfound for role '{role}': {e}")
                continue
    
    except Exception as e:
        logger.error(f"General error in Wellfound scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Wellfound")
    return jobs

def scrape_hirist(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Hirist"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:3]:
            search_url = f"https://hirist.com/jobs/{role.replace(' ', '-').lower()}"
            
            try:
                response = fetch_html(search_url, session)
                soup = BeautifulSoup(response.text, 'lxml')
                
                job_cards = soup.find_all('div', class_='job-card') or soup.find_all('div', {'data-job-id': True})
                
                for card in job_cards[:10]:
                    try:
                        title_elem = card.find('h3') or card.find('a', class_='job-title')
                        if not title_elem:
                            continue
                            
                        title = title_elem.get_text(strip=True)
                        
                        link_elem = card.find('a') or title_elem
                        link = urljoin('https://hirist.com', link_elem.get('href', '')) if link_elem else ''
                        
                        company_elem = card.find('div', class_='company-name') or card.find('span', class_='company')
                        company = company_elem.get_text(strip=True) if company_elem else 'Tech Company'
                        
                        location_elem = card.find('span', class_='location')
                        location_text = location_elem.get_text(strip=True) if location_elem else 'India'
                        
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': location_text,
                            'link': link,
                            'jd': '',
                            'source': 'Hirist'
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error parsing Hirist job card: {e}")
                        continue
                
                add_random_delay()
                
            except Exception as e:
                logger.debug(f"Error scraping Hirist for role '{role}': {e}")
                continue
    
    except Exception as e:
        logger.error(f"General error in Hirist scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Hirist")
    return jobs

def scrape_cutshort(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Cutshort"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:3]:
            search_url = f"https://cutshort.io/search/jobs?q={quote_plus(role)}"
            
            try:
                response = fetch_html(search_url, session)
                soup = BeautifulSoup(response.text, 'lxml')
                
                job_cards = soup.find_all('div', class_='job-card-container') or soup.find_all('a', class_='job-card')
                
                for card in job_cards[:10]:
                    try:
                        if card.name == 'a':
                            link = urljoin('https://cutshort.io', card.get('href', ''))
                            title_elem = card.find('h3') or card
                            title = title_elem.get_text(strip=True)
                        else:
                            link_elem = card.find('a')
                            if not link_elem:
                                continue
                            link = urljoin('https://cutshort.io', link_elem.get('href', ''))
                            title = link_elem.get_text(strip=True)
                        
                        company_elem = card.find('div', class_='company-name') or card.find('span', class_='company')
                        company = company_elem.get_text(strip=True) if company_elem else 'Startup'
                        
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': 'India/Remote',
                            'link': link,
                            'jd': '',
                            'source': 'Cutshort'
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error parsing Cutshort job card: {e}")
                        continue
                
                add_random_delay()
                
            except Exception as e:
                logger.debug(f"Error scraping Cutshort for role '{role}': {e}")
                continue
    
    except Exception as e:
        logger.error(f"General error in Cutshort scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Cutshort")
    return jobs

def scrape_foundit(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Foundit"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:3]:
            for location in ['Bangalore', 'Hyderabad', 'Pune']:
                search_url = f"https://www.foundit.in/jobs/{role.replace(' ', '-').lower()}-jobs-in-{location.lower()}"
                
                try:
                    response = fetch_html(search_url, session)
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    job_cards = soup.find_all('div', class_='jobTuple') or soup.find_all('article', class_='job')
                    
                    for card in job_cards[:10]:
                        try:
                            title_elem = card.find('h3') or card.find('a', class_='job-title')
                            if not title_elem:
                                continue
                                
                            if title_elem.name != 'a':
                                title_link = title_elem.find('a')
                            else:
                                title_link = title_elem
                                
                            if not title_link:
                                continue
                                
                            title = title_link.get_text(strip=True)
                            link = urljoin('https://www.foundit.in', title_link.get('href', ''))
                            
                            company_elem = card.find('div', class_='company') or card.find('span', class_='company-name')
                            company = company_elem.get_text(strip=True) if company_elem else 'Company'
                            
                            jobs.append({
                                'title': title,
                                'company': company,
                                'location': location,
                                'link': link,
                                'jd': '',
                                'source': 'Foundit'
                            })
                            
                        except Exception as e:
                            logger.debug(f"Error parsing Foundit job card: {e}")
                            continue
                    
                    add_random_delay()
                    
                except Exception as e:
                    logger.debug(f"Error scraping Foundit for role '{role}' in '{location}': {e}")
                    continue
    
    except Exception as e:
        logger.error(f"General error in Foundit scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Foundit")
    return jobs

def scrape_instahyre(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from Instahyre"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:3]:
            search_url = f"https://www.instahyre.com/search-jobs/{quote_plus(role)}/"
            
            try:
                response = fetch_html(search_url, session)
                soup = BeautifulSoup(response.text, 'lxml')
                
                job_cards = soup.find_all('div', class_='job-card') or soup.find_all('div', {'data-job-id': True})
                
                for card in job_cards[:10]:
                    try:
                        title_elem = card.find('h3') or card.find('a', class_='job-title')
                        if not title_elem:
                            continue
                            
                        title = title_elem.get_text(strip=True)
                        
                        link_elem = card.find('a')
                        link = urljoin('https://www.instahyre.com', link_elem.get('href', '')) if link_elem else ''
                        
                        company_elem = card.find('div', class_='company') or card.find('span', class_='company-name')
                        company = company_elem.get_text(strip=True) if company_elem else 'Company'
                        
                        location_elem = card.find('span', class_='location')
                        location_text = location_elem.get_text(strip=True) if location_elem else 'India'
                        
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': location_text,
                            'link': link,
                            'jd': '',
                            'source': 'Instahyre'
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error parsing Instahyre job card: {e}")
                        continue
                
                add_random_delay()
                
            except Exception as e:
                logger.debug(f"Error scraping Instahyre for role '{role}': {e}")
                continue
    
    except Exception as e:
        logger.error(f"General error in Instahyre scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from Instahyre")
    return jobs

def scrape_freshersworld(roles: List[str], locations: List[str]) -> List[Dict[str, Any]]:
    """Scrape jobs from FreshersWorld"""
    jobs = []
    session = requests.Session()
    
    try:
        for role in roles[:3]:
            search_url = f"https://www.freshersworld.com/jobs/jobsearch/{quote_plus(role)}-jobs"
            
            try:
                response = fetch_html(search_url, session)
                soup = BeautifulSoup(response.text, 'lxml')
                
                job_cards = soup.find_all('div', class_='job-container') or soup.find_all('div', class_='joblist')
                
                for card in job_cards[:10]:
                    try:
                        title_elem = card.find('h3') or card.find('a', class_='job-title')
                        if not title_elem:
                            continue
                            
                        title = title_elem.get_text(strip=True)
                        
                        link_elem = card.find('a')
                        link = urljoin('https://www.freshersworld.com', link_elem.get('href', '')) if link_elem else ''
                        
                        company_elem = card.find('div', class_='company') or card.find('span', class_='company-name')
                        company = company_elem.get_text(strip=True) if company_elem else 'Company'
                        
                        location_elem = card.find('span', class_='location')
                        location_text = location_elem.get_text(strip=True) if location_elem else 'India'
                        
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': location_text,
                            'link': link,
                            'jd': 'Fresher opportunity',
                            'source': 'FreshersWorld'
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error parsing FreshersWorld job card: {e}")
                        continue
                
                add_random_delay()
                
            except Exception as e:
                logger.debug(f"Error scraping FreshersWorld for role '{role}': {e}")
                continue
    
    except Exception as e:
        logger.error(f"General error in FreshersWorld scraper: {e}")
    
    logger.info(f"Scraped {len(jobs)} jobs from FreshersWorld")
    return jobs