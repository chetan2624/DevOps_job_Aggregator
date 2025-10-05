#!/usr/bin/env python3
"""
DevOps Job Aggregator - Main orchestrator
Coordinates job scraping, processing, and email delivery
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

from helpers import (
    load_config,
    send_email_html,
    extract_keywords_and_skills,
    load_seen_jobs,
    save_seen_jobs,
    is_job_seen,
    add_job_to_seen
)
from platforms import (
    scrape_naukri,
    scrape_linkedin,
    scrape_indeed,
    scrape_wellfound,
    scrape_hirist,
    scrape_cutshort,
    scrape_foundit,
    scrape_instahyre,
    scrape_freshersworld
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# UPDATED Job search configuration - FRESHER/ENTRY-LEVEL ROLES ONLY
JOB_ROLES = [
    # Entry-level variations
    "Junior DevOps Engineer",
    "Fresher DevOps Engineer",
    "Entry Level DevOps Engineer",
    "Associate DevOps Engineer",
    "Graduate DevOps Engineer",
    "Trainee DevOps Engineer",
    
    # Technical roles - Entry level
    "Junior Technical Consultant",
    "Junior Network Engineer",
    "Junior Linux Administrator",
    "Junior System Administrator",
    "Junior Cloud Engineer",
    "Junior AWS Engineer",
    
    # Support roles - Entry level
    "Technical Support Engineer",
    "DevOps Support Engineer",
    "Cloud Support Engineer",
    
    # Other entry-level DevOps roles
    "Junior Site Reliability Engineer",
    "Junior SRE",
    "Junior Platform Engineer",
    "Junior Release Engineer",
    "Associate Cloud Engineer",
    "Graduate Site Reliability Engineer"
]

# INDIA-ONLY LOCATIONS
LOCATIONS = [
    "Bengaluru", "Bangalore", 
    "Hyderabad", 
    "Pune", 
    "NCR", "Gurgaon", "Noida", "Delhi", "New Delhi",
    "Indore", 
    "Ahmedabad", 
    "Jaipur", 
    "Mumbai", 
    "Chennai",
    "Kolkata",
    "India"
]

# Keywords to filter FRESHER jobs (include)
FRESHER_KEYWORDS = [
    'fresher', 'graduate', 'entry level', 'junior', 'trainee', 
    '0-1 year', '0-2 year', '0 year', 'recent graduate',
    'associate', 'beginner', 'starting', 'early career'
]

# Keywords to filter OUT experienced jobs (exclude)
EXPERIENCE_EXCLUDE_KEYWORDS = [
    '3+ year', '4+ year', '5+ year', '6+ year', '7+ year',
    '3-5 year', '5-7 year', '4-6 year', '5+ years',
    'senior', 'lead', 'principal', 'architect', 'manager',
    'staff engineer', 'sr.', 'sr '
]

def is_fresher_job(title: str, jd: str, location: str) -> bool:
    """
    Check if job is suitable for freshers/entry-level candidates
    Returns True only for fresher jobs
    """
    title_lower = title.lower()
    jd_lower = jd.lower() if jd else ""
    combined_text = f"{title_lower} {jd_lower}"
    
    # EXCLUDE if it's clearly for experienced candidates
    for exclude_keyword in EXPERIENCE_EXCLUDE_KEYWORDS:
        if exclude_keyword.lower() in combined_text:
            logger.debug(f"Excluding experienced job: {title} (found: {exclude_keyword})")
            return False
    
    # INCLUDE if it mentions fresher-related keywords
    for fresher_keyword in FRESHER_KEYWORDS:
        if fresher_keyword in combined_text:
            logger.debug(f"Including fresher job: {title} (found: {fresher_keyword})")
            return True
    
    # If no clear indicator, check title for junior/entry patterns
    if any(word in title_lower for word in ['junior', 'trainee', 'associate', 'graduate', 'fresher', 'entry']):
        return True
    
    # Default: exclude if unclear (to avoid experienced jobs)
    logger.debug(f"Excluding unclear job: {title}")
    return False

def is_india_job(location: str, company: str, jd: str) -> bool:
    """
    Check if job is located in India (not international)
    """
    if not location:
        return False
    
    location_lower = location.lower()
    jd_lower = jd.lower() if jd else ""
    
    # EXCLUDE international locations
    international_keywords = [
        'united states', 'usa', 'us,', ', us', 'uk', 'united kingdom', 
        'canada', 'australia', 'singapore', 'dubai', 'uae',
        'europe', 'germany', 'france', 'netherlands', 'poland',
        'saudi arabia', 'riyadh', 'jeddah',
        'phoenix', 'virginia', 'maryland', 'california', 'texas',
        'london', 'manchester', 'toronto', 'vancouver'
    ]
    
    for intl_keyword in international_keywords:
        if intl_keyword in location_lower:
            logger.debug(f"Excluding international job: {location}")
            return False
    
    # INCLUDE Indian locations
    indian_keywords = [
        'india', 'bangalore', 'bengaluru', 'hyderabad', 'pune', 
        'mumbai', 'delhi', 'ncr', 'gurgaon', 'gurugram', 'noida',
        'chennai', 'kolkata', 'ahmedabad', 'jaipur', 'indore',
        'karnataka', 'maharashtra', 'telangana', 'haryana'
    ]
    
    for indian_keyword in indian_keywords:
        if indian_keyword in location_lower:
            return True
    
    # Check if "remote" but company seems Indian
    if 'remote' in location_lower:
        # Check if job description mentions India
        if 'india' in jd_lower or any(city in jd_lower for city in ['bangalore', 'bengaluru', 'hyderabad', 'pune']):
            return True
    
    return False

def normalize_location_type(location: str) -> str:
    """Classify job location as Remote/Hybrid/Onsite"""
    if not location:
        return "Not specified"
    
    location_lower = location.lower()
    if any(term in location_lower for term in ['remote', 'work from home', 'wfh']):
        return "Remote"
    elif any(term in location_lower for term in ['hybrid', 'flexible']):
        return "Hybrid" 
    else:
        return "Onsite"

def collect_all_jobs() -> List[Dict[str, Any]]:
    """Scrape jobs from all configured platforms"""
    all_jobs = []
    
    # ALL Platform scrapers with error handling
    scrapers = [
        ("Naukri", scrape_naukri),
        ("LinkedIn", scrape_linkedin), 
        ("Indeed", scrape_indeed),
        ("Wellfound", scrape_wellfound),
        ("Hirist", scrape_hirist),
        ("Cutshort", scrape_cutshort),
        ("Foundit", scrape_foundit),
        ("Instahyre", scrape_instahyre),
        ("FreshersWorld", scrape_freshersworld)
    ]
    
    for platform_name, scraper_func in scrapers:
        try:
            logger.info(f"Scraping {platform_name}...")
            jobs = scraper_func(JOB_ROLES, LOCATIONS)
            logger.info(f"Found {len(jobs)} jobs from {platform_name}")
            all_jobs.extend(jobs)
        except Exception as e:
            logger.error(f"Error scraping {platform_name}: {e}")
            continue
    
    logger.info(f"Total jobs collected: {len(all_jobs)}")
    return all_jobs

def deduplicate_jobs_in_memory(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    CRITICAL FIX: Deduplicate jobs BEFORE processing
    This prevents the same job from appearing multiple times in the table
    """
    seen_in_batch = set()
    unique_jobs = []
    
    for job in jobs:
        # Create unique identifier
        job_key = f"{job.get('title', '').lower()}|{job.get('company', '').lower()}|{job.get('link', '')}"
        
        if job_key not in seen_in_batch:
            seen_in_batch.add(job_key)
            unique_jobs.append(job)
        else:
            logger.debug(f"Removing duplicate in batch: {job.get('title')} at {job.get('company')}")
    
    logger.info(f"Removed {len(jobs) - len(unique_jobs)} duplicates from current batch")
    return unique_jobs

def process_and_dedupe_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process jobs, extract data, and remove duplicates"""
    seen_jobs = load_seen_jobs()
    processed_jobs = []
    
    # CRITICAL: Deduplicate FIRST to avoid multiple entries in table
    jobs = deduplicate_jobs_in_memory(jobs)
    
    for job in jobs:
        # Validate required fields
        if not all(job.get(field) for field in ['title', 'company', 'link']):
            continue
        
        # FILTER 1: Check if it's India-based job
        if not is_india_job(job.get('location', ''), job.get('company', ''), job.get('jd', '')):
            logger.debug(f"Skipping non-India job: {job['title']} at {job.get('location', 'Unknown')}")
            continue
        
        # FILTER 2: Check if it's a fresher/entry-level job
        if not is_fresher_job(job['title'], job.get('jd', ''), job.get('location', '')):
            logger.debug(f"Skipping experienced job: {job['title']}")
            continue
            
        # Skip if already seen in previous runs
        job_id = f"{job['title']}|{job['company']}|{job['link']}"
        if is_job_seen(seen_jobs, job_id):
            logger.debug(f"Skipping previously seen job: {job['title']}")
            continue
        
        # Extract keywords and skills from job description
        jd_text = job.get('jd', '')
        if jd_text and len(jd_text) > 20:
            keywords, skills = extract_keywords_and_skills(jd_text)
        else:
            # If no JD available, extract from title
            keywords, skills = extract_keywords_and_skills(job['title'])
        
        # Ensure we have at least some data
        if not keywords:
            keywords = ['DevOps', 'Cloud', 'Linux', 'Automation', 'Infrastructure']
        if not skills:
            skills = ['CI/CD', 'Docker', 'Kubernetes', 'Git', 'Linux']
        
        # Normalize location
        location_type = normalize_location_type(job.get('location', ''))
        
        processed_job = {
            'title': job['title'],
            'company': job['company'], 
            'location': job.get('location', 'Not specified'),
            'location_type': location_type,
            'link': job['link'],
            'keywords': keywords,
            'skills': skills,
            'source': job.get('source', 'Unknown')
        }
        
        processed_jobs.append(processed_job)
        add_job_to_seen(seen_jobs, job_id)
    
    # Save updated seen jobs
    save_seen_jobs(seen_jobs)
    
    logger.info(f"New FRESHER jobs in INDIA after filtering: {len(processed_jobs)}")
    return processed_jobs

def create_html_report(jobs: List[Dict[str, Any]]) -> str:
    """Generate HTML email report from job data"""
    if not jobs:
        return """
        <html>
        <body>
            <h2>Daily DevOps Fresher Job Digest</h2>
            <p>No new fresher/entry-level jobs found in today's search.</p>
            <p><small>Searched roles: Junior DevOps, Entry-level SRE, Technical Support (India only)</small></p>
        </body>
        </html>
        """
    
    # Create DataFrame for HTML table
    table_data = []
    for job in jobs:
        # Ensure keywords and skills are not empty
        keywords_str = ', '.join(job['keywords'][:10]) if job['keywords'] else 'DevOps, Cloud, Linux'
        skills_str = ', '.join(job['skills'][:10]) if job['skills'] else 'Docker, Kubernetes, CI/CD'
        
        table_data.append({
            'Job Title': job['title'],
            'Company Name': job['company'],
            'Job Location (Remote/Hybrid/Onsite)': f"{job['location_type']} — {job['location']}",
            'Direct Apply Link': f'<a href="{job["link"]}" target="_blank">Apply Now</a>',
            '10 Common Keywords': keywords_str,
            '10 Technical Skills': skills_str
        })
    
    df = pd.DataFrame(table_data)
    
    # Generate HTML table
    html_table = df.to_html(index=False, escape=False, classes='job-table')
    
    # Create full HTML document with styling
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h2 {{ color: #2c3e50; }}
            .job-table {{ 
                border-collapse: collapse; 
                width: 100%; 
                margin-top: 20px; 
            }}
            .job-table th {{ 
                background-color: #27ae60; 
                color: white; 
                padding: 12px; 
                text-align: left; 
                border: 1px solid #ddd;
            }}
            .job-table td {{ 
                padding: 12px; 
                border: 1px solid #ddd; 
                vertical-align: top;
            }}
            .job-table tr:nth-child(even) {{ background-color: #f2f2f2; }}
            .job-table a {{ color: #27ae60; text-decoration: none; font-weight: bold; }}
            .job-table a:hover {{ text-decoration: underline; }}
            .summary {{ 
                background-color: #d5f4e6; 
                padding: 15px; 
                border-radius: 5px; 
                margin-bottom: 20px;
                border-left: 4px solid #27ae60;
            }}
        </style>
    </head>
    <body>
        <h2>🎓 Daily DevOps Fresher Job Digest - {datetime.now().strftime('%B %d, %Y')}</h2>
        
        <div class="summary">
            <strong>Found {len(jobs)} new FRESHER/ENTRY-LEVEL job opportunities in India!</strong><br>
            <small>Roles: Junior DevOps, Technical Support, Entry-level SRE, Cloud Support | Locations: India (Remote/Hybrid/Onsite)</small>
        </div>
        
        {html_table}
        
        <br>
        <p><small>
            <strong>Note:</strong> This digest shows ONLY fresher/entry-level positions in India.<br>
            Jobs sourced from: Naukri, LinkedIn, Indeed, Wellfound, Hirist, Cutshort, Foundit, Instahyre, FreshersWorld.<br>
            <em>Filtered to exclude: Senior roles, 3+ years experience requirements, and international locations.</em>
        </small></p>
    </body>
    </html>
    """
    
    return html_content

def main():
    """Main execution function"""
    logger.info("Starting DevOps Fresher Job Aggregator (India Only)...")
    
    try:
        # Load configuration
        config = load_config()
        
        # Collect jobs from all platforms  
        all_jobs = collect_all_jobs()
        
        # Process, filter, and deduplicate
        new_jobs = process_and_dedupe_jobs(all_jobs)
        
        # Generate HTML report
        html_report = create_html_report(new_jobs)
        
        # Send email or save to file
        if config.get('DRY_RUN', 'true').lower() == 'true':
            # Dry run - save to file
            with open('last_run.html', 'w', encoding='utf-8') as f:
                f.write(html_report)
            logger.info("Dry run completed. Report saved to 'last_run.html'")
            logger.info(f"Total unique fresher jobs in India found: {len(new_jobs)}")
        else:
            # Send email
            subject = f"DevOps Fresher Job Digest (India) - {datetime.now().strftime('%Y-%m-%d')} - {len(new_jobs)} New Jobs"
            recipient = config.get('RECIPIENT_EMAIL')
            
            if not recipient:
                logger.error("RECIPIENT_EMAIL not configured")
                return
            
            send_email_html(subject, html_report, recipient)
            logger.info(f"Email sent successfully to {recipient}")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()