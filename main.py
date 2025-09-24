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
    scrape_company_pages
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Job search configuration
JOB_ROLES = [
    "DevOps Engineer",
    "Junior DevOps Engineer", 
    "Site Reliability Engineer",
    "SRE",
    "DevOps Specialist",
    "Cloud DevOps Engineer"
]

LOCATIONS = [
    "Bengaluru", "Bangalore", "Hyderabad", "Pune", 
    "NCR", "Gurgaon", "Noida", "Delhi", "Indore", 
    "Ahmedabad", "Jaipur", "Mumbai", "Chennai", 
    "Remote", "India"
]

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
    
    # Platform scrapers with error handling
    scrapers = [
        ("Naukri", scrape_naukri),
        ("LinkedIn", scrape_linkedin), 
        ("Indeed", scrape_indeed),
        ("Wellfound", scrape_wellfound),
        ("Hirist", scrape_hirist),
        ("Cutshort", scrape_cutshort),
        ("Foundit", scrape_foundit),
        ("Company Pages", scrape_company_pages)
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

def process_and_dedupe_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process jobs, extract data, and remove duplicates"""
    seen_jobs = load_seen_jobs()
    processed_jobs = []
    
    for job in jobs:
        # Validate required fields
        if not all(job.get(field) for field in ['title', 'company', 'link']):
            continue
            
        # Skip if already seen
        job_id = f"{job['title']}|{job['company']}|{job['link']}"
        if is_job_seen(seen_jobs, job_id):
            continue
        
        # Extract keywords and skills
        keywords, skills = extract_keywords_and_skills(job.get('jd', ''))
        
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
    
    logger.info(f"New jobs after deduplication: {len(processed_jobs)}")
    return processed_jobs

def create_html_report(jobs: List[Dict[str, Any]]) -> str:
    """Generate HTML email report from job data"""
    if not jobs:
        return """
        <html>
        <body>
            <h2>Daily DevOps Job Digest</h2>
            <p>No new jobs found in today's search.</p>
            <p><small>Searched roles: DevOps Engineer, SRE, Junior DevOps Engineer</small></p>
        </body>
        </html>
        """
    
    # Create DataFrame for HTML table
    table_data = []
    for job in jobs:
        table_data.append({
            'Job Title': job['title'],
            'Company Name': job['company'],
            'Job Location (Remote/Hybrid/Onsite)': f"{job['location_type']} — {job['location']}",
            'Direct Apply Link (Company Career Page preferred)': f'<a href="{job["link"]}" target="_blank">Apply Now</a>',
            '10 Common Keywords from the Job Description': ', '.join(job['keywords'][:10]),
            '10 Technical Skills Mentioned (e.g., CI/CD tools, cloud platforms, scripting)': ', '.join(job['skills'][:10])
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
                background-color: #3498db; 
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
            .job-table a {{ color: #3498db; text-decoration: none; }}
            .job-table a:hover {{ text-decoration: underline; }}
            .summary {{ 
                background-color: #ecf0f1; 
                padding: 15px; 
                border-radius: 5px; 
                margin-bottom: 20px; 
            }}
        </style>
    </head>
    <body>
        <h2>🚀 Daily DevOps Job Digest - {datetime.now().strftime('%B %d, %Y')}</h2>
        
        <div class="summary">
            <strong>Found {len(jobs)} new job opportunities!</strong><br>
            <small>Roles: DevOps Engineer, SRE, Junior DevOps Engineer | Locations: Major Indian cities + Remote</small>
        </div>
        
        {html_table}
        
        <br>
        <p><small>
            This digest was automatically generated by your DevOps Job Aggregator.<br>
            Jobs sourced from: LinkedIn, Naukri, Indeed, Wellfound, Hirist, Cutshort, Foundit, and company career pages.
        </small></p>
    </body>
    </html>
    """
    
    return html_content

def main():
    """Main execution function"""
    logger.info("Starting DevOps Job Aggregator...")
    
    try:
        # Load configuration
        config = load_config()
        
        # Collect jobs from all platforms  
        all_jobs = collect_all_jobs()
        
        # Process and deduplicate
        new_jobs = process_and_dedupe_jobs(all_jobs)
        
        # Generate HTML report
        html_report = create_html_report(new_jobs)
        
        # Send email or save to file
        if config.get('DRY_RUN', 'true').lower() == 'true':
            # Dry run - save to file
            with open('last_run.html', 'w', encoding='utf-8') as f:
                f.write(html_report)
            logger.info("Dry run completed. Report saved to 'last_run.html'")
        else:
            # Send email
            subject = f"DevOps Job Digest - {datetime.now().strftime('%Y-%m-%d')} - {len(new_jobs)} New Jobs"
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