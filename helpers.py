"""
Helper functions for configuration, email, extraction, and deduplication
"""

import os
import json
import smtplib
import logging
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Dict, List, Tuple, Any, Set
import re
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Technical skills to match in job descriptions
TECHNICAL_SKILLS = [
    "AWS", "GCP", "AZURE", "GOOGLE CLOUD", "AMAZON WEB SERVICES",
    "DOCKER", "KUBERNETES", "K8S", "TERRAFORM", "ANSIBLE", "PUPPET", "CHEF",
    "JENKINS", "GITLAB CI", "GITHUB ACTIONS", "CIRCLECI", "TRAVIS CI", "CI/CD",
    "PROMETHEUS", "GRAFANA", "DATADOG", "NEW RELIC", "CLOUDWATCH", "ELK STACK",
    "PYTHON", "BASH", "GO", "GOLANG", "RUBY", "PERL", "POWERSHELL",
    "LINUX", "UBUNTU", "CENTOS", "RHEL", "WINDOWS SERVER",
    "NGINX", "APACHE", "HAPROXY", "LOAD BALANCER",
    "MYSQL", "POSTGRESQL", "MONGODB", "REDIS", "ELASTICSEARCH",
    "HELM", "ISTIO", "LINKERD", "VAULT", "CONSUL",
    "GIT", "SVN", "JIRA", "CONFLUENCE"
]

# Common stop words to exclude from keywords
STOP_WORDS = {
    'the', 'and', 'for', 'are', 'with', 'you', 'this', 'that', 'will', 'have',
    'been', 'from', 'they', 'know', 'want', 'been', 'good', 'much', 'some',
    'time', 'very', 'when', 'come', 'here', 'how', 'just', 'like', 'long',
    'make', 'many', 'over', 'such', 'take', 'than', 'them', 'well', 'were',
    'work', 'year', 'years', 'job', 'role', 'position', 'company', 'team'
}

def load_config() -> Dict[str, Any]:
    """Load configuration from environment variables"""
    load_dotenv()
    
    return {
        'EMAIL_HOST': os.getenv('EMAIL_HOST', 'smtp.gmail.com'),
        'EMAIL_PORT': int(os.getenv('EMAIL_PORT', '587')),
        'EMAIL_USER': os.getenv('EMAIL_USER'),
        'EMAIL_PASS': os.getenv('EMAIL_PASS'),
        'RECIPIENT_EMAIL': os.getenv('RECIPIENT_EMAIL'),
        'COMPANY_CAREER_PAGES': os.getenv('COMPANY_CAREER_PAGES', ''),
        'USE_PLAYWRIGHT': os.getenv('USE_PLAYWRIGHT', 'false').lower() == 'true',
        'DRY_RUN': os.getenv('DRY_RUN', 'true')
    }

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_html(url: str, session: requests.Session = None, timeout: int = 15) -> requests.Response:
    """Fetch HTML with retry logic and proper headers"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    if session is None:
        session = requests.Session()
    
    response = session.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response

def fetch_with_playwright(url: str) -> str:
    """Fetch content using Playwright for JavaScript-heavy sites"""
    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            page.goto(url, wait_until='networkidle')
            content = page.content()
            browser.close()
            return content
    except ImportError:
        logger.warning("Playwright not installed, falling back to requests")
        response = fetch_html(url)
        return response.text

def send_email_html(subject: str, html_body: str, recipient: str) -> None:
    """Send HTML email via SMTP"""
    config = load_config()
    
    if not all([config['EMAIL_USER'], config['EMAIL_PASS'], recipient]):
        raise ValueError("Email configuration incomplete")
    
    msg = MIMEMultipart('alternative')
    msg['From'] = config['EMAIL_USER']
    msg['To'] = recipient
    msg['Subject'] = subject
    msg['Date'] = formatdate(localtime=True)
    
    html_part = MIMEText(html_body, 'html', 'utf-8')
    msg.attach(html_part)
    
    try:
        with smtplib.SMTP(config['EMAIL_HOST'], config['EMAIL_PORT']) as server:
            server.starttls()
            server.login(config['EMAIL_USER'], config['EMAIL_PASS'])
            server.send_message(msg)
        logger.info(f"Email sent successfully to {recipient}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise

def extract_keywords_and_skills(job_description: str) -> Tuple[List[str], List[str]]:
    """Extract top keywords and technical skills from job description"""
    if not job_description:
        return ([], [])
    
    # Clean and tokenize text
    text = job_description.lower()
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text)
    
    # Count word frequency excluding stop words
    word_counts = Counter(word for word in words if word not in STOP_WORDS)
    
    # Get top keywords
    keywords = [word.title() for word, _ in word_counts.most_common(15)][:10]
    
    # Find technical skills
    text_upper = job_description.upper()
    found_skills = []
    
    for skill in TECHNICAL_SKILLS:
        if skill in text_upper and skill not in found_skills:
            found_skills.append(skill)
            if len(found_skills) >= 10:
                break
    
    return keywords, found_skills

def load_seen_jobs() -> Set[str]:
    """Load previously seen job IDs from JSON file"""
    try:
        with open('seen_jobs.json', 'r') as f:
            data = json.load(f)
            return set(data.get('seen_jobs', []))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def save_seen_jobs(seen_jobs: Set[str]) -> None:
    """Save seen job IDs to JSON file"""
    try:
        data = {'seen_jobs': list(seen_jobs)[-1000:]}  # Keep only last 1000 to prevent file bloat
        with open('seen_jobs.json', 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save seen jobs: {e}")

def is_job_seen(seen_jobs: Set[str], job_id: str) -> bool:
    """Check if job has been seen before"""
    return job_id in seen_jobs

def add_job_to_seen(seen_jobs: Set[str], job_id: str) -> None:
    """Add job ID to seen set"""
    seen_jobs.add(job_id)