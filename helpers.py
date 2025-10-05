"""
Helper functions for configuration, email, extraction, and deduplication
UPDATED: Improved keyword and skill extraction
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

# EXPANDED Technical skills to match in job descriptions
TECHNICAL_SKILLS = [
    # Cloud Platforms
    "AWS", "AMAZON WEB SERVICES", "EC2", "S3", "LAMBDA", "ECS", "EKS",
    "AZURE", "MICROSOFT AZURE", "AZURE DEVOPS",
    "GCP", "GOOGLE CLOUD", "GOOGLE CLOUD PLATFORM", "GKE",
    
    # Containerization & Orchestration
    "DOCKER", "CONTAINERS", "CONTAINERIZATION",
    "KUBERNETES", "K8S", "HELM", "KUBECTL",
    "OPENSHIFT", "RANCHER",
    
    # CI/CD Tools
    "JENKINS", "JENKINS PIPELINE",
    "GITLAB", "GITLAB CI", "GITLAB CI/CD",
    "GITHUB ACTIONS", "GITHUB WORKFLOWS",
    "CIRCLECI", "CIRCLE CI",
    "TRAVIS CI", "TRAVIS",
    "BAMBOO", "TEAMCITY",
    "AZURE PIPELINES", "AZURE DEVOPS",
    "CI/CD", "CONTINUOUS INTEGRATION", "CONTINUOUS DEPLOYMENT",
    
    # Infrastructure as Code
    "TERRAFORM", "TERRAGRUNT",
    "ANSIBLE", "ANSIBLE PLAYBOOKS",
    "PUPPET", "CHEF", "SALTSTACK",
    "CLOUDFORMATION", "ARM TEMPLATES",
    
    # Monitoring & Logging
    "PROMETHEUS", "GRAFANA",
    "DATADOG", "NEW RELIC",
    "CLOUDWATCH", "STACKDRIVER",
    "ELK", "ELK STACK", "ELASTICSEARCH", "LOGSTASH", "KIBANA",
    "SPLUNK", "NAGIOS", "ZABBIX",
    
    # Programming & Scripting
    "PYTHON", "BASH", "SHELL SCRIPTING",
    "GO", "GOLANG",
    "RUBY", "PERL", "POWERSHELL",
    "JAVASCRIPT", "NODE.JS", "NODEJS",
    
    # Operating Systems
    "LINUX", "UNIX",
    "UBUNTU", "CENTOS", "RHEL", "RED HAT",
    "DEBIAN", "FEDORA", "AMAZON LINUX",
    "WINDOWS SERVER",
    
    # Web Servers & Load Balancers
    "NGINX", "APACHE", "APACHE HTTPD",
    "HAPROXY", "LOAD BALANCER", "ALB", "ELB",
    "TRAEFIK", "ENVOY",
    
    # Databases
    "MYSQL", "MARIADB",
    "POSTGRESQL", "POSTGRES",
    "MONGODB", "MONGO",
    "REDIS", "MEMCACHED",
    "CASSANDRA", "DYNAMODB",
    
    # Service Mesh & Networking
    "ISTIO", "LINKERD", "CONSUL",
    "VAULT", "HASHICORP VAULT",
    "NETWORKING", "DNS", "VPC", "SECURITY GROUPS",
    
    # Version Control & Collaboration
    "GIT", "GITHUB", "GITLAB", "BITBUCKET",
    "SVN", "SUBVERSION",
    "JIRA", "CONFLUENCE", "SLACK",
    
    # Testing & Quality
    "TESTING", "UNIT TESTING", "INTEGRATION TESTING",
    "SELENIUM", "JUNIT", "PYTEST",
    
    # Agile & Methodologies
    "AGILE", "SCRUM", "KANBAN", "DEVOPS"
]

# Common stop words to exclude from keywords
STOP_WORDS = {
    'the', 'and', 'for', 'are', 'with', 'you', 'this', 'that', 'will', 'have',
    'been', 'from', 'they', 'know', 'want', 'been', 'good', 'much', 'some',
    'time', 'very', 'when', 'come', 'here', 'how', 'just', 'like', 'long',
    'make', 'many', 'over', 'such', 'take', 'than', 'them', 'well', 'were',
    'work', 'year', 'years', 'job', 'role', 'position', 'company', 'team',
    'our', 'your', 'their', 'has', 'had', 'can', 'may', 'also', 'should',
    'would', 'could', 'not', 'but', 'what', 'which', 'who', 'where', 'why',
    'all', 'each', 'both', 'few', 'more', 'most', 'other', 'some', 'such',
    'only', 'own', 'same', 'than', 'too', 'very', 'can', 'will', 'just',
    'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
    'between', 'under', 'again', 'further', 'then', 'once'
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
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
            page.goto(url, wait_until='networkidle', timeout=30000)
            content = page.content()
            browser.close()
            return content
    except ImportError:
        logger.warning("Playwright not installed, falling back to requests")
        response = fetch_html(url)
        return response.text
    except Exception as e:
        logger.error(f"Playwright error: {e}")
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
    """
    Extract top keywords and technical skills from job description
    IMPROVED: Better extraction with proper filtering
    """
    if not job_description or len(job_description.strip()) < 10:
        # Return defaults if no JD
        return (
            ['DevOps', 'Cloud', 'Linux', 'Automation', 'CI/CD', 'Docker', 'Kubernetes', 'AWS', 'Git', 'Monitoring'],
            ['DOCKER', 'KUBERNETES', 'AWS', 'CI/CD', 'JENKINS', 'GIT', 'LINUX', 'PYTHON', 'TERRAFORM', 'ANSIBLE']
        )
    
    # Clean text
    text = job_description.lower()
    text_upper = job_description.upper()
    
    # === EXTRACT KEYWORDS ===
    # Tokenize and clean
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text)
    
    # Count word frequency excluding stop words
    word_counts = Counter(word for word in words if word not in STOP_WORDS)
    
    # Get top keywords (more than before)
    top_words = [word for word, _ in word_counts.most_common(50)]
    
    # Filter out generic tech words that aren't meaningful
    generic_tech = {'system', 'server', 'software', 'application', 'service', 'platform', 
                   'development', 'engineering', 'technology', 'tools', 'environment'}
    
    keywords = []
    for word in top_words:
        if word not in generic_tech:
            # Capitalize properly
            if word in ['devops', 'sre', 'aws', 'gcp', 'api', 'ci', 'cd']:
                keywords.append(word.upper())
            else:
                keywords.append(word.capitalize())
            
            if len(keywords) >= 10:
                break
    
    # Ensure we have at least some keywords
    if len(keywords) < 5:
        default_keywords = ['DevOps', 'Cloud', 'Automation', 'Infrastructure', 'Deployment', 
                          'Configuration', 'Monitoring', 'Scripting', 'Linux', 'Networking']
        keywords.extend(default_keywords[:10-len(keywords)])
    
    # === EXTRACT TECHNICAL SKILLS ===
    found_skills = []
    
    # Search for each technical skill
    for skill in TECHNICAL_SKILLS:
        # Check if skill is in the text
        if skill in text_upper:
            # Avoid duplicates (e.g., AWS and AMAZON WEB SERVICES)
            skill_base = skill.split()[0]  # Get first word
            
            # Check if we already have a similar skill
            already_added = False
            for existing_skill in found_skills:
                if skill_base in existing_skill or existing_skill in skill_base:
                    already_added = True
                    break
            
            if not already_added:
                found_skills.append(skill)
                
                if len(found_skills) >= 10:
                    break
    
    # If we found very few skills, add some defaults based on keywords
    if len(found_skills) < 3:
        default_skills = ['DOCKER', 'KUBERNETES', 'CI/CD', 'GIT', 'LINUX', 
                         'AWS', 'JENKINS', 'PYTHON', 'ANSIBLE', 'TERRAFORM']
        
        for skill in default_skills:
            if skill not in found_skills:
                found_skills.append(skill)
                if len(found_skills) >= 10:
                    break
    
    return keywords[:10], found_skills[:10]

def load_seen_jobs() -> Set[str]:
    """Load previously seen job IDs from JSON file"""
    try:
        if os.path.exists('seen_jobs.json'):
            with open('seen_jobs.json', 'r') as f:
                data = json.load(f)
                return set(data.get('seen_jobs', []))
        else:
            return set()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load seen jobs: {e}")
        return set()

def save_seen_jobs(seen_jobs: Set[str]) -> None:
    """Save seen job IDs to JSON file"""
    try:
        # Keep only last 1000 to prevent file bloat
        jobs_to_save = list(seen_jobs)[-1000:]
        data = {'seen_jobs': jobs_to_save}
        
        with open('seen_jobs.json', 'w') as f:
            json.dump(data, f, indent=2)
            
        logger.debug(f"Saved {len(jobs_to_save)} job IDs to seen_jobs.json")
    except Exception as e:
        logger.error(f"Failed to save seen jobs: {e}")

def is_job_seen(seen_jobs: Set[str], job_id: str) -> bool:
    """Check if job has been seen before"""
    return job_id in seen_jobs

def add_job_to_seen(seen_jobs: Set[str], job_id: str) -> None:
    """Add job ID to seen set"""
    seen_jobs.add(job_id)