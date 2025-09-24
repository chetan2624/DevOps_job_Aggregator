# DevOps Job Aggregator

A comprehensive job scraping and aggregation system that searches multiple platforms for DevOps, SRE, and related roles in India, compiles results into an HTML email digest, and sends daily notifications.

## 🎯 Features

- **Multi-platform scraping**: LinkedIn, Naukri, Indeed, Wellfound, Hirist, Cutshort, Foundit, and direct company career pages
- **Smart deduplication**: Tracks previously seen jobs to avoid duplicates
- **Keyword & skill extraction**: Identifies top 10 keywords and technical skills from job descriptions
- **HTML email reports**: Professional table format with clickable apply links
- **Location filtering**: Focus on major Indian tech hubs (Bengaluru, Hyderabad, Pune, NCR, etc.)
- **Configurable**: Easy setup via environment variables

## 🚀 Quick Setup

### 1. Clone and Install
```bash
git clone <your-repo-url>
cd devops-job-aggregator
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt