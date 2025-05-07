"""
Utility functions for the distributed web crawling system.
"""
from urllib.parse import urlparse, urlunparse
import hashlib
import re
from urllib.robotparser import RobotFileParser


def get_domain(url):
    """Extract the domain from a URL."""
    parsed = urlparse(url)
    return parsed.netloc

def normalize_url(url):
    """Normalize a URL by removing fragments and trailing slashes."""
    if not url:
        return None
        
    # Add scheme if missing
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
    
    
    parsed = urlparse(url)
    
    # Remove fragment
    parsed = parsed._replace(fragment='')
    
    # Normalize path (remove trailing slash)
    path = parsed.path
    if path.endswith('/') and len(path) > 1:
        path = path[:-1]
    parsed = parsed._replace(path=path)
    
    return urlunparse(parsed)

def url_to_filename(url):
    """Convert a URL to a valid filename."""
    # Create a hash of the URL to ensure uniqueness and valid filename
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return f"{url_hash}.html"

def extract_text_from_html(html_content):
    """Extract plain text from HTML content."""
    # Simple regex-based text extraction (for a more robust solution, use BeautifulSoup)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def is_allowed_by_robots(url, user_agent):
    """Check if crawling a URL is allowed by robots.txt."""
    try:
        domain = get_domain(url)
        robots_url = f"http://{domain}/robots.txt"
        
        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.read()
        
        return parser.can_fetch(user_agent, url)
    except Exception:
        # If there's an error accessing robots.txt, assume it's allowed
        return True