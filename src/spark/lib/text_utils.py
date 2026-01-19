# src/spark/lib/text_utils.py
import re
from typing import Optional

def normalize_title(title: str) -> Optional[str]:
    """Normalize job title for matching."""
    if not title:
        return None

    title = title.lower().strip()

    # Remove common suffixes
    title = re.sub(r'\s*[-–]\s*(urgent|immediate|asap).*$', '', title, flags=re.IGNORECASE)

    # Normalize common variations
    replacements = {
        r'\bsr\.?\b': 'senior',
        r'\bjr\.?\b': 'junior',
        r'\bmgr\.?\b': 'manager',
        r'\beng\.?\b': 'engineer',
        r'\bdev\.?\b': 'developer',
        r'\bsw\b': 'software',
        r'\bqa\b': 'quality assurance',
    }

    for pattern, replacement in replacements.items():
        title = re.sub(pattern, replacement, title)

    return title.strip()

def normalize_company(company: str) -> Optional[str]:
    """Normalize company name."""
    if not company:
        return None

    company = company.lower().strip()

    # Remove common suffixes
    suffixes = [
        r'\s*\(pvt\)\s*ltd\.?',
        r'\s*pvt\.?\s*ltd\.?',
        r'\s*private\s*limited',
        r'\s*plc\.?',
        r'\s*inc\.?',
        r'\s*limited',
        r'\s*ltd\.?',
    ]

    for suffix in suffixes:
        company = re.sub(suffix, '', company, flags=re.IGNORECASE)

    return company.strip()

def extract_district(location: str) -> Optional[str]:
    """Extract Sri Lankan district from location string."""
    if not location:
        return None

    location_lower = location.lower()

    districts = {
        'colombo': 'Colombo',
        'gampaha': 'Gampaha',
        'kandy': 'Kandy',
        'galle': 'Galle',
        'jaffna': 'Jaffna',
        'kurunegala': 'Kurunegala',
        'batticaloa': 'Batticaloa',
        'anuradhapura': 'Anuradhapura',
        'ratnapura': 'Ratnapura',
        'badulla': 'Badulla',
        'matara': 'Matara',
        'trincomalee': 'Trincomalee',
        'kalutara': 'Kalutara',
        'negombo': 'Gampaha',  # Map to district
        'wattala': 'Gampaha',
        'nugegoda': 'Colombo',
        'dehiwala': 'Colombo',
        'mount lavinia': 'Colombo',
    }

    for keyword, district in districts.items():
        if keyword in location_lower:
            return district

    return 'Unknown'