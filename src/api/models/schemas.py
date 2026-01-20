from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class JobBase(BaseModel):
    job_id: str
    title: str
    company: Optional[str]
    district: Optional[str]
    job_url: Optional[str]
    scraped_at: Optional[datetime]

class JobWithSkills(JobBase):
    skills: List[str] = []

    class Config:
        from_attributes = True

class SkillTrending(BaseModel):
    skill_name: str
    job_count: int
    rank: int

class SkillDemandTrend(BaseModel):
    skill_name: str
    week_start: datetime
    job_count: int
    growth_pct: Optional[float]

class DistrictStats(BaseModel):
    district: str
    job_count: int
    companies: int

class PaginatedResponse(BaseModel):
    items: List
    total: int
    page: int
    per_page: int
    pages: int