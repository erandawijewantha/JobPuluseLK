from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from src.api.models.database import get_db
from src.api.models.schemas import SkillTrending, SkillDemandTrend

router = APIRouter(prefix="/api/v1/skills", tags=["skills"])

@router.get("/trending", response_model=List[SkillTrending])
def get_trending_skills(
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Get trending skills by job count."""
    sql = """
        SELECT skill_name, job_count, rank
        FROM v_trending_skills
        LIMIT :limit
    """
    result = db.execute(text(sql), {"limit": limit})
    return [dict(row._mapping) for row in result]

@router.get("/{skill}/trend", response_model=List[SkillDemandTrend])
def get_skill_trend(skill: str, db: Session = Depends(get_db)):
    """Get demand trend for a specific skill."""
    sql = """
        SELECT skill_name, week_start, job_count, growth_pct
        FROM v_skill_demand_trend
        WHERE skill_name = :skill
        ORDER BY week_start DESC
        LIMIT 12
    """
    result = db.execute(text(sql), {"skill": skill})
    return [dict(row._mapping) for row in result]

@router.get("/{skill}/jobs")
def get_jobs_by_skill(
    skill: str,
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Get jobs requiring a specific skill."""
    sql = """
        SELECT j.job_id, j.title, j.company, j.district, j.job_url, j.scraped_at
        FROM jobs j
        JOIN job_skills js ON j.job_id = js.job_id
        WHERE js.skill_name = :skill
        ORDER BY j.scraped_at DESC
        LIMIT :limit
    """
    result = db.execute(text(sql), {"skill": skill, "limit": limit})
    return [dict(row._mapping) for row in result]