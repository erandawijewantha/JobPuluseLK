from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from src.api.models.database import get_db
from src.api.models.schemas import JobWithSkills, PaginatedResponse

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])

@router.get("", response_model=PaginatedResponse)
def list_jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    district: Optional[str] = None,
    skill: Optional[str] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List jobs with filtering and pagination."""
    offset = (page - 1) * per_page

    # Build query
    where_clauses = []
    params = {}

    if district:
        where_clauses.append("j.district = :district")
        params["district"] = district

    if skill:
        where_clauses.append("js.skill_name = :skill")
        params["skill"] = skill

    if search:
        where_clauses.append("j.title ILIKE :search")
        params["search"] = f"%{search}%"

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Count query
    count_sql = f"""
        SELECT COUNT(DISTINCT j.job_id)
        FROM jobs j
        LEFT JOIN job_skills js ON j.job_id = js.job_id
        WHERE {where_sql}
    """
    total = db.execute(text(count_sql), params).scalar()

    # Data query
    data_sql = f"""
        SELECT j.job_id, j.title, j.company, j.district, j.job_url, j.scraped_at,
               ARRAY_AGG(js.skill_name) FILTER (WHERE js.skill_name IS NOT NULL) as skills
        FROM jobs j
        LEFT JOIN job_skills js ON j.job_id = js.job_id
        WHERE {where_sql}
        GROUP BY j.job_id
        ORDER BY j.scraped_at DESC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = per_page
    params["offset"] = offset

    result = db.execute(text(data_sql), params)
    jobs = [dict(row._mapping) for row in result]

    return PaginatedResponse(
        items=jobs,
        total=total,
        page=page,
        per_page=per_page,
        pages=(total + per_page - 1) // per_page
    )

@router.get("/{job_id}", response_model=JobWithSkills)
def get_job(job_id: str, db: Session = Depends(get_db)):
    """Get single job by ID."""
    sql = """
        SELECT j.*, ARRAY_AGG(js.skill_name) FILTER (WHERE js.skill_name IS NOT NULL) as skills
        FROM jobs j
        LEFT JOIN job_skills js ON j.job_id = js.job_id
        WHERE j.job_id = :job_id
        GROUP BY j.job_id
    """
    result = db.execute(text(sql), {"job_id": job_id}).first()

    if not result:
        raise HTTPException(status_code=404, detail="Job not found")

    return dict(result._mapping)