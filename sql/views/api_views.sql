-- Trending skills (last 30 days)
CREATE OR REPLACE VIEW v_trending_skills AS
SELECT
    ds.skill_name,
    ds.job_count,
    ds.unique_jobs,
    ds.last_seen,
    RANK() OVER (ORDER BY ds.job_count DESC) as rank
FROM dim_skills ds
WHERE ds.last_seen >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY ds.job_count DESC;

-- Jobs with skills (denormalized for API)
CREATE OR REPLACE VIEW v_jobs_with_skills AS
SELECT
    j.job_id,
    j.title,
    j.company,
    j.district,
    j.job_url,
    j.scraped_at,
    ARRAY_AGG(js.skill_name) FILTER (WHERE js.skill_name IS NOT NULL) as skills
FROM jobs j
LEFT JOIN job_skills js ON j.job_id = js.job_id
GROUP BY j.job_id, j.title, j.company, j.district, j.job_url, j.scraped_at;

-- Salary by role (when we add salary parsing)
CREATE OR REPLACE VIEW v_jobs_by_district AS
SELECT
    district,
    COUNT(*) as job_count,
    COUNT(DISTINCT company_normalized) as companies,
    ARRAY_AGG(DISTINCT category) FILTER (WHERE category IS NOT NULL) as categories
FROM jobs
WHERE scraped_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY district
ORDER BY job_count DESC;

-- Skill demand over time
CREATE OR REPLACE VIEW v_skill_demand_trend AS
SELECT
    skill_name,
    week_start,
    job_count,
    LAG(job_count) OVER (PARTITION BY skill_name ORDER BY week_start) as prev_week,
    CASE
        WHEN LAG(job_count) OVER (PARTITION BY skill_name ORDER BY week_start) > 0
        THEN ROUND(
            (job_count - LAG(job_count) OVER (PARTITION BY skill_name ORDER BY week_start))::NUMERIC
            / LAG(job_count) OVER (PARTITION BY skill_name ORDER BY week_start) * 100,
            2
        )
        ELSE NULL
    END as growth_pct
FROM skill_trends
ORDER BY week_start DESC, job_count DESC;