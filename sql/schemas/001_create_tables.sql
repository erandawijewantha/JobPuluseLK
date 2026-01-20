-- Jobs table (main fact)
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    source_job_id VARCHAR(100) NOT NULL,
    title VARCHAR(500),
    title_normalized VARCHAR(500),
    company VARCHAR(500),
    company_normalized VARCHAR(500),
    location VARCHAR(255),
    district VARCHAR(100),
    job_url TEXT,
    category VARCHAR(100),
    scraped_at TIMESTAMP,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Skills dimension
CREATE TABLE IF NOT EXISTS dim_skills (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) UNIQUE NOT NULL,
    job_count INTEGER DEFAULT 0,
    unique_jobs INTEGER DEFAULT 0,
    last_seen TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job-Skills bridge table
CREATE TABLE IF NOT EXISTS job_skills (
    job_id VARCHAR(64) REFERENCES jobs(job_id),
    skill_name VARCHAR(100),
    PRIMARY KEY (job_id, skill_name)
);

-- Companies dimension
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id SERIAL PRIMARY KEY,
    company_normalized VARCHAR(500) UNIQUE NOT NULL,
    total_jobs INTEGER DEFAULT 0,
    unique_roles INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily aggregates
CREATE TABLE IF NOT EXISTS fact_daily_jobs (
    id SERIAL PRIMARY KEY,
    post_date DATE NOT NULL,
    source VARCHAR(50),
    district VARCHAR(100),
    category VARCHAR(100),
    job_count INTEGER,
    unique_companies INTEGER,
    UNIQUE(post_date, source, district, category)
);


-- Skill trends (weekly)
CREATE TABLE IF NOT EXISTS skill_trends (
    id SERIAL PRIMARY KEY,
    week_start DATE NOT NULL,
    skill_name VARCHAR(100) NOT NULL,
    job_count INTEGER,
    UNIQUE(week_start, skill_name)
);

-- Indexes for query performance
CREATE INDEX idx_jobs_source ON jobs(source);
CREATE INDEX idx_jobs_district ON jobs(district);
CREATE INDEX idx_jobs_scraped_at ON jobs(scraped_at);
CREATE INDEX idx_jobs_title_normalized ON jobs(title_normalized);
CREATE INDEX idx_job_skills_skill ON job_skills(skill_name);
CREATE INDEX idx_fact_daily_date ON fact_daily_jobs(post_date);
CREATE INDEX idx_skill_trends_week ON skill_trends(week_start);