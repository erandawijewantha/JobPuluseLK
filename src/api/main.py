from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routers import jobs, skills
from src.config import settings

app = FastAPI(
    title="JobsPulse LK API",
    description="Sri Lanka Job Market Intelligence API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(jobs.router)
app.include_router(skills.router)

@app.get("/")
def root():
    return {
        "name": "JobsPulse LK API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.API_HOST, port=settings.API_PORT)