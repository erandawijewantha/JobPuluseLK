from loguru import logger
import sys
from src.config import settings

# Remove default handler
logger.remove()

# Add console handler
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level="INFO"
)

# Add file handler
logger.add(
    (settings.BASE_DIR / "logs" / "jobspulselk_{time:YYYY-MM-DD}.log").mkdir(exist_ok=True),
    rotation="1 day",
    retention="7 days",
    level="DEBUG"
)

def get_logger(name: str):
    return logger.bind(name=name)