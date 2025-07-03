from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import asyncio
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from news_parser.spiders.graininfo import GraininfoSpider
from news_parser.spiders.pnp import PnpSpider
from news_parser.spiders.forbes import ForbesSpider
import logging
import json
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="News Parser API",
    description="API for parsing news from various sources",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SpiderResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None

class SpiderStatus(BaseModel):
    status: str
    progress: Optional[float] = None
    items_count: Optional[int] = None
    error: Optional[str] = None

# Store for spider tasks
spider_tasks = {}

def run_spider(spider_name: str, task_id: str):
    """Run spider in a separate process"""
    try:
        settings = get_project_settings()
        process = CrawlerProcess(settings)
        
        # Select spider based on name
        if spider_name == "graininfo":
            process.crawl(GraininfoSpider)
        elif spider_name == "pnp":
            process.crawl(PnpSpider)
        elif spider_name == "forbes":
            process.crawl(ForbesSpider)
        else:
            raise ValueError(f"Unknown spider: {spider_name}")
            
        process.start()
        
        # Update task status
        spider_tasks[task_id]["status"] = "finished"
        spider_tasks[task_id]["progress"] = 100.0
        
        # Read results from output file
        output_file = f"output/{spider_name}_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.json"
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                items = json.load(f)
                spider_tasks[task_id]["items_count"] = len(items)
        
    except Exception as e:
        logger.error(f"Error running spider {spider_name}: {str(e)}")
        spider_tasks[task_id]["status"] = "failed"
        spider_tasks[task_id]["error"] = str(e)

@app.post("/api/v1/spiders/{spider_name}/start", response_model=SpiderResponse)
async def start_spider(spider_name: str, background_tasks: BackgroundTasks):
    """Start a spider crawl"""
    if spider_name not in ["graininfo", "pnp", "forbes"]:
        raise HTTPException(status_code=400, detail="Invalid spider name")
    
    task_id = f"{spider_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Initialize task status
    spider_tasks[task_id] = {
        "status": "running",
        "progress": 0.0,
        "items_count": 0,
        "error": None
    }
    
    # Run spider in background
    background_tasks.add_task(run_spider, spider_name, task_id)
    
    return SpiderResponse(
        status="started",
        message=f"Spider {spider_name} started",
        task_id=task_id
    )

@app.get("/api/v1/spiders/{task_id}/status", response_model=SpiderStatus)
async def get_spider_status(task_id: str):
    """Get status of a spider task"""
    if task_id not in spider_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = spider_tasks[task_id]
    return SpiderStatus(
        status=task["status"],
        progress=task["progress"],
        items_count=task["items_count"],
        error=task["error"]
    )

@app.get("/api/v1/spiders", response_model=List[str])
async def list_spiders():
    """List available spiders"""
    return ["graininfo", "pnp", "forbes"]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 