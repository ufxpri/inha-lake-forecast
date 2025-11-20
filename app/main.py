"""
FastAPI application for Ingyeongho Beauty Score Service.
Provides HTMX-powered web interface for real-time beauty score display.
"""
from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
import os
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from app.database import init_db, close_db, execute_command, get_pool
from app.cache import init_redis, close_redis, get_cached_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup resources on app startup/shutdown."""
    # Startup
    await init_db()
    await init_redis()
    yield
    # Shutdown
    await close_db()
    await close_redis()


app = FastAPI(
    title="Ingyeongho Beauty Score Service",
    description="Real-time beauty score for Ingyeongho Lake",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
os.makedirs("app/static/generated", exist_ok=True)
os.makedirs("app/static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Jinja2 templates configuration
templates = Jinja2Templates(directory="app/templates")


# Error response utilities
def create_error_response(
    error_type: str, 
    message: str, 
    details: Optional[str] = None,
    status_code: int = 500
) -> Dict[str, Any]:
    """Create consistent error response format."""
    response = {
        "success": False,
        "error": {
            "type": error_type,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
    }
    if details:
        response["error"]["details"] = details
    return response


def create_error_html(
    title: str,
    message: str,
    details: Optional[str] = None,
    retry_action: Optional[str] = None
) -> str:
    """Create user-friendly error HTML for HTMX responses."""
    details_html = f'<p class="text-xs mt-1">{details}</p>' if details else ''
    retry_html = f'<button class="mt-2 px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600" {retry_action}>다시 시도</button>' if retry_action else ''
    
    return f'''
    <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-lg">
        <p class="font-bold">❌ {title}</p>
        <p class="text-sm">{message}</p>
        {details_html}
        {retry_html}
    </div>
    '''


# Global exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with consistent error format."""
    logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail}")
    
    # Check if request expects HTML (HTMX) or JSON
    accept_header = request.headers.get("accept", "")
    hx_request = request.headers.get("hx-request")
    
    if hx_request or "text/html" in accept_header:
        # Return HTML error for HTMX requests
        error_html = create_error_html(
            title="요청 오류",
            message=str(exc.detail),
            retry_action='hx-get="/" hx-target="body"'
        )
        return HTMLResponse(content=error_html, status_code=exc.status_code)
    else:
        # Return JSON error for API requests
        error_response = create_error_response(
            error_type="http_error",
            message=str(exc.detail),
            status_code=exc.status_code
        )
        return JSONResponse(content=error_response, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors."""
    logger.error(f"Validation Error: {exc.errors()}")
    
    hx_request = request.headers.get("hx-request")
    
    if hx_request:
        error_html = create_error_html(
            title="입력 오류",
            message="요청 데이터가 올바르지 않습니다",
            details="입력 값을 확인해주세요"
        )
        return HTMLResponse(content=error_html, status_code=422)
    else:
        error_response = create_error_response(
            error_type="validation_error",
            message="Invalid request data",
            details=str(exc.errors())
        )
        return JSONResponse(content=error_response, status_code=422)


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
    
    hx_request = request.headers.get("hx-request")
    
    if hx_request:
        error_html = create_error_html(
            title="서버 오류",
            message="일시적인 오류가 발생했습니다",
            details="잠시 후 다시 시도해주세요",
            retry_action='hx-get="/" hx-target="body"'
        )
        return HTMLResponse(content=error_html, status_code=500)
    else:
        error_response = create_error_response(
            error_type="internal_error",
            message="Internal server error occurred"
        )
        return JSONResponse(content=error_response, status_code=500)


@app.get("/")
async def root(request: Request):
    """Main dashboard page."""
    try:
        return templates.TemplateResponse("index.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering main page: {e}")
        raise HTTPException(status_code=500, detail="페이지를 불러올 수 없습니다")


@app.get("/score")
async def get_score_partial(request: Request):
    """
    Return current score HTML partial from Redis.
    Implements fallback logic when Redis cache is empty.
    Requirements: 1.1, 1.2, 6.2, 7.2
    """
    try:
        from app.cache import get_cached_data
        from datetime import datetime
        
        # Try to get current score from Redis cache
        score_data = await get_cached_data("current_score")
        
        if score_data:
            # Data found in cache
            context = {
                "request": request,
                "score": score_data.get("score", 0),
                "status_message": score_data.get("status_message", "데이터 없음"),
                "weather_condition": score_data.get("weather_condition", "알 수 없음"),
                "updated_at": score_data.get("updated_at", datetime.now().isoformat())
            }
        else:
            # Fallback when Redis cache is empty
            logger.warning("No score data available in Redis cache")
            context = {
                "request": request,
                "score": "--",
                "status_message": "데이터를 불러올 수 없습니다",
                "weather_condition": "캐시된 데이터가 없습니다",
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        return templates.TemplateResponse("partials/score.html", context)
        
    except Exception as e:
        logger.error(f"Error getting score partial: {e}")
        # Return error HTML for HTMX
        return HTMLResponse(
            content=create_error_html(
                title="점수 로딩 실패",
                message="뷰티 스코어를 불러올 수 없습니다",
                details="잠시 후 다시 시도해주세요",
                retry_action='hx-get="/score" hx-target="#score-box"'
            ),
            status_code=500
        )


@app.get("/api/current-score")
async def get_current_score_json():
    """
    Return current score as JSON data.
    Implements fallback logic when Redis cache is empty.
    Requirements: 1.1, 1.2, 7.2
    """
    try:
        from app.cache import get_cached_data
        from datetime import datetime
        
        # Try to get current score from Redis cache
        score_data = await get_cached_data("current_score")
        
        if score_data:
            return {
                "success": True,
                "data": score_data
            }
        else:
            # Fallback when Redis cache is empty
            logger.warning("No score data available in Redis cache for API request")
            return {
                "success": False,
                "error": "No cached data available",
                "data": {
                    "score": None,
                    "status_message": "데이터 없음",
                    "weather_condition": "알 수 없음",
                    "updated_at": datetime.now().isoformat()
                }
            }
            
    except Exception as e:
        logger.error(f"Error getting current score JSON: {e}")
        return JSONResponse(
            content=create_error_response(
                error_type="cache_error",
                message="Failed to retrieve score data",
                details=str(e)
            ),
            status_code=500
        )


@app.get("/bg-image")
async def get_background_image():
    """
    Check for new background images and return updated container.
    Returns the background image container HTML with smooth transition effect.
    
    Requirements: 6.3
    """
    try:
        from app.cache import get_cached_data
        
        # Try to get latest background image path from Redis
        bg_data = await get_cached_data("latest_bg_image")
        
        if bg_data and bg_data.get("image_path"):
            image_path = bg_data.get("image_path")
            generated_at = bg_data.get("generated_at", "")
            
            # Return container with background image and smooth transition
            return HTMLResponse(
                content=f'''<div 
                    id="bg-image-container" 
                    class="absolute inset-0 bg-cover bg-center opacity-30 transition-all duration-1000 ease-in-out"
                    style="background-image: url('{image_path}');"
                    hx-get="/bg-image" 
                    hx-trigger="every 60s"
                    hx-swap="outerHTML"
                    hx-swap-oob="true"
                    data-generated-at="{generated_at}"
                ></div>'''
            )
        else:
            # No background image available, return empty container with gradient fallback
            logger.info("No background image available, using gradient fallback")
            return HTMLResponse(
                content='''<div 
                    id="bg-image-container" 
                    class="absolute inset-0 bg-gradient-to-br from-blue-400 to-indigo-600 opacity-30 transition-all duration-1000 ease-in-out"
                    hx-get="/bg-image" 
                    hx-trigger="every 60s"
                    hx-swap="outerHTML"
                    hx-swap-oob="true"
                ></div>'''
            )
            
    except Exception as e:
        logger.error(f"Error getting background image: {e}")
        # Return fallback gradient on error
        return HTMLResponse(
            content='''<div 
                id="bg-image-container" 
                class="absolute inset-0 bg-gradient-to-br from-gray-400 to-gray-600 opacity-30 transition-all duration-1000 ease-in-out"
                hx-get="/bg-image" 
                hx-trigger="every 60s"
                hx-swap="outerHTML"
                hx-swap-oob="true"
                title="배경 이미지 로딩 실패"
            ></div>'''
        )


@app.get("/api/hourly-forecast")
async def get_hourly_forecast(request: Request):
    """
    Serve hourly predictions with HTML partial template.
    Implements "Best" badge highlighting logic for highest score period.
    Requirements: 2.1, 2.2
    """
    try:
        from app.cache import get_cached_data
        from datetime import datetime, date
        
        # Get today's date for cache key
        today = date.today().isoformat()
        cache_key = f"hourly_forecast:{today}"
        
        # Try to get hourly forecast from Redis cache
        forecast_data = await get_cached_data(cache_key)
        
        if forecast_data and isinstance(forecast_data, list):
            # Find the highest score for "Best" badge highlighting
            max_score = max((item.get("score", 0) for item in forecast_data), default=0)
            
            # Mark the highest score period(s) with is_best flag
            # If multiple periods have the same highest score, mark the first one
            best_marked = False
            for item in forecast_data:
                if item.get("score") == max_score and not best_marked:
                    item["is_best"] = True
                    best_marked = True
                else:
                    item["is_best"] = False
            
            context = {
                "request": request,
                "forecast_data": forecast_data
            }
        else:
            # Fallback: generate sample data structure when cache is empty
            logger.warning(f"No hourly forecast data available for {today}")
            context = {
                "request": request,
                "forecast_data": None
            }
        
        return templates.TemplateResponse("partials/hourly_forecast.html", context)
        
    except Exception as e:
        logger.error(f"Error getting hourly forecast: {e}")
        # Return error HTML for HTMX
        return HTMLResponse(
            content=create_error_html(
                title="예보 로딩 실패",
                message="시간별 예보를 불러올 수 없습니다",
                details="잠시 후 다시 시도해주세요",
                retry_action='hx-get="/api/hourly-forecast" hx-target="#forecast-section"'
            ),
            status_code=500
        )


@app.get("/calendar/view")
async def get_calendar_view(year: int = None, month: int = None):
    """
    Serve pre-rendered calendar HTML from Redis cache.
    
    Requirements: 3.1, 6.5
    
    Args:
        year: Year for calendar (defaults to current year)
        month: Month for calendar (defaults to current month)
    """
    try:
        from app.cache import get_cached_html
        from datetime import date
        
        # Default to current year/month if not provided
        today = date.today()
        year = year or today.year
        month = month or today.month
        
        # Validate year and month ranges
        if not (1900 <= year <= 2100):
            raise HTTPException(status_code=400, detail="잘못된 연도입니다")
        if not (1 <= month <= 12):
            raise HTTPException(status_code=400, detail="잘못된 월입니다")
        
        # Try to get pre-rendered calendar from Redis
        cache_key = f"calendar_html:{year}-{month:02d}"
        calendar_html = await get_cached_html(cache_key)
        
        if calendar_html:
            return HTMLResponse(content=calendar_html)
        else:
            # Fallback: return placeholder if cache is empty
            logger.warning(f"No calendar data available for {year}-{month:02d}")
            return HTMLResponse(content='''
            <div class="text-center py-8">
                <p class="text-gray-600">캘린더 데이터를 생성 중입니다</p>
                <p class="text-sm text-gray-500 mt-2">잠시 후 다시 시도해주세요</p>
                <button class="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600" 
                        hx-get="/calendar/view" hx-target="#calendar-section">
                    다시 시도
                </button>
            </div>
            ''')
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting calendar view: {e}")
        return HTMLResponse(
            content=create_error_html(
                title="캘린더 로딩 실패",
                message="캘린더를 불러올 수 없습니다",
                details="잠시 후 다시 시도해주세요",
                retry_action='hx-get="/calendar/view" hx-target="#calendar-section"'
            ),
            status_code=500
        )


@app.get("/calendar/detail")
async def get_calendar_detail(request: Request, date: str):
    """
    Return date-specific information modal for calendar.
    
    Requirements: 3.1, 6.5
    
    Args:
        date: Date string in YYYY-MM-DD format
    """
    try:
        from app.cache import get_cached_data
        from datetime import datetime
        from app.utils import get_status_message, get_seasonal_icon
        
        # Parse and validate date
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return HTMLResponse(
                content=create_error_html(
                    title="날짜 오류",
                    message="잘못된 날짜 형식입니다",
                    details="YYYY-MM-DD 형식으로 입력해주세요"
                ),
                status_code=400
            )
        
        # Get calendar data from cache
        cache_key = f"calendar_data:{target_date.year}-{target_date.month:02d}"
        calendar_data = await get_cached_data(cache_key)
        
        if calendar_data and date in calendar_data:
            day_data = calendar_data[date]
            score = day_data.get("score")
            icon = day_data.get("icon")
            
            # Build modal content
            context = {
                "request": request,
                "date": target_date.strftime("%Y년 %m월 %d일"),
                "score": score,
                "status_message": get_status_message(score) if score else "데이터 없음",
                "icon": icon,
                "has_data": score is not None
            }
        else:
            # No data available for this date
            logger.warning(f"No calendar data available for date: {date}")
            context = {
                "request": request,
                "date": target_date.strftime("%Y년 %m월 %d일"),
                "score": None,
                "status_message": "데이터 없음",
                "icon": get_seasonal_icon(target_date),
                "has_data": False
            }
        
        return templates.TemplateResponse("partials/calendar_detail.html", context)
        
    except Exception as e:
        logger.error(f"Error getting calendar detail for date {date}: {e}")
        return HTMLResponse(
            content=create_error_html(
                title="상세 정보 오류",
                message="날짜 상세 정보를 불러올 수 없습니다",
                details="잠시 후 다시 시도해주세요"
            ),
            status_code=500
        )


@app.get("/api/monthly-calendar")
async def get_monthly_calendar_json(year: int = None, month: int = None):
    """
    Return monthly calendar data as JSON.
    
    Requirements: 3.1, 6.5
    
    Args:
        year: Year for calendar (defaults to current year)
        month: Month for calendar (defaults to current month)
    """
    try:
        from app.cache import get_cached_data
        from datetime import date
        
        # Default to current year/month if not provided
        today = date.today()
        year = year or today.year
        month = month or today.month
        
        # Validate year and month ranges
        if not (1900 <= year <= 2100):
            return JSONResponse(
                content=create_error_response(
                    error_type="validation_error",
                    message="Invalid year parameter",
                    details="Year must be between 1900 and 2100"
                ),
                status_code=400
            )
        if not (1 <= month <= 12):
            return JSONResponse(
                content=create_error_response(
                    error_type="validation_error",
                    message="Invalid month parameter",
                    details="Month must be between 1 and 12"
                ),
                status_code=400
            )
        
        # Get calendar data from Redis cache
        cache_key = f"calendar_data:{year}-{month:02d}"
        calendar_data = await get_cached_data(cache_key)
        
        if calendar_data:
            return {
                "success": True,
                "year": year,
                "month": month,
                "data": calendar_data
            }
        else:
            logger.warning(f"No calendar data available for {year}-{month:02d}")
            return {
                "success": False,
                "error": "Calendar data not available",
                "year": year,
                "month": month,
                "data": {}
            }
            
    except Exception as e:
        logger.error(f"Error getting monthly calendar JSON: {e}")
        return JSONResponse(
            content=create_error_response(
                error_type="cache_error",
                message="Failed to retrieve calendar data",
                details=str(e)
            ),
            status_code=500
        )


@app.get("/health")
async def health_check():
    """Health check endpoint with database and cache connectivity."""
    try:
        health_status = {"status": "healthy", "timestamp": datetime.now().isoformat()}
        
        # Check database connectivity
        try:
            pool = get_pool()
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            health_status["database"] = "connected"
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")
            health_status["database"] = "disconnected"
            health_status["status"] = "degraded"
        
        # Check Redis connectivity
        try:
            from app.cache import get_cached_data
            await get_cached_data("health_check")
            health_status["cache"] = "connected"
        except Exception as e:
            logger.warning(f"Cache health check failed: {e}")
            health_status["cache"] = "disconnected"
            health_status["status"] = "degraded"
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return JSONResponse(
            content=create_error_response(
                error_type="health_check_error",
                message="Health check failed",
                details=str(e)
            ),
            status_code=500
        )


@app.get("/upload/form")
async def get_upload_form(request: Request):
    """
    Return upload form HTML partial.
    
    Requirements: 5.1, 5.3, 5.4
    """
    try:
        return templates.TemplateResponse("partials/upload_form.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering upload form: {e}")
        return HTMLResponse(
            content=create_error_html(
                title="폼 로딩 실패",
                message="업로드 폼을 불러올 수 없습니다",
                details="잠시 후 다시 시도해주세요",
                retry_action='hx-get="/upload/form" hx-target="#upload-section"'
            ),
            status_code=500
        )


@app.post("/upload/image")
async def upload_image(request: Request, file: UploadFile = File(...)):
    """
    Handle image upload with validation and AI classification.
    
    Requirements: 5.1, 5.3, 5.4
    
    Validates:
    - File size (max 10MB)
    - File extension (jpg, jpeg, png)
    - Image content using AI classification
    
    Returns HTML partial with success/error message.
    """
    file_path = None
    try:
        # Validate file is provided
        if not file or not file.filename:
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="파일이 선택되지 않았습니다",
                    details="이미지 파일을 선택해주세요"
                ),
                status_code=400
            )
        
        # File size validation (max 10MB)
        MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB in bytes
        
        # Read file content
        try:
            file_content = await file.read()
            file_size = len(file_content)
        except Exception as e:
            logger.error(f"Error reading uploaded file: {e}")
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="파일을 읽을 수 없습니다",
                    details="파일이 손상되었거나 형식이 올바르지 않습니다"
                ),
                status_code=400
            )
        
        if file_size == 0:
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="빈 파일입니다",
                    details="유효한 이미지 파일을 선택해주세요"
                ),
                status_code=400
            )
        
        if file_size > MAX_FILE_SIZE:
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="파일 크기가 너무 큽니다 (최대 10MB)",
                    details=f"현재 파일: {file_size / (1024*1024):.2f}MB"
                ),
                status_code=400
            )
        
        # File extension validation
        allowed_extensions = {".jpg", ".jpeg", ".png"}
        file_extension = Path(file.filename).suffix.lower()
        
        if file_extension not in allowed_extensions:
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="지원하지 않는 파일 형식입니다",
                    details="허용된 형식: JPG, JPEG, PNG"
                ),
                status_code=400
            )
        
        # Generate unique filename and create directory
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        upload_dir = Path("app/static/uploads")
        
        try:
            upload_dir.mkdir(parents=True, exist_ok=True)
            file_path = upload_dir / unique_filename
        except Exception as e:
            logger.error(f"Error creating upload directory: {e}")
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="파일 저장 공간을 준비할 수 없습니다",
                    details="시스템 관리자에게 문의하세요"
                ),
                status_code=500
            )
        
        # Save file temporarily for processing
        try:
            with open(file_path, "wb") as f:
                f.write(file_content)
        except Exception as e:
            logger.error(f"Error saving uploaded file: {e}")
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="파일을 저장할 수 없습니다",
                    details="디스크 공간이 부족하거나 권한이 없습니다"
                ),
                status_code=500
            )
        
        # Import AI classification and EXIF extraction functions
        try:
            from app.image_processing import classify_image, extract_exif_metadata
        except ImportError as e:
            logger.error(f"Error importing image processing modules: {e}")
            # Clean up uploaded file
            if file_path and file_path.exists():
                file_path.unlink(missing_ok=True)
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="이미지 처리 모듈을 불러올 수 없습니다",
                    details="시스템 설정을 확인해주세요"
                ),
                status_code=500
            )
        
        # Perform AI classification
        try:
            classification_result = await classify_image(str(file_path))
        except Exception as e:
            logger.error(f"Error in AI classification: {e}")
            # Clean up uploaded file
            if file_path and file_path.exists():
                file_path.unlink(missing_ok=True)
            return HTMLResponse(
                content=create_error_html(
                    title="업로드 실패",
                    message="이미지 분석 중 오류가 발생했습니다",
                    details="잠시 후 다시 시도해주세요"
                ),
                status_code=500
            )
        
        # Check if image is approved (landscape/lake/nature content)
        is_approved = classification_result.get("is_approved", False)
        rejection_reason = classification_result.get("rejection_reason")
        
        if not is_approved:
            # Delete rejected image
            if file_path and file_path.exists():
                file_path.unlink(missing_ok=True)
            
            return HTMLResponse(
                content=f'''
                <div class="bg-yellow-100 border border-yellow-400 text-yellow-800 px-4 py-3 rounded-lg">
                    <p class="font-bold">⚠️ 업로드 거부</p>
                    <p class="text-sm">{rejection_reason or "인경호 풍경 사진이 아닙니다"}</p>
                    <p class="text-xs mt-1">인경호의 자연 풍경 사진만 업로드해주세요</p>
                    <button class="mt-2 px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600" 
                            hx-get="/upload/form" hx-target="#upload-section">
                        다시 업로드
                    </button>
                </div>
                ''',
                status_code=400
            )
        
        # Extract EXIF metadata
        try:
            exif_data = extract_exif_metadata(str(file_path))
            exif_timestamp = exif_data.get("timestamp")
        except Exception as e:
            logger.warning(f"Error extracting EXIF data: {e}")
            exif_timestamp = None
        
        # Get current beauty score from cache (with fallback)
        try:
            current_score_data = await get_cached_data("current_score")
            beauty_score_at_time = current_score_data.get("score") if current_score_data else None
        except Exception as e:
            logger.warning(f"Error getting current score for upload: {e}")
            beauty_score_at_time = None
        
        # Store in database with error handling
        try:
            pool = get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO user_uploads 
                    (file_path, original_filename, exif_timestamp, ai_classification_result, is_approved, beauty_score_at_time)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    f"/static/uploads/{unique_filename}",
                    file.filename,
                    exif_timestamp,
                    classification_result,
                    is_approved,
                    beauty_score_at_time
                )
        except Exception as e:
            logger.error(f"Error storing upload metadata in database: {e}")
            # Don't fail the upload if database write fails, but log it
            # The file is still saved and can be processed later
        
        # Success response
        exif_info = ""
        if exif_timestamp:
            exif_info = f'<p class="text-xs mt-1">📅 촬영 시간: {exif_timestamp.strftime("%Y-%m-%d %H:%M:%S")}</p>'
        
        score_info = ""
        if beauty_score_at_time is not None:
            score_info = f'<p class="text-xs">📊 업로드 시점 뷰티 스코어: {beauty_score_at_time}점</p>'
        
        return HTMLResponse(
            content=f'''
            <div class="bg-green-100 border border-green-400 text-green-800 px-4 py-3 rounded-lg">
                <p class="font-bold">✅ 업로드 성공!</p>
                <p class="text-sm">인경호 사진이 성공적으로 업로드되었습니다</p>
                {exif_info}
                {score_info}
                <p class="text-xs mt-2 text-green-700">검토 후 서비스에 활용될 예정입니다</p>
                <button class="mt-2 px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600" 
                        hx-get="/upload/form" hx-target="#upload-section">
                    다른 사진 업로드
                </button>
            </div>
            <div class="mt-4">
                <img src="/static/uploads/{unique_filename}" alt="Uploaded image" class="rounded-lg shadow-md max-h-64 mx-auto">
            </div>
            ''',
            status_code=200
        )
        
    except Exception as e:
        # Log error and clean up file if it exists
        logger.error(f"Unexpected error in upload_image: {e}", exc_info=True)
        
        if file_path and file_path.exists():
            try:
                file_path.unlink(missing_ok=True)
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up file after upload failure: {cleanup_error}")
        
        return HTMLResponse(
            content=create_error_html(
                title="업로드 실패",
                message="이미지 처리 중 예상치 못한 오류가 발생했습니다",
                details="잠시 후 다시 시도해주세요",
                retry_action='hx-get="/upload/form" hx-target="#upload-section"'
            ),
            status_code=500
        )
