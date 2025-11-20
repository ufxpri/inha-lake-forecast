"""
Minimal FastAPI application for Ingyeongho Beauty Score Service.
Only handles image upload functionality.
Main page and weather components are served by Nginx as static files.
"""
from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import os
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from database import init_db, close_db, get_pool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Ingyeongho Image Upload Service",
    description="Image upload service for Ingyeongho Beauty Score",
    version="1.0.0"
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for uploads only
os.makedirs("app/static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup."""
    await init_db()


@app.on_event("shutdown") 
async def shutdown_event():
    """Close database connection on shutdown."""
    await close_db()


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


# Only image upload endpoint - all other functionality handled by Airflow + Nginx


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
            from image_processing import classify_image, extract_exif_metadata
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
        
        # No need to get beauty score - handled by Airflow
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
