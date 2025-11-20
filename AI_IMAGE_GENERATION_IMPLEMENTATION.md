# AI Background Image Generation Implementation

## Overview
Implemented complete AI background image generation system for the Ingyeongho Beauty Score service. The system automatically generates weather-appropriate background images using AI and updates the web interface in real-time.

## Components Implemented

### 1. AI Image Generation DAG (`airflow/dags/ai_image_dag.py`)

**Schedule**: Every 3 hours (fallback) + triggered by Weather ETL DAG on significant weather changes

**Tasks**:
- `generate_prompt`: Creates weather-appropriate AI prompts based on:
  - Current weather conditions (clear, cloudy, rain, snow, etc.)
  - Season (spring: cherry blossoms, summer: lush greenery, autumn: colorful leaves, winter: snow)
  - Time of day (morning, afternoon, evening, night with appropriate lighting)
  
- `generate_image`: Calls AI image generation API:
  - Supports OpenAI DALL-E 3
  - Supports Stable Diffusion XL
  - Mock mode for development/testing
  
- `save_and_cache_image`: Handles image storage:
  - Downloads and saves images to `app/static/generated/` directory
  - Stores metadata in PostgreSQL `ai_generated_images` table
  - Updates Redis cache with latest image path (key: `latest_bg_image`)

### 2. Weather ETL DAG Updates (`airflow/dags/weather_etl_dag.py`)

**New Tasks**:
- `check_weather_change`: Detects significant weather changes:
  - Weather condition changes (e.g., clear → rain)
  - Temperature changes > 5°C
  - AQI changes > 50 points
  
- `trigger_ai_image_generation`: Triggers AI Image DAG when weather changes significantly

**Task Dependencies**:
```
[fetch_weather, fetch_air_quality] → calculate_score → check_weather_change → trigger_ai_image_generation
```

### 3. Web Interface Updates

#### FastAPI Endpoint (`app/main.py`)
- **GET /bg-image**: Returns background image container HTML
  - Polls every 60 seconds via HTMX
  - Includes smooth transition CSS classes
  - Falls back to gradient if no image available

#### Frontend (`app/templates/index.html`)
- HTMX polling configured on `#bg-image-container`
- JavaScript event handlers for smooth transitions:
  - Fade out old image
  - Fade in new image
  - Logging for debugging

#### Styling (`app/templates/base.html`)
- CSS transitions for background image changes (1s ease-in-out)
- Fade-in animation for new images
- Proper opacity handling (0.3 for background visibility)

### 4. Database Schema (`app/schema.sql`)

**New Table**: `ai_generated_images`
```sql
- id (SERIAL PRIMARY KEY)
- file_path (VARCHAR 255) - Web path to image
- prompt (TEXT) - AI generation prompt
- season (VARCHAR 20) - Spring, summer, autumn, winter
- time_of_day (VARCHAR 20) - Morning, afternoon, evening, night
- weather_condition (VARCHAR 50) - Clear, cloudy, rain, etc.
- service (VARCHAR 50) - openai, stability, mock
- generated_at (TIMESTAMP) - Generation timestamp
- metadata (JSONB) - Additional metadata
```

**Indexes**:
- `idx_ai_images_generated_at` - Time-based queries
- `idx_ai_images_weather` - Weather-based queries
- `idx_ai_images_season` - Seasonal queries

### 5. Configuration (`.env`)

**New Environment Variables**:
```bash
AI_IMAGE_SERVICE=mock  # Options: 'openai', 'stability', 'mock'
OPENAI_API_KEY=your_key_here  # For DALL-E
STABILITY_API_KEY=your_key_here  # For Stable Diffusion
```

## Features

### Automatic Generation
- Runs every 3 hours as fallback
- Triggered immediately on significant weather changes
- Generates contextually appropriate images

### Weather-Aware Prompts
- Adapts to current weather conditions
- Includes seasonal elements (cherry blossoms, autumn leaves, etc.)
- Adjusts lighting based on time of day
- Creates photorealistic Korean lake landscapes

### Smooth User Experience
- 60-second polling interval (non-intrusive)
- 1-second fade transitions between images
- Graceful fallback to gradient if no image available
- No page refresh required

### Flexible AI Service Support
- OpenAI DALL-E 3 (1792x1024, high quality)
- Stable Diffusion XL (1792x1024, 30 steps)
- Mock mode using placeholder images (for development)

## Data Flow

```
Weather ETL DAG (hourly)
  ↓
Check Weather Change
  ↓ (if significant change)
Trigger AI Image DAG
  ↓
Generate Weather Prompt
  ↓
Call AI API (DALL-E/Stable Diffusion)
  ↓
Download & Save Image
  ↓
Store Metadata in PostgreSQL
  ↓
Update Redis Cache
  ↓
Web Interface Polls /bg-image (every 60s)
  ↓
Smooth Transition to New Image
```

## Requirements Satisfied

✅ **Requirement 9.1**: AI_Image_DAG triggered by Weather_ETL_DAG on significant weather changes
✅ **Requirement 9.2**: Weather-appropriate prompts with seasonal and time-of-day variations
✅ **Requirement 9.3**: Stable Diffusion/DALL-E API integration with image generation
✅ **Requirement 9.4**: Image storage in static directory and metadata in PostgreSQL
✅ **Requirement 9.5**: 3-hour fallback schedule for automatic execution
✅ **Requirement 6.3**: HTMX auto-update every 60 seconds with smooth transitions

## Testing

### Syntax Validation
- ✅ `ai_image_dag.py` - Python syntax valid
- ✅ `weather_etl_dag.py` - Python syntax valid
- ✅ `app/main.py` - No diagnostics errors

### Manual Testing Steps
1. Start Airflow and FastAPI services
2. Verify AI Image DAG appears in Airflow UI
3. Trigger Weather ETL DAG manually
4. Check if AI Image DAG is triggered on weather change
5. Verify image appears in `app/static/generated/`
6. Check Redis cache for `latest_bg_image` key
7. Open web interface and verify background image loads
8. Wait 60 seconds and verify polling works
9. Trigger new image generation and verify smooth transition

## Production Deployment

### Before Production
1. Set `AI_IMAGE_SERVICE=openai` or `AI_IMAGE_SERVICE=stability`
2. Add valid API keys to `.env` file
3. Ensure `app/static/generated/` directory has write permissions
4. Configure Airflow email alerts for DAG failures
5. Set up monitoring for image generation success rate
6. Consider adding image size limits and cleanup for old images

### API Key Setup
```bash
# For OpenAI DALL-E
export OPENAI_API_KEY="sk-..."

# For Stability AI
export STABILITY_API_KEY="sk-..."
```

### Cost Considerations
- OpenAI DALL-E 3: ~$0.04 per image (1792x1024)
- Stable Diffusion XL: ~$0.01-0.02 per image
- Expected usage: ~8-12 images per day (3-hour schedule + weather triggers)
- Monthly cost estimate: $10-20 depending on service and weather variability

## Future Enhancements
- Image quality optimization and compression
- User voting on generated images
- A/B testing different prompts
- Seasonal prompt templates
- Image caching and CDN integration
- Fallback to user-uploaded images when AI service unavailable
