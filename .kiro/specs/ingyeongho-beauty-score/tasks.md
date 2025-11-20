# Implementation Plan

- [x] 1. Set up project infrastructure and core dependencies
  - Docker Compose configuration with Airflow, PostgreSQL, and Redis is complete
  - Environment variables configured in .env file
  - Airflow directories (dags, logs, plugins, config) are set up
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [x] 2. Create FastAPI web application structure



  - [x] 2.1 Create FastAPI application directory structure and dependencies


    - Create `app/` directory with subdirectories: main.py, models/, templates/, static/
    - Create requirements.txt with FastAPI, Jinja2, Redis, psycopg2, Pillow, and transformers
    - Set up basic FastAPI app with CORS middleware, static file mounting, and Jinja2 templates
    - Create database connection utilities for PostgreSQL
    - Create Redis connection utilities for caching
    - _Requirements: 6.2, 8.4, 10.1, 10.2, 10.4_

  - [x] 2.2 Implement database schema initialization


    - Create SQL schema file for user_uploads table with proper indexes
    - Implement database initialization script to create tables on startup
    - Add connection pooling configuration for PostgreSQL
    - _Requirements: 10.1, 10.5_

  - [ ]* 2.3 Write property test for cache data freshness
    - **Property 9: Cache Data Freshness**
    - **Validates: Requirements 10.4**

- [x] 3. Implement core web interface with HTMX








  - [x] 3.1 Create base HTML templates with HTMX and Tailwind CSS

    - Create base.html template with HTMX library (via CDN) and Tailwind CSS (via CDN)
    - Create index.html with hero section for beauty score display
    - Add responsive layout structure for mobile and desktop
    - _Requirements: 6.1, 6.4, 6.5_


  - [x] 3.2 Implement main dashboard API endpoints

    - Create GET / endpoint to render main dashboard page
    - Implement GET /score endpoint to return current score HTML partial from Redis
    - Add GET /api/current-score endpoint for JSON score data
    - Implement fallback logic when Redis cache is empty
    - _Requirements: 1.1, 1.2, 6.2, 7.2_


  - [x] 3.3 Add hourly forecast display

    - Create GET /api/hourly-forecast endpoint to serve hourly predictions
    - Implement HTML partial template for hourly forecast grid
    - Add "Best" badge highlighting logic for highest score period
    - _Requirements: 2.1, 2.2_

  - [ ]* 3.4 Write property test for score range validity
    - **Property 1: Score Range Validity**
    - **Validates: Requirements 1.1**

  - [x] 3.5 Write property test for status message mapping




    - **Property 2: Status Message Mapping**
    - **Validates: Requirements 1.2**

- [x] 4. Create Weather ETL Airflow DAG





  - [x] 4.1 Implement Weather ETL DAG structure

    - Create `airflow/dags/weather_etl_dag.py` with hourly schedule (@hourly)
    - Define DAG with proper default_args (retries, retry_delay, email_on_failure)
    - Set up task dependencies using >> operator
    - _Requirements: 4.1, 4.2, 4.3, 4.4_


  - [x] 4.2 Implement weather data fetching tasks

    - Create PythonOperator task to fetch Korea Meteorological Administration API data
    - Create PythonOperator task to fetch Korea Environment Corporation air quality data
    - Implement basic data validation and error handling with try-except blocks
    - Store raw data in XCom for downstream tasks
    - _Requirements: 4.1, 4.2, 7.1_



  - [x] 4.3 Implement beauty score calculation and caching




    - Create PythonOperator task to calculate beauty score using algorithm weights
    - Implement score calculation: weather state (40%), temperature (30%), air quality (20%), time (10%)
    - Add golden hour detection and precipitation/air quality penalties
    - Store calculated score in PostgreSQL with timestamp
    - Update Redis cache with current score and 1-hour TTL
    - _Requirements: 1.5, 2.3, 2.4, 2.5, 4.3, 4.4, 10.2_

  - [ ]* 4.4 Write property test for hourly forecast completeness
    - **Property 3: Hourly Forecast Completeness**
    - **Validates: Requirements 2.1**

  - [ ]* 4.5 Write property test for best time highlighting
    - **Property 4: Best Time Highlighting**
    - **Validates: Requirements 2.2**

- [x] 5. Implement calendar functionality



  - [x] 5.1 Create calendar generation utilities


    - Implement Python function to generate monthly calendar grid HTML
    - Add color intensity mapping based on score ranges (0-30: red, 80-100: green)
    - Create seasonal icon assignment logic based on date ranges
    - _Requirements: 3.1, 3.4_

  - [x] 5.2 Implement calendar API endpoints


    - Create GET /calendar/view endpoint to serve pre-rendered calendar HTML
    - Implement GET /calendar/detail endpoint for date-specific information modal
    - Add GET /api/monthly-calendar endpoint for JSON calendar data
    - _Requirements: 3.1, 6.5_

  - [x] 5.3 Create Calendar Generation DAG


    - Create `airflow/dags/calendar_generation_dag.py` with daily schedule
    - Implement task to fetch medium-term forecast (D+0 to D+10) from weather API
    - Add task for statistical prediction (D+11 to month end) using historical averages
    - Create task to render calendar HTML and cache in Redis with 24-hour TTL
    - _Requirements: 3.2, 3.3, 10.2_

  - [ ]* 5.4 Write property test for calendar color coding
    - **Property 5: Calendar Color Coding**
    - **Validates: Requirements 3.1**

  - [ ]* 5.5 Write property test for seasonal icon display
    - **Property 6: Seasonal Icon Display**
    - **Validates: Requirements 3.4**

- [x] 6. Implement image upload and AI classification




  - [x] 6.1 Create image upload endpoint and form


    - Implement POST /upload/image endpoint with file upload handling
    - Add file size validation (max 10MB) and allowed extensions check
    - Create upload form HTML template with HTMX for async submission
    - Add progress indicator and success/error message display
    - _Requirements: 5.1, 5.3, 5.4_



  - [x] 6.2 Implement AI image classification




    - Integrate CLIP or MobileNet model using transformers library
    - Create classification function to detect landscape/lake/nature content
    - Implement rejection logic for inappropriate content (selfies, documents, food)
    - Add EXIF metadata extraction using Pillow library
    - Store validated images and metadata in PostgreSQL user_uploads table
    - _Requirements: 5.1, 5.2, 5.3, 5.5_

  - [ ]* 6.3 Write property test for image classification accuracy
    - **Property 7: Image Classification Accuracy**
    - **Validates: Requirements 5.1, 5.2**

  - [ ]* 6.4 Write property test for EXIF metadata extraction
    - **Property 8: EXIF Metadata Extraction**
    - **Validates: Requirements 5.3**

- [x] 7. Implement AI background image generation



  - [x] 7.1 Create AI Image Generation DAG


    - Create `airflow/dags/ai_image_dag.py` with 3-hour schedule and trigger capability
    - Set up external trigger from Weather ETL DAG on significant weather changes
    - Configure task dependencies for prompt generation and image creation
    - _Requirements: 9.1, 9.5_

  - [x] 7.2 Implement weather-based prompt generation


    - Create task to generate AI prompts based on current weather conditions
    - Add seasonal variations (spring: cherry blossoms, autumn: leaves)
    - Implement time-of-day variations (morning, afternoon, evening, night)
    - _Requirements: 9.2_

  - [x] 7.3 Implement AI image generation and storage


    - Create task to call Stable Diffusion API or OpenAI DALL-E with generated prompt
    - Implement image download and storage in app/static/generated/ directory
    - Store image metadata (prompt, file path, timestamp) in PostgreSQL
    - Update Redis cache with latest image path
    - _Requirements: 9.3, 9.4_

  - [x] 7.4 Add background image auto-update to web interface


    - Create GET /bg-image endpoint to check for new background images
    - Add HTMX polling (every 60 seconds) to update background image
    - Implement smooth transition effect when image changes
    - _Requirements: 6.3_

- [x] 8. Add comprehensive error handling





  - [x] 8.1 Implement API error handling


    - Add try-except blocks to all FastAPI endpoints
    - Create consistent error response format with status codes
    - Implement fallback to cached data when database is unavailable
    - Add user-friendly error messages for upload failures
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [x] 8.2 Implement DAG error handling and retry logic



    - Add retry configuration to all external API tasks (retries=3, retry_delay=5min)
    - Implement graceful degradation when external services fail
    - Add on_failure_callback for error logging and alerting
    - _Requirements: 7.1, 7.4_

  - [ ]* 8.3 Write property test for error response consistency
    - **Property 10: Error Response Consistency**
    - **Validates: Requirements 7.1, 7.3, 7.4**

- [ ] 9. Final integration and deployment
  - [ ] 9.1 Add FastAPI service to Docker Compose
    - Add fastapi service definition to docker-compose.yaml
    - Configure networking to connect with postgres and redis services
    - Set up volume mounts for app code, templates, and static files
    - Expose port 8000 for FastAPI application
    - _Requirements: 8.1, 8.2, 8.4, 8.5_

  - [ ] 9.2 Test end-to-end data flow
    - Verify Weather ETL DAG runs and populates Redis cache
    - Test FastAPI endpoints serve data from Redis correctly
    - Verify HTMX updates work without page refresh
    - Test image upload and classification workflow
    - Confirm calendar generation and display
    - _Requirements: All requirements integration_

  - [ ]* 9.3 Write integration tests for critical user flows
    - Test main dashboard loading and score display
    - Test calendar navigation and modal functionality
    - Test image upload workflow end-to-end
    - _Requirements: 1.1, 1.2, 2.1, 3.1, 5.1_