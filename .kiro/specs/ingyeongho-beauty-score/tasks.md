# Implementation Plan

- [x] 1. Set up project infrastructure and core dependencies
  - Create project directory structure for Airflow DAGs, FastAPI app, and static files
  - Set up Docker Compose configuration for Airflow, PostgreSQL, and Redis
  - Configure Python virtual environment and install core dependencies (FastAPI, Airflow, Redis, PostgreSQL drivers)
  - Create basic configuration files and environment variables setup
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [ ] 2. Implement basic FastAPI server with HTMX interface
  - [ ] 2.1 Create FastAPI application structure with Jinja2 templates
    - Set up FastAPI app with static file serving and template rendering
    - Create base HTML template with HTMX and Tailwind CSS integration
    - Implement basic routing structure for main dashboard and API endpoints
    - _Requirements: 6.1, 6.2, 6.4_

  - [ ] 2.2 Implement Redis connection and cache utilities
    - Create Redis connection pool and basic cache operations
    - Implement cache key management and TTL handling
    - Add cache fallback mechanisms for when Redis is unavailable
    - _Requirements: 10.2, 10.4_

  - [ ]* 2.3 Write property test for cache data freshness
    - **Property 9: Cache Data Freshness**
    - **Validates: Requirements 10.4**

  - [ ] 2.4 Create main dashboard template with HTMX components
    - Design responsive layout with hero section, forecast, and calendar areas
    - Implement HTMX attributes for dynamic content loading
    - Add Tailwind CSS styling for mobile and desktop responsiveness
    - _Requirements: 6.1, 6.4, 6.5_

- [ ] 3. Implement core API endpoints for data serving
  - [ ] 3.1 Create current score API endpoint
    - Implement `/score` endpoint to serve current beauty score from Redis cache
    - Add status message mapping based on score ranges
    - Handle cache miss scenarios with appropriate fallbacks
    - _Requirements: 1.1, 1.2_

  - [ ]* 3.2 Write property test for score range validity
    - **Property 1: Score Range Validity**
    - **Validates: Requirements 1.1**

  - [ ]* 3.3 Write property test for status message mapping
    - **Property 2: Status Message Mapping**
    - **Validates: Requirements 1.2**

  - [ ] 3.4 Create hourly forecast API endpoint
    - Implement `/api/hourly-forecast` endpoint to serve forecast data from cache
    - Add best time highlighting logic for highest score periods
    - Format data for HTMX consumption with proper HTML structure
    - _Requirements: 2.1, 2.2_

  - [ ]* 3.5 Write property test for hourly forecast completeness
    - **Property 3: Hourly Forecast Completeness**
    - **Validates: Requirements 2.1**

  - [ ]* 3.6 Write property test for best time highlighting
    - **Property 4: Best Time Highlighting**
    - **Validates: Requirements 2.2**

- [ ] 4. Implement calendar functionality
  - [ ] 4.1 Create calendar view API endpoint
    - Implement `/calendar/view` endpoint to serve pre-rendered calendar HTML from cache
    - Add calendar detail modal endpoint for date-specific information
    - Handle month navigation and date range calculations
    - _Requirements: 3.1, 3.2, 3.3_

  - [ ]* 4.2 Write property test for calendar color coding
    - **Property 5: Calendar Color Coding**
    - **Validates: Requirements 3.1**

  - [ ]* 4.3 Write property test for seasonal icon display
    - **Property 6: Seasonal Icon Display**
    - **Validates: Requirements 3.4**

  - [ ] 4.4 Implement calendar HTML generation utilities
    - Create calendar grid rendering functions with Tailwind CSS classes
    - Implement color intensity mapping for beauty scores
    - Add seasonal icon assignment logic based on dates and historical patterns
    - _Requirements: 3.1, 3.4_

- [ ] 5. Implement image upload and processing system
  - [ ] 5.1 Create image upload API endpoints
    - Implement `/upload/image` POST endpoint for file uploads
    - Add file validation and size limits for uploaded images
    - Create upload form HTML partial for HTMX integration
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ] 5.2 Implement AI image classification
    - Integrate lightweight vision model (MobileNet or CLIP) for content classification
    - Create classification logic to identify landscape/lake/nature content
    - Implement rejection logic for inappropriate content with user feedback
    - _Requirements: 5.1, 5.2, 5.5_

  - [ ]* 5.3 Write property test for image classification accuracy
    - **Property 7: Image Classification Accuracy**
    - **Validates: Requirements 5.1, 5.2**

  - [ ] 5.4 Implement EXIF metadata extraction
    - Add EXIF data parsing for uploaded images
    - Extract timestamp information and store in database
    - Handle images without EXIF data gracefully
    - _Requirements: 5.3_

  - [ ]* 5.5 Write property test for EXIF metadata extraction
    - **Property 8: EXIF Metadata Extraction**
    - **Validates: Requirements 5.3**

- [ ] 6. Set up Airflow infrastructure and basic DAGs
  - [ ] 6.1 Configure Airflow with Docker Compose
    - Set up Airflow webserver, scheduler, and worker containers
    - Configure Airflow connections for external APIs and databases
    - Create DAG directory structure and basic configuration
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [ ] 6.2 Create Weather ETL DAG structure
    - Implement basic DAG definition with hourly scheduling
    - Create task structure for weather data fetching and processing
    - Add task dependencies and error handling configuration
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [ ] 6.3 Implement weather data fetching tasks
    - Create tasks to fetch data from Korea Meteorological Administration API
    - Add air quality data fetching from Korea Environment Corporation API
    - Implement data validation and error handling for API responses
    - _Requirements: 4.1, 4.2_

  - [ ] 6.4 Implement beauty score calculation in DAG
    - Create score calculation task with configurable algorithm parameters
    - Implement weather condition change detection logic
    - Add Redis cache update tasks for calculated scores
    - _Requirements: 4.3, 4.4, 4.5_

- [ ] 7. Implement AI image generation DAG
  - [ ] 7.1 Create AI Image Generation DAG structure
    - Set up DAG with trigger-based and scheduled execution
    - Create task dependencies for prompt generation and image creation
    - Configure external API connections for Stable Diffusion/DALL-E
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [ ] 7.2 Implement weather-based prompt generation
    - Create prompt generation logic based on weather conditions and time
    - Add seasonal and time-of-day variations to prompts
    - Implement prompt templates for different weather scenarios
    - _Requirements: 9.2_

  - [ ] 7.3 Implement AI image generation and storage
    - Create tasks to call external AI APIs for image generation
    - Implement image file storage and static file management
    - Add image metadata caching in Redis
    - _Requirements: 9.3, 9.4_

- [ ] 8. Implement calendar generation DAG
  - [ ] 8.1 Create Calendar Generation DAG
    - Set up daily DAG execution for calendar data updates
    - Create task structure for forecast and historical data processing
    - Implement calendar HTML pre-rendering tasks
    - _Requirements: 3.2, 3.3_

  - [ ] 8.2 Implement forecast and historical prediction tasks
    - Create tasks to fetch medium-term forecast data (D+0 to D+10)
    - Implement statistical prediction based on historical data (D+11 to month end)
    - Add seasonal icon assignment logic for calendar dates
    - _Requirements: 3.2, 3.3, 3.4_

  - [ ] 8.3 Implement calendar HTML rendering and caching
    - Create calendar grid HTML generation with Tailwind CSS
    - Implement color coding for beauty scores and seasonal icons
    - Add Redis caching for pre-rendered calendar HTML partials
    - _Requirements: 3.1, 3.4_

- [ ] 9. Implement error handling and system reliability
  - [ ] 9.1 Add comprehensive error handling to API endpoints
    - Implement consistent error response formats across all endpoints
    - Add fallback mechanisms for cache and database failures
    - Create user-friendly error messages for upload failures
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ]* 9.2 Write property test for error response consistency
    - **Property 10: Error Response Consistency**
    - **Validates: Requirements 7.1, 7.3, 7.4**

  - [ ] 9.3 Implement DAG error handling and retry logic
    - Add retry mechanisms for external API calls in DAGs
    - Implement graceful degradation when external services fail
    - Add monitoring and alerting for DAG failures
    - _Requirements: 7.1, 7.4_

- [ ] 10. Final integration and testing
  - [ ] 10.1 Integrate all components and test end-to-end functionality
    - Connect FastAPI endpoints with Airflow-generated cache data
    - Test HTMX interface with real data from Redis cache
    - Verify image upload and processing workflow
    - _Requirements: All requirements integration_

  - [ ]* 10.2 Write integration tests for critical user flows
    - Test main dashboard loading and score display
    - Test calendar navigation and detail views
    - Test image upload and classification workflow
    - _Requirements: 1.1, 1.2, 2.1, 2.2, 3.1, 5.1, 5.2_

- [ ] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.