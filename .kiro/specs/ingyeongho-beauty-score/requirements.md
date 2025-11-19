# Requirements Document

## Introduction

인경호 뷰티 스코어 서비스는 학교의 랜드마크인 인경호의 현재 아름다움을 정량적 지표(0-100점)로 제공하여 학생들이 방문 여부를 즉각적으로 결정할 수 있도록 돕는 웹 애플리케이션입니다. Apache Airflow를 사용한 데이터 파이프라인으로 기상 데이터를 수집하고, HTMX를 활용한 동적 웹 인터페이스를 통해 실시간 정보를 제공합니다.

## Glossary

- **Beauty_Score_System**: 인경호의 아름다움을 0-100점으로 계산하는 시스템
- **Airflow_DAG**: Apache Airflow에서 실행되는 데이터 파이프라인 워크플로우
- **Weather_ETL_DAG**: 기상 데이터 수집 및 뷰티 스코어 계산을 담당하는 Airflow DAG
- **AI_Image_DAG**: 생성형 AI를 사용하여 배경 이미지를 생성하는 Airflow DAG
- **HTMX_Interface**: JavaScript 없이 HTML 속성만으로 비동기 통신을 구현하는 웹 인터페이스
- **FastAPI_Server**: Python FastAPI를 사용한 백엔드 서버
- **User_Upload_System**: 사용자가 인경호 사진을 업로드할 수 있는 시스템

## Requirements

### Requirement 1

**User Story:** As a student, I want to see the current beauty score of Ingyeongho Lake, so that I can decide whether to visit now.

#### Acceptance Criteria

1. WHEN a user accesses the main page, THE Beauty_Score_System SHALL display the current beauty score as an integer between 0 and 100
2. WHEN the beauty score is displayed, THE Beauty_Score_System SHALL show an appropriate status message based on score ranges (80-100: "인생샷 각", 0-30: "방콕 추천")
3. WHEN the current score is shown, THE Dynamic_Illustration_Generator SHALL display a weather-appropriate illustration reflecting current conditions
4. WHEN weather conditions change, THE Beauty_Score_System SHALL update the score within 1 hour of the change
5. THE Beauty_Score_System SHALL calculate scores using weather state (40%), temperature comfort (30%), air quality (20%), and time adjustment (10%) weightings

### Requirement 2

**User Story:** As a photography enthusiast, I want to see hourly beauty score predictions for today, so that I can plan the optimal time for taking photos.

#### Acceptance Criteria

1. WHEN a user views the short-term forecast section, THE Beauty_Score_System SHALL display hourly scores from 09:00 to 22:00 for the current day
2. WHEN displaying hourly predictions, THE Beauty_Score_System SHALL highlight the highest score period with a "Best" badge
3. WHEN the golden hour occurs (30 minutes before/after sunset), THE Beauty_Score_System SHALL add 10 points to the base score
4. WHEN precipitation probability exceeds 60%, THE Beauty_Score_System SHALL apply a -50 point penalty
5. WHEN air quality reaches "very bad" levels, THE Beauty_Score_System SHALL apply a -30 point penalty

### Requirement 3

**User Story:** As a student planning future activities, I want to see monthly beauty score predictions, so that I can schedule visits in advance.

#### Acceptance Criteria

1. WHEN a user views the monthly calendar, THE Beauty_Score_System SHALL display daily representative scores with color-coded intensity
2. WHEN showing calendar data for the next 10 days, THE Beauty_Score_System SHALL use Korea Meteorological Administration medium-term forecast data
3. WHEN showing calendar data beyond 10 days, THE Beauty_Score_System SHALL use statistical predictions based on 3 years of historical weather data
4. WHEN displaying calendar entries, THE Beauty_Score_System SHALL show special icons for seasonal features (autumn leaves, cherry blossoms, ducks)

### Requirement 4

**User Story:** As a system administrator, I want weather data to be automatically collected and processed using Airflow DAGs, so that beauty scores are always up-to-date.

#### Acceptance Criteria

1. WHEN the scheduled time arrives, THE Weather_ETL_DAG SHALL execute every hour at 00 minutes to fetch weather data from Korea Meteorological Administration API
2. WHEN weather data is retrieved, THE Weather_ETL_DAG SHALL fetch air quality data from Korea Environment Corporation API
3. WHEN external data is collected, THE Weather_ETL_DAG SHALL calculate beauty scores using the defined algorithm
4. WHEN scores are calculated, THE Weather_ETL_DAG SHALL store results in both PostgreSQL database and Redis cache
5. WHEN weather conditions change significantly, THE Weather_ETL_DAG SHALL trigger the AI_Image_DAG for background image regeneration

### Requirement 5

**User Story:** As a user, I want to upload photos of Ingyeongho Lake, so that I can contribute to the service's data collection.

#### Acceptance Criteria

1. WHEN a user uploads an image, THE User_Upload_System SHALL validate that the image contains landscape/lake/nature content using AI classification
2. WHEN an uploaded image contains inappropriate content (selfies, documents, food), THE User_Upload_System SHALL reject the upload with an appropriate message
3. WHEN a valid image is uploaded, THE User_Upload_System SHALL extract EXIF metadata including timestamp for data reliability
4. WHEN image processing is complete, THE User_Upload_System SHALL store the image and metadata in the database
5. THE User_Upload_System SHALL use lightweight vision models (MobileNet or CLIP) for real-time image classification

### Requirement 6

**User Story:** As a user, I want to access the service through a fast-loading HTMX-powered web interface, so that I can quickly check the beauty score without page refreshes.

#### Acceptance Criteria

1. THE HTMX_Interface SHALL update beauty scores without full page refresh when the refresh button is clicked
2. WHEN a user accesses the application, THE FastAPI_Server SHALL render the main dashboard using Jinja2 templates within 3 seconds
3. WHEN displaying background images, THE HTMX_Interface SHALL automatically update every 60 seconds to check for new AI-generated images
4. THE HTMX_Interface SHALL be responsive and work on both desktop and mobile devices using Tailwind CSS
5. WHEN calendar dates are clicked, THE HTMX_Interface SHALL display detailed information in a modal without page navigation

### Requirement 7

**User Story:** As a developer, I want the system to handle errors gracefully, so that users have a reliable experience.

#### Acceptance Criteria

1. WHEN external API calls fail, THE Weather_Data_Pipeline SHALL use the most recent cached data and log the error
2. WHEN the database is unavailable, THE Beauty_Score_System SHALL serve data from Redis cache
3. WHEN image upload fails, THE User_Upload_System SHALL display a clear error message and allow retry
4. WHEN beauty score calculation encounters invalid data, THE Beauty_Score_System SHALL use default values and flag the issue
5. THE Beauty_Score_System SHALL maintain 99% uptime for score retrieval operations

### Requirement 8

**User Story:** As a system architect, I want clear separation between data collection, score calculation, and web serving components, so that the system is maintainable and scalable.

#### Acceptance Criteria

1. WHEN data pipeline components are modified, THE Web_Application SHALL continue functioning without changes
2. WHEN web interface is updated, THE Weather_Data_Pipeline SHALL operate independently
3. WHEN score calculation algorithms are updated, THE User_Upload_System SHALL remain unaffected
4. THE Beauty_Score_System SHALL expose RESTful APIs for loose coupling between components
5. THE Weather_Data_Pipeline SHALL be containerized and deployable independently of other components
###
 Requirement 9

**User Story:** As a system administrator, I want AI-generated background images to be automatically created and updated, so that users see visually appealing and contextually appropriate imagery.

#### Acceptance Criteria

1. WHEN weather conditions change significantly, THE AI_Image_DAG SHALL be triggered by the Weather_ETL_DAG
2. WHEN the AI_Image_DAG executes, THE AI_Image_DAG SHALL generate weather-appropriate prompts based on current conditions and time
3. WHEN prompts are generated, THE AI_Image_DAG SHALL call Stable Diffusion API or OpenAI DALL-E to create background images
4. WHEN images are generated, THE AI_Image_DAG SHALL save them to the static files directory and update the database record
5. THE AI_Image_DAG SHALL execute automatically every 3 hours as a fallback even without weather condition triggers

### Requirement 10

**User Story:** As a developer, I want the system to use efficient data storage and caching strategies, so that the application performs well under load.

#### Acceptance Criteria

1. THE Beauty_Score_System SHALL store weather measurements and scores in PostgreSQL with proper indexing on timestamp fields
2. WHEN scores are calculated, THE Beauty_Score_System SHALL cache current scores in Redis with 1-hour expiration
3. WHEN AI images are generated, THE Beauty_Score_System SHALL store image metadata including prompts and file paths in PostgreSQL
4. THE FastAPI_Server SHALL serve cached data from Redis for real-time requests rather than querying PostgreSQL
5. WHEN database queries are needed, THE Beauty_Score_System SHALL use connection pooling for optimal performance