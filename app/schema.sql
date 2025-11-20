-- Database schema for Ingyeongho Beauty Score Service
-- This schema defines tables for beauty scores, user uploads and related metadata

-- Beauty scores table for storing calculated scores from Weather ETL DAG
CREATE TABLE IF NOT EXISTS beauty_scores (
    id SERIAL PRIMARY KEY,
    score INTEGER NOT NULL,
    status_message VARCHAR(50),
    weather_condition VARCHAR(50),
    temperature FLOAT,
    aqi INTEGER,
    is_golden_hour BOOLEAN,
    precipitation_probability INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB,
    CONSTRAINT valid_score CHECK (score >= 0 AND score <= 100),
    CONSTRAINT valid_precipitation CHECK (precipitation_probability IS NULL OR (precipitation_probability >= 0 AND precipitation_probability <= 100))
);

-- Index on timestamp for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_beauty_scores_timestamp ON beauty_scores(timestamp DESC);

-- Index on score for score-based queries
CREATE INDEX IF NOT EXISTS idx_beauty_scores_score ON beauty_scores(score);

-- User uploads table for storing uploaded images and AI classification results
CREATE TABLE IF NOT EXISTS user_uploads (
    id SERIAL PRIMARY KEY,
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    file_path VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255),
    exif_timestamp TIMESTAMP WITH TIME ZONE,
    ai_classification_result JSONB,
    is_approved BOOLEAN DEFAULT FALSE,
    beauty_score_at_time INTEGER,
    CONSTRAINT valid_beauty_score CHECK (beauty_score_at_time IS NULL OR (beauty_score_at_time >= 0 AND beauty_score_at_time <= 100))
);

-- Index on uploaded_at for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_user_uploads_uploaded_at ON user_uploads(uploaded_at);

-- Index on is_approved for filtering approved images
CREATE INDEX IF NOT EXISTS idx_user_uploads_is_approved ON user_uploads(is_approved);

-- Index on beauty_score_at_time for score-based queries
CREATE INDEX IF NOT EXISTS idx_user_uploads_beauty_score ON user_uploads(beauty_score_at_time) WHERE beauty_score_at_time IS NOT NULL;

-- AI Generated Images table for storing AI-generated background images
CREATE TABLE IF NOT EXISTS ai_generated_images (
    id SERIAL PRIMARY KEY,
    file_path VARCHAR(255) NOT NULL,
    prompt TEXT,
    season VARCHAR(20),
    time_of_day VARCHAR(20),
    weather_condition VARCHAR(50),
    service VARCHAR(50),
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB
);

-- Index on generated_at for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_ai_images_generated_at ON ai_generated_images(generated_at DESC);

-- Index on weather_condition for weather-based queries
CREATE INDEX IF NOT EXISTS idx_ai_images_weather ON ai_generated_images(weather_condition);

-- Index on season for seasonal queries
CREATE INDEX IF NOT EXISTS idx_ai_images_season ON ai_generated_images(season);
