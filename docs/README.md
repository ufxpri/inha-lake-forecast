# Simplified Airflow DAGs

## Overview

The Ingyeongho Beauty Score System now uses **3 simple DAGs** instead of 9 complex ones.

## The 3 DAGs

### 1. `current_data_dag` (Every 30 minutes)
**Main real-time pipeline**
- Fetches weather and air quality data
- Calculates beauty scores
- Generates hourly predictions
- Updates all UI components
- Stores data in PostgreSQL and Redis

**Replaces:** data_collection_dag, score_calculation_dag, component_generation_dag, weather_etl_dag

### 2. `daily_data_dag` (Daily at 6 AM)
**Daily calendar generation**
- Generates monthly calendar with forecasts
- Calculates best dates for the month
- Identifies special dates
- Updates calendar components

**Replaces:** calendar_generation_dag

### 3. `monthly_data_dag` (Monthly on 1st at 3 AM)
**Monthly maintenance tasks**
- Calculates monthly statistics
- Archives old data (keeps last 3 months)
- Generates AI background images
- Optimizes database performance

**Replaces:** ai_image_dag

## Benefits

✅ **Simpler**: 3 DAGs instead of 9
✅ **Clearer**: Each DAG has a single purpose
✅ **Easier to maintain**: Less code duplication
✅ **Better performance**: Fewer DAG parsing overhead
✅ **Easier to debug**: Linear task flow

## Task Flow

### current_data_dag
```
fetch_all_data → calculate_scores → store_data → generate_components
```

### daily_data_dag
```
generate_calendar_data → [generate_calendar_html, generate_calendar_components] → store_calendar_data
```

### monthly_data_dag
```
[calculate_monthly_stats, cleanup_old_data, generate_ai_background] → vacuum_database
```

## Old DAGs

Old DAGs have been moved to `old_dags_backup/` for reference. You can safely delete this folder once you've verified the new DAGs work correctly.

## Migration Notes

- All functionality from the old 9 DAGs is preserved
- Database schema remains the same
- Redis cache keys remain the same
- Component file paths remain the same
- No changes needed to your Flask app

## Testing

To test the new DAGs:

```bash
# Test current data pipeline
airflow dags test current_data_dag 2025-11-21

# Test daily data pipeline
airflow dags test daily_data_dag 2025-11-21

# Test monthly data pipeline
airflow dags test monthly_data_dag 2025-11-21
```
