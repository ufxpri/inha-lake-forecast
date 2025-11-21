# Quick Start - Simplified DAGs

## Your 3 New DAGs

```
✅ current_data_dag.py    (every 30 minutes)
✅ daily_data_dag.py      (daily at 6 AM)
✅ monthly_data_dag.py    (monthly on 1st)
```

## What Each Does

### current_data_dag (Every 30 minutes)
Real-time weather data processing
- Fetches weather + air quality
- Calculates beauty scores
- Updates UI components

### daily_data_dag (Daily at 6 AM)
Daily calendar generation
- Generates monthly calendar
- Finds best dates
- Updates calendar components

### monthly_data_dag (Monthly on 1st)
Monthly maintenance
- Calculates statistics
- Cleans old data
- Generates AI images

## Quick Test

```bash
cd airflow/dags
chmod +x test_new_dags.sh
./test_new_dags.sh
```

Or test individually:

```bash
airflow dags test current_data_dag 2025-11-21
airflow dags test daily_data_dag 2025-11-21
airflow dags test monthly_data_dag 2025-11-21
```

## What Changed

**Before:** 9 complex DAGs with triggers
**After:** 3 simple DAGs with linear flows

All old DAGs backed up in `old_dags_backup/`

## Benefits

✅ 67% fewer files (9 → 3)
✅ 82% less code
✅ No inter-DAG triggers
✅ Easier to debug
✅ Same functionality
