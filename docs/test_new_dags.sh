#!/bin/bash

# Test script for new simplified DAGs

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          Testing New Simplified Airflow DAGs                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test date
TEST_DATE="2025-11-21"

echo "Test Date: $TEST_DATE"
echo ""

# Function to test a DAG
test_dag() {
    local dag_name=$1
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Testing: $dag_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if airflow dags test "$dag_name" "$TEST_DATE" 2>&1 | grep -q "success"; then
        echo -e "${GREEN}✓ $dag_name passed${NC}"
        return 0
    else
        echo -e "${RED}✗ $dag_name failed${NC}"
        return 1
    fi
}

# Test each DAG
echo "Starting DAG tests..."
echo ""

# Test 1: Current Data DAG
test_dag "current_data_dag"
current_result=$?
echo ""

# Test 2: Daily Data DAG
test_dag "daily_data_dag"
daily_result=$?
echo ""

# Test 3: Monthly Data DAG
test_dag "monthly_data_dag"
monthly_result=$?
echo ""

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "                        SUMMARY                              "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

total_tests=3
passed_tests=0

if [ $current_result -eq 0 ]; then
    echo -e "${GREEN}✓${NC} current_data_dag"
    ((passed_tests++))
else
    echo -e "${RED}✗${NC} current_data_dag"
fi

if [ $daily_result -eq 0 ]; then
    echo -e "${GREEN}✓${NC} daily_data_dag"
    ((passed_tests++))
else
    echo -e "${RED}✗${NC} daily_data_dag"
fi

if [ $monthly_result -eq 0 ]; then
    echo -e "${GREEN}✓${NC} monthly_data_dag"
    ((passed_tests++))
else
    echo -e "${RED}✗${NC} monthly_data_dag"
fi

echo ""
echo "Results: $passed_tests/$total_tests tests passed"
echo ""

# Check components
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "                  CHECKING COMPONENTS                        "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

components=(
    "current-status"
    "hero-image"
    "best-time"
    "hourly-chart"
    "best-date"
    "monthly-calendar"
    "special-dates"
)

component_dir="/opt/airflow/nginx_html/components"

if [ -d "$component_dir" ]; then
    for component in "${components[@]}"; do
        if [ -f "$component_dir/$component" ]; then
            echo -e "${GREEN}✓${NC} $component exists"
        else
            echo -e "${YELLOW}⚠${NC} $component not found (will be generated on next run)"
        fi
    done
else
    echo -e "${YELLOW}⚠${NC} Component directory not found: $component_dir"
    echo "   Components will be created on first DAG run"
fi

echo ""

# Final result
if [ $passed_tests -eq $total_tests ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              ALL TESTS PASSED! ✓                             ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║              SOME TESTS FAILED ✗                             ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Check the Airflow logs for more details:"
    echo "  docker-compose logs airflow-scheduler"
    exit 1
fi
