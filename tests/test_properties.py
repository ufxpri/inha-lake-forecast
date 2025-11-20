"""
Property-based tests for Ingyeongho Beauty Score Service.

**Feature: ingyeongho-beauty-score**
"""
import pytest
from hypothesis import given, strategies as st

from app.utils import get_status_message


class TestStatusMessageMapping:
    """
    **Property 2: Status Message Mapping**
    
    For any beauty score value, the system should display the correct 
    status message according to score ranges.
    
    **Validates: Requirements 1.2**
    """
    
    @given(st.integers(min_value=80, max_value=100))
    def test_high_score_range_returns_best_message(self, score: int):
        """
        Property: Scores in range [80, 100] should return "인생샷 각"
        """
        result = get_status_message(score)
        assert result == "인생샷 각", (
            f"Score {score} in range [80, 100] should return '인생샷 각', "
            f"but got '{result}'"
        )
    
    @given(st.integers(min_value=0, max_value=29))
    def test_low_score_range_returns_stay_home_message(self, score: int):
        """
        Property: Scores in range [0, 29] should return "방콕 추천"
        """
        result = get_status_message(score)
        assert result == "방콕 추천", (
            f"Score {score} in range [0, 29] should return '방콕 추천', "
            f"but got '{result}'"
        )
    
    @given(st.integers(min_value=0, max_value=100))
    def test_all_valid_scores_return_non_empty_message(self, score: int):
        """
        Property: All valid scores [0, 100] should return a non-empty string
        """
        result = get_status_message(score)
        assert isinstance(result, str), (
            f"Status message should be a string, got {type(result).__name__}"
        )
        assert len(result) > 0, (
            f"Status message should not be empty for score {score}"
        )
    
    @given(st.integers(min_value=0, max_value=100))
    def test_status_message_is_deterministic(self, score: int):
        """
        Property: Same score should always return the same status message
        """
        result1 = get_status_message(score)
        result2 = get_status_message(score)
        assert result1 == result2, (
            f"Status message for score {score} should be deterministic, "
            f"but got different results: '{result1}' vs '{result2}'"
        )
    
    @given(st.integers(min_value=60, max_value=79))
    def test_good_score_range_returns_appropriate_message(self, score: int):
        """
        Property: Scores in range [60, 79] should return "좋은 시간"
        """
        result = get_status_message(score)
        assert result == "좋은 시간", (
            f"Score {score} in range [60, 79] should return '좋은 시간', "
            f"but got '{result}'"
        )
    
    @given(st.integers(min_value=40, max_value=59))
    def test_okay_score_range_returns_appropriate_message(self, score: int):
        """
        Property: Scores in range [40, 59] should return "괜찮은 편"
        """
        result = get_status_message(score)
        assert result == "괜찮은 편", (
            f"Score {score} in range [40, 59] should return '괜찮은 편', "
            f"but got '{result}'"
        )
    
    @given(st.integers(min_value=30, max_value=39))
    def test_mediocre_score_range_returns_appropriate_message(self, score: int):
        """
        Property: Scores in range [30, 39] should return "그저 그래요"
        """
        result = get_status_message(score)
        assert result == "그저 그래요", (
            f"Score {score} in range [30, 39] should return '그저 그래요', "
            f"but got '{result}'"
        )
    
    def test_boundary_score_0(self):
        """Test boundary: score = 0"""
        assert get_status_message(0) == "방콕 추천"
    
    def test_boundary_score_30(self):
        """Test boundary: score = 30"""
        assert get_status_message(30) == "그저 그래요"
    
    def test_boundary_score_40(self):
        """Test boundary: score = 40"""
        assert get_status_message(40) == "괜찮은 편"
    
    def test_boundary_score_60(self):
        """Test boundary: score = 60"""
        assert get_status_message(60) == "좋은 시간"
    
    def test_boundary_score_80(self):
        """Test boundary: score = 80"""
        assert get_status_message(80) == "인생샷 각"
    
    def test_boundary_score_100(self):
        """Test boundary: score = 100"""
        assert get_status_message(100) == "인생샷 각"
    
    @given(st.integers(max_value=-1))
    def test_negative_scores_raise_value_error(self, score: int):
        """
        Property: Negative scores should raise ValueError
        """
        with pytest.raises(ValueError, match="Score must be between 0 and 100"):
            get_status_message(score)
    
    @given(st.integers(min_value=101))
    def test_scores_above_100_raise_value_error(self, score: int):
        """
        Property: Scores above 100 should raise ValueError
        """
        with pytest.raises(ValueError, match="Score must be between 0 and 100"):
            get_status_message(score)
    
    @given(st.one_of(st.floats(), st.text(), st.none()))
    def test_non_integer_scores_raise_type_error(self, score):
        """
        Property: Non-integer scores should raise TypeError
        """
        with pytest.raises(TypeError, match="Score must be an integer"):
            get_status_message(score)
