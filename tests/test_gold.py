"""
Tests for Gold Layer KPI computation using pure Pandas.
"""
import pytest
import pandas as pd
from etl_modules.gold import (
    compute_airline_performance,
    compute_seasonal_analysis,
    compute_popular_routes
)


@pytest.fixture
def sample_flight_data():
    """Create sample flight data for testing."""
    return pd.DataFrame({
        'Airline': ['Airline A', 'Airline A', 'Airline B', 'Airline C'],
        'Source': ['DAC', 'DAC', 'CGP', 'DAC'],
        'Destination': ['CXB', 'CXB', 'DAC', 'CGP'],
        'Total Fare (BDT)': [5000, 6000, 4500, 7000],
        'Seasonality': ['Winter Holiday', 'Eid ul Fitr', 'Regular Season', 'Summer Peak']
    })


class TestAirlinePerformance:
    """Tests for airline performance KPI."""
    
    def test_aggregation_counts(self, sample_flight_data):
        """Test that booking counts are correct."""
        result = compute_airline_performance(sample_flight_data)
        
        airline_a = result[result['airline'] == 'Airline A'].iloc[0]
        assert airline_a['booking_count'] == 2
        
    def test_aggregation_averages(self, sample_flight_data):
        """Test that average fares are calculated correctly."""
        result = compute_airline_performance(sample_flight_data)
        
        airline_a = result[result['airline'] == 'Airline A'].iloc[0]
        assert airline_a['avg_fare'] == 5500.0  # (5000 + 6000) / 2
        

class TestSeasonalAnalysis:
    """Tests for seasonal fare variation KPI."""
    
    def test_peak_categorization(self, sample_flight_data):
        """Test that Winter/Eid flights are categorized as Peak."""
        result = compute_seasonal_analysis(sample_flight_data)
        
        peak_row = result[result['season_category'] == 'Peak'].iloc[0]
        # Peak should include Winter Holiday (5000) and Eid (6000)
        assert peak_row['avg_seasonal_fare'] == 5500.0
        
    def test_non_peak_categorization(self, sample_flight_data):
        """Test that regular flights are categorized as Non-Peak."""
        result = compute_seasonal_analysis(sample_flight_data)
        
        non_peak_row = result[result['season_category'] == 'Non-Peak'].iloc[0]
        # Non-Peak should include Regular (4500) and Summer (7000)
        assert non_peak_row['avg_seasonal_fare'] == 5750.0  # (4500 + 7000) / 2


class TestPopularRoutes:
    """Tests for popular routes KPI."""
    
    def test_route_counting(self, sample_flight_data):
        """Test that routes are counted correctly."""
        result = compute_popular_routes(sample_flight_data)
        
        # DAC -> CXB appears twice
        top_route = result.iloc[0]
        assert top_route['source'] == 'DAC'
        assert top_route['destination'] == 'CXB'
        assert top_route['route_booking_count'] == 2
        
    def test_sorting_descending(self, sample_flight_data):
        """Test that routes are sorted by count descending."""
        result = compute_popular_routes(sample_flight_data)
        
        counts = result['route_booking_count'].tolist()
        assert counts == sorted(counts, reverse=True)