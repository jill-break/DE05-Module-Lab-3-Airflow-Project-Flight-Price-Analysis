from spark.transformation.transform_flight_data import validate_fare

def test_fare_math_correction():
    # Case: Total is recorded wrong in CSV
    input_data = {"base_fare": 4500, "tax": 500, "total_fare": 9999}
    result = validate_fare(input_data)
    
    # Assert the logic forced the correct sum
    assert result["total_fare"] == 5000

def test_invalid_airline_dropped():
    # Case: Airline is missing
    input_data = {"airline": "", "total_fare": 3000}
    result = validate_fare(input_data)
    
    # Assert that dirty records return None to be filtered out
    assert result is None