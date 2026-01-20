def test_seasonal_categorization(spark_fixture):
    # Create mock data
    data = [("Flight during Eid Holiday", 10000), ("Random Trip", 5000)]
    df = spark_fixture.createDataFrame(data, ["event_name", "fare"])
    
    # Apply your KPI logic
    from jobs.transformation.transform_flight_data import add_season_logic
    output_df = add_season_logic(df)
    
    # Check if 'Eid' was caught by the regex
    row = output_df.filter(output_df.event_name.contains("Eid")).first()
    assert row["season_category"] == "Peak"