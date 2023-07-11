if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import psycopg2

@custom
def transform_custom(data, *args, **kwargs):
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()

    try:
        conn.autocommit = False

        command = """
            CREATE TABLE IF NOT EXISTS DimCondWind (
                wind_cond_id SERIAL,
                wind_cond VARCHAR(5),
                CONSTRAINT pk_DimCondWind PRIMARY KEY(wind_cond_id)
            );

            CREATE TABLE IF NOT EXISTS DimWind(
                wind_id SERIAL,
                Wind_Chill REAL,
                Wind_Speed REAL,
                Precipitation Real,
                wind_cond_id SERIAL,
                CONSTRAINT pk_DimWind PRIMARY KEY(wind_id),
                CONSTRAINT fk_DimWind_DimCondWind 
                    FOREIGN KEY(wind_cond_id)
                    REFERENCES DimCondWind(wind_cond_id)
            );

            CREATE TABLE IF NOT EXISTS DimSource(
                source_id SERIAL,
                source VARCHAR(8),
                CONSTRAINT pk_DimSource PRIMARY KEY(source_id)
            );

            CREATE TABLE IF NOT EXISTS DimAirportCode(
                airport_code_id SERIAL,
                airport_code VARCHAR(8),
                CONSTRAINT pk_DimAirportCode PRIMARY KEY(airport_code_id)
            );


            CREATE TABLE IF NOT EXISTS DimLocation(
                pk_dim_location_id SERIAL,
                Street VARCHAR(50),
                City VARCHAR(20),
                Zipcode VARCHAR(20),
                State VARCHAR(20),
                County VARCHAR(20),
                Country VARCHAR(20),
                Timezone VARCHAR(20),
                CONSTRAINT pk_DimLocation PRIMARY KEY(pk_dim_location_id)
            );

            CREATE TABLE IF NOT EXISTS DimAccidentLocation (
                start_lat real,
                start_lng real,
                airport_code_id SERIAL,
                pk_dim_location_id SERIAL,
                CONSTRAINT pk_DimAccidentLocation PRIMARY KEY(start_lat,start_lng),
                CONSTRAINT fk_DimAccidentLocation_DimAirportCode 
                    FOREIGN KEY (airport_code_id)
                    REFERENCES DimAirportCode(airport_code_id),
                CONSTRAINT fk_DimAccidentLocation_DimLocation
                    FOREIGN KEY (pk_dim_location_id)
                    REFERENCES DimLocation(pk_dim_location_id)
            );

            CREATE TABLE IF NOT EXISTS DimWeatherCond(
                dim_weather_cond_id SERIAL,
                Weather_Condition VARCHAR(20),
                CONSTRAINT pk_DimWeatherCond PRIMARY KEY (dim_weather_cond_id)
            );

            CREATE TABLE IF NOT EXISTS DimWeather(
                Weather_Timestamp TIMESTAMP,
                dim_weather_cond_id SERIAL,
                CONSTRAINT pk_DimWeather PRIMARY KEY (Weather_Timestamp),
                CONSTRAINT fk_DimWeather_DimWeatherCond
                    FOREIGN KEY (dim_weather_cond_id)
                    REFERENCES DimWeatherCond(dim_weather_cond_id)
            );

            CREATE TABLE IF NOT EXISTS FactAccident (
                ID VARCHAR(18),
                Severity CHAR(1),
                Start_Time TIMESTAMP,
                End_Time TIMESTAMP,
                start_lat REAL,
                start_lng REAL,
                End_Lat REAL,
                End_Lng REAL,
                Distance REAL,
                Description VARCHAR(280),
                Temperature numeric(3,1),
                Humidity SMALLINT,
                Pressure REAL,
                Visibility REAL,
                binary_road_attribute_id VARCHAR(18),
                binary_road_period_id VARCHAR(18),
                wind_id SERIAL,
                source_id SERIAL,
                Weather_Timestamp TIMESTAMP,
                CONSTRAINT pk_FactAccident PRIMARY KEY(ID),
                CONSTRAINT fk_FactAccident_DimAccidentLocation
                    FOREIGN KEY (start_lat,start_lng)
                    REFERENCES DimAccidentLocation (start_lat,start_lng),
                CONSTRAINT fk_FactAccident_DimWeather
                    FOREIGN KEY (Weather_Timestamp)
                    REFERENCES DimWeather (Weather_Timestamp),
                CONSTRAINT fk_FactAccident_DimWind
                    FOREIGN KEY (wind_id)
                    REFERENCES DimWind (wind_id),
                CONSTRAINT fk_FactAccident_DimSource
                    FOREIGN KEY (source_id)
                    REFERENCES DimSource (source_id)
            );




        """
        cur.execute(command)

        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Error: ", e)
    finally:
        conn.autocommit = True

    cur.close()
    conn.close()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
