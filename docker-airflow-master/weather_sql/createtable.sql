CREATE TABLE IF NOT EXISTS weather (
    state varchar(50),
    description varchar(100), 
    Temperature_In_Celsius float, 
    Feels_Like_Temperature_In_Celsius float,
    Min_Temperature_In_Celsius float,
    Max_Temperature_In_Celsius float, 
    Humidity_In_Percentage float,
    Clouds varchar(10)
);