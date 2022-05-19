import requests
import csv

citi2 = ["Delhi, India"]
cities = ["Bihar,India", "West bengal,India", "karnataka,India", "Madhya Pradesh,India", "Uttar Pradesh, India",
          "Maharashtra,India", "Jammu and Kashmir,India", "Delhi,India", "Hyderabad,India", "tamilnadu, India"]
weather_list = []


def createAndLoadDataInCSV(data):
    new_file = open("/usr/local/airflow/weather_csv/weather_csv_file.csv", 'w')
    csv_writer = csv.writer(new_file, delimiter=',')

    csv_writer.writerow(data[0].keys())

    for i in range(0, len(data)):
        csv_writer.writerow(data[i].values())


def cleanData(weatherList):
    headers = ["State", "Description", "Temperature", "Feels Like Temperature", "Min Temperature", "Max Temperatures",
               "Humidity", "Clouds"]

    stateWiseWeatherInfo = []
    for stateWeatherInfo in weatherList:
        temperature = round((float(stateWeatherInfo['main']['temp'])-32)*(5/9),2)
        feelsLikeTemp = round((float(stateWeatherInfo['main']['feels_like']) - 32) * (5 / 9), 2)
        minTemperature = round((float(stateWeatherInfo['main']['temp_min']) - 32) * (5 / 9), 2)
        maxTemperature = round((float(stateWeatherInfo['main']['temp_max']) - 32) * (5 / 9), 2)
        humidity = float(stateWeatherInfo['main']['humidity'])
        clouds = stateWeatherInfo['clouds']['all']
        description = stateWeatherInfo['weather'][0]['description']
        state = stateWeatherInfo['name']

        dictFormatData = {headers[0]: state, headers[1]: description, headers[2]: temperature, headers[3]: feelsLikeTemp,
                          headers[4]: minTemperature, headers[5]: maxTemperature,
                          headers[6]: humidity,
                          headers[7]: clouds
                          }

        stateWiseWeatherInfo.append(dictFormatData)

    return stateWiseWeatherInfo


def city_forecast(cityCountry):
    url = "https://community-open-weather-map.p.rapidapi.com/weather"

    querystring = {"q": cityCountry, "lat": "0", "lon": "0", "callback": "test", "id": "2172797", "lang": "null",
                   "units": "imperial"}

    headers = {
        "X-RapidAPI-Host": "community-open-weather-map.p.rapidapi.com",
        "X-RapidAPI-Key": "420194fe1fmshbd66af44739ba1fp1b983djsn17ab9bf7c389"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.text

def execute_required():
    for city in cities  :
        weather_data = city_forecast(city)
        weather_list.append(eval(weather_data[5:len(weather_data) - 1]))


    for val in weather_list:
        #print(val)
        print(type(val))
    print(type(weather_list))

    cleanDataLst = cleanData(weather_list)

    for val in cleanDataLst:
        print(val)
    createAndLoadDataInCSV(cleanDataLst)


