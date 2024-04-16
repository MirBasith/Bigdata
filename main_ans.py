import redis
import requests
import json
import matplotlib.pyplot as plt

class WeatherDataProcessor:

    """
    A class to process JSON data from an API and perform various tasks.

    Attributes:
        api_url (str): The URL of the API to fetch JSON data.
        r(redis.Redis): Redis client to interact with Redis server.

    """


    def __init__(self, redis_host='redis-13146.c100.us-east-1-4.ec2.cloud.redislabs.com', redis_port=13146, redis_password='Basith1998%'):

        """
        Initializes the DataProcessor with API URL and Redis connection details.

        Args:
            redis_host (str): The hostname of the Redis server (default is localhost).
            redis_port (int): The port number of the Redis server (default is 6379).
            redis_db (int): The Redis database index (default is 0).

        """
        self.r = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            password=redis_password
        )



    def fetch_weather_data(self, api_url):

        """
        Fetch weather data from the given API URL.

        Args:
        - api_url (str): URL of the weather API.

        Returns:
        - dict: Parsed JSON response containing weather data.
        """

        response = requests.get(api_url)

        if response.status_code == 200:

            print('JSON Data fetched Successfullty from API\n \n')
            return response.json()
    
        else:
            raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")
        


    def insert_into_redis(self, data):

        """
        Insert weather data into Redis.

        Args:
        - data (dict): Weather data to insert into Redis.

        """

        for entry in data['list']:

            # Use the 'dt' field as the Redis key
            key = str(entry['dt'])
            
            # Convert the entry to a JSON string before storing
            value = json.dumps(entry)

            # Insert into Redis
            self.r.set(key, value)
    

        print("Data inserted into Redis successfully!\n \n")
        print("stored data: ",self.r.get('1713506400'), '\n \n \n')
            

    def process_weather_data(self, api_url):

        """
        Fetches weather data from the API, inserts it into Redis, and performs processing.

        Args:
        - api_url (str): URL of the weather API.
        """

        weather_data = self.fetch_weather_data(api_url)
        self.insert_into_redis(weather_data)

        # Store data in Redis
        for item in weather_data['list']:
            timestamp = item['dt']
            temperature = item['main']['temp']
            self.r.set(timestamp, temperature)

        # Example processing: Plot temperature over time
        timestamps = []
        temperatures = []
        for item in weather_data['list']:
            timestamps.append(item['dt'])
            temperatures.append(item['main']['temp'])

        

        plt.plot(timestamps, temperatures)
        plt.xlabel('Timestamp')
        plt.ylabel('Temperature (K)')
        plt.title('Temperature Over Time')
        plt.show()

        print('Time Stamp List: ',timestamps)



        # Aggregate temperature data
        interval = 3600  # Aggregate temperature data hourly 
        aggregated_data = {}

        for item in weather_data['list']:
            timestamp = item['dt']
            temperature = item['main']['temp']
            interval_start = timestamp - (timestamp % interval)

            if interval_start not in aggregated_data:
                aggregated_data[interval_start] = []

            aggregated_data[interval_start].append(temperature)

        # Calculate aggregate statistics for each interval
        aggregated_stats = {}

        for interval_start, temperatures in aggregated_data.items():
            average_temp = sum(temperatures) / len(temperatures)
            max_temp = max(temperatures)
            min_temp = min(temperatures)
            aggregated_stats[interval_start] = {
                'average_temp': average_temp,
                'max_temp': max_temp,
                'min_temp': min_temp
            }

        print('\n\n-------------------------------------   aggregation     ------------------------------------\n')

        # Print aggregated statistics 
        for interval_start, stats in aggregated_stats.items():
            print(f"Interval Start: {interval_start},\t Average Temp: {stats['average_temp']},\t Max Temp: {stats['max_temp']},\t Min Temp: {stats['min_temp']}")



        # Search for temperature data based on criteria of threshold value
        def search_temperature_data(start_timestamp, end_timestamp, min_temp=None, max_temp=None):
            result = []

            for item in weather_data['list']:
                timestamp = item['dt']
                temperature = item['main']['temp']

                if start_timestamp <= timestamp <= end_timestamp:
                    if (min_temp is None or temperature >= min_temp) and (max_temp is None or temperature <= max_temp):
                        result.append({'timestamp': timestamp, 'temperature': temperature})

            return result

        # Search for temperature data between two timestamps with a minimum temperature threshold 
        print('\n\n-------------------------------------         Search       ----------------------------------------------\n')
        print('Search for temperature data between two timestamps with a minimum temperature threshold\n\n')
        start_timestamp = int(input("Enter start timestamp:"))
        end_timestamp = int(input("Enter end timestamp:"))
        min_temp_threshold = int(input("Enter temperature threshold value:"))

        search_result = search_temperature_data(start_timestamp, end_timestamp, min_temp=min_temp_threshold)

        print('\n\nOutput  --->\n')
        # Print search result
        for item in search_result:
            print(f"Timestamp: {item['timestamp']}, Temperature: {item['temperature']}")

        


if __name__ == "__main__":

    # Initialize WeatherDataProcessor
    processor = WeatherDataProcessor()

    # API URL for weather data
    api_url = "https://api.openweathermap.org/data/2.5/forecast?lat=44.34&lon=10.99&appid=71e402bef09a4c128c86ae333395ab6a"

    # Process weather data
    processor.process_weather_data(api_url)
