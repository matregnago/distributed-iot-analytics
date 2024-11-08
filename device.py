class Sensor:
    def __init__(self, value: float, interval_start_date: str, interval_end_date: str):
        self.value = value
        self.interval_start_date = interval_start_date
        self.interval_end_date = interval_end_date

class Device:
    def __init__(self, temperature: Sensor, humidity: Sensor, luminosity: Sensor):
        self.temperature = temperature
        self.humidity = humidity
        self.luminosity = luminosity