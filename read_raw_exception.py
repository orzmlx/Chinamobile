class ReadRawException(Exception):
    def __init__(self, manufacturer, raw_message, message):
        self.message = message
        self.raw_message = raw_message
        self.manufacturer = manufacturer

    def __str__(self):
        return self.message
