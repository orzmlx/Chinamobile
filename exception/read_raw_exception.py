class ReadRawException(Exception):
    def __init__(self, manufacturer=None, system=None,
                 raw_message=None, message=None, code=-1, model=None):
        self.message = message
        self.raw_message = raw_message
        self.manufacturer = manufacturer
        self.model = model
        self.code = code
        self.system = system

    def __str__(self):
        return self.message
