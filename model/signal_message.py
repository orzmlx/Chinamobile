

class message:

    def __init__(self,
                 code: int = 1,
                 signal_message
                 :str = None):
        """

        :type code: object
        """
        self.code = code
        self.signal_message = signal_message
        
    def get_message(self):
        return self.signal_message

