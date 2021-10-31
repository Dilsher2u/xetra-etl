"""
Custome Exceptions
"""

class WrongFileFormatException(Exception):
    """
    Wrong Format Exception
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message