
class BunniesException(Exception):
    pass

class NoSuchFile(BunniesException):
    def __init__(self, url):
        super(NoSuchFile, self).__init__("no such file: " + url)
        self.url = url

class LambdaException(BunniesException):
    pass

class UnmarshallException(BunniesException):
    pass

class NotImpl(BunniesException):
    pass
