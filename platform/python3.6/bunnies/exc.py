
class BunniesException(Exception):
    pass

class BuildException(Exception):
    """ problem with build """
    pass

class NoSuchFile(BunniesException):
    def __init__(self, url):
        super(NoSuchFile, self).__init__("no such file: " + url)
        self.url = url

class IntegrityException(BunniesException):
    pass

class LambdaException(BunniesException):
    pass

class UnmarshallException(BunniesException):
    pass

class NotImpl(BunniesException):
    pass
