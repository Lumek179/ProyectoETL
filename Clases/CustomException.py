class CustomException(Exception):
    def __init__(self,name,exception):
        self.name=name
        self.message = exception
        super().__init__(self.message)