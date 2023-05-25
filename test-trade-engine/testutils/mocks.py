

def default_response(*args, **kwargs: None):
    return None

class MockActor():

    def __init__(self, return_func=default_response):
        self.received = []
        self.return_func = return_func

    def ask(self, *args, **kwargs):
        self.received.append(args[0])
        return self.return_func(*args, **kwargs)

    def tell(self, *args, **kwargs):
        self.received.append(args[0])
