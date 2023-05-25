

class MockActor():
    def __init__(self): self.received = []
    def ask(self, *args, **kwargs): self.received.append(args[0])
    def tell(self, *args, **kwargs): self.received.append(args[0])
