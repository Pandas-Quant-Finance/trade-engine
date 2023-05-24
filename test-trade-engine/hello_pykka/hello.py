from typing import Any

import pykka


class Actor1(pykka.ThreadingActor):

    def on_receive(self, message: Any) -> Any:
        print(Actor1.__name__, message, flush=True)


class Actor2(pykka.ThreadingActor):

    def on_receive(self, message: Any) -> Any:
        print(Actor2.__name__, message, flush=True)


if __name__ == "__main__":
    a1 = Actor1.start()
    a2 = Actor2.start()

    #for a in [a1, a2]:
    for i in range(20):
        a1.ask(i)

    for i in range(20):
        a2.tell(i)

    pykka.ActorRegistry.stop_all()