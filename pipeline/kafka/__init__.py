__all__ = ["HandProducer", "WarehouseSink"]


def __getattr__(name):
    if name == "HandProducer":
        from .producer import HandProducer

        return HandProducer
    if name == "WarehouseSink":
        from .consumer import WarehouseSink

        return WarehouseSink
    raise AttributeError(name)
