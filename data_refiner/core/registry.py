from loguru import logger


class Registry:
    def __init__(self):
        self.ops = {}

    def register(self, op_name: str, op_class):
        if op_name in self.ops:
            logger.warning(
                f"Operator '{op_name}' is already registered. It will be overwritten by {op_class.__name__}."
            )

        self.ops[op_name] = op_class
        logger.info(f"Registered {op_class.__name__} as '{op_name}'")
        return op_class

    def get_op(self, op_name: str):
        if op_name in self.ops:
            return self.ops[op_name]
        raise ValueError(f"Operator '{op_name}' is not registered.")

    def list_ops(self):
        return self.ops

    def remove(self, op_name):
        del self.ops[op_name]

    def clear(self):
        self.ops.clear()

    @property
    def count(self):
        return len(self.ops)


registry = Registry()
