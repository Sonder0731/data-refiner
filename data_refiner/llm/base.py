class LLMClient:
    def __init__(self, *args, **kwargs):
        self.base_url = kwargs.get("base_url")
        self.model_name = kwargs.get("model_name")
        self.api_key = kwargs.get("api_key")
        self.temperature = kwargs.get("temperature")
        self.max_tokens = kwargs.get("max_tokens")
        self.max_retries = kwargs.get("max_retries")
        self.frequency_penalty = kwargs.get("frequency_penalty")

    def invoke(self, *args, **kwargs):
        raise NotImplementedError("Client not implement `invoke` method")

    def invoke_once(self, *args, **kwargs):
        raise NotImplementedError("Client not implement `invoke_once` method")

    async def ainvoke(self, *args, **kwargs):
        raise NotImplementedError("Client not implement `ainvoke` method")

    async def ainvoke_once(self, *args, **kwargs):
        raise NotImplementedError("Client not implement `ainvoke_once` method")
