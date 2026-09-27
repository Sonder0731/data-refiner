from typing import Dict, Union
from json_repair import repair_json
from pydantic import BaseModel
from data_refiner.llm.schema import Memory, Message
from tenacity import Retrying, stop_after_attempt, wait_exponential
from data_refiner.llm.base import LLMClient
from litellm import acompletion, completion


class LiteLLMClient(LLMClient):
    def invoke(
        self, memory: Memory, return_format=None, validation_model: BaseModel = None
    ) -> Union[BaseModel, Dict, str]:
        """
        Invoke the language model to generate a response to the user's input.
        The response will attach to the memory.
        """
        for attempt in Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            reraise=True,
        ):
            with attempt:
                messages = memory.to_dict_list()
                resp = completion(
                    model=self.model_name,
                    base_url=self.base_url,
                    messages=messages,
                    temperature=self.temperature,
                    api_key=self.api_key,
                    max_tokens=self.max_tokens,
                    frequency_penalty=self.frequency_penalty,
                )
                answer: str = resp.choices[0].message.content
                memory.add_message(Message.assistant_message(answer))
                if return_format == "json":
                    res = repair_json(answer, return_objects=True)
                    if res:
                        res = validation_model.model_validate(res) if validation_model else res
                        return res
                    return answer
                else:
                    memory.add_message(Message.assistant_message(answer))
                    return answer

    async def ainvoke(
        self, memory: Memory, return_format=None, validation_model: BaseModel = None
    ) -> Union[BaseModel, Dict, str]:
        """
        Invoke the language model to generate a response to the user's input.
        The response will attach to the memory.
        """
        for attempt in Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            reraise=True,
        ):
            with attempt:
                messages = memory.to_dict_list()
                resp = await acompletion(
                    model=self.model_name,
                    base_url=self.base_url,
                    messages=messages,
                    temperature=self.temperature,
                    api_key=self.api_key,
                    max_tokens=self.max_tokens,
                    frequency_penalty=self.frequency_penalty,
                )
                answer = resp.choices[0].message.content
                if return_format == "json":
                    res = repair_json(answer, return_objects=True)
                    if res:
                        res = validation_model.model_validate(res) if validation_model else res
                        memory.add_message(Message.assistant_message(answer))
                        return res
                    memory.add_message(Message.assistant_message(answer))
                    return answer
                else:
                    memory.add_message(Message.assistant_message(answer))
                    return answer

    def invoke_once(
        self, query, return_format=None, validation_model: BaseModel = None
    ) -> Union[BaseModel, Dict, str]:
        """
        Invoke the language model to generate a response to the user's input in one go.
        """
        for attempt in Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            reraise=True,
        ):
            with attempt:
                memory = Memory(max_messages=1)
                memory.add_message(Message.user_message(query))
                memory = memory.to_dict_list()
                resp = completion(
                    model=self.model_name,
                    base_url=self.base_url,
                    messages=memory,
                    temperature=self.temperature,
                    api_key=self.api_key,
                    max_tokens=self.max_tokens,
                    frequency_penalty=self.frequency_penalty,
                )
                answer = resp.choices[0].message.content
                if return_format == "json":
                    res = repair_json(answer, return_objects=True)
                    if res:
                        return validation_model.model_validate(res) if validation_model else res
                    return answer
                else:
                    return answer

    async def ainvoke_once(
        self, query, return_format=None, validation_model: BaseModel = None
    ) -> Union[BaseModel, Dict, str]:
        """
        Invoke the language model to generate a response to the user's input in one go.
        """
        for attempt in Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            reraise=True,
        ):
            with attempt:
                memory = Memory(max_messages=1)
                memory.add_message(Message.user_message(query))
                memory = memory.to_dict_list()
                resp = await acompletion(
                    model=self.model_name,
                    base_url=self.base_url,
                    messages=memory,
                    temperature=self.temperature,
                    api_key=self.api_key,
                    max_tokens=self.max_tokens,
                    frequency_penalty=self.frequency_penalty,
                )
                answer = resp.choices[0].message.content
                if return_format == "json":
                    res = repair_json(answer, return_objects=True)
                    if res:
                        return validation_model.model_validate(res) if validation_model else res
                    return answer
                else:
                    return answer
