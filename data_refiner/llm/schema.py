from typing import List, Literal, Optional
from pydantic import BaseModel, Field


class Message(BaseModel):
    """Represents a chat message in the conversation"""

    role: Literal["system", "user", "assistant", "tool"] = Field(...)
    content: Optional[str] = Field(default=None)
    name: Optional[str] = Field(default=None)
    tool_call_id: Optional[str] = Field(default=None)

    def __add__(self, other) -> List["Message"]:
        """支持 Message + list 或 Message + Message 的操作"""
        if isinstance(other, list):
            return [self] + other
        elif isinstance(other, Message):
            return [self, other]
        else:
            raise TypeError(
                f"unsupported operand type(s) for +: '{type(self).__name__}' and '{type(other).__name__}'"
            )

    def __radd__(self, other) -> List["Message"]:
        """支持 list + Message 的操作"""
        if isinstance(other, list):
            return other + [self]
        else:
            raise TypeError(
                f"unsupported operand type(s) for +: '{type(other).__name__}' and '{type(self).__name__}'"
            )

    def to_dict(self) -> dict:
        """Convert message to dictionary format"""
        message = {"role": self.role}
        if self.content is not None:
            message["content"] = self.content
        if self.name is not None:
            message["name"] = self.name
        return message

    def __repr__(self):
        return self.content[:100] + "..." + self.content[-100:]

    @classmethod
    def user_message(cls, content: str) -> "Message":
        """Create a user message"""
        return cls(role="user", content=content)

    @classmethod
    def assistant_message(cls, content: Optional[str] = None) -> "Message":
        """Create an assistant message"""
        return cls(role="assistant", content=content)


class Memory(BaseModel):
    messages: List[Message] = Field(default_factory=list)
    max_messages: int = Field(default=6)

    def add_message(self, message: Message) -> None:
        """Add a message to memory"""
        self.messages.append(message)
        # Optional: Implement message limit
        if len(self.messages) > self.max_messages:
            self.messages = self.messages[-2:]

    def add_messages(self, messages: List[Message]) -> None:
        """Add multiple messages to memory"""
        self.messages.extend(messages)

    def clear(self) -> None:
        """Clear all messages"""
        self.messages.clear()

    def get_all_messages(self) -> List[Message]:
        return self.messages

    def get_recent_messages(self, n: int) -> List[Message]:
        """Get n most recent messages"""
        return self.messages[-n:]

    def to_dict_list(self) -> List[dict]:
        """Convert messages to list of dicts"""
        return [msg.to_dict() for msg in self.messages]

    def keep_n_previous_messages(self, n: int) -> None:
        self.messages = self.messages[:n]

    def is_empty(self) -> bool:
        return len(self.messages) == 0

    def __repr__(self):
        return "\n".join(str(msg) for msg in self.messages)

    def __len__(self):
        return len(self.messages)
