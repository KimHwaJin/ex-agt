"""Import-safe graph registration: no DB, Redis, LLM or HTTP at import time."""

from ex_agent.dev_chat.client import AgentApiClient
from ex_agent.dev_chat.graph import build_chat_graph
from ex_agent.dev_chat.settings import ChatSettings

graph = build_chat_graph(AgentApiClient(ChatSettings()))
