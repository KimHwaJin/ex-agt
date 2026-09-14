"""Agent invocation and checkpointed completion receipt."""

from uuid import UUID, uuid5

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig

from .state import AssistantState


def answer_id(run_id: str) -> str:
    return str(uuid5(UUID(run_id), "assistant-answer"))


def public_text(message) -> str:
    # Never project reasoning_content, tool calls or arbitrary metadata.
    if isinstance(message.content, str):
        return message.content
    return "".join(
        block.get("text", "")
        for block in message.content
        if isinstance(block, dict) and block.get("type") == "text"
    )


def response_node(agent, message_limit: int):
    async def respond(state: AssistantState, config: RunnableConfig):
        result = await agent.ainvoke(
            {"messages": state["messages"][-message_limit:]},
            config=config,
        )
        text = public_text(result["messages"][-1])
        if not text.strip():
            raise ValueError("Model returned no public text")
        return {
            "messages": [
                AIMessage(content=text, id=answer_id(state["run_id"]))
            ],
            "answer": text,
        }

    return respond


def complete(state: AssistantState):
    return {"completed_run_id": state["run_id"]}
