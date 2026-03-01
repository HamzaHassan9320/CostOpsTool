from app.llm.model import get_open_ai_model
from app.llm.schemas import RouterOutput

SYSTEM = """
You are a router for a FinOps tool. Your job is ONLY to output JSON for the requested action.
Never request or accept AWS access keys. Only use a local AWS profile name.
Valid actions: aws_config.savings_scan
"""

def route(prompt: str) -> RouterOutput:
    model = get_open_ai_model()
    structured = model.with_structured_output(RouterOutput.model_json_schema(), method="json_schema")
    return structured.invoke(SYSTEM + "\nUser: " + prompt)