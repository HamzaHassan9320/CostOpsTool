import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()  # will read .env in repo root when you run from repo root


@dataclass(frozen=True)
class OpenAIModelConfig:
    model: str


def get_open_ai_model():
    """
    Return a configured OpenAI client and selected model name.
    Raises RuntimeError when API key is missing or openai is not installed.
    """
    try:
        from openai import OpenAI
    except Exception as ex:  # pragma: no cover - import depends on environment
        raise RuntimeError("openai package is not installed.") from ex

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
    return OpenAI(api_key=api_key), OpenAIModelConfig(model=model)
