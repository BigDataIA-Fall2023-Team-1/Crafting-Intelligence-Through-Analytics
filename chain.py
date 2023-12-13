import os
from typing import Any, Callable, Optional
from template import CONDENSE_QUESTION_PROMPT
from dotenv import load_dotenv
from langchain.chains import ConversationalRetrievalChain, LLMChain
from langchain.chains.question_answering import load_qa_chain
from langchain.chat_models import ChatOpenAI
from langchain.llms import OpenAI
from pydantic import BaseModel, validator

# Load environment variables from the .env file
load_dotenv()

class ModelConfig(BaseModel):
    model_type: str
    callback_handler: Optional[Callable] = None

    @validator("model_type", pre=True, always=True)
    def validate_model_type(cls, v):
        if v not in ["gpt"]:
            raise ValueError(f"Unsupported model type: {v}")
        return v

class ModelWrapper:
    def __init__(self, config: ModelConfig):
        self.model_type = config.model_type
        self.callback_handler = config.callback_handler
        self.setup()

    def setup(self):
        if self.model_type == "gpt":
            self.setup_gpt()

    def setup_gpt(self):
        self.llm = ChatOpenAI(
            model_name="gpt-3.5-turbo-16k",
            temperature=0.2,
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            max_tokens=500,
            callbacks=[self.callback_handler],
            streaming=True,
        )


    def get_chain(self):
        if not self.llm:
            raise ValueError("Model has not been properly initialized.")
        return LLMChain(llm=self.llm, prompt=CONDENSE_QUESTION_PROMPT, )

def load_chain(model_name="GPT-3.5", callback_handler=None):
    if "GPT-3.5" in model_name:
        model_type = "gpt"

    config = ModelConfig(model_type=model_type, callback_handler=callback_handler)
    model = ModelWrapper(config)
    return model.get_chain()
