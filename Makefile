SHELL := /bin/bash
VENV_NAME=venv

install:
	( \
       python3 -m venv venv; \
       venv/bin/pip install -r requirements.txt; \
    )

runserver: 
	( \
	   source venv/bin/activate; \
	   cd fast_api; \
	   uvicorn user_registration:app --reload --host 127.0.0.1 --port 8000; \
    )

streamlit:
	( \
		source venv/bin/activate; \
		streamlit run Login.py; \
	)
