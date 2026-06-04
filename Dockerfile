FROM python:3.10-slim

WORKDIR /app

COPY requirements-ui.txt .

RUN pip install --no-cache-dir -r requirements-ui.txt

COPY ai/app.py ./ai/app.py


EXPOSE 8501

CMD ["streamlit", "run", "ai/app.py", "--server.address=0.0.0.0", "--server.port=8501"]