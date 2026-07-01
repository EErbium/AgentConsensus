FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY node_app/ node_app/
EXPOSE 8000
CMD ["uvicorn", "node_app.main:app", "--host", "0.0.0.0", "--port", "8000"]
