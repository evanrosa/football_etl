FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the fastapi
COPY . .

# Expose port
EXPOSE 8000

# Run the fastapi
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
