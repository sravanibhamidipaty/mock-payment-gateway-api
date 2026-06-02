# 1. Start with a pristine, lightweight Linux computer that already has Python 3.12 installed
FROM python:3.12-slim

# 2. Create a folder inside the new computer called /app and step inside it
WORKDIR /app

# 3. Copy ONLY the requirements.txt from your Mac into the container first
COPY requirements.txt .

# 4. Install the python packages (the --no-cache-dir keeps the box lightweight)
RUN pip install --no-cache-dir -r requirements.txt

# 5. Now copy the rest of your code (src, tests, pytest.ini) into the /app folder
COPY . .

ENV PYTHONPATH=/app/src

# 6. Tell the container that we plan to use port 8000
EXPOSE 8000

# 7. The exact terminal command to run when the box is turned on
# Notice we use 'src.main:app' and bind to '0.0.0.0' so it can communicate with the outside world
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]