# Use the latest Python runtime as a parent image
FROM python:latest

# Set the working directory in the container to /app
WORKDIR /

ADD ./requirements.txt /requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Add the current directory contents into the container at /app
COPY . .

# Run the app using Gunicorn
CMD ["python", "/main.py"]
