# Use an official Python runtime as a base image
FROM python:3.12.5

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .

RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port that Flask wilsl run on
EXPOSE 5001

# Run the Flask application
CMD ["python", "server.py"]
