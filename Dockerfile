# Use an official Python runtime as a base image
FROM python:3.12.5

# Install system dependencies and Microsoft SQL Server requirements
RUN apt-get update && apt-get install -y \
    gnupg2 \
    curl \
    unixodbc \
    unixodbc-dev \
    libodbc2 \
    libodbc1 \
    odbcinst1debian2 \
    odbcinst \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify ODBC installation and drivers
RUN odbcinst -j && cat /etc/odbcinst.ini

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port that Flask will run on
EXPOSE 5000

# Run the Flask application
CMD ["python", "server.py"]