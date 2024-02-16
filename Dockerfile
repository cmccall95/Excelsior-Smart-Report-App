# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Upgrade pip and setuptools
RUN pip install --upgrade pip setuptools==69.0.3

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the .env file into the working directory in the container
COPY .env /usr/src/app/.env

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV NAME World

# Run app.py when the container launches
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "main:flask_server"]
