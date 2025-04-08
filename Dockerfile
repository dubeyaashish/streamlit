# Use an official lightweight Python image as a base image
FROM python:3.8-slim

# Set environment variables:
# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Prevents Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt /app/

# Install the dependencies from the requirements file
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the application code into the container
COPY . /app/

# Expose the port that Streamlit runs on
EXPOSE 8501

# Define the command to run the Streamlit app
CMD ["streamlit", "run", "app.py", "--server.enableCORS", "false"]
