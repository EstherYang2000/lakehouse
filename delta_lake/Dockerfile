# Use the deltaio/delta-docker image as a base
FROM deltaio/delta-docker:latest

# Switch to root user to install additional packages
USER root
# Install Python and necessary packages
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install pandas pyarrow deltalake

# Copy your script into the container
COPY app.py /app.py

# Set the entrypoint to run your script
ENTRYPOINT ["python3", "/app.py"]
