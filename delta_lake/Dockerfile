# Use the deltaio/delta-docker image as a base
FROM deltaio/delta-docker:latest

# Switch to root user to install additional packages
USER root
# Install Python and necessary packages
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install pandas pyarrow deltalake

WORKDIR /src
# Copy your script into the container
COPY src /src

# Set the entrypoint to run your script
ENTRYPOINT ["python3", "/src/acid.py"]
