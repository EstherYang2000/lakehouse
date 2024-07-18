FROM bitsondatadev/hive-metastore:latest

# Switch to root to change permissions
USER root

# Change ownership of the hive directory
# RUN chown -R hive:hive /usr/local/hive-0.9.0-bin

# Create the configuration directory and set permissions
RUN mkdir -p /opt/hive/conf && \
    chown -R hive:hive /opt/hive/conf

# Copy the configuration file
COPY data/conf/hive-site.xml /opt/hive/conf/hive-site.xml

# Ensure the correct permissions
RUN chown hive:hive /opt/hive/conf/hive-site.xml

# Copy the entrypoint script
# COPY entrypoint.sh /usr/local/bin/entrypoint.sh
# RUN chmod +x /usr/local/bin/entrypoint.sh

# USER hive

# ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
