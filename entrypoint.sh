#!/bin/bash

# Initialize the schema if not already initialized
/opt/hive/bin/schematool -dbType mysql -initSchema || true

# Start the Hive Metastore service
/opt/hive/bin/hive --service metastore