# lakehouse

## MINIO

- `docker-compose up -d`

## Delta Lake
 * Run the Delta Lake application using Docker.
 * Mount the `deltalake` directory to `/tmp/deltars_table` inside the container.
*/
- `docker run -v ./deltalake:/tmp/deltars_table delta-lake-app`

- delta-docker
  - <https://github.com/delta-io/delta-docker>
- Delta Sharing: An Open Protocol for Secure Data Sharing 
  - <https://github.com/delta-io/delta-sharing/blob/main/README.md>
  - <https://hub.docker.com/r/deltaio/delta-sharing-server>
  - <https://stackoverflow.com/questions/78707635/connecting-to-delta-lake-hosted-on-minio-from-dask>
  - <https://s3fs.readthedocs.io/en/latest/#s3-compatible-storage>



