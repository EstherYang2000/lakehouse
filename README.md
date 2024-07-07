# lakehouse

## MINIO

- `docker-compose up -d`

## Delta Lake
 * Run the Delta Lake application using Docker.
 * Mount the `deltalake` directory to `/tmp/deltars_table` inside the container.
*/
- `docker run -v ./deltalake:/tmp/deltars_table my-delta-lake-app`



