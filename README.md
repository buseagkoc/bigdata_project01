# bigdata_project01
This project loads data from the City of New York dataset into an Elasticsearch (OpenSearch) about the analyzing 15k of NYC fire incident Ddispatch data.

Dockerfile - Contains instructions to build the Docker image.
requirements.txt - Lists the Python dependencies.
src/ - Contains the main Python script main.py.

Building the Docker Image
In the terminal, navigate to the project01 directory and run the following command:
docker build -t project01:1.0 .


Background Information

This project retrieves data provided by the City of New York. The data contains information about various incidents and service requests in New York City, such as response times, boroughs, and incident classifications.

Record Count Verification
The data loader script managed to count the records on the domain endpoint, which can be used to verify the successful insertion of data into the Elasticsearch cluster.
