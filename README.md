# Data Engineering

This is the documentation for the project data engineering course at the IU.

## Overview

The project is a data pipeline processing energy data from photovoltaic systems in Germany.
It consists of these components:

- **Kafka Cluster**: Three Kafka nodes forming a cluster as the backbone of the data pipeline
- **Kafka Admin**: Initializes the Kafka cluster by creating the necessary topics
- **Produce**: Python script producing messages to a Kafka topic from the downloaded data files
- **Enrich**: Consumes raw incoming data, enriches it with city information and produces it to another Kafka topic
- **Process**: Consumes enriched data, does aggregations and writes it to a database
- **Database**: Stores the processed data for later use, e.g. for visualization
- **Grafana**: Visualization tool to show the data from the database


## Requirements

This project should run on any Linux system that has **Docker** installed. It is tested on Fedora 43 and Ubuntu 24.04.
The setup and the actual processing will be done in containers, so there are no further requirements.

The downloaded data will take about **5 GB** of free space.
The extracted data will take about **15 GB** of free space.
The data in the container volumes will take about **10 GB** of free space, which sounds a lot,
but because Kafka runs as cluster with three nodes, it will replicate the data three times.

For checking out the code, you will need **git**.
Alternatively, you can also download the code as a zip file from GitHub.


## Setup

Setup will be done in a container too to make sure all requirements are met.
Especially since the JSON file for city data can only be downloaded from a Javascript-capable browser, this approach 
is pretty helpful here. The setup script is basically the entrypoint of the container. All involved scripts can be 
inspected in [setup](docker/setup).

### Cloning the repository

To clone the repository, execute the following command in the terminal:

```bash
git clone https://github.com/hw-iu/data-engineering.git
```

### Enter the project directory

After cloning the repository, you need to enter the project directory. You can do this with the following command:

```bash
cd data-engineering
```

### Download and extract data

All containers that acces the downloaded data, expect to find it in the `/data` directory.
To make things easier, we will download into a local `data` directory too.

The final setup call will be based on the then released version, but currently sticks on `latest`.

To run the setup, execute the following command in the terminal. Make sure to run it in the root directory of
this project, so that the `data` directory is created in the right place.

```bash
docker run \
        --interactive \
        --rm \
        --tty \
        --volume "$(pwd)/data:/data" \
        ghcr.io/hw-iu/data-engineering/setup:latest
```

### Configuring environment

The file [dot_env](dot_env) contains all environment variables that are needed for the project. It comes with sensible
defaults, but you can change them if you want.

Docker Compose will only read the `.env` file in the root directory. Because unixoid systems hide dot files by default,
it is named `dot_env` to avoid confusion. For actual use, it needs to be copied to `.env` or symlinked, just like this:

```bash
cp dot_env .env
```

Using a copy makes the setup independent of upstream changes in the git repository.

The most interesting variable is `DELAY_BETWEEN_MESSAGES` in the service `produce`, because it controls how fast
the simulated data is spooled into the Kafka cluster. If it is set to `0`, the producer will send messages as fast
as possible.

## Processing data

When everything is set up, you can start the data pipeline with docker compose:

```bash
docker compose up --detached
``` 

This will start all containers in the background. You can check the logs with:

```bash
docker compose logs --follow --timestamps
```


## Watching data

The setup contains a Grafana instance that is configured to show the data in the database.
You can access it at [http://localhost:3000](http://localhost:3000) with the credentials from the `.env` file.