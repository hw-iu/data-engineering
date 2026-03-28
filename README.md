# data-engineering

This is the documentation for the project data engineering course at the IU.

## Requirements

This project should run on any system that has Docker installed. The setup and the actual processing will be done in
containers, so there are no further requirements.

The downloaded data will take about 5 GB of free space. The extracted data will take about 15 GB of free space.


## Setup

Setup will be done in a container too to make sure all requirements are met.
Especially since the JSON file for city data can only be downloaded from a Javascript-capable browser, this approach 
is pretty helpful here. The setup script is basically the entrypoint of the container. All involved scripts can be 
inspected in [setup](docker/setup).

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


##

