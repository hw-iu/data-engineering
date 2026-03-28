# Data Engineering

This is the documentation for the project data engineering course at the IU.

## Requirements

This project should run on any Linux system that has **Docker** installed. It is tested on Fedora 43 and Ubuntu 24.04.
The setup and the actual processing will be done in containers, so there are no further requirements.

The downloaded data will take about 5 GB of free space. The extracted data will take about 15 GB of free space.

For checking out the code, you will need **git**. Alternatively, you can also download the code as a zip file from GitHub.


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
ln -s dot_env .env
```


## Processing data



