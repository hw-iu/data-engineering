# data-engineering

This is the documentation for the project data engineering course at the IU.

## Setup

Setup will be done in a container too to make sure all requirements are met.

Will look like this:

```bash
docker run \
        --interactive \
        --rm \
        --tty \
        --volume "$(pwd)/data:/data" \
        ghcr.io/hw-iu/data-engineering/setup:latest
```

🚧 We need about 22 GB of free space, including the downloaded zipped and extracted data.
