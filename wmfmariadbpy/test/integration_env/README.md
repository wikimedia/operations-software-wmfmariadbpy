The integration environment uses dbdeployer in a docker container to run database
instances.

As the database versions come in large binary tarballs (400M - 1G) that expand to very
large sizes (1-3GB), they are not baked into the docker image itself. Instead they are
downloaded to the cache/ dir, and unpacked into a docker volume when the container starts
(if they're not already in the docker volume). This means that rebuilding the docker
image and starting the container (after the first time) is very quick.
