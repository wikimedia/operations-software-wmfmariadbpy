
testwmfmariadbpy: Containerfile.tox
	podman build -f Containerfile.tox -t testwmfmariadbpy .

tox: testwmfmariadbpy
	# Run tox on bookworm
	podman run --rm -v $(PWD):/app:Z -w /app testwmfmariadbpy tox -v

delete-tox-container:
	podman rmi -f testwmfmariadbpy
