
testwmfmariadbpy: Containerfile.tox
	podman build -f Containerfile.tox -t testwmfmariadbpy .

tox: testwmfmariadbpy
	# Run tox on bookworm
	podman run --rm -t -v $(PWD):/app:Z -w /app testwmfmariadbpy tox -v

test-integ::
	podman run --rm -t -v $(PWD):/app:Z -w /app testwmfmariadbpy tox -e py3-integration

test-unit::
	podman run --rm -t -v $(PWD):/app:Z -w /app testwmfmariadbpy tox -e py3-unit

test-unit-cov::
	podman run --rm -t -v $(PWD):/app:Z -w /app testwmfmariadbpy tox -e py3-unit -- --cov=wmfmariadbpy
	
delete-tox-container:
	podman rmi -f testwmfmariadbpy
