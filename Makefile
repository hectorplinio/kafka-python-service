.PHONY: lint format

# Linting targets
lint:
	flake8 src/ tests/

# Formatting targets
format:
	black src/ tests/
	isort src/ tests/

# Testing
test:
	pytest tests/