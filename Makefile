.PHONY: lint test package clean

lint:
	black --check src/ tests/
	flake8 src/ tests/ --max-line-length=120

test:
	pytest tests/ -v

package:
	python -m build
