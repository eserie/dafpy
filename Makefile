DOCS_DIR = docs
SPHINXBUILD = sphinx-build
GRAPHVIZ_DOT = dot

build-docs:
	$(SPHINXBUILD) -b html -D graphviz_dot=$(GRAPHVIZ_DOT) -d $(DOCS_DIR)/_build/doctrees $(DOCS_DIR) $(DOCS_DIR)/_build/html


docapi:
	sphinx-apidoc -f -o docs/source ./

build-doc:
	sphinx-build  docs/  docs/_build/


test:
	py.test -v --doctest-modules ./dafpy ./tests

coverage:
	py.test -v --doctest-modules --cov dafpy --cov-report html --cov-report term  ./dafpy ./tests

test-all: test-all coverage


clean: clean-docs clean-py clean-coverage

clean-docs:
	rm -rf $(DOCS_DIR)/_build/*
	find . -type f  \( -name '*.mf' -o -name "*.mu" \) -exec rm -f {} \;


clean-py:
	find . -type f -name '*.pyc' -exec rm -f {} \;
	rm -rf *.egg-info dist

clean-coverage:
	rm -rf htmlcov/

pep8:
	flake8
