PROJECT := c3s_eqc_toolbox_template
CONDA := conda
CONDAFLAGS :=
COV_REPORT := html
WP :=

default: qa type-check

qa:
	pre-commit run --all-files

unit-tests:
	python -m pytest -vv --cov=. --cov-report=$(COV_REPORT) --doctest-glob="*.md" --doctest-glob="*.rst"

type-check:
	python -m mypy scripts

conda-env-update:
	$(CONDA) install -y -c conda-forge conda-merge
	$(CONDA) run conda-merge environment.yml ci/environment-ci.yml environments/environment_wp$(WP).yml > ci/combined-environment-ci.yml
	$(CONDA) env update $(CONDAFLAGS) -f ci/combined-environment-ci.yml

docker-build:
	docker build -t $(PROJECT) .

docker-run:
	docker run --rm -ti -v $(PWD):/srv $(PROJECT)

template-update:
	pre-commit autoupdate --repo https://github.com/nbQA-dev/nbQA --repo https://github.com/kynan/nbstripout --repo https://github.com/koalaman/shellcheck-precommit
	pre-commit run --all-files cruft -c .pre-commit-config-cruft.yaml

docs-build:
	cp README.md docs/. && cd docs && rm -fr _api && make clean && make html

# DO NOT EDIT ABOVE THIS LINE, ADD COMMANDS BELOW
execute-notebooks:
	for notebook in **/*.ipynb **/**/*.ipynb ; do \
        jupyter execute $$notebook || exit ; \
    done

execute-wp-notebooks:
	for notebook in **/wp*/*.ipynb ; do \
		jupyter execute $$notebook || exit ; \
	done
