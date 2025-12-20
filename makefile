#################################################################################
# Makefile to build the project
#################################################################################

# set variables:
PROJECT_NAME = de-totesys-project-
REGION = eu-west-2
PYTHON_INTERPRETER = python
PWD=$(shell pwd) # run command pwd, store result in var PWD
PYTHONPATH=${PWD}
SHELL := /bin/bash # force Makefile to execute commands in bash
# PROFILE = default
PIP:=pip


# make a virtual environment:
.create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PYTHON_INTERPRETER) -m venv venv; \
	)

# Define var to help calling 
# Python from the venv:
ACTIVATE_ENV := source venv/bin/activate
# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

# Build the requirements.txt file 
# from the requirements.in file: 
.requirements: .create-environment
	$(call execute_in_env, $(PIP) install pip-tools)
	$(call execute_in_env, pip-compile requirements.in)
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

#################################################################################
# Set Up
.bandit:## Install bandit
	$(call execute_in_env, $(PIP) install bandit)


.black:## Install black
	$(call execute_in_env, $(PIP) install black)


.coverage:## Install coverage
	$(call execute_in_env, $(PIP) install pytest-cov)

## Set up dev requirements (bandit, black & coverage)
.dev-setup: .black .bandit .coverage

#################################################################################
# Tests
.security-test:## Run the security test (bandit + safety)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

.run-black:## Run the black code check
	$(call execute_in_env, black  ./src/*.py ./tests/*.py)

.unit-test:## Run the unit tests
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vv)


##.check-coverage:## Run the coverage check
##	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src tests/)

.check-coverage: ## Run the coverage check with a 90% threshold for passing:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src --cov-fail-under=90 tests/)

## Run all checks
.run-checks: .security-test .run-black .unit-test #.check-coverage

##Default "make" command
all: .requirements .run-checks

#################################################################################
#Add things here

pytest: .requirements
	@echo Running pytest
	( \
	. venv/bin/activate ; \
	pytest -vvvrP ; \
	)




#################################################################################
#Delete Runfiles
clean:
	rm -rf ./venv
	rm -rf __pycache__
