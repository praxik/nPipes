PYTHON_TARGET_VER=3.6
PYTHON_EXE=python3.6

cocos := $(wildcard npipes/*.coco)
pythons := $(wildcard npipes/*.py)

cocofiles: $(cocos)
pythonfiles: $(pythons)

build: cocofiles pythonfiles
	mkdir -p build
	cp -r npipes/*.coco build/.
	cp -r npipes/*.py build/.
	$(PYTHON_EXE) `which coconut` -l -t $(PYTHON_TARGET_VER) build/. --mypy

install: build
	rm -rf install
	mkdir -p  install
	cp build/*.py install/.
	touch install/__init__.py

run:
	$(PYTHON_EXE) install/npipes/experiments.py

test: install
	PYTHONPATH=install $(PYTHON_EXE) tests/processorTests.py

test-run: 
	PYTHONPATH=install $(PYTHON_EXE) tests/processorTests.py
