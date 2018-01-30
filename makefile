PYTHON_VER=3.5

cocos := $(wildcard npipes/*.coco)
pythons := $(wildcard npipes/*.py)

cocofiles: $(cocos)
pythonfiles: $(pythons)

build: cocofiles pythonfiles
	mkdir -p build
	cp -r npipes/*.coco build/.
	cp -r npipes/*.py build/.
	python3.6 `which coconut` -l -t $(PYTHON_VER) build/. --mypy

install: build
	rm -rf install
	mkdir -p  install
	cp build/*.py install/.

run:
	python3.6 install/npipes/experiments.py

test: install
	PYTHONPATH=install python3.6 tests/processorTests.py

test-run: 
	PYTHONPATH=install python3.6 tests/processorTests.py
