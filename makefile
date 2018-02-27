PYTHON_TARGET_VER=3.6
PYTHON_EXE=python
PYTHON_SHELL=bpython-curses

cocos := $(shell find npipes/ -type f -name '*.coco')
pythons := $(shell find npipes/ -type f -name '*.py')

cocofiles: $(cocos)
pythonfiles: $(pythons)

build: cocofiles pythonfiles
	mkdir -p build
	cp -r npipes/* build/.
	$(PYTHON_EXE) `which coconut` --jobs sys -l -t $(PYTHON_TARGET_VER) build/. --mypy --ignore-missing-imports

install: build
	mkdir -p install/npipes
	mkdir -p install/npipes/producers
	mkdir -p install/npipes/triggers
	cd build && find -type f -name '*.py' -exec cp '{}' ../install/npipes/'{}' ';' && cd ..
	touch install/npipes/__init__.py

build-clean:
	rm -rf build

install-clean:
	rm -rf install

clean: build-clean install-clean

repl: FORCE
	cd install && $(PYTHON_SHELL)

FORCE: ;

test: install
	PYTHONPATH=install $(PYTHON_EXE) tests/processorTests.py

test-run: 
#	PYTHONPATH=install $(PYTHON_EXE) tests/processorTests.py
	PYTHONPATH=install $(PYTHON_EXE) tests/serializeTests.py
