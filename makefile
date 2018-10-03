PYTHON_TARGET_VER=3.6
PYTHON_EXE=python
PYTHON_SHELL=bpython-curses

pythons := $(shell find npipes/ -type f -name '*.py')

pythonfiles: $(pythons)

check: pythonfiles
	env MYPYPATH=.env/lib/python3.6/site-packages/dataclasses/ mypy --ignore-missing-imports -p npipes

clean: FORCE
	rm -rf npipes/__pycache__
	rm -rf npipes/**/__pycache__

package: FORCE
	env SOURCE_DATE_EPOCH=315532800 python3 setup.py sdist bdist_wheel

FORCE: ;

test:
	PYTHONPATH=. $(PYTHON_EXE) tests/processorTests.py
	PYTHONPATH=. $(PYTHON_EXE) tests/serializeTests.py
