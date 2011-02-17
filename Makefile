# build number, increment with each new build, reset on new version
RELEASE = 1`rpm --eval %{?dist}`

PY_VERSION := $(shell python -V 2>&1)

ifeq ($(strip $(DESTDIR)),)
	root=
else
	root=--root $(DESTDIR)
endif

default:
	python setup.py build

install:
	python setup.py install $(root)

rpm:
	echo $(PY_VERSION)
	python setup.py bdist_rpm --release="$(RELEASE)"

clean:
	rm -f MANIFEST
	rm -rf dist build
	find . -name "*.pyc" -exec rm -f {} \;
	find . -name "*~" -exec rm -f {} \;
	find . -name "#*#" -exec rm -f {} \;
