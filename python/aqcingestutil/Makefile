# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

aqcdir=$(prefix)/python/aqcingestutil

SHELL=/bin/sh

# INSTALL_FILES is also used to remove installed files, so it needs to be
# resolved here against this directory rather than the install directory.
INSTALL_FILES:=$(wildcard *.py)

.PHONY: all clean install uninstall

all:
	@true

install:
	@if [ "$(aqcdir)" = "/python/aqcingestutil" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: installing to $(aqcdir)"
	mkdir -p $(aqcdir)
	cp $(INSTALL_FILES) $(aqcdir)

uninstall:
	@if [ "$(aqcdir)" = "/python/aqcingestutil" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> uninstall"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: uninstalling $(aqcdir)"
	-cd $(aqcdir) && rm $(INSTALL_FILES)
	-rmdir $(aqcdir)
	-rmdir $(prefix)/python

clean: 
	rm -f  *~ \#*\#
	rm -rf *.pyc
