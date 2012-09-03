# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

upsdir=$(prefix)/ups

# INSTALL_FILES is used to remove installed files from a directory that could
# have files installed from other projects, so this wildcard needs to be
# resolved here against the local directory.
INSTALL_FILES:=$(wildcard *.table)

SHELL=/bin/sh

.PHONY: all clean install uninstall

all:
	@true

install:
	@if [ "$(upsdir)" = "/ups" ]; then \
	    echo "DatabaseApps:  usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: installing to $(upsdir)";
	mkdir -p $(upsdir)
	cp $(INSTALL_FILES) $(upsdir)

uninstall:
	@if [ "$(upsdir)" = "/ups" ]; then \
	    echo "DatabaseApps:  usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: uninstalling from $(upsdir)";
	-cd $(upsdir) && rm $(INSTALL_FILES)
	-rmdir $(upsdir)

clean:
	@true
