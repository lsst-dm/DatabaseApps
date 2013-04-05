# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

dbdir=$(prefix)/perl/DB

SHELL=/bin/sh

# INSTALL_FILES is also used to remove installed files, so it needs to be
# resolved here against this directory rather than the install directory.
INSTALL_FILES:=$(wildcard *.pm)

.PHONY: all clean install uninstall

all:
	@true

install:
	@if [ "$(dbdir)" = "/perl/DB" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: installing to $(dbdir)"
	mkdir -p $(dbdir)
	cp $(INSTALL_FILES) $(dbdir)

uninstall:
	@if [ "$(dbdir)" = "/perl/DB" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> uninstall"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: uninstalling $(dbdir)"
	-cd $(dbdir) && rm $(INSTALL_FILES)
	-rmdir $(dbdir)
	-rmdir $(prefix)/perl

clean: 
	rm -f  *~ \#*\#
