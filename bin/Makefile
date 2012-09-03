# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

exec_prefix=$(prefix)
bindir=$(exec_prefix)/bin

SHELL=/bin/sh

INSTALL_FILES:=$(filter-out Makefile, $(wildcard *))

.PHONY: all clean install uninstall

all:
	@true

install:
	@if [ "$(bindir)" = "/bin" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: installing to $(bindir)";
	mkdir -p $(bindir)
	cp $(INSTALL_FILES) $(bindir)

uninstall:
	@if [ "$(bindir)" = "/bin" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> uninstall"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: uninstalling from $(bindir)";
	-cd $(bindir) && rm $(INSTALL_FILES)
	-rmdir $(bindir)

clean:
	rm -f  *~ \#*\#
	rm -rf *.pyc
