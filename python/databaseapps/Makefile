# $Id: Makefile 19109 2014-02-14 16:37:31Z mgower $
# $Rev:: 19109                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2014-02-14 10:37:31 #$:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

pydir=$(prefix)/python/databaseapps

SHELL=/bin/sh

dirs:=`ls -d */`

# INSTALL_FILES is also used to remove installed files, so it needs to be
# resolved here against this directory rather than the install directory.
INSTALL_FILES:=$(wildcard *.py)

.PHONY: all clean install uninstall

all:
	for d in $(dirs) ; do $(MAKE) -C $$d $@ ; done

install:
	@if [ "$(pydir)" = "/python/databaseapps" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: installing to $(pydir)"
	mkdir -p $(pydir)
	cp $(INSTALL_FILES) $(pydir)
	for d in $(dirs) ; do $(MAKE) prefix=$(prefix) -C $$d $@ ; done

uninstall:
	for d in $(dirs) ; do $(MAKE) prefix=$(prefix) -C $$d $@ ; done
	@if [ "$(pydir)" = "/python/databaseapps" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> uninstall"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@echo "DatabaseApps: uninstalling $(pydir)"
	-cd $(pydir) && rm $(INSTALL_FILES)
	-rmdir $(pydir)

clean: 
	rm -f  *~ \#*\#
	rm -rf *.pyc
	for d in $(dirs) ; do $(MAKE) -C $$d $@ ; done



