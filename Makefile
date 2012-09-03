# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

ifeq ($(strip $(prefix)),)
export prefix:=$(DES_HOME)
install_warning=DatabaseApps: Warning: DES_HOME is deprecated as installation target
endif

unittestargs=--quiet

SHELL=/bin/sh
dirs=`ls -d */ | grep -v lib`

.PHONY: all check clean install uninstall

all:
	for d in $(dirs) ; do $(MAKE) -C $$d $@ ; done

check:
	$(MAKE) -C tests -- unittestargs=$(unittestargs) $@

install:
	@if [ -z "$(prefix)" ]; then \
	    echo "DatabaseApps: usage: make prefix=<install_dir> install"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@[ -n "$(install_warning)" ] && echo "$(install_warning)"; true #$(MAKE)
	@echo "DatabaseApps: installing to $(prefix)"
	mkdir -p $(prefix)
	for d in $(dirs) ; do $(MAKE) prefix=$(prefix) -C $$d $@ ; done

uninstall:
	@if [ -z "$(prefix)" ]; then \
	    echo "DatabaseApps:  usage: make prefix=<install_dir> uninstall"; \
	    false; \
	fi #$(MAKE) -- Do this test even with make -n
	@[ -n "$(install_warning)" ] && echo "$(install_warning)"; true #$(MAKE)
	@echo "DatabaseApps: uninstalling from $(prefix)"
	for d in $(dirs) ; do $(MAKE) prefix=$(prefix) -C $$d $@ ; done
	-rmdir $(prefix)

clean:
	for d in $(dirs) ; do $(MAKE) -C $$d $@ ; done
	rm -f  *~ \#*\#
