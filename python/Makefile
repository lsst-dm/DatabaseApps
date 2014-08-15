# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

SHELL=/bin/sh

dirs:=`ls -d */`

.PHONY: all clean install uninstall

all:
	for d in $(dirs) ; do $(MAKE) -C $$d $@ ; done

install:
	for d in $(dirs) ; do $(MAKE) prefix=$(prefix) -C $$d $@ ; done

uninstall:
	for d in $(dirs) ; do $(MAKE) prefix=$(prefix) -C $$d $@ ; done

clean:
	for d in $(dirs) ; do $(MAKE) -C $$d $@ ; done
	rm -f  *~ \#*\#
