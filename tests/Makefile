# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

unittestargs=--quiet

# The oraTest and pgTest variables are evaluated later to define prerequisites
# for the check target.  They use serviceAccess to search the environment for
# the unit test sections and generate the name of the appropriate make target
# for the sections found.

saCommand:=serviceAccess
oraSec=db-oracle-unittest
pgSec=db-postgres-unittest
oraTest=$(shell $(saCommand) -s $(oraSec) "test_oracle" 2>/dev/null)
pgTest=$(shell $(saCommand) -s $(pgSec) "test_postgres" 2>/dev/null)
oraMsg=DatabaseApps: Warning: Missing $(oraSec) section; Oracle tests skipped.
pgMsg=DatabaseApps: Warning: Missing $(pgSec) section; PostgreSQL tests skipped.
errMsg=DatabaseApps: Error: No database tests could be performed.

SHELL=/bin/sh
export PYTHONPATH:=../python:$(PYTHONPATH)
export PATH:=../bin:$(PATH)

.PHONY: all check clean install uninstall test_aqcingestutil
.PHONY: test_aqcingestutil_oracle test_aqcingestutil_postgres

all:
	@true

install:
	@true

uninstall:
	@true

check: $(oraTest) $(pgTest)
	@[ -n "$(oraTest)" ] || echo "$(oraMsg)"
	@[ -n "$(pgTest)" ] || echo "$(pgMsg)"
	@if [ -z "$(oraTest)$(pgTest)" ]; then echo "$(errMsg)"; false; fi

test_aqcingestutil: test_aqcingestutil_oracle test_aqcingestutil_postgres

test_aqcingestutil_oracle:
	./test_aqcingestutil oracle $(unittestargs)

test_aqcingestutil_postgres:
	./test_aqcingestutil postgres $(unittestargs)

test_oracle: test_aqcingestutil_oracle

test_postgres: test_aqcingestutil_postgres

clean: 
	rm -f  *~ \#*\#
	rm -rf *.pyc
