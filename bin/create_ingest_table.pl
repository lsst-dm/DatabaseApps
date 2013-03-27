#!/usr/bin/env perl
########################################################################
#
#  $Id: arrm 1577 2008-08-14 20:04:19Z dadams $
#
#  $Rev:: 1577                             $:  # Revision of last commit.
#  $LastChangedBy:: dadams                 $:  # Author of last commit. 
#  $LastChangedDate:: 2008-08-14 15:04:19 #$:  # Date of last commit.
#
#  Author: 
#         Darren Adams (dadams@ncsa.uiuc.edu)
#
#  Developed at: 
#  The National Center for Supercomputing Applications (NCSA).
#
#  Copyright (C) 2007 Board of Trustees of the University of Illinois. 
#  All rights reserved.
#
################################################################################
use strict;
use warnings;
use Carp;
use Getopt::Long;
use Pod::Usage;
use FindBin;
use Exception::Class::DBI;
#use lib ("$FindBin::Bin/../lib/perl5","$FindBin::Bin/../lib");
#use DB::DESUtil;
use coreutils::DESUtil;
use DB::IngestUtils;

$| = 1;

my ($run,$table_name,$main_table,$tablespace,$temp_table);

Getopt::Long::GetOptions(
   'run=s' => \$run,
   'table-name=s' => \$table_name,
   'main-table=s' => \$main_table,
   'tablespace=s' => \$tablespace,
   'temp-table-name=s' => \$temp_table,
);

if (!defined($main_table)) {
    Pod::Usage::pod2usage (
        -verbose => 0, -exitval => 2, -output => \*STDERR,
        -message => 'Must provide the name of the main table which will be used as a template for the temp table.'
    );
}

if (!defined($temp_table)) {
    Pod::Usage::pod2usage (
        -verbose => 0, -exitval => 2, -output => \*STDERR,
        -message => 'Must provide the name of the temporary table which will be used to stage the data that is being loaded into the table named after the run.'
    );
}

# Make a new table name
if (!$table_name) {
  if ($run) {
    $table_name = "P$run";
  }
  else {
    Pod::Usage::pod2usage (
    -verbose => 0, -exitval => 2, -output => \*STDERR,
    -message => 'Must provide a run string or table-name to name tmp table'
  );
  }
}

my $db = coreutils::DESUtil->new();

$db->createTableAs({
  'table_name' => $table_name,
  'as' => $main_table,
  'existok' => 1,
  'commit' => 0,
  'tablespace' => $tablespace
});

$db->createTableAs({
  'table_name' => $temp_table,
  'as' => $main_table,
  'existok' => 1,
  'commit' => 0,
  'temp' => 1
}) if not checkIfTableExists($db,$temp_table);

# No longer need index on tmp table
#my $index_name = 'I'.$table_name;
#my $index_sql = "CREATE INDEX $index_name ON $table_name(catalogid) PARALLEL 4 NOLOGGING";
#$db->do($index_sql);

$db->disconnect;

exit 0;

################################################################################
# Documentation
################################################################################

=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 OPTIONS
