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
#use lib ("$FindBin::Bin/../lib/perl5","$FindBin::Bin/../lib");
#use DB::DESUtil;
use coreutils::DESUtil;
use DB::EventUtils;

my ($project,$run,$target_table,$src_table,$overwrite,$verbose);
my ($source_schema,$target_schema,$part_name);
my $validate = undef;

Getopt::Long::GetOptions(
   'project=s' => \$project,
   'file_run=s' => \$run,
   'source-table|stbl=s' => \$src_table,
   'target-table|target_table|object-table|object_table|ttbl=s' => \$target_table,
   'overwrite' => \$overwrite,
   'validate' => \$validate,
   'verbose=i' => \$verbose,
   'source-schema=s' => \$source_schema,
   'target-schema=s' => \$target_schema,
   'partition-name=s' => \$part_name
);

# validation will not work, so disable it for now
$validate = undef;

if (defined($validate) && ! $project) {
    Pod::Usage::pod2usage (
    -verbose => 0, -exitval => 2, -output => \*STDERR,
    -message => 'Must provide a project argument for validation, which is the PROJECT used for the run that created the files being ingested.'
  );
}

if (defined($validate) && ! $run) {
  Pod::Usage::pod2usage (
  -verbose => 0, -exitval => 2, -output => \*STDERR,
  -message => 'Must provide a file_run argument for validation, which is the RUN that created the files being ingested.'
  );
}

if (! $src_table) {
  Pod::Usage::pod2usage (
  -verbose => 0, -exitval => 2, -output => \*STDERR,
  -message => 'Must provide a source table or run argument.'
  );
}

if (! $target_table) {
 Pod::Usage::pod2usage (
    -verbose => 0, -exitval => 2, -output => \*STDERR,
    -message => 'Must provide a target (to be merged to) database table name.'
  );
}

if (defined($overwrite)) {
	$overwrite = 1;
} else {
	$overwrite = 0;
}

my $db = coreutils::DESUtil->new();


# Do some checking:
if (defined($validate)) {
    print "Validating Contents of $src_table.\n";

    my $start = time;
    my $cats = $db->selectall_hashref( "select a.id,a.objects from catalog a,location b where a.project='$project' and a.run='$run' and a.catalogtype='red_cat' and a.id=b.id and regexp_like(b.archivesites,'[^N]')",'id') ; 
    print "Catalog query took ", time - $start, " seconds\n";

    $start = time;
    my $tmpobjs = $db->selectall_hashref( "select catalogid, count(*) from $src_table group by catalogid", 'catalogid'); 
    print "Tmp table query took ", time - $start, " seconds\n";

    my $ncats = scalar keys %$cats;
    print "Number of catalogs for run: $ncats\n";

    my $ntmpobjs = scalar keys %$tmpobjs;
    print "Number of catalogs in src_table: $ntmpobjs\n";


    my $error = 0;

    my %allcatids = ();
    foreach my $id (keys %$cats) {
       $allcatids{$id} = 1;
    }
    foreach my $id (keys %$tmpobjs) {
       $allcatids{$id} = 1;
    }

    my $sumactual = 0;
    my $sumexpected = 0;
    foreach my $id (keys %allcatids) {
        my $Nactual = -1;
        my $Nexpected = -1;
        if (defined($cats->{$id})) {
            $Nexpected = $cats->{$id}{'objects'};
            $sumexpected += $Nexpected;
        }
        if (defined($tmpobjs->{$id})) {
            $Nactual = $tmpobjs->{$id}{'count(*)'};
            $sumactual += $Nactual;
        }


        if ($Nexpected != 0) {
            printf("CatalogID: %10d  Expected Objects: %10d  Ingested Objects: %10d   ", $id, $Nexpected, $Nactual);
            if ($Nexpected != $Nactual) {
                print "***";
                $error = 1;
            }
            print "\n";
        }
    }

    print "\n\n";
    reportEvent($verbose,"STATUS",1,"Total expected = $sumexpected, Total actual = $sumactual");

    if ($error) {
        reportEvent($verbose,"STATUS",5,"Exiting due to errors caught by validation");
        $db->disconnect;
        exit 255;
    }
}
else {
    print "Skipping validation of $src_table.\n";
}


#print "Calling mergeTmpTablePrc\n";
$db->mergeTmpTablePrc ({
  'source_table' => $src_table,
  'dest_table' => $target_table,
  'overwrite' => $overwrite,
  'source_schema' => $source_schema,
  'dest_schema' => $target_schema
});

$db->disconnect;

exit 0;

################################################################################
# Documentation
################################################################################

=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 OPTIONS
