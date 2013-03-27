#!/usr/bin/env perl

#
# catalog_ingest.pl
#
# DESCRIPTION:
#
# AUTHOR:  
# Tony Darnell (tdarnell@uiuc.edu)
#
# $Rev: 7367 $
# $LastChangedBy: tomashek $
# $LastChangedDate: 2013-03-27 11:47:19 -0600 (Wed, 27 Mar 2013) $
#

use strict;
use Astro::FITS::CFITSIO qw( :constants );
use Astro::FITS::CFITSIO qw( :longnames );
use Benchmark;
use Cwd;
use Data::Dumper;
use Exception::Class::DBI;
use FindBin;
use File::Basename;
use File::Path;
use File::stat;
use Getopt::Long;
use Time::localtime;

use coreutils::DESUtil;
use DB::EventUtils;
use DB::FileUtils;
use DB::IngestUtils;

$| = 1;

my ($fileList,$archiveNode,$imageId,$band,$equinox,
    $isCoadd,$tmpTable,$batchSize,$mergeTable,$partKey) = undef;

Getopt::Long::GetOptions(
   "filelist=s" => \$fileList,
   "archivenode=s" => \$archiveNode,
   "tmptable=s" => \$tmpTable,
   "batchsize=s" => \$batchSize,
   "mergetable=s" => \$mergeTable
) or usage("Invalid command line options\n");

usage("\n\nYou must supply a filelist") unless defined $fileList;
usage("\n\nYou must supply a mergetable") unless defined $mergeTable;
usage("\n\nYou must supply a tmptable") unless defined $tmpTable;

$partKey = $mergeTable;
$batchSize = 1000 if not defined $batchSize;

my $t0 = new Benchmark;
my $errorStr;

#
# Read in the filelist
#
my @files;
readFileList($fileList,\@files);

#
# Get all files for a runID
#
my $runIds;
my $nites;

#
# Make a database connection
#
my $desdbh = coreutils::DESUtil->new
  (
             DBIattr => {
              AutoCommit => 0,
              RaiseError => 1,
              PrintError => 0
             }
           
  );

my $objTableInfoHashRef = getTableInfo($desdbh,'OBJECTS_CURRENT');

#
# Main file loop
#
my $numFiles = 0;
my $numObjs  = 0;
my $localPath = q{};
my $catFile = q{};
my @resolvedFilenames = ();
my $eventStr = q{};


my $tCount = 0;
foreach my $file (@files){

    my $catFile = $file->{'localfilename'};
    my $localPath = $file->{'localpath'};

#
# if localpath is not absolute, set it to the current dir and append
# localpath to cwd.
#

    if ( defined $localPath ){
      if ( $localPath !~ m{^\/} ){
        $localPath = cwd() . qq{/$localPath};
      }
    } else {
      $localPath = cwd();
    }

    $eventStr =  "$tCount:  Resolving $catFile\n";
    reportEvent(2,'STATUS',1,$eventStr);
    
    my $fileInfo = {};
    $fileInfo->{'localfilename'} = $catFile;
    $fileInfo->{'localpath'} = $localPath;

    $tCount++;

    push @resolvedFilenames, $fileInfo;

}

$numFiles = scalar(@resolvedFilenames);

#
# Create the global temporary table if it doesn't already exist
#
my $tableExists = checkIfTableExists($desdbh,$tmpTable);
createTmpTable($desdbh, $tmpTable, 'OBJECTS_CURRENT') if not $tableExists;


#
# Build the hashRef to ingest from all catalog files
#

my $runIdHashRef;
$tCount = 0;

while ($tCount <= $numFiles-1){

    my $catFile = $resolvedFilenames[$tCount];
    my $catFilename = $catFile->{'localfilename'};
    my $localPath =  $catFile->{'localpath'};

    my $band = "";

    if (!$partKey){
      $eventStr = qq{Partkey id undefined for $catFilename};
      reportEvent(2,'STATUS',4,$eventStr);
    }


    #
    # Check if this catalog is ingested
    #

    #if (isCatalogIngested($desdbh,$catFile,$tmpTable)){
    #  my $errorStr = qq{Objects from this catalog have already been ingested, moving to next catalog...};
    #  reportEvent(2,'STATUS',1,$errorStr);
    #  $tCount++;
    #  next;
    #}

    $eventStr = "$tCount: Reading catalog: $catFilename ";
    reportEvent(2,'STATUS',1,$eventStr);

    #
    # Open up the fits file 
    #
    my $status = 0;
    my $fptr = Astro::FITS::CFITSIO::open_file(
        "$localPath/$catFilename",
        READONLY,
        $status
        );

    if ($status){
      warn "Problem opening $catFilename:  $status\n";
      $status=0;
    }
    my ($hduType,$nHdus,$nCols,$nRows,$zeropoint,$comment) = 0;

    Astro::FITS::CFITSIO::fits_get_num_hdus($fptr,$nHdus,$status);
    #
    # Get the ZEROPOINT from the first header
    #
    if ($nHdus == 2){
      Astro::FITS::CFITSIO::fits_read_key_flt($fptr,'SEXMGZPT',$zeropoint,$comment,$status);
      Astro::FITS::CFITSIO::fits_movabs_hdu($fptr,2,$hduType,$status);
      Astro::FITS::CFITSIO::fits_read_key_str($fptr,'BAND',$band,$comment,$status);
    } elsif ($nHdus == 3){
    #
    # TODO:  Look into where zeropoint comes from in LDAC catalogs
    #
      $zeropoint = 25.0;
      Astro::FITS::CFITSIO::fits_movabs_hdu($fptr,3,$hduType,$status);
    } else {
      $errorStr = "Input file not in FITS_1.0 or FITS_LDAC format, check file";
      reportEvent(2,'STATUS',4,$errorStr);
    }

    Astro::FITS::CFITSIO::fits_get_hdu_type($fptr,$hduType,$status);
    Astro::FITS::CFITSIO::fits_get_num_rows($fptr,$nRows,$status);
    Astro::FITS::CFITSIO::fits_get_num_cols($fptr,$nCols,$status);

    if ($hduType == IMAGE_HDU){
      $errorStr = "Error: this program only reads tables, not images";
      reportEvent(2,'STATUS',4,$errorStr);
    }

    #
    # Read in catalog table and populate the insertHashRef that will do
    # the insert.
    # TODO:  Replace the following code with DB::FileUtils::readCatalog()
    #

    my ($typeCode,$colName,$width,$repeat,$nullPointer,$anyNul) = 0;
    $status=0;
    my $newStatus=0;
    my $insertHashRef;
    for (my $i=1; $i<=$nCols; $i++){
      my @output = ();
      fits_get_coltype($fptr,$i,$typeCode,$repeat,$width,$status);
      fits_get_colname($fptr,CASEINSEN,'*',$colName,$i,$newStatus);
      $colName = 'OBJECT_NUMBER' if ($colName eq 'NUMBER');

    #
    # These two columns are really 2D arrays that are 6 cols by nRow rows.
    # Sorting this using normal C style indexing for clarity.
    #
      if ( $repeat > 1 ) {

        # $repeat has number of elements in the vector.
        fits_read_col($fptr,$typeCode,$i,1,1,$nRows*$repeat,0,\@output,$anyNul,$status);
        for (my $k=1;$k<=$repeat;$k++){
          my @tempArr;
          for (my $j=0; $j<$nRows; $j++){
            $tempArr[$j] = $output[$j*$repeat+($k-1)];
          }
          my $tmpColName = $colName . "_$k";
          $insertHashRef->{$tmpColName}=\@tempArr;
        }

      } else {
        fits_read_col($fptr,$typeCode,$i,1,1,$nRows,0,\@output,$anyNul,$status);
        $insertHashRef->{$colName}=\@output;
      }
    }
    fits_close_file($fptr,$status);
    print "($nRows objects)\n";

    my @catnames   = ($catFilename) x $nRows;
    my @bands      = ($band) x $nRows;
    my @zeroPoints = ($zeropoint) x $nRows;
    my @runs       = ($partKey) x $nRows;

    my @equinoxes = ('2000') x $nRows;
    my @softIds   = ('1000') x $nRows;
    $insertHashRef->{'BAND'}      = \@bands;
    $insertHashRef->{'EQUINOX'}   = \@equinoxes;
    $insertHashRef->{'SOFTID'}    = \@softIds;
    $insertHashRef->{'ZEROPOINT'} = \@zeroPoints;
    $insertHashRef->{'PARTKEY'}   = \@runs;
    $insertHashRef->{'CATALOGNAME'} = \@catnames;
    #
    # Insert whatever is in  ALPHA_J2000 and DELTA_J2000 in ra, dec
    #
    $insertHashRef->{'RA'}  = $insertHashRef->{'ALPHA_J2000'} ?
                               $insertHashRef->{'ALPHA_J2000'} :
                               $insertHashRef->{'ALPHAMODEL_J2000'} ;
    $insertHashRef->{'DEC'} = $insertHashRef->{'DELTA_J2000'} ?
                               $insertHashRef->{'DELTA_J2000'} :
                               $insertHashRef->{'DELTAMODEL_J2000'};

    #
    # Get CX,CY,CZ
    #

    #my @cxs    = ();
    #my @cys    = ();
    #my @czs    = ();
    #my @htmIDs = ();
    #my ($cx,$cy,$cz) = 0;

    #for (my $i = 0; $i <= $#{$insertHashRef->{'RA'}};$i++){
    
    #  my $htmDepth = 20;
    #  my $ra  = $insertHashRef->{'RA'}->[$i];
    #  my $dec = $insertHashRef->{'DEC'}->[$i];
    #  ($cx,$cy,$cz) = getXYZ($ra, $dec);

    #  if ($cx == 0 || $cy == 0 || $cz == 0){
    #    $errorStr = "Problem calculating CX,CY,CZ for object number $i";
    #    reportEvent(2,'STATUS',4,$errorStr);
    #  }

    #  push @cxs,$cx;
    #  push @cys,$cy;
    #  push @czs,$cz;
    #
    #}

    #$insertHashRef->{'CX'} = \@cxs;
    #$insertHashRef->{'CY'} = \@cys;
    #$insertHashRef->{'CZ'} = \@czs;
    #$insertHashRef->{'HTMID'} = \@htmIDs;

    #
    # Push this file into the hash ref sorted by tmpTable
    #
    foreach my $col (keys %$insertHashRef){
      next if not defined $insertHashRef->{$col};
      push @{$runIdHashRef->{$tmpTable}->{$col}},@{$insertHashRef->{$col}};
    }

    if ( (($tCount != 0) && !($tCount % $batchSize)) || 
       ($tCount == $#resolvedFilenames) ){
      foreach my $tmpObjTable (keys %$runIdHashRef){
        my $numToIngest = 
            scalar @{$runIdHashRef->{$tmpObjTable}->{'OBJECT_NUMBER'}};
        print "Ingesting $numToIngest objects into $tmpObjTable\n";
        my $i0 = new Benchmark;
        my $insertHashRef = $runIdHashRef->{$tmpObjTable};
        my $numObjIngested = 
          ingestObjectsHashRef($desdbh, $tmpObjTable, $insertHashRef);
        print "Ingested $numObjIngested objects into $tmpObjTable\n";
        my $i1 = new Benchmark;
        my $ingestdiff = timediff($i1,$i0);
        print "Total ingest time:  ",timestr($ingestdiff,'all'),"\n";
        $numObjs += $numObjIngested;
        delete $runIdHashRef->{$tmpObjTable};
      }
      $runIdHashRef = undef;
    }

    $tCount++;
}


#########################################################
#
# Added by tomashek, 6/9/2011
# The temp table (referenced by tmpTable) was changed to
# a Global Temporary Table, so the next step is needed to
# move the data to a permanent table partition (mergeTable)
#
#########################################################

$desdbh->loadTable({
  'source_table' => $tmpTable,
  'target_table' => $mergeTable
});

$desdbh->commit();
$desdbh->disconnect;

$eventStr =  "ingested $numFiles files and $numObjs objects.\n";
reportEvent(2,'STATUS',1,$eventStr);

my $t1 = new Benchmark;
my $diff = timediff($t1,$t0);
print "total time:  ",timestr($diff,'all'),"\n";

exit(0);

#
# Subroutines
#
  
sub usage {

   my $message = $_[0];
   if (defined $message && length $message) {
      $message .= "\n"
         unless $message =~ /\n$/;
   }

   my $command = $0;
   $command =~ s#^.*/##;

   print STDERR (
      $message,
      "\n" .
      "usage: $command " .
      " -filelist files -archivenode archive -tmptable temptable " .
      " -batchsize  batchsize\n" .
      "       filelist contains the list of files along with the full path\n" .
      "       archivenode corresponds to one of the known archive nodes:\n" .
      "          bcs, des1, etc...\n" .
      "       tmptable is the name of the temp table to use during\n" .
      "       ingestion (default is PAR_RUN)\n" .
      "       batchsize is the number of files to read in before doing an\n" .
      "       insert.  Default is 1000 "
   );

   die("\n")

}

