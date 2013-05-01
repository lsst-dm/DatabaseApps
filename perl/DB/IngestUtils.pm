#
# IngestUtils.pm
#
# DESCRIPTION:
#
# This module contains methods for general DB interaction used by
# all ingestion codes.
#
# AUTHOR:  Tony Darnell (tdarnell@uiuc.edu)
#

package DB::IngestUtils;

use strict;
use Benchmark;
use Data::Dumper;
use Exception::Class::DBI;
use File::stat;
use Math::Trig;
use Regexp::Common;
use Time::localtime;

use DB::EventUtils;

require Exporter;
our @ISA = qw(Exporter);

our @EXPORT = qw{
  batchIngest
  ccRaDec2ID
  checkIfCatalogIngested
  checkIfCoaddCatalogIngested
  checkIfPhotozCatalogIngested
  checkIfWLCatalogIngested
  checkIfIngested
  checkIfTableExists
  cleanObjHashRef
  createTmpTable
  deleteCoaddObjects
  deleteWLObjects
  dropTmpObjectsTable
  getArchiveNodeInfo
  getCoaddCatParentId
  getCoaddZeropoint
  getExposureID
  getFileInfo
  getFilesForRunID
  getIDs
  getMESObjects
  getNextFileID
  getNextZPID
  getParentID
  getTableInfo
  getWLObjects
  getXYZ
  ingestCoaddObjectsHashRef
  ingestCoaddZeropoints
  ingestObjectsHashRef
  ingestWLHashRef
  insertHashRefInDB
  isCatalogIngested
  updateCoaddObjectsHashRef
  updateHashRefInDB
  validateInsertHashRef
};

#
# batchIngest
#
sub batchIngest {

  my ($dbh,$hashRef,$doUpdate) = @_;
  my $sql = q{};
  my $eventStr = q{};
  my $num=0;

  foreach my $table (keys %$hashRef){
    my $valueStr;
    my $keyStr;
    if ($doUpdate){
      #$valueStr = join ' = ?,', keys %{$hashRef->{$table}};
      foreach (keys %{$hashRef->{$table}}){
        if (($_ eq 'FILEDATE') || ($_ eq 'INSERT_DATE')){
          $valueStr .= qq{$_ = to_date(?,'Dy Mon DD HH24:MI:SS YYYY'),};
        } else {
          $valueStr .= qq{$_ = ?,};
        }
      }
      chop $valueStr;
      #$valueStr .= ' = ?';
      $sql = qq{ UPDATE $table SET $valueStr WHERE ID = ? };
      my $sth = $dbh->prepare($sql);
      my @tupleStatus = ();
      my $tuples = $sth->execute_array(
         { ArrayTupleStatus => \@tupleStatus },
         (values %{$hashRef->{$table}},$hashRef->{$table}->{'ID'})
      );
      if ($tuples){
         $eventStr = "Successfully updated $tuples rows in $table";
         reportEvent(2,'STATUS',1,$eventStr);
      } else{
        my $arrRef = $hashRef->{$table}->{'ID'};
        $num = scalar(@$arrRef);
        for my $tuple (0..@$arrRef-1) {
          my $status = $tupleStatus[$tuple];
          $status = [0, "Skipped"] unless defined $status;
          next unless ref $status;
          $eventStr = sprintf("Failed to insert (%s): %s\n",
              $hashRef->{$table}->{'ID'}->[$tuple],
              $status->[1]);
          reportEvent(2,'STATUS',5,$eventStr);
          $num--;
        }
      }
    } else {

      $keyStr = join ",", keys %{$hashRef->{$table}};
      foreach (keys %{$hashRef->{$table}}){
        if (($_ eq 'FILEDATE') || ($_ eq 'INSERT_DATE')){
          $valueStr .= qq{to_date(?,'Dy Mon DD HH24:MI:SS YYYY'),};
        } else {
          $valueStr .= '?,';
        }
      }
      chop $valueStr;

      $sql = qq{
        INSERT INTO $table ($keyStr) VALUES ($valueStr)
      };

      my $sth = $dbh->prepare($sql);

      my @tupleStatus;

      my $tuples = $sth->execute_array(
         { ArrayTupleStatus => \@tupleStatus },
         (values %{$hashRef->{$table}})
      );

      if ($tuples) {
        $eventStr = "Successfully inserted $tuples rows in $table";
        reportEvent(2,'STATUS',1,$eventStr);
      } else {
        my $arrRef = $hashRef->{$table}->{'FLAG'};
        $num = scalar(@$arrRef);
        for my $tuple (0..@$arrRef-1) {
          my $status = $tupleStatus[$tuple];
          $status = [0, "Skipped"] unless defined $status;
          next unless ref $status;
          $eventStr = sprintf("Failed to insert id: %s %s",
              $hashRef->{$table}->{'ID'}->[$tuple],
              $status->[1]);
          reportEvent(2,'STATUS',4,$eventStr);

          $num--;
        }
      }
    }
  }

}

#
# insertHashRefInDB will do an insert using the keys as column names
# and values as the insertion data.  Make sure all keys and values
# are filled out to assure a complete insertion.
#
# No checks are done to assure accuracy, an exception will be thrown
# by dbi if insert fails.
#
sub insertHashRefInDB {

  my ($dbh,$tableName,$hashRef,$debug) = @_;

  print qq(Doing an insert on $tableName: $hashRef->{'ID'}\n);

  my $tableHashRef = getTableInfo($dbh,$tableName);

#
# Put single quotes around values that aren't numbers
#
  while (my ($key,$value) = each %$hashRef){
    next if not defined $tableHashRef->{$key};
    if ( ($tableName eq 'LOCATION') && ($key eq 'FILEDATE') ){
      $hashRef->{$key} = qq{to_date('$value','Dy Mon DD HH24:MI:SS YYYY')};
    }
    if ($tableHashRef->{$key}->{'type'} eq 'VARCHAR2'){
      if (defined $value){
        $hashRef->{$key} = qq{'$value'};
      } else {
        $hashRef->{$key} = qq{};
      }
    }
  }

  my $keyStr = join ',', keys %$hashRef;
  my $valueStr = join ',', values %$hashRef;

  my $sqlInsert = qq{
      INSERT INTO $tableName
      ($keyStr) VALUES ($valueStr)
  };

  print "$sqlInsert\n" if ($debug);

  my $sthInsert=$dbh->prepare($sqlInsert);
  my $numRows = $sthInsert->execute(); 

#
# Cleanup hashRef in case others need it.
#
  foreach (keys %$hashRef){
    $hashRef->{$_} =~ s/'//gis;
  }

  return $numRows;
}

#
# updateHashRefInDB()
#
# This method will attempt to do an update on a table.  If the fileId
# for that entry does not exist, insertHashRefInDB() is called.
#
sub updateHashRefInDB {

  my ($dbh,$tableName,$hashRef,$debug) = @_;

  my $fileId = $hashRef->{'ID'};

  my $tableHashRef = getTableInfo($dbh,$tableName);

  if ( not defined $fileId ){
    warn "UPDATE WARNING:  No file id";
    return undef;
  }

#
# Make sure entry exists for this fileId in this table
#

  print "Doing an update on $tableName: $fileId\n";

  my $valueStr = q{};
  while ( my ($key,$value) = each(%$hashRef) ){
    next if not defined $tableHashRef->{$key};
    if ( ($tableName eq 'LOCATION') && ($key eq 'FILEDATE') ){
      $value = qq{to_date('$value','Dy Mon DD HH24:MI:SS YYYY')};
    }
    if ($tableHashRef->{$key}->{'type'} eq 'VARCHAR2'){
      $valueStr .= qq{$key = '$value',};
    } else {
      $value = qq{} if not defined $value;
      $valueStr .= qq{$key = $value,};
    }
  }

#
# Get rid of trailing comma
#
  chop($valueStr);

  my $sql = qq{
      UPDATE $tableName
      SET $valueStr
      WHERE id = ?
  };

  print "$sql\n" if ($debug);

  my $sth=$dbh->prepare($sql);
  my $numRows = $sth->execute($fileId); 
  $sth->finish();

  return $numRows;
}

sub checkIfTableExists {

  my ($dbh,$tableName) = @_;
  my $tableExists = undef;

  my $sql = qq{ 
     SELECT COUNT(TABLE_NAME) FROM ALL_TABLES WHERE TABLE_NAME = ?
  };

  my $sth = $dbh->prepare($sql);
  $sth->execute(uc($tableName));
  $sth->bind_columns(\$tableExists);
  $sth->fetch();
  $sth->finish();

  return $tableExists;

}

sub createTmpTable {

  my ($dbh, $tableName, $templateTableName) = @_;

  print "Creating $tableName using $templateTableName as a template...";

  my $sql= qq{ CREATE GLOBAL TEMPORARY TABLE $tableName AS SELECT * FROM $templateTableName WHERE 1=0 };

  my $sth = $dbh->prepare($sql);
  eval {
    $sth->execute();
  };

  if ( my $ex = Exception::Class::DBI->caught() ){
    
    my $eventStr = q{};
    if ($ex->err == 955) {
      $eventStr =  qq{The temp table $tableName exists, continuing...};
      reportEvent(2,'STATUS',1,$eventStr);
    } else {
      $eventStr =  "DBI EXCEPTION:\n";
      $eventStr .=  "Exception Type: " . ref $ex . "\n";
      $eventStr .=  "Error: " . $ex->error . "\n";
      $eventStr .=  "Err: " . $ex->err . "\n";
      reportEvent(2,'STATUS',5,$eventStr);
    }
  }

  $sth->finish();
#
# Commit this to the database so other transactions can see it if this code
# is run in parallel.
#
  $dbh->commit();

  print "\n";

  return 1;

}

#
# This method checks to see if a list of entries exists in $tableName
#
# Returns an array of id's that exist.
#
sub checkIfIngested {
    
  my ($dbh,$tableHashRef,$hashRef,$debug) = @_;
  
  my @whereClause;
  my $whereClause;
  my $tableName;

  if ($tableName eq 'LOCATION'){
    while (my ($key,$value) = each(%$hashRef)){
      next if !(exists $tableHashRef->{$key});
      next if ($key =~ m/(ID|FILEDATE|FILESIZE|ARCHIVESITES)/);   
      next if ($value eq qq{}); # Don't include null values
      $value = qq{'$value'};
      push @whereClause, qq{$key = $value};
    }

    $whereClause = join (' AND ', @whereClause);

  } else {

    $whereClause = qq(ID = $hashRef->{'ID'});

  }


  my $sql = qq{
    SELECT id FROM $tableName
    WHERE $whereClause
  };

  print "$sql\n" if ($debug);

  my $fileId = undef;
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  $sth->bind_columns(\$fileId);
  $sth->fetch();
  $sth->finish();

  return (defined $fileId) ? $fileId : 0;

}

sub getFilesForRunID {

  my ($dbh,$hashRef,$nites,$project,$debug) = @_;

  my $whereClause = qq{PROJECT = '$project' AND };

  $whereClause .= '(';
  foreach my $runId (keys %$hashRef){
    $whereClause .= qq{RUN = ? OR };
  }
  foreach my $nite (keys %$nites){
    $whereClause .= qq{(FILECLASS='src' AND NITE = ?) OR };
  }

  $whereClause = substr($whereClause,0,-3);
  $whereClause .= ')';

  my $sql = qq{
    SELECT *
      FROM LOCATION
      WHERE $whereClause
  };

  print "$sql\n" if ($debug);

  my $fileId = undef;
  my $sth = $dbh->prepare($sql);
  if ($nites){
    $sth->execute(keys %$hashRef, keys %$nites);
  } else {
    $sth->execute(keys %$hashRef);
  }

  my $results = $sth->fetchall_arrayref({});
  $sth->finish();

  my $retHashRef;

  foreach my $file (@$results){

    my $eventStr = q{};
    my $fileType = $file->{'filetype'};
    my $fileName = $file->{'filename'};
    my $run      = $file->{'run'};
    my $nite     = $file->{'nite'};
    my $exposureName = $file->{'exposurename'};

#
# Issue warning if some of the keys are missing
#
    if ( !$run && !$nite){
      $eventStr = qq{either run or nite are defined in Big Query Hash Ref for $file->{'id'}};
      reportEvent(2,'STATUS',5,$eventStr);
      exit(0);
    }

      if ( !$fileType && !$fileName ){
        $eventStr = qq{filetype or filename is missing in Big Query Hash Ref for $file->{'id'}};
        reportEvent(2,'STATUS',5,$eventStr);
        exit(0);
    }

    if (not defined $run){
      if (exists $retHashRef->{$fileType}->{$fileName}->{$nite}){
        $eventStr = qq{Duplicate entry found in DB:  $file->{'id'}, $run };
        reportEvent(2,'STATUS',4,$eventStr);
      }
      $retHashRef->{$fileType}->{$fileName}->{$nite} = $file;
    } elsif (not defined $exposureName){
      if (exists $retHashRef->{$fileType}->{$fileName}->{$run}){
        $eventStr = qq{Duplicate entry found in DB:  $file->{'id'}, $run };
        reportEvent(2,'STATUS',4,$eventStr);
      }
      $retHashRef->{$fileType}->{$fileName}->{$run} = $file;
    } else {
      if (exists $retHashRef->{$fileType}->{$fileName}->{$run}->{$exposureName}){
        $eventStr = qq{Duplicate entry found in DB:  $file->{'id'}, $run };
        reportEvent(2,'STATUS',4,$eventStr);
      }
      $retHashRef->{$fileType}->{$fileName}->{$run}->{$exposureName} = $file;
    }

  }

  return $retHashRef;

}

sub getNextZPID{ 

  my $dbh = shift;

  my $zpId = 0;
  my $sql = qq{
      SELECT zeropoint_seq.nextval FROM dual
  };

  my $sth=$dbh->prepare($sql);
  $sth->execute();
  $sth->bind_columns(\$zpId);
  $sth->fetch();
  $sth->finish();

  return $zpId;
}

#
# Query the oracle sequencer for the location table
#
sub getNextFileID {

  my $dbh = shift;

  my $fileId = 0;
  my $sql = qq{
      SELECT location_seq.nextval FROM dual
  };

  my $sth=$dbh->prepare($sql);
  $sth->execute();
  $sth->bind_columns(\$fileId);
  $sth->fetch();
  $sth->finish();

  return $fileId;

}

sub getTableInfo {

  my ($dbh,$table) = @_;

  my @tables = split /,/, $table;
  my $tableStr = q{'} . (join q{','}, @tables) . q{'};

#
# Get column names and data types for this table
# precision is the total size of the number including decimal point
# scale is the number of places to the right of the decimal
# NUMBER(6,2) can hold a number between -999.99 and 999.99
#
  
  my $tableHashRef = {};

  my $sql = qq{SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
                 FROM ALL_TAB_COLUMNS 
                 WHERE TABLE_NAME in ($tableStr) AND OWNER=USER};

  my $sth = $dbh->prepare($sql);
  $sth->execute( );

  while( my $cols = $sth->fetchrow_hashref() ) {
      $tableHashRef->{ $cols->{'column_name'} } = {
           'type'      => $cols->{'data_type'},
           'length'    => $cols->{'data_length'},
           'precision' => $cols->{'data_precision'},
           'scale'     => $cols->{'data_scale'},
      };
  }
  $sth->finish();

  return $tableHashRef;

}

#
# Get the archive node information from the database
# and construct the archiveSiteStr.
#
sub getArchiveNodeInfo {

  my ($dbh, $archiveNode) = @_;

  my ($locationId, $archiveHost, $archiveRoot) = q{};

  my @archiveSites = ("N","N","N","N","N","N","N","N","N","N",
                      "N","N","N","N","N","N","N","N","N","N",
                      "N","N","N","N","N","N","N","N","N","N");

  my $sql = qq{
    SELECT location_id,archive_host,archive_root
      FROM archive_sites
      WHERE location_name = ?
  };

  my $sth = $dbh->prepare($sql);
  $sth->execute($archiveNode);

  $sth->bind_columns(\$locationId, \$archiveHost, \$archiveRoot);
  $sth->fetch();
  $sth->finish();

  $archiveSites[$locationId-1]="Y";

  my $archiveSiteStr = join(q{},@archiveSites);

  return ($locationId,$archiveHost,$archiveRoot,$archiveSiteStr);

}

#
# Get the ID of the entry in the EXPOSURE table
#
sub getExposureID {

  my ($dbh,$project,$exposureName,$nite) = @_;

  my @ids = ();
  my $id = 0;
  my $eventStr = q{};

  my $sql = qq{
    select ID from LOCATION where 
           FILETYPE     = 'src' AND
           PROJECT      = ? AND
           EXPOSURENAME = ? AND
           NITE         = ?
  };

  my $sth = $dbh->prepare($sql);
  $sth->execute($project,$exposureName,$nite);
  $sth->bind_columns(\$id);
  while ( $sth->fetch() ) {
    push @ids, $id;
  }

  $sth->finish();

  if ( scalar(@ids) > 1 ) {
    my $str = join ',',@ids;
    $eventStr =  
      qq{WARNING:  More than one id exists for this query in the EXPOSURE table:\n $str};
    reportEvent(2,'STATUS',4,$eventStr);
  } 

  if ( scalar(@ids) == 0 ) {
    $eventStr =  
      qq{WARNING:  EXPOSURE ID does not exist in the EXPOSURE table\n};
    reportEvent(2,'STATUS',4,$eventStr);
  } 

  return defined($id) ? $id : 0;

}

#
# Get the imageId of the parent file
#
sub getParentID {

  my ($dbh,$queryHashRef) = @_;

  my @ids = ();
  my $id = 0;
  my $ccd = q{};
  my $band = q{};
  my $eventStr = q{};
  my $childType = $queryHashRef->{'FILETYPE'};
  my $tileName = $queryHashRef->{'TILENAME'};
  my $childName = $queryHashRef->{'FILENAME'};
  $tileName = q{} if not $tileName;

#
# parentHash below hold the mapping of the children to the parent files
# The keys are the filetypes as they exist in the database.
# The values are an arrayRef with the first element = parent filetype
# the second element = the string in the filename that must be replaced in the
# filename, the third element = the string that goes into the filename to
# create the parent filename for the query.
#
  my %parentHash;
  $parentHash{'red'}          = ['raw_obj','',''];
  $parentHash{'red_cat'}      = ['red','_cat|_vig',''];
  $parentHash{'red_scamp'}    = ['red','_scamp',''];
  $parentHash{'red_shpltall'} = ['red','_shpltall',''];
  $parentHash{'red_shpltpsf'} = ['red','_shpltpsf',''];
  $parentHash{'red_psfcat'}   = ['red','_psfcat',''];
  $parentHash{'red_psf'}      = ['red_psfcat','_psf','_psfcat'];
  $parentHash{'remap'}        = ['red',"_$tileName",""];
  $parentHash{'remap_cat'}    = ['remap','_cat',''];
  $parentHash{'diff'}         = ['red','','$1'];
  $parentHash{'illumcor'}     = ['supersky','illumcor','supersky'];
  $parentHash{'fringecor'}    = ['supersky','fringecor','supersky'];

#
# TODO: Select all raw/red for a run and compare with that instead of a 
# query for each time.
#

#
# Parent filetype is the first element in the arrayRef
#
  my $parentType = $parentHash{$childType}->[0];

  $queryHashRef->{'FILETYPE'} = $parentType;

  #print "Child type:  $childType\n";
  #print "Parent type: $parentType\n";
  #print "Child file:  $queryHashRef->{'FILENAME'}\n";

#
# Replace string in filename with the third element in the arrayRef
#
  if ($parentType ne 'raw_obj'){
  $queryHashRef->{'FILENAME'} =~ 
     s/$parentHash{$childType}->[1]/$parentHash{$childType}->[2]/;
  } else {
  $queryHashRef->{'FILENAME'} =~ 
     s/$parentHash{$childType}->[1]/$1/;
  }

  #print "Parent file: $queryHashRef->{'FILENAME'}\n";
  delete $queryHashRef->{'TILENAME'} if ($tileName);

  my $whereClause = join " = ? AND ",keys(%$queryHashRef);
  $whereClause .= "= ?";

  my $sql = qq{SELECT ID,CCD,BAND FROM LOCATION WHERE $whereClause};

  my $sth = $dbh->prepare($sql);
  $sth->execute(values(%$queryHashRef));
  $sth->bind_columns(\$id,\$ccd,\$band);
  while ( $sth->fetch() ) {
    push @ids, $id;
  } 
  
  $sth->finish();

  if ( scalar(@ids) > 1 ) {
    my $str = join ',',@ids;
    $eventStr =  qq{ WARNING:  
    More than one parent id exists for this query in the IMAGE table:
    $str\n
    };
    reportEvent(2,'STATUS',4,$eventStr);
  } 

  if ( scalar(@ids) == 0 ) {
    $eventStr =  qq{ 
    WARNING:  
      The Parent Image of this file does not exist in the LOCATION Table
      childType:  $childType
      parentType: $parentType
      childName:  $childName
      parentName: $queryHashRef->{'FILENAME'}
    };
    reportEvent(2,'STATUS',4,$eventStr);
  } 

  return ($id,$ccd,$band);

}

sub ingestObjectsHashRef {

  my ($dbh,$tmpObjectsTable,$tmpObjectsHashRef) = @_;

  #my $valueClause = q{objects_seq.nextval,};
  my $valueClause = q{};
  my $objTableInfoHashRef = getTableInfo($dbh,$tmpObjectsTable);
  my $insertHashRef;
  my $rows = $tmpObjectsHashRef->{'OBJECT_NUMBER'};
  my $nRows = scalar(@$rows);
  my @emptyArr = (0) x scalar($nRows);
  my $num=0;
  my $eventStr = q{};

  my $t1 = new Benchmark;

  foreach my $key (keys %$objTableInfoHashRef){
    if ($key eq 'OBJECT_ID'){
      next;
    } else {
      $valueClause .= '?,';
      $insertHashRef->{$key} = $tmpObjectsHashRef->{$key};
    }
    $insertHashRef->{$key} = \@emptyArr if (! $tmpObjectsHashRef->{$key})
  }
  chop($valueClause);

  #my $keyStr = q{OBJECT_ID,} . join ',',keys %$insertHashRef;
  my $keyStr = join ',',keys %$insertHashRef;
  my $sql = qq{
    INSERT INTO $tmpObjectsTable ($keyStr) VALUES ($valueClause)
  };
  #foreach my $key (keys %$insertHashRef){
  #  print "$key:  $insertHashRef->{$key}->[0]\n";
  #}
  #print $sql,"\n";
  #exit(0);

  my $sth = $dbh->prepare($sql);
  my @tupleStatus;

  my $tuples = $sth->execute_array(
      { ArrayTupleStatus => \@tupleStatus },
      (values %$insertHashRef)
  );

  if ($tuples) {
      $eventStr =  "Successfully inserted $tuples objects";
      reportEvent(2,'STATUS',1,$eventStr);
  } else {
      my $arrRef = $insertHashRef->{'OBJECT_NUMBER'};
      $num = scalar(@$arrRef);
      for my $tuple (0..@$arrRef-1) {
          my $status = $tupleStatus[$tuple];
          $status = [0, "Skipped"] unless defined $status;
          next unless ref $status;
          printf "Failed to insert (%s,%s): %s\n",
              $insertHashRef->{'OBJECT_NUMBER'}->[$tuple],
              $insertHashRef->{'MAGERR_APER_5'}->[$tuple], 
              $status->[1];
          $num--;
      }
  }

  $sth->finish();

  my $t2 = new Benchmark;

  my $td1 = timediff($t2,$t1);
  my $timeStr = timestr($td1);

  my $returnObj = ($tuples) ? $tuples : $num;

  #$eventStr = "ingestObjectsHashRef:  Ingested $returnObj objects:\n$timeStr\n";
  #reportEvent(2,'STATUS',1,$eventStr);

  return $returnObj;

}

sub ingestCoaddObjectsHashRef {

  my ($dbh,$coaddObjectsHashRef,$objTableInfoHashRef) = @_;

  my $valueClause = q{coadd_objects_seq.nextval,};
  my $insertHashRef;
  my $rows = $coaddObjectsHashRef->{'OBJECT_NUMBER'};
  my $nRows = scalar(@$rows);
  my @emptyArr = (0) x scalar($nRows);
  my $num=0;
  my $eventStr = q{};

  my $t1 = new Benchmark;
  my $count =1;
  #print Dumper($coaddObjectsHashRef->{'ELLIPTICITY_Y'});
  #exit(0);
  #foreach (keys %$objTableInfoHashRef){
  #  print "$count -> $_\n";
  #  $count++;
  #}
  #print $k[109],"\n";
  #exit(0);

  foreach my $key (keys %$objTableInfoHashRef){
    if ($key eq 'COADD_OBJECTS_ID'){
      next;
    } else {
      $valueClause .= '?,';
      $insertHashRef->{$key} = $coaddObjectsHashRef->{$key};
    }
    $insertHashRef->{$key} = \@emptyArr if (! $coaddObjectsHashRef->{$key})
  }
  chop($valueClause);

#  my $i = 1;
#  foreach my $key (keys %$insertHashRef){
#    if ($i == 66){
#      foreach (@{$insertHashRef->{'XMAX_IMAGE'}}){
#        print "$_\n" if ($_ > 9999);
#      }
#      exit(0);
#    }
#    $i++;
#    my $precision = $objTableInfoHashRef->{$key}->{'precision'};
#    my $scale = $objTableInfoHashRef->{$key}->{'scale'};
#    my $type = $objTableInfoHashRef->{$key}->{'type'};
#    if ($type eq 'NUMBER'){
#      my $ind = 0;
#      foreach my $t (@{$insertHashRef->{$key}}){
#        my $tempNum = $insertHashRef->{$key}[$ind];
#        if ($scale){
#          $tempNum = sprintf("%${precision}.${scale}f",$tempNum);
#          #print "$key (float)  $tempNum\n";
#        } else {
#          $tempNum = sprintf("%${precision}d",$tempNum);
#          #print "$key (int)  $tempNum\n";
#        }
#        $insertHashRef->{$key}[$ind] = $tempNum;
#        $ind++;
#      }
#    }
#
#  }

  my $keyStr = q{COADD_OBJECTS_ID,} . join ',',keys %$insertHashRef;
  my $sql = qq{
    INSERT INTO COADD_OBJECTS ($keyStr) VALUES ($valueClause)
  };

  my $sth = $dbh->prepare($sql);
  my @tupleStatus;

  my $tuples = $sth->execute_array(
      { ArrayTupleStatus => \@tupleStatus },
      (values %$insertHashRef)
  );

  if ($tuples) {
      $eventStr = "Successfully inserted $tuples objects";
      reportEvent(2,'STATUS',1,$eventStr);
  } else {
      my $arrRef = $insertHashRef->{'OBJECT_NUMBER'};
      $num = scalar(@$arrRef);
      print "$num\n";
      for my $tuple (0..@$arrRef-1) {
          my $status = $tupleStatus[$tuple];
          $status = [0, "Skipped"] unless defined $status;
          next unless ref $status;
          printf "Failed to insert (%s,%s,%s): %s\n",
              $insertHashRef->{'OBJECT_NUMBER'}->[$tuple],
              $insertHashRef->{'RA'}->[$tuple],
              $status->[1];
          $num--;
      }
  }

  $sth->finish();

  my $t2 = new Benchmark;

  my $td1 = timediff($t2,$t1);
  my $timeStr = timestr($td1);

  my $returnObj = ($tuples) ? $tuples : $num;

  $eventStr = "ingestObjectsHashRef:  Ingested $returnObj objects:\n$timeStr\n";
  reportEvent(2,'STATUS',1,$eventStr);

  return $returnObj;

}

sub updateCoaddObjectsHashRef {

  my ($dbh,$coaddObjectsHashRef,$coaddTableInfoHashRef,$catalogId) = @_;

  #$dbh->trace(5);
  my $rows = $coaddObjectsHashRef->{'OBJECT_NUMBER'};
  my $nRows = scalar(@$rows);
  my @emptyArr = (0) x scalar($nRows);
  my $num=0;
  my $eventStr = q{};

  my $t1 = new Benchmark;
  my $updateHashRef;
  my $idArrRef;

  foreach my $key (keys %$coaddTableInfoHashRef){
    if ($key eq 'COADD_OBJECTS_ID'){
      next;
    } else {
      $updateHashRef->{$key} = $coaddObjectsHashRef->{$key};
    }
    $updateHashRef->{$key} = \@emptyArr if !$coaddObjectsHashRef->{$key};
  }

  my $valueStr = join ' = ?, ', keys %$updateHashRef;
  $valueStr .= ' = ?';

  my $sql = qq{
    UPDATE COADD_OBJECTS SET ($valueStr) WHERE CATALOGID_G = ?
  };

  my $sth = $dbh->prepare($sql);

  my @tupleStatus;

  my $tuples = $sth->execute_array(
      { ArrayTupleStatus => \@tupleStatus },
      (values %$updateHashRef),
      $catalogId
  );

  if ($tuples) {
      $eventStr = "Successfully updated $tuples objects";
      reportEvent(2,'STATUS',1,$eventStr);
  } else {
      my $arrRef = $coaddObjectsHashRef->{'OBJECT_NUMBER'};
      $num = scalar(@$arrRef);
      for my $tuple (0..@$arrRef-1) {
          my $status = $tupleStatus[$tuple];
          $status = [0, "Skipped"] unless defined $status;
          next unless ref $status;
          printf "Failed to update (%s): %s\n",
              $coaddObjectsHashRef->{'OBJECT_NUMBER'}->[$tuple],
              $status->[1];
          $num--;
      }
  }

  $sth->finish();

  my $t2 = new Benchmark;

  my $td1 = timediff($t2,$t1);
  my $timeStr = timestr($td1);

  #my $returnObj = ($tuples) ? $tuples : $num;

  #print "ingestObjectsHashRef:  Ingested $returnObj objects:\n$timeStr\n";

  #return $returnObj;
  return 1;

}
sub getFileInfo {

  my ($dbh,$queryHashRef) = @_;
  my $eventStr = q{};

  my $whereClause = join " = ? AND ",keys %$queryHashRef;
  $whereClause .= " = ?";

  my $sql = qq{
    SELECT * FROM LOCATION WHERE $whereClause
  };

  my $sth = $dbh->prepare($sql);
  $sth->execute(values %$queryHashRef);
  
#
# $result is an array of hash refs with keys=column names 
#  and values=column value
#
  my $result = $sth->fetchall_arrayref({});
  $sth->finish();

  return $result;

}

#
# Drop a temporary table if it exists
#
sub dropTmpObjectsTable {

  my ($dbh,$tblName) = @_;

  my $tableExists = checkIfTableExists($dbh,$tblName);
  return if not $tableExists;

  my $sql = qq{
    drop table $tblName
  };

  my $sth = $dbh->prepare($sql);
  eval{
    $sth->execute();
  };

  if ($@){
    warn "Database error: $@\n";
    $sth->finish();
    return 0;
  }

  $sth->finish();

  print "$tblName dropped from database\n";

  return 1;

}

sub cleanObjHashRef{

  my ($objHashRef,$tblHashRef) = @_;

  foreach my $key (keys %$tblHashRef){
    if ($tblHashRef->{$key}->{'type'} eq 'VARCHAR2'){
    } else {
      my $precision = $tblHashRef->{$key}->{'precision'};
      my $scale = $tblHashRef->{$key}->{'scale'};
      my $str ="%" . $precision . "." . $scale . "f" 
         if (defined $scale && defined $precision);
      my @tmpArr = ();
      foreach my $elem (@{$objHashRef->{$key}}){
        my $value = sprintf("$str",$elem);
#
# TODO:  Make this smarter and don't key on one specific field
#
        if ($key =~ m/MAGERR_APER/){
          $value = 99.00 if ($elem > 10);
        }
        push @tmpArr,$value;
      }
      $objHashRef->{$key} = \@tmpArr;
    }
  }

  return $objHashRef;

}

sub getXYZ {

  my ($ra,$dec) = @_;

  my ($x,$y,$z) = 0;

  my $pr = pi/180.0;

  $ra *= $pr;
  $dec *= $pr;

  my $cd = cos($dec);
  $x = cos($ra) * $cd;
  $y = sin($ra) * $cd;
  $z = sin($dec);

  return ($x,$y,$z);

}

sub getIDs {

  my ($dbh,$queryHashRef) = @_;
  my $whereClause = join " = ? AND ",keys(%$queryHashRef);
  $whereClause .= "= ?";

  my $sql = qq{ SELECT ID FROM LOCATION WHERE $whereClause };
  my $sth = $dbh->prepare($sql);
  $sth->execute(values %$queryHashRef);
  my $result = $sth->fetchall_arrayref();
  $sth->finish();

  return $result;
}

sub ingestCoaddZeropoints{

  my ($dbh, $zeropointHashRef) = @_;
  
  my $todayStr = ctime();
  $zeropointHashRef->{'ADD_DATE'} = 
     qq{to_date('$todayStr','Dy Mon DD HH24:MI:SS YYYY')};
  my $keyStr = join ',',(keys %$zeropointHashRef);
  my $valueStr = join ',', (values %$zeropointHashRef);

  my $sql = qq{
    INSERT INTO zeropoint (ZP_N,$keyStr)
           VALUES (zeropoint_seq.nextval,$valueStr)
  };
  my $sth = $dbh->prepare($sql);
  my $result = $sth->execute();
  $sth->finish();
  
  return $result;

}

sub getCoaddCatParentId {

  my ($dbh, $coaddCatId) = @_;
  
  my $sql = qq{SELECT PARENTID FROM CATALOG WHERE ID = ?};
  my $sth = $dbh->prepare($sql);
  $sth->execute($coaddCatId);
  my $result = $sth->fetchrow();
  $sth->finish();

  return defined $result ? $result : 0;

}

sub getCoaddZeropoint {

  my ($dbh,$coaddImageId) = @_;

  my $sql = qq{
    SELECT *
      FROM ZEROPOINT
      WHERE IMAGEID = ?
  };

  my $sth = $dbh->prepare($sql);
  $sth->execute($coaddImageId);
  my $result = $sth->fetchall_arrayref({});
  $sth->finish();

  return $result;
}

sub checkIfCatalogIngested{

  my ($dbh,$fileInfoHashRef,$tmpTable) = @_;
  my $catId = $fileInfoHashRef->{'CATALOGID'};
  my $year = (localtime())->[5]+1900;

  my $sql = qq{};
  my $sth;
  my $results;
  my $isIngested = 0;
    
  if (checkIfTableExists($dbh,$tmpTable)){

    $sql = qq{ SELECT COUNT(*) FROM $tmpTable WHERE CATALOGID = ?};
     $sth = $dbh->prepare($sql);
     $sth->execute($catId);
     $results = $sth->fetchall_arrayref();
     $isIngested = $results->[0][0];
  
  } else {

    $sql = qq{SELECT COUNT(*) FROM OBJECTS_CURRENT WHERE CATALOGID = ? };
    $sth = $dbh->prepare($sql);
    $sth->execute($catId);
    $results = $sth->fetchall_arrayref();
    $isIngested = $results->[0][0];

  }
  
  $sth->finish();

  return $isIngested;

}

sub checkIfCoaddCatalogIngested{

  my ($dbh,$catId,$band) = @_;
  $band = uc($band);

  my $sql = qq{
    SELECT count(*) FROM COADD_OBJECTS
           WHERE CATALOGID_$band = ?
  };
 
  my $sth = $dbh->prepare($sql);
  $sth->execute($catId);
  my $results = $sth->fetchall_arrayref();
  $sth->finish();

  return $results;

}

sub checkIfPhotozCatalogIngested{

  my ($dbh,$catId) = @_;

  my $sql = qq{
    SELECT count(*) FROM PHOTO_Z WHERE CATALOGID = ?
  };
 
  my $sth = $dbh->prepare($sql);
  $sth->execute($catId);
  my $results = $sth->fetchall_arrayref();
  $sth->finish();

  return $results;

}

sub checkIfWLCatalogIngested{

  my ($dbh,$catId,$tableName, $band) = @_;

if (not defined $band)
{
  $band = "";
}
else
{
  $band = "_".$band;
  $band = uc $band;
}

  my $sql = qq{
    SELECT count(*) FROM $tableName WHERE CATALOGID$band = ?
  };

  my $sth = $dbh->prepare($sql);
  $sth->execute($catId);
  my $results = $sth->fetchall_arrayref();
  $sth->finish();

  return $results;

}

sub deleteCoaddObjects{

   my ($dbh,$catalogId) = @_;

   my $sql = qq{ DELETE FROM COADD_OBJECTS WHERE CATALOGID_G=?};

   my $sth = $dbh->prepare($sql);
   my $numDeleted = $sth->execute($catalogId);
   $sth->finish();

   return $numDeleted;

}

sub isCatalogIngested{

  my ($dbh,$fileInfoHashRef,$tmpTable) = @_;

  my $fileType = $fileInfoHashRef->{'FILETYPE'};

  if ($fileType =~ m/red_cat|diff_cat|remap_cat/){
    return checkIfCatalogIngested($dbh,$fileInfoHashRef,$tmpTable);

  } elsif ($fileType =~ m/coadd_cat|coadd_psfcat/){
    return checkIfCoaddCatalogIngested($dbh,$fileInfoHashRef);

  }

}

sub ingestWLHashRef {

  my ($dbh,$insertHashRef,$wlTableInfoHashRef,$tableName,$sequencerName) = @_;

  my $valueClause = q{};
  my $keyStr = q{};
  my $rows = $insertHashRef->{'OBJECT_NUMBER'};
  my $nRows = scalar(@$rows);
  my @emptyArr = (0) x scalar($nRows);
  my $num=0;
  my $eventStr = q{};

  my $t1 = new Benchmark;
  my $count =1;

#
# Query the sequencer for that table
#
  if ($sequencerName) {
	$valueClause = "$sequencerName.nextval,";
  } else {
    $eventStr = "You must provide a sequencer for table $tableName";
    reportEvent(2,'STATUS',5,$eventStr);
    return;
  }

  # The PARTKEY is the same for all new rows
  $valueClause .= "'" . $insertHashRef->{'PARTKEY'} . "',";
  delete($insertHashRef->{'PARTKEY'});
  $keyStr = q{ID,PARTKEY,};

  # sorting keys not for sql, but easier to debug if problem
  my @thevalues = ();
  foreach my $key (sort keys %$wlTableInfoHashRef){
      if (! ($key eq 'ID' || $key eq 'PARTKEY')) {
          $valueClause .= '?,';
          $keyStr .= "$key,";

          # save arrays of values in exact same order 
          # as keys for use in execute_array
          if (! defined($insertHashRef->{$key})) {
              push(@thevalues, \@emptyArr);
          }
          else {
             push(@thevalues, $insertHashRef->{$key});
          }
      }
  }
  # chop off extra comma at end of clauses
  chop($valueClause);
  chop($keyStr);

  my $sql = qq{
    INSERT INTO $tableName ($keyStr) VALUES ($valueClause)
  };

  my $sth = $dbh->prepare($sql);
  my @tupleStatus;

  my $tuples = $sth->execute_array(
      { ArrayTupleStatus => \@tupleStatus },
      @thevalues
  );

  if ($tuples) {
      $eventStr = "Successfully inserted $tuples objects";
      reportEvent(2,'STATUS',1,$eventStr);
  } else {
      my $arrRef = $insertHashRef->{'OBJECT_NUMBER'};
      $num = scalar(@$arrRef);
      print "$num\n";
      for my $tuple (0..@$arrRef-1) {
          my $status = $tupleStatus[$tuple];
          $status = [0, "Skipped"] unless defined $status;
          next unless ref $status;
          printf "Failed to insert (%s): %s\n",
              $insertHashRef->{'OBJECT_NUMBER'}[$tuple],
              $status->[1];
          $num--;
      }
  }

  $sth->finish();

  my $t2 = new Benchmark;

  my $td1 = timediff($t2,$t1);
  my $timeStr = timestr($td1);

  my $returnObj = ($tuples) ? $tuples : $num;

  $eventStr = "ingestWLHashRef:  Ingested $returnObj objects:\n$timeStr\n";
  reportEvent(2,'STATUS',1,$eventStr);

  return $returnObj;

}

sub getWLObjects{

  my ($dbh, $wlCatalogId) = @_;

#
# Get all objectids from the red_cat from which this was made
#
  
  my $sql = qq{
         SELECT a.object_id FROM objects_current a, wl b WHERE 
                a.imageid = b.imageid AND b.id = $wlCatalogId
                ORDER BY a.object_number
  };

  my $sth=$dbh->prepare($sql);
  $sth->execute();
  my $wlObjectIds = $sth->fetchall_arrayref([]);
  $sth->finish();
  my @retArr;
  foreach my $ref (@{$wlObjectIds}){
    push @retArr,shift @$ref;
  }

  if (scalar @retArr == 0){
    my $eventStr = "No WL Objects returned for this catalogid:  $wlCatalogId";
    reportEvent(2,'STATUS',4,$eventStr);
  }

  return \@retArr;

}

sub deleteWLObjects{

  my ($dbh, $wlCatalogId, $tableName) = @_;

#
# Delete the WLObjects from WL object table
#
  
  my $sql = qq{
         DELETE from $tableName where catalogid = $wlCatalogId
  };

  my $sth=$dbh->prepare($sql);
  my $numDeleted = $sth->execute();
  $sth->finish();

  return $numDeleted;

}

sub getMESObjects{

  my ($dbh, $wlCatalogId, $band) = @_;

#
# Get all objectids from the coadd from which this was made
#
  
  my $sql = qq{
         SELECT a.coadd_objects_id FROM coadd_objects a, wl b WHERE 
                a.imageid_$band = b.imageid AND b.id = $wlCatalogId
                ORDER BY a.object_number
  };

  my $sth=$dbh->prepare($sql);
  $sth->execute();
  my $wlObjectIds = $sth->fetchall_arrayref([]);
  $sth->finish();
  my @retArr;
  foreach my $ref (@{$wlObjectIds}){
    push @retArr,shift @$ref;
  }

  if (scalar @retArr == 0){
    my $eventStr = "No WL Objects returned for this catalogid:  $wlCatalogId";
    reportEvent(2,'STATUS',4,$eventStr);
  }

  return \@retArr;

}

#
# Only return keys that exist in the table for ingetion
#
sub validateInsertHashRef{

  my ($dbh, $insertHashRef) = @_;
  my $washedHashRef;

  foreach my $tableName (keys %$insertHashRef){
    my $tableInfoHashRef = getTableInfo($dbh,$tableName);
    foreach my $key (keys %$tableInfoHashRef){
      next if ($key eq 'ID');
      $washedHashRef->{$tableName}->{$key} = 
             $insertHashRef->{$tableName}->{$key};
    }
  }

  return $washedHashRef;
}

1;
