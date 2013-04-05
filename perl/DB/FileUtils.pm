#
# FileUtils.pm
#
# DESCRIPTION:
#
# This module contains methods to use for general file processing
# for insertion into the DES database.
#
# AUTHOR:  Tony Darnell (tdarnell@uiuc.edu)
#
# $Rev: 7367 $
# $LastChangedBy:Ankit Chandra $
# $LastChangedDate: 2011-11-07 11:47:19 -0600 (Mon, 07 Nov 2011) $
#

package DB::FileUtils;

use strict;
use Astro::FITS::CFITSIO qw( :constants );
use Astro::FITS::CFITSIO qw( :longnames );
use Benchmark;
use Cwd;
use Data::Dumper;
use Regexp::Common;
use DB::EventUtils;
use DB::IngestUtils;
use File::Basename;
use File::Path;
use File::stat;
use Time::localtime;
use Switch;

require Exporter;
our @ISA = qw(Exporter);

our @EXPORT = qw{
  cleanHeader
  getMetaTable
  getParentIdFromHashRef
  getWLinfo
  headerResolve
  initializeTablesHashRefs
  filenameResolve
  isIngested
  parseFilelist
  printHashRef
  raconvert
  decconvert
  readCatalog
  readPhotozCatalog
  readWLCatalog
  setUpResolvePath
  sniffForCompressedFile
  updateFilelist
  validateHashRef
  updateArchiveSitesStr
  readFileList
  removeFileFromList
  verifyCoaddList
  writeFileList
  writeFitsBinaryTable
  rows2cols
};

#
# Reads Config::General style file list quickly.
# readFileList (<string filename IN>, <hasref INOUT>)
#
sub readFileList {
  my $file = shift;
  my $files = shift;
  my ($filehref);

  open (FH, "$file") or die "Cannot open $file";
  my @lines=<FH>;
  #while (my $line = <FH>) {
  foreach my $line (@lines) {
    chomp($line);
    $line =~ s/^\s*//;
    $line =~ s/\s*$//;
    if ($line eq "</file>") {
      push (@$files, $filehref);
    }
    elsif ($line eq "<file>") {
      $filehref = {};
    }
    else {
      (my $left, my $right) = split /\s*=\s*/, $line;
      $filehref->{"$left"} = $right;
    }
  }
  close FH;

	#return $files;
}


#
# write out filelist from array ref
#
sub writeFileList {

  my ($fileName, $filesArrRef) = @_;

  open (my $FH, ">$fileName") or die "Cannot open $fileName";

  foreach my $fileHashRef (@$filesArrRef){
    print $FH "<file>\n";
    while (my ($key, $value) = each %$fileHashRef){
      print $FH "\t$key = $value\n";
    }
    print $FH "</file>\n";
  }

  close ($FH)

}

#
# Clean header values of single quotes, leading and trailing whitespace.
#
sub cleanHeader{

  my ($hdrHashRef) = @_;

  foreach my $key (keys %$hdrHashRef){
    # remove single quotes
    $hdrHashRef->{$key} =~ s/'//gis;
    # remove leading spaces
    $hdrHashRef->{$key} =~ s/^\s+//;
    # remove trailing spaces
    $hdrHashRef->{$key} =~ s/\s+$//;
  }

  return $hdrHashRef;

}

sub filenameResolve{

  my ($fileNameWithPath) = @_;

  my $eventStr;
  my ($project,$fileClass,$run,$fileType,$exposureName,$fileName,$tileName) 
     = q{};
  
  my @splitArr = split /\//, $fileNameWithPath;
  
  my $numElems = scalar (@splitArr);
  if ($numElems == 6) {
    ($project,$fileClass,$run,$fileType,$exposureName,$fileName) = @splitArr;
  } elsif ($numElems == 5){
    ($project,$fileClass,$run,$fileType,$fileName) = @splitArr;
  } else {
    return -1;
  }

  my $tmpFileName = $fileName;
  $tmpFileName =~ s/(_im\.fits?|\.fit|\.fits|\.fits\.gz|\.fits\.fz)$//;

  my ($tmp,$band,$nite,$oldRun,$imageName,$tmpFiletype) = q{};
  my $ccdNo = 0;
  ($tmp,$nite) = split /_/, $run; 

#
# These hash refs are not used, may be deleted at some point
#
  my $shearFiletypesHashRef;
  $shearFiletypesHashRef->{'shpltpsf'} = 'shapelet_shpltpsf';
  $shearFiletypesHashRef->{'shpltall'} = 'shapelet_shpltall';
  $shearFiletypesHashRef->{'shear'}    = 'shapelet_shear';
  $shearFiletypesHashRef->{'psfmodel'} = 'shapelet_psfmodel';
  $shearFiletypesHashRef->{'shpltcor'} = 'shapelet_shpltcor';

  my $coaddFiletypesHashRef;
  $coaddFiletypesHashRef->{'cat'} = 'coadd_cat';
  $coaddFiletypesHashRef->{'vig'} = 'coadd_psfcat';
  $coaddFiletypesHashRef->{'psf'} = 'coadd_psf';
  $coaddFiletypesHashRef->{'det'} = 'coadd_det';
  $coaddFiletypesHashRef->{'msk'} = 'mask';
  $coaddFiletypesHashRef->{'nrm'} = 'norm';

  my @tmpArr = split /_/,$tmpFileName;
  if ($fileClass eq 'red'){

    if ($fileType eq 'raw'){
      $ccdNo = pop @tmpArr;
      $exposureName = join '_', @tmpArr;
    } elsif ( ($fileType eq 'bpm') || ($fileType eq 'biascor')) {
      $ccdNo = pop @tmpArr;
    } elsif ( ($fileType eq 'flatcor') || ($fileType eq 'illumcor') ) {
      $ccdNo = pop @tmpArr;
      $band = pop @tmpArr;
    } elsif ($fileType eq 'pupil') {
      $ccdNo = pop @tmpArr;
    } elsif ($fileType eq 'fringecor') {
      $ccdNo = pop @tmpArr;
      $band = pop @tmpArr;
      ($fileType,$band,$ccdNo) = split /_/,$tmpFileName;
    } elsif ($fileType eq 'darkcor') {
      ($fileType,$ccdNo) = split /_/,$tmpFileName;
    } elsif ($fileType eq 'supersky') {
      ($fileType,$band,$ccdNo) = split /_/,$tmpFileName;
    } elsif ($fileType eq 'diff') {
      my @tmpArr = split /_/,$tmpFileName;
      pop @tmpArr;
      $ccdNo = pop @tmpArr;
      $exposureName = join '_', @tmpArr;
      $fileType = 'diff_nitecmb' if ($tmpFileName =~ m/\_nitecmb/);
      $fileType = 'diff_distmp' if ($tmpFileName =~ m/\_distmp/);
      $fileType = 'diff_ncdistmp' if ($tmpFileName =~ m/\_ncdistmp/);
    } elsif ($fileType eq 'red') {
      my @tmpArr = split /_/,$tmpFileName;
      if ( $fileName =~ m/(red)?_cat/ ){
        $fileType = 'red_cat';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      }
      elsif($fileName =~ m/(red)?_ccat/) ## Change : Ankit added a new file type red_ccat
      { $fileType = 'red_ccat';
	pop @tmpArr;
	$ccdNo = pop @tmpArr;
	$exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/_psf\.fits/) {
        $fileType = 'red_psf';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/_psfcat/) {
        $fileType = 'red_psfcat';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/_scamp\.fits/) {
        $fileType = 'red_scamp';
        pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ( $fileName =~ m/shpltall/ ){
        $fileType = 'red_shpltall';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/shpltpsf/){
        $fileType = 'red_shpltpsf';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/_bkg/){
        $fileType = 'red_bkg';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/_vig/){
        $fileType = 'red_cat';
        pop @tmpArr;
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/_fullscamp/){
        $fileType = 'red_fullscamp';
        pop @tmpArr;
        $ccdNo = undef;
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/\.ahead/){
        $ccdNo = undef;
        pop @tmpArr;
        $fileType = 'red_ahead';
        $exposureName = join '_', @tmpArr;
      } elsif ($fileName =~ m/\.head/){
        $ccdNo = undef;
        pop @tmpArr;
        $fileType = 'red_head';
        $exposureName = join '_', @tmpArr;
      } else {
        $ccdNo = pop @tmpArr;
        $exposureName = join '_', @tmpArr;
      }
    } elsif ($fileType eq 'remap') {
      my @tmpArr = split /_/,$tmpFileName;
      if ($fileName =~ m/_cat|_psfcat/){
        pop @tmpArr;
      }
      $tileName = pop @tmpArr;
      $ccdNo = pop @tmpArr;
#
# This deals with tilenames that have E[0-9]_ in them
#
      if ($ccdNo =~ m/E[0-9]$/){
        $tileName = join '_', ($ccdNo,$tileName);
        $ccdNo = pop @tmpArr;
      }

      $exposureName = join '_', @tmpArr;
      if ($fileName =~ m/_cat\.fits/){
        $fileType = 'remap_cat';
      } elsif ($fileName =~ m/_psf\.fits/){
        $fileType = 'remap_psf';
      } elsif ($fileName =~ m/_psfcat/){
        $fileType = 'remap_psfcat';
      }
    } elsif ($fileType eq 'aux') {
      if ($fileName =~ m/astrostds/){
        $fileType = 'aux_astrostds';
      }
    } elsif ($fileType eq 'diff') {
      my @tmpArr = split /_/,$tmpFileName;
      $ccdNo = pop @tmpArr;
      $exposureName = join '_', @tmpArr;
      if ($fileName =~ m/_cat/){
        $fileType = 'catalog';
      }
    } elsif ($fileType =~ m/shapelet|shear|shpltall|shpltpsf/) {
      my @tmpArr = split /_/,$tmpFileName;
      my $tmpFiletype = pop @tmpArr;
      $ccdNo = pop @tmpArr;
      $exposureName = join '_', @tmpArr;
      $fileType = $shearFiletypesHashRef->{$tmpFiletype};
    }

  } elsif ($fileClass eq 'coadd'){

    my @fileBits = split /_/,$tmpFileName;
    my $lastBit = pop(@fileBits);
    $ccdNo = undef;
    $nite = undef;

    if ($fileType eq 'coadd'){

      if ($lastBit =~ m/(cat|vig|psf)/){
        $fileType = 'coadd_cat' if ($tmpFileName =~ m/_cat/);
        $fileType = 'coadd_psfcat' if ($tmpFileName =~ m/_vig/ || ($tmpFileName =~ m/_psfcat/ && $tmpFileName !~ m/\.psf/));
        $fileType = 'coadd_psf' if ($tmpFileName =~ m/_psfcat\.psf/);
        $band = pop(@fileBits);
        $tileName = join '_',@fileBits;
      } elsif ($lastBit =~ m/det/){
        $fileType = 'coadd_det' if ($tmpFileName =~ m/_det/);
        $tileName = join '_',@fileBits;
      } elsif (length($lastBit) == 1){
        $band = $lastBit;
        $tileName = join '_',@fileBits;
      }
      else{ # Change Ankit. Added this else condition to address JIRA 2142. This issue is coming up since the project requirements for CP are different. In this case, the band will have more than 1 character in it. 
        $band = $lastBit;
        $tileName = join '_',@fileBits;
	  my $eventStr = qq{Defaulting the band information to $lastBit. The file details are : project: $project,file class: $fileClass, run: $run,filetype: $fileType,File Name: $fileName,Tile Name: $tileName };
	  reportEvent(2,'STATUS',2,$eventStr);
      }

    } elsif ($fileType eq 'mask'){

      $tileName = $lastBit;
      my $t1 = shift @fileBits;
      my $t2 = shift @fileBits;
      $oldRun = join '_', ($t1,$t2);
      $ccdNo = pop @fileBits;
      $exposureName = join '_',@fileBits;

    } elsif ($fileType eq 'norm'){

      my $t1 = shift @fileBits;
      my $t2 = shift @fileBits;
      $oldRun = join '_', ($t1,$t2);
      $ccdNo = pop @fileBits;
      $exposureName = join '_', @fileBits;
      if ($tmpFileName =~ m/norm\_psfcat/){
        $fileType = 'norm_psfcat';
        $tileName = pop @fileBits;
        $ccdNo = pop @fileBits;
        $exposureName = join '_', @fileBits;
      }
      if ($tmpFileName =~ m/norm\_kern/){
        $fileType = 'norm_kern';
        $tileName = pop @fileBits;
        $ccdNo = pop @fileBits;
        $exposureName = join '_', @fileBits;
      }

      $tileName = $lastBit if ($fileType eq 'norm');

    } elsif ($fileType eq 'photoz'){

      $tileName = shift @fileBits;
      $fileType = 'photoz_zcat' if ($tmpFileName =~ m/\_zcat/);
      $fileType = 'photoz_cat' if ($tmpFileName =~ m/\_cat/);

    } elsif ($fileType eq 'shapelet'){
      $tileName = shift @fileBits;
      $band = shift @fileBits;
      $fileType = 'shapelet_mes' if ($tmpFileName =~ m/\_mes/);

    } else {

      ($tmp, $tileName) = split /\_/,$run;
      
    }

  } elsif ($fileClass eq 'diff'){

    if ($fileType eq 'diff'){
      pop @tmpArr if ($tmpFileName =~ m/\.cat$|\_distmp|\_ncdistmp|\_nitecmb|\_nc$/);
      $ccdNo = pop @tmpArr;
      $exposureName = join '_', @tmpArr;
      $fileType = 'diff_nitecmb' if ($tmpFileName =~ m/\_nitecmb/);
      $fileType = 'diff_distmp' if ($tmpFileName =~ m/\_distmp/);
      $fileType = 'diff_cat' if ($tmpFileName =~ m/\_cat\.cat$/);
      $fileType = 'diff_ncdistmp' if ($tmpFileName =~ m/\_ncdistmp/);
      $fileType = 'diff_nc' if ($tmpFileName =~ m/\_nc$/);
      $fileType = 'diff_nccat' if ($tmpFileName =~ m/\_nccat\.cat/);
    }
   elsif($fileType eq 'diff_nitecmb_diff')
   {
	if( $fileName =~ /SEARCH\-(.*)_(.*)_expos([0-9]{1,2})\+(.*)\.(.*)/ )
	{
		#$fileName = "decam-25-42-".$1.'-'.$3.'_'.$2;
	      $exposureName = 'decam--25--42-'.$1.'-'.$3;  #join '_', @tmpArr;
	}
	elsif( $fileName =~ /SEARCH\-(.*)_(.*)_nitecmb\+(.*)\.(.*)/ )
	{
		switch($1)
                {
                        case 'g' {
                               # $fileName = "decam-25-42-".$1.'-1'.'_'.$2;
			      $exposureName = 'decam--25--42-'.$1.'-1';  #join '_', @tmpArr;
                        }
                        case 'r' {
                                #$fileName = "decam-25-42-".$1.'-3'.'_'.$2;
			      $exposureName = 'decam--25--42-'.$1.'-3';  #join '_', @tmpArr;
                        }
                        case 'i' {
                                #$fileName = "decam-25-42-".$1.'-7'.'_'.$2;
			      $exposureName = 'decam--25--42-'.$1.'-7';  #join '_', @tmpArr;
                        }
                        case 'z' {
                                #$fileName = "decam-25-42-".$1.'-13'.'_'.$2;
			      $exposureName = 'decam--25--42-'.$1.'-13';  #join '_', @tmpArr;
                        }
                }
	}
	else
	{
	    $eventStr =  "Cannot match the filetype in filenameResolve to get the exposure name for filetype $fileType ";
	    reportEvent(2,'STATUS',3,$eventStr);
	}
  }
  elsif( $fileType eq 'diff_nitecmb_srch')
  {
	if( $fileName =~ /(.*)\-(.*)\-(.*)\_([0-9]{1,2})\+(.*)\.(.*)/ )
	{
	      $exposureName = 'decam--25--42-'.$2.'-'.$3;  #join '_', @tmpArr;
		#$fileName = "decam-25-42-".$2.'-'.$3.'_'.$4;
	}
	else
	{
	    $eventStr =  "Cannot match the filetype in filenameResolve to get the exposure name for filetype $fileType";
	    reportEvent(2,'STATUS',3,$eventStr);
	}
  }
  elsif( $fileType eq 'diff_nitecmb_temp'){

	if( $fileName =~ /(.*)-(.*)_(.*)\_expos([0-9]{1,2})\.(.*)/ )
	{
		#$fileName = "decam-25-42-".$2.'-'.$4.'_'.$3;
	      $exposureName = 'decam--25--42-'.$2.'-'.$4;  #join '_', @tmpArr;
	}
	else
	{
	    $eventStr =  "Cannot match the filetype in filenameResolve to get the exposure name for filetype $fileType";
	    reportEvent(2,'STATUS',3,$eventStr);
	}
  }
	

  } elsif ($fileClass eq 'src'){

#
# $run will equal the nite of observing in this case.
# Filetype of src can lead to two cases:
#   src:  MEF's from telecope
#
    $nite = $run;
    $run = q{};
    $ccdNo = undef;
    $exposureName = $tmpFileName;
  } elsif ($fileClass eq 'cal'){
    $nite = q{};
    if ($fileType =~ m/(bpm|pupil|biascor|darkcor|photflatcor)/){
      ($fileType,$ccdNo) = split /_/, $tmpFileName;
      $exposureName = $tmpFileName;
    } elsif ($fileType !~ m/xtalk|fixim/) {
      ($fileType,$band,$ccdNo) = split /_/, $tmpFileName;
    } elsif ($fileType eq 'supersky') { # JIRA 2257
        ($fileType,$band,$ccdNo) = split /_/,$tmpFileName;
    } elsif ($fileType eq 'flatcor') { # JIRA 2257
      $ccdNo = pop @tmpArr;
      $band = pop @tmpArr;
    }
 
  } elsif ($fileClass eq 'wl'){
    #if ($fileType !~ m/aux|log|etc|xml|qcshapelet|runtime/){
    if ($fileType !~ m/aux|qa|log|etc|xml|qcshapelet|runtime/){
      ($oldRun,$nite,$imageName,$tmp,$tmpFiletype) = split /_/,$tmpFileName;
      $oldRun = join '_', ($oldRun,$nite);
      ($tmp,$tileName) = split /_/, $run;
      $ccdNo = $tmp if ($tmp !~ m/shear|shplt|psfmodel/);
      $exposureName = $imageName;
      $fileType = $shearFiletypesHashRef->{$tmpFiletype};
    }
  }

#
# Stuff the returned hash ref
#
  my $retHashRef ;
  $retHashRef->{'FILECLASS'}    = $fileClass; 
  $retHashRef->{'FILETYPE'}     = $fileType; 
  $retHashRef->{'RUN'}          = $run;
  $retHashRef->{'OLDRUN'}       = defined $oldRun ? $oldRun : undef;
  $retHashRef->{'NITE'}         = defined $nite ? $nite : undef;
  $retHashRef->{'BAND'}         = defined $band ? $band : undef;
  $retHashRef->{'TILENAME'}     = defined $tileName ? $tileName : undef;
  $retHashRef->{'EXPOSURENAME'} = defined $exposureName ? $exposureName : undef;
  $retHashRef->{'CCD'}          = defined $ccdNo ? sprintf("%d",$ccdNo) : undef;
  $retHashRef->{'PROJECT'}      = $project;
  $retHashRef->{'FILENAME'}     = $fileName;

  return $retHashRef;

}

#
# This routine loops through all keys in a fits header hash ref
# and compares the datatypes appropriate for the table being
# ingested.
#
sub headerResolve {

  my ($dbh,$hdrHashRef,$tableHashRef,$filename,$notSrc) = @_;
  my $eventStr = qq{Resolving header for: $filename};
  #reportEvent(2,'STATUS',1,$eventStr);

#
# Get column names and data types for this table
#
  
  my $resolvedHashRef = {};

#
# Go through each element in the tableHashRef and insert
# the proper value and type into it from hdrHashRef
# If the type is not a VARCHAR, assume it's a number.
# Still have to make sure the value in hdrHasRef is a valid
# number.  Sometimes NOAO inserts error messages where numbers
# should be.
#
  #print Dumper $tableHashRef;

#
# Validate hdrHashRef data first, make sure numbers are numbers and
# strings are strings, etc.
#

  foreach my $key (keys %$tableHashRef){

    if ($tableHashRef->{$key}->{'type'} eq 'VARCHAR2'){ # It's a string
      if ($key eq 'TELESCOPE'){ # Translate some keywords
          if (not $hdrHashRef->{'TELESCOP'}) {
            $eventStr =  "Header value for $key is NOT defined in $filename.";
            reportEvent(2,'STATUS',4,$eventStr);
            next;
          }
        if ( $hdrHashRef->{'TELESCOP'} =~ m/CTIO 4.0 meter telescope/ ){
          $resolvedHashRef->{$key} = qq{Blanco 4m};
        } elsif ( $hdrHashRef->{'TELESCOP'} =~ m/KPNO 4.0 meter telescope/ ){
          $resolvedHashRef->{$key} = qq{KPNO 4m};
        } else {
          $resolvedHashRef->{$key} = $hdrHashRef->{'TELESCOP'};
        }
      } elsif ($key eq 'IMAGETYPE'){
          $resolvedHashRef->{'IMAGETYPE'} = $hdrHashRef->{'OBSTYPE'};
      } elsif ($key eq 'BAND'){
        next if $filename =~ m/focus|distmp|zcat|shplt|shear|bpm|bias|pupil|cat|scamp|vig|psf|det|kern|photflatcor|mes/;
        if (not $hdrHashRef->{'FILTER'}){
          $eventStr =  "Header value for $key is NOT defined in $filename";
          reportEvent(2,'STATUS',4,$eventStr);
          next;
        }
        if (length($hdrHashRef->{'FILTER'}) > 1){
          $eventStr = qq{WARNING:  headerResolve() Header value for FILTER is longer than 1 char: $hdrHashRef->{'FILTER'} };
          reportEvent(2,'STATUS',3,$eventStr) if $notSrc;
        }
        $resolvedHashRef->{$key} = $hdrHashRef->{'FILTER'};
      } elsif ($key eq 'DATE_OBS'){
        $resolvedHashRef->{$key} = $hdrHashRef->{'DATE-OBS'};
      } elsif ($key eq 'TIME_OBS'){
        $resolvedHashRef->{$key} = $hdrHashRef->{'TIME-OBS'};
      } elsif ($key eq 'OBSERVATORY'){
        $resolvedHashRef->{$key} = $hdrHashRef->{'OBSERVAT'};
      } elsif ($key eq 'MJD_OBS'){
        $resolvedHashRef->{$key} = $hdrHashRef->{'MJD-OBS'};
      } elsif ($key eq 'LATITUDE'){
        if ($hdrHashRef->{'LATITUD'}){
          $resolvedHashRef->{$key} = $hdrHashRef->{'LATITUD'};
        } elsif ($hdrHashRef->{'OBS-LAT'}){
          $resolvedHashRef->{$key} = $hdrHashRef->{'OBS-LAT'};
        } else {
          $resolvedHashRef->{$key} = "-30.1662500";
        }
      } elsif ($key eq 'LONGITUDE'){
        if ($hdrHashRef->{'LONGITUD'}){
          $resolvedHashRef->{$key} = $hdrHashRef->{'LONGITUD'};
        } elsif ($hdrHashRef->{'OBS-LONG'}){
          $resolvedHashRef->{$key} = $hdrHashRef->{'OBS-LONG'};
        } else {
          $resolvedHashRef->{$key} = "-70.8151111";
        }
      } elsif ($key eq 'ALTITUDE'){
        if ($hdrHashRef->{'OBS-ELEV'}){
          $resolvedHashRef->{$key} = $hdrHashRef->{'OBS-ELEV'};
        } else {
          $resolvedHashRef->{$key} = 'NULL';
        }
      } elsif ($key eq 'WINDSPD'){ # These are supposed to be numbers
        $resolvedHashRef->{$key} = $hdrHashRef->{$key};
      } elsif ($key eq 'WINDDIR'){ 
        $resolvedHashRef->{$key} = $hdrHashRef->{$key};
      } elsif ($key eq 'HUMIDITY'){
        $resolvedHashRef->{$key} = $hdrHashRef->{$key};
      } elsif ($key eq 'PRESSURE'){
        $resolvedHashRef->{$key} = $hdrHashRef->{$key};
      } elsif ($key eq 'INSTRUMENT'){
        $resolvedHashRef->{$key} = $hdrHashRef->{'INSTRUME'};
      } else { # Use what's in the header, no need to translate
        if (defined $hdrHashRef->{$key}) {
          $resolvedHashRef->{$key} = $hdrHashRef->{$key};
        } else {
          $resolvedHashRef->{$key} = qq{};
        }
      }
      #$resolvedHashRef->{$key} =~ s/(')//gis; # In case we missed something
    } else { # It's a number

      $hdrHashRef->{$key} = 0.0 if not defined $hdrHashRef->{$key};

      if ( $key eq 'TELRA') {
        $hdrHashRef->{$key} =~ s/(\s|')//gis;
        next if $filename =~ m/flat|bpm|bias/;
        next if $hdrHashRef->{'OBSTYPE'} =~ m/flat|bpm|bias/i;

        my $ra = sprintf("%0.5f",raconvert($hdrHashRef->{'TELRA'}) );
        $resolvedHashRef->{$key} = $ra;

        if ($ra == 999) {
          $eventStr =  qq{WARNING:  HeaderResolve() Header value for $key is NOT valid in $filename};
          reportEvent(2,'STATUS',3,$eventStr);
        }
      } elsif ($key eq 'TELDEC'){
        next if $filename =~ m/flat|bpm|bias/;
        next if $hdrHashRef->{'OBSTYPE'} =~ m/flat|bpm|bias/i;

        my $dec = sprintf("%0.5f",decconvert($hdrHashRef->{'TELDEC'}) );

        $resolvedHashRef->{$key} = $dec;

        if ($dec == 999){
          $eventStr =  qq{WARNING:  HeaderResolve() Header value for $key is NOT valid in $filename};
          reportEvent(2, 'STATUS',3,$eventStr);
        }

      } elsif ($key eq 'MOONANGLE'){
        $resolvedHashRef->{$key} = $hdrHashRef->{'MOONANGL'};
      } else { 

        my $testNum = $hdrHashRef->{$key};
        $testNum = 
          ($testNum =~ m/^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/) ? 
          $testNum : sprintf("%f",0.0);
        $hdrHashRef->{$key} = $testNum;
        $resolvedHashRef->{$key} = $testNum;

        if ($key eq 'TELEQUIN'){
          $resolvedHashRef->{$key} = sprintf("%f",2000.0);
        } elsif ($key eq 'CCD') { 
          next if $filename =~ m/distmp|zcat|shplt|shear|bpm|pupil|cat|scamp|vig|psf|kern|mes/;
          if (not $hdrHashRef->{'CCDNUM'}) {
            $eventStr =  "Header value for $key is NOT defined in $filename.";
            reportEvent(2,'STATUS',4,$eventStr);
            next;
          } else {
            $hdrHashRef->{'CCDNUM'} =~ s/(\s|')//gis;
          }
          $resolvedHashRef->{'CCD'} = $hdrHashRef->{'CCDNUM'};
        } elsif ( $key eq 'MJD_OBS' ) {
          $resolvedHashRef->{$key} = $hdrHashRef->{'MJD-OBS'};
        } else { # Use whatever's in the hashref, no need to translate
          if (defined $hdrHashRef->{$key}){
            $hdrHashRef->{$key} =~ s/(\s|')//gis;
            $resolvedHashRef->{$key} = $hdrHashRef->{$key};
          } else {
            $resolvedHashRef->{$key} = 0.0;
          }
        }
      }
    }
  }

  $resolvedHashRef->{'MGZPTERR'} = $hdrHashRef->{'MGZPTERR'} 
    if $hdrHashRef->{'MGZPTERR'};
  $resolvedHashRef->{'SEXMGZPT'} = $hdrHashRef->{'SEXMGZPT'}
    if $hdrHashRef->{'SEXMGZPT'};

  return $resolvedHashRef;

}

sub raconvert {

  my $rastring = shift;

  $rastring =~ s/(\s|')//gis;
  return 999 if ( $rastring =~ /available/gis );

  my ($rah,$ram,$ras) = split /:/,$rastring;

  #return -999999 if ( (not defined ($rah)) || ($rah !~ m/^$RE{num}{int}$/));
  return 999 if ( not defined ($rah) );
  return 999 if ( not defined ($ram) );
  return 999 if ( not defined ($ras) );

  my $val = $rah + ($ram/60.0) + ($ras/3600.0);
  $val *= 15.0;

  return $val;

}

sub decconvert {

  my $decstring = shift;
  $decstring =~ s/(\s|')//gis;

  my $val = 999;
  return $val if ( $decstring =~ /available/gis );

  my ($decd,$decm,$decs) = split /:/,$decstring;

  #return $val if ( (not defined ($decd)) || ($decd !~ m/^$RE{num}{int}$/));
  return $val if (not defined ($decd));
  return $val if (not defined ($decm));
  return $val if (not defined ($decs));
  
  if ($decd >= 0.0){
    $val = $decd + ($decm/60.0) + ($decs/3600.0);
  } else {
    $val = $decd - ($decm/60.0) - ($decs/3600.0);
  }

  return $val;

}

#
# Build up filepath to be resolved using DESDM archive schema
# $fileHashRef is the hashRef constructed from reading a file
# from Config::General
#
sub setUpResolvePath {

  my ($fileHashRef) = @_;

  my $resolvePath = q{};

  if ( $fileHashRef->{'project'}   &&
       $fileHashRef->{'fileclass'} &&
       ($fileHashRef->{'run'} || $fileHashRef->{'nite'}) &&
       $fileHashRef->{'filetype'}
     )
  {
    $resolvePath = uc(qq($fileHashRef->{'project'}/)) .
                    qq($fileHashRef->{'fileclass'}/) .
                    (defined($fileHashRef->{'run'}) ?
                    qq($fileHashRef->{'run'}/) : qq($fileHashRef->{'nite'}/)) .
                    qq($fileHashRef->{'filetype'}) .
                    (defined($fileHashRef->{'exposurename'}) ?
                       qq(/$fileHashRef->{'exposurename'}) : q{});
   } else {

     my $tempPath = $fileHashRef->{'localpath'};
     $tempPath = $fileHashRef->{'LOCALPATH'} if not defined $tempPath;
     $tempPath =~ s/^(.*?)\/Archive\///;
     $resolvePath = $tempPath;

   }
     

  return $resolvePath;

}

sub readCatalog {

  my ($filenameWithPath) = @_;
  my $eventStr = q{};

#
# Open up the fits file 
#
    my $status = 0;
    my $fptr = Astro::FITS::CFITSIO::open_file(
        $filenameWithPath,
        READONLY,
        $status
        );

    my $catFilename = basename($filenameWithPath);
    if ($status){
      $eventStr =  "Problem opening $catFilename:  $status\n";
      reportEvent(2,'STATUS',4,$eventStr);
      $status=0;
    }
    my ($hduType,$nHdus,$nCols,$nRows) = 0;

    Astro::FITS::CFITSIO::fits_get_num_hdus($fptr,$nHdus,$status);

    if ($nHdus == 2){
      Astro::FITS::CFITSIO::fits_movabs_hdu($fptr,2,$hduType,$status);
    } elsif ($nHdus == 3){
      Astro::FITS::CFITSIO::fits_movabs_hdu($fptr,3,$hduType,$status);
    } else {
      $eventStr =  "$catFilename not in FITS_1.0 or FITS_LDAC format, check file\n";
      reportEvent(2,'STATUS',4,$eventStr);
    }

    Astro::FITS::CFITSIO::fits_get_hdu_type($fptr,$hduType,$status);
    Astro::FITS::CFITSIO::fits_get_num_rows($fptr,$nRows,$status);
    Astro::FITS::CFITSIO::fits_get_num_cols($fptr,$nCols,$status);

    if ($hduType == IMAGE_HDU){
      $eventStr =  "Error: this program only displays tables, not images\n";
      reportEvent(2,'STATUS',4,$eventStr);
    }

#
# Read in catalog table and populate the catalogHashRef that will do
# the insert.
#

    my ($typeCode,$colName,$width,$repeat,$nullPointer,$anyNul) = 0;
    $status=0;
    my $newStatus=0;
    my $catalogHashRef;
    for (my $i=1; $i<=$nCols; $i++){
      my @output = ();
      fits_get_coltype($fptr,$i,$typeCode,$repeat,$width,$status);
      fits_get_colname($fptr,CASEINSEN,'*',$colName,$i,$newStatus);
      $colName = 'OBJECT_NUMBER' if ($colName eq 'NUMBER');

#
# These two columns are really 2D arrays that are 6 cols by nRow rows.
# Sorting this using normal C style indexing for clarity.
# $repeat has the number of elements in the vector
#
      if ( $repeat > 1 ) {
        fits_read_col($fptr,$typeCode,$i,1,1,$nRows*$repeat,0,\@output,$anyNul,$status);
        for (my $k=1;$k<=$repeat;$k++){
          my @tempArr;
          for (my $j=0; $j<$nRows; $j++){
            $tempArr[$j] = $output[$j*$repeat+($k-1)];
          }
          my $tmpColName = $colName . "_$k";
          $catalogHashRef->{$tmpColName}=\@tempArr;
        }
      } else {
        fits_read_col($fptr,$typeCode,$i,1,1,$nRows,0,\@output,$anyNul,$status);
        $catalogHashRef->{$colName}=\@output;
      }
    }
    fits_close_file($fptr,$status);

  return $catalogHashRef;

}

sub readWLCatalog {

  my ($filenameWithPath,$wlType) = @_;
  my $eventStr = q{};
  my $nCoeffs;

  $nCoeffs = 28 if ($wlType eq 'shapelet_shear');
  $nCoeffs = 66 if ($wlType eq 'shapelet_shpltpsf');

#
# Open up the fits file 
#
    my $status = 0;
    my $fptr = Astro::FITS::CFITSIO::open_file(
        $filenameWithPath,
        READONLY,
        $status
        );

    my $catFilename = basename($filenameWithPath);
    if ($status){
      $eventStr =  "Problem opening $catFilename:  $status\n";
      reportEvent(2,'STATUS',4,$eventStr);
      $status=0;
    }
    my ($hduType,$nHdus,$nCols,$nRows) = 0;

    Astro::FITS::CFITSIO::fits_get_num_hdus($fptr,$nHdus,$status);
    Astro::FITS::CFITSIO::fits_movabs_hdu($fptr,$nHdus,$hduType,$status);

    Astro::FITS::CFITSIO::fits_get_hdu_type($fptr,$hduType,$status);
    Astro::FITS::CFITSIO::fits_get_num_rows($fptr,$nRows,$status);
    Astro::FITS::CFITSIO::fits_get_num_cols($fptr,$nCols,$status);

    if ($hduType != BINARY_TBL){
      $eventStr =  "Error: this program only reads tables\n";
      reportEvent(2,'STATUS',4,$eventStr);
    }

#
# Read in catalog table and populate the catalogHashRef
#

    my ($typeCode,$colName,$width,$repeat,$nullPointer,$anyNul) = 0;
    $status=0;
    my $newStatus=0;
    my $catalogHashRef;
    for (my $i=1; $i<=$nCols; $i++){
      my @output = ();
      fits_get_coltype($fptr,$i,$typeCode,$repeat,$width,$status);
      fits_get_colname($fptr,CASEINSEN,'*',$colName,$i,$newStatus);
      $colName = 'OBJECT_NUMBER' if ($colName eq 'id');

#
# These columns are really 2D arrays that are nCoeffs cols by nRow rows.
# Sorting this using normal C style indexing for clarity.
#
      if ( 
           ($colName eq 'shapelets_prepsf')  ||
           ($colName eq 'shapelets')
         ){

        fits_read_col($fptr,$typeCode,$i,1,1,$nRows*$nCoeffs,0,\@output,$anyNul,$status);
        for (my $k=1;$k<=$nCoeffs;$k++){
          my @tempArr;
          for (my $j=0; $j<$nRows; $j++){
            $tempArr[$j] = $output[$j*$nCoeffs+($k-1)];
          }
          my $tmpColName = "COEFFS_" . "$k";
          $catalogHashRef->{$tmpColName}=\@tempArr;
        }
      } else {
        fits_read_col($fptr,$typeCode,$i,1,1,$nRows,0,\@output,$anyNul,$status);
        $catalogHashRef->{uc($colName)}=\@output;
      }
    }
    fits_close_file($fptr,$status);

  return $catalogHashRef;
}

sub readPhotozCatalog {

  my ($filenameWithPath,$photozType) = @_;
  my $eventStr = q{};

#
# Open up the fits file 
#
    my $status = 0;
    my $fptr = Astro::FITS::CFITSIO::open_file(
        $filenameWithPath,
        READONLY,
        $status
        );

    my $catFilename = basename($filenameWithPath);
    if ($status){
      $eventStr =  "Problem opening $catFilename:  $status\n";
      reportEvent(2,'STATUS',4,$eventStr);
      $status=0;
    }
    my ($hduType,$nHdus,$nCols,$nRows) = 0;

    Astro::FITS::CFITSIO::fits_get_num_hdus($fptr,$nHdus,$status);
    Astro::FITS::CFITSIO::fits_movabs_hdu($fptr,$nHdus,$hduType,$status);

    Astro::FITS::CFITSIO::fits_get_hdu_type($fptr,$hduType,$status);
    Astro::FITS::CFITSIO::fits_get_num_rows($fptr,$nRows,$status);
    Astro::FITS::CFITSIO::fits_get_num_cols($fptr,$nCols,$status);

    if ($hduType != BINARY_TBL){
      $eventStr =  "Error: this program only reads tables\n";
      reportEvent(2,'STATUS',4,$eventStr);
    }
    
#
# Read in catalog table and populate the catalogHashRef
#

    my ($typeCode,$colName,$width,$repeat,$nullPointer,$anyNul) = 0;
    $status=0;
    my $newStatus=0;
    my $catalogHashRef;
    for (my $i=1; $i<=$nCols; $i++){

      my @output = ();
      fits_get_coltype($fptr,$i,$typeCode,$repeat,$width,$status);
      fits_get_colname($fptr,CASEINSEN,'*',$colName,$i,$newStatus);
      fits_read_col($fptr,$typeCode,$i,1,1,$nRows,0,\@output,$anyNul,$status);
      $catalogHashRef->{uc($colName)}=\@output;

    }

    fits_close_file($fptr,$status);

  return $catalogHashRef;

}

sub printHashRef{
   
   my $hashRef = shift;

   foreach my $key (sort(keys %$hashRef)){

    print "$key -> $hashRef->{$key}\n";

   }

}

#
# Parses filelist an returns a hashRef sorted by filetype
#
sub parseFilelist {

  my ($fileList,$archiveSiteStr,$getKeywords,$skipOnFileId,$quiet) = @_;

  my @resolvedFilenamesArr  = ();
  my %runIDS;
  my %nites;
  my $eventStr = q{};

  my @raws  = ();
  my @reds  = ();
  my @red_psfcats   = ();
  my @masks   = ();
  my @norm_psfcats   = ();
  my @remaps   = ();
  my @superskys   = ();
  my @coadds   = ();
  my @coadd_cats   = ();
  my @shpltalls   = ();
  my @shpltpsfs   = ();
  my @shears   = ();
  my @diffs   = ();
  my @diff_ncs   = ();
  my @diff_nitecmbs   = ();
  my @diff_distmps   = ();
  my @diff_ncdistmps   = ();
  my @photoz_cats   = ();
  my @everythingElses   = ();
  my $currProject = undef;

#
# $skipOnFileId overrides file_ingest.pl behavior of not parsing the file
# if the fileId > 0
# If skipOnFileId = 1, then file will not be parsed if fileid > 0 (default)
# If skipOnFileId = 0, file will be parsed if fileid > 0.
#
  $skipOnFileId = 1 if not defined $skipOnFileId;
# Change: Added by Ankit to avoid running the updateFileList function. This variable will capture the position of an object in the file array and hence can be used to directly update the field with fileid

  my $fileLoopPosition = 0;
#Change end
  foreach my $file (@$fileList){
#
# Don't bother with any of this if the file is marked as ingested
#
    if ( ($skipOnFileId == 1) && $file->{'fileid'} ){
      $eventStr =  "Skipping: $file->{'localfilename'}";
      reportEvent(2,'STATUS',1,$eventStr);
      next;
    }

    my $baseName = $file->{'localfilename'};

    #$eventStr =  "Parsing: $baseName";
    #reportEvent(2,'STATUS',1,$eventStr);

    if (not defined $baseName){
      $eventStr =  qq{local filename is undefined};
      reportEvent(2,'STATUS',4,$eventStr);
      next;
    }

    my $localPath = $file->{'localpath'};
#
# if localpath is not absolute, set it to the current dir and append
# localpath to cwd.
#

    if ( not defined $localPath ){
      $localPath = cwd();
    }

#
# Get the properties to build the location data with.
# These properties MUST MATCH the archive directory structure
# or filenameResolve will fail.
#
    my $resolvePath = setUpResolvePath($file);

#
# Initial population of $fileInfoHashRef
#
    my $fileName = qq{$resolvePath/} .
      (defined($file->{'filename'}) ?
       $file->{'filename'} : $baseName);

    my $fileInfoHashRef   = filenameResolve($fileName);

    if ($fileInfoHashRef == -1){
      $eventStr = qq{FileName parsing error:  $fileName is not a valid DES fileName. FilenameResolve found issues with the filepath breakup. Do you have the correct filepath structure?};
      reportEvent(2,'STATUS',4,$eventStr);
      next;
    }

    if (defined($currProject) && $fileInfoHashRef->{'PROJECT'} ne $currProject){
      $eventStr = qq{More than one project in this filelist};
      reportEvent(2,'STATUS',4,$eventStr);
    }
    $currProject = $fileInfoHashRef->{'PROJECT'};

    $fileInfoHashRef->{'ARCHIVESITES'} = $archiveSiteStr;
    $fileInfoHashRef->{'LOCALPATH'}    = $localPath;
    $fileInfoHashRef->{'FILEID'}       = $file->{'fileid'};

# CHANG(: Ankit. insert a file iteration number into the fileinfo hash ref. this will help us identify the location of the file in the file list.
    $fileInfoHashRef->{'FILE_LOOP_POSITION'}       = $fileLoopPosition;
	#print "the file loop position is::"; print Dumper( $fileInfoHashRef->{'FILE_LOOP_POSITION'});
    $fileLoopPosition++;

# CHANGE: Ankit .

#
# Use any keys provided in the filelist instead of what was found in the 
# filename, path or header.  Set overwrite flag for that key so that when
# the header is read in, it doesn't get replaced.
#
    if ($getKeywords){
      foreach my $key (keys %$file){
        next if $key =~ m/(localfilename|localpath|fileid)/gis;
        if ($key =~ m/ccd/i){
          $file->{$key} = sprintf("%d",$file->{$key});
        }
        $fileInfoHashRef->{uc($key)} = $file->{$key};
        my $keyFlagStr = uc($key) . "_FLAG";
        $fileInfoHashRef->{$keyFlagStr} = 1;
      }
    }

    my $isValid =  validateHashRef($fileInfoHashRef);

    if (!$isValid){
      reportEvent(2,'STATUS',4,qq{$fileName is not a valid DES file. File Information: });
	print Dumper($fileInfoHashRef);
      next;
    }

    my $fileType = $fileInfoHashRef->{'FILETYPE'};
    my $fileClass = $fileInfoHashRef->{'FILECLASS'};

    if ($fileClass ne 'src' ){
      $runIDS{$fileInfoHashRef->{'RUN'}}++ if $fileInfoHashRef->{'RUN'};
      $nites{$fileInfoHashRef->{'NITE'}}++ if $fileInfoHashRef->{'NITE'};
      if ($fileClass eq 'wl' || $fileType eq 'mask' || $fileType =~ m/norm/){
        my @fileBits = split /_/,$fileInfoHashRef->{'FILENAME'};
        my $oldRun = join '_',($fileBits[0],$fileBits[1]);
        $runIDS{$oldRun}++ if $oldRun;
      }
    } else {
      $nites{$fileInfoHashRef->{'NITE'}}++ if $fileInfoHashRef->{'NITE'};
    }

    my $matchStr = qq{_cat|scamp|shplt|psf};
    if ($fileType eq 'raw'){
      push @raws, $fileInfoHashRef;
    } elsif ($fileType eq 'red' && ($baseName !~ m/($matchStr)/ )){
      push @reds, $fileInfoHashRef;
    } elsif ($fileType eq 'red_psfcat'){
      push @red_psfcats, $fileInfoHashRef;
    } elsif ($fileType eq 'mask'){
      push @masks, $fileInfoHashRef;
    } elsif ($fileType eq 'norm_psfcat'){
      push @norm_psfcats, $fileInfoHashRef;
    } elsif ($fileType eq 'remap'){
      push @remaps, $fileInfoHashRef;
    } elsif ($fileType eq 'supersky'){
      push @superskys, $fileInfoHashRef;
    } elsif ($fileType eq 'coadd'){
      push @coadds, $fileInfoHashRef;
    } elsif ($fileType eq 'coadd_cat'){
      push @coadd_cats, $fileInfoHashRef;
    } elsif ($fileType eq 'shapelet_shpltall'){
      $fileInfoHashRef->{'WLTABLE'} = 'WL_FINDSTARS';
      push @shpltalls, $fileInfoHashRef;
    } elsif ($fileType eq 'shapelet_shpltpsf'){
      $fileInfoHashRef->{'WLTABLE'} = 'WL_PSF';
      push @shpltpsfs, $fileInfoHashRef;
    } elsif ($fileType eq 'shapelet_shear'){
      $fileInfoHashRef->{'WLTABLE'} = 'WL_SHEAR';
      push @shears, $fileInfoHashRef;
    } elsif ($fileType eq 'shapelet_mes'){
      $fileInfoHashRef->{'WLTABLE'} = 'WL_ME_SHEAR';
      push @shears, $fileInfoHashRef;
    } elsif ($fileType eq 'photoz_cat'){
      push @photoz_cats, $fileInfoHashRef;
    } elsif ($fileType eq 'diff_nitecmb'){
      push @diff_nitecmbs, $fileInfoHashRef;
    } elsif ($fileType eq 'diff_distmp'){
      push @diff_distmps, $fileInfoHashRef;
    } elsif ($fileType eq 'diff_ncdistmp'){
      push @diff_ncdistmps, $fileInfoHashRef;
    } elsif ($fileType eq 'diff_nc'){
      push @diff_ncs, $fileInfoHashRef;
    } elsif ($fileType eq 'diff'){
      push @diffs, $fileInfoHashRef;
    } else {
      push @everythingElses, $fileInfoHashRef;
    }

  }

#
# Push onto the resolved array, all filetypes according to
# parent priority.  Anything that can be a parent must be
# ingested first, followed by any children.  Below, raws
# are ingested first, followed by reds, etc. 
#
# If this is not done, parentids will not be found because
# they may not have been ingested yet.
#
  push @resolvedFilenamesArr, \@raws if @raws;
  push @resolvedFilenamesArr, \@reds if @reds;
  push @resolvedFilenamesArr, \@remaps if @remaps;
  push @resolvedFilenamesArr, \@diff_nitecmbs if @diff_nitecmbs;
  push @resolvedFilenamesArr, \@diff_distmps if @diff_distmps;
  push @resolvedFilenamesArr, \@diff_ncdistmps if @diff_ncdistmps;
  push @resolvedFilenamesArr, \@diff_ncs if @diff_ncs;
  push @resolvedFilenamesArr, \@diffs if @diffs;
  push @resolvedFilenamesArr, \@masks if @masks;
  push @resolvedFilenamesArr, \@coadds if @coadds;
  push @resolvedFilenamesArr, \@coadd_cats if @coadd_cats;
  push @resolvedFilenamesArr, \@norm_psfcats if @norm_psfcats;
  push @resolvedFilenamesArr, \@red_psfcats if @red_psfcats;
  push @resolvedFilenamesArr, \@superskys if @superskys;
  push @resolvedFilenamesArr, \@shpltalls if @shpltalls;
  push @resolvedFilenamesArr, \@shpltpsfs if @shpltpsfs;
  push @resolvedFilenamesArr, \@shears if @shears;
  push @resolvedFilenamesArr, \@photoz_cats if @photoz_cats;
  push @resolvedFilenamesArr, \@everythingElses if @everythingElses;

  return (\@resolvedFilenamesArr,\%runIDS,\%nites,$currProject);

}

sub updateFilelist {

  my ($fileList,$fileName,$localPath,$fileId) = @_;

  foreach my $file (@$fileList){
    if (
        $fileName eq $file->{'localfilename'} &&
        $localPath eq $file->{'localpath'}
       ){
      $file->{'fileid'} = $fileId;
    }
  }

}

sub getMetaTable {

  my ($fileType) = @_;
  my $metaTable;

#Change Ankit. Added ccat filetype to metatable
  if ( ( $fileType  =~ m{_cat|_ccat|red_psfcat|red_scamp|red_fullscamp|red_shpltall|red_shpltpsf|coadd_psfcat|norm_psfcat|_nccat} )
     )
  {
    $metaTable = 'CATALOG';
  } elsif ( $fileType =~ m{src} ) {
    $metaTable = 'EXPOSURE';
  } elsif ( $fileType eq 'coadd' || $fileType eq 'coadd_det') {
    $metaTable = 'COADD';
  } elsif ( $fileType eq 'red_ahead' || $fileType eq 'red_head' ) {
    return;
  } elsif ( $fileType =~ m/photoz/) {
    $metaTable = 'CATALOG';
  } elsif ( $fileType =~ m/shapelet/) {
    $metaTable = 'WL';
  } elsif ( 
         ($fileType  =~ m{red|_bkg|remap|diff|distmp|bpm|obj|mask}) ||
         ($fileType  =~ m{biascor|flatcor|illumcor|pupil}) ||
         ($fileType  =~ m{fringecor|darkcor|supersky|raw|coadd_psf} ) ||
         ($fileType  =~ m{photflatcor} ) ||
         ($fileType eq 'norm') || ($fileType eq 'norm_kern') ||
         ($fileType eq 'diff') || ($fileType eq 'diff_nitecmb') 
          ) {
    $metaTable = 'IMAGE';
  } else {
    $metaTable = q{};
  }

  return $metaTable;

}

sub validateHashRef {

  my ($fileHashRef) = @_;
  my $fileType = $fileHashRef->{'FILETYPE'};
  my $fileClass = $fileHashRef->{'FILECLASS'};

  return 0 if !$fileType;

  if ($fileClass eq 'src'){

    return 1 if (
      $fileHashRef->{'PROJECT'} &&
      $fileHashRef->{'FILECLASS'} &&
      $fileHashRef->{'NITE'} &&
      $fileHashRef->{'FILENAME'}
    );

  } elsif ($fileType =~ m/coadd/ || 
           $fileType =~ m/photoz/ || 
           $fileType =~ m/norm/ 
          ){

    return 1 if (
      $fileHashRef->{'PROJECT'} &&
      $fileHashRef->{'FILECLASS'} &&
      $fileHashRef->{'FILETYPE'} &&
      $fileHashRef->{'TILENAME'} &&
      $fileHashRef->{'FILENAME'}
    );

  } else {

    return 1 if (
      $fileHashRef->{'PROJECT'} &&
      $fileHashRef->{'FILECLASS'} &&
      $fileHashRef->{'RUN'} &&
      #( ( $fileType !~ m/bpm|bias|flat|illum|pupil|fringe|dark|super|etc|log|xml|aux|runtime|xtalk|fixim|shapelet/ ) ?
      ( ( $fileType !~ m/bpm|bias|flat|illum|pupil|fringe|dark|super|etc|log|xml|aux|qa|runtime|xtalk|fixim|shapelet/ ) ?
      $fileHashRef->{'EXPOSURENAME'} : 1 ) &&
      $fileHashRef->{'FILENAME'}
    );

  }

  return 0;
}

sub isIngested {

  my ($fileInfoHashRef, $ingestedFile) = @_;

  foreach my $key (keys %$ingestedFile){
    next if ($key eq 'id');
    next if ($key eq 'filedate');
    next if ($key eq 'filesize');
    next if ($key eq 'archivesites');
    my $ucKey = uc($key);
    next if not defined $ingestedFile->{$key};
    next if not defined $fileInfoHashRef->{$ucKey};
#
# get rid of regex metachars in key values first
#
    my $ingestedKey = $ingestedFile->{$key};
    my $fileInfoKey = $fileInfoHashRef->{$ucKey};
    $ingestedKey =~ s/\+//g;
    $fileInfoKey =~ s/\+//g;
    return 0 if ( $ingestedKey !~ m/$fileInfoKey/);
  }

  return 1;

}

sub updateArchiveSitesStr {

  my ($archiveSiteStr, $currentArchive, $compressionType) = @_;

  my $newChar = 'Y';
  $newChar = 'F' if (defined $compressionType && $compressionType eq 'fz');
  $newChar = 'G' if (defined $compressionType && $compressionType eq 'gz');

  my @arr = split(//,$currentArchive);
  my $pos = 0;
  foreach my $elem (@arr){
    if ($elem ne 'N'){
      substr ($archiveSiteStr,$pos,1) = $newChar;
    }
    $pos++;
  }

  return $archiveSiteStr;

}

sub removeFileFromList{

  my ($catFile, $fileList) = @_;

  my $i = 0;
  foreach my $file (@$fileList){
    if ($file->{'localfilename'} eq $catFile){
      splice @$fileList,$i,1;
      last;
    }
    $i++;
  }

}

sub getParentIdFromHashRef{

  my ($ingestedFiles,$queryHashRef,$runIDS,$otherParentInfo) = @_;

  my $eventStr = q{};
  my $childType = $queryHashRef->{'FILETYPE'};
  my $childClass = $queryHashRef->{'FILECLASS'};
  my $tileName = $queryHashRef->{'TILENAME'};
  my $childName = $queryHashRef->{'FILENAME'};
  my $exposureName = $queryHashRef->{'EXPOSURENAME'};
  my $currentRun  = $queryHashRef->{'RUN'};
  my $temp;
  $tileName = q{} if not $tileName;

#
# Escape the plus signs in the tilename
#
  if ($tileName ne q()){
    $childName =~ s/\+/\\\+/ if ($childClass eq 'coadd' && $childType ne 'remap');
    $tileName =~ s/\+/\\\+/ if ($childType eq 'remap');
  }

  my $nullStr = q{};

  my %parentHash;
  $parentHash{'red'}            = ['raw_obj',$nullStr,$nullStr];
  $parentHash{'red_cat'}        = ['red','_cat|_vig',$nullStr];
  $parentHash{'red_ccat'}       = ['red_cat','_ccat|_vig','_cat']; ## Change added a new file type 
  $parentHash{'red_scamp'}      = ['red','_scamp',$nullStr];
  $parentHash{'red_shpltall'}   = ['red','_shpltall',$nullStr];
  $parentHash{'red_shpltpsf'}   = ['red','_shpltpsf',$nullStr];
  $parentHash{'red_psfcat'}     = ['red','_psfcat',$nullStr];
  $parentHash{'red_psf'}        = ['red_psfcat','_psf','_psfcat'];
  $parentHash{'red_bkg'}        = ['red','_bkg',$nullStr];
  $parentHash{'remap'}          = ['red',"_$tileName",""];
  $parentHash{'remap_cat'}      = ['remap','_cat',''];
  $parentHash{'remap_psfcat'}   = ['remap',"_psfcat",$nullStr];
  $parentHash{'remap_psf'}      = ['remap_psfcat','_psf','_psfcat'];
  $parentHash{'coadd_cat'}      = ['coadd','_cat',$nullStr];
  $parentHash{'coadd_psfcat'}      = ['coadd','_psfcat',$nullStr]; # Added for Jira 2328
  $parentHash{'coadd_psf'}      = ['coadd','psf',$nullStr]; # Added for Jira 2328: Changed _psf to psf (Aug 23 2011)
  $parentHash{'mask'}           = ['red',$nullStr,$nullStr];
  $parentHash{'norm'}           = ['remap',$nullStr,$nullStr];
  $parentHash{'norm_psfcat'}    = ['remap','_norm_psfcat',$nullStr];
  $parentHash{'norm_kern'}      = ['norm_psfcat','_norm_kern','_norm_psfcat'];

  $parentHash{'diff_distmp'}    = ['red','_distmp',$nullStr];
  $parentHash{'diff'}           = ['diff_distmp','\.fits','_distmp'];
  $parentHash{'diff_cat'}       = ['diff','_cat.cat',$nullStr];

  $parentHash{'diff_nitecmb'}   = ['red',$nullStr,$nullStr];
  $parentHash{'diff_ncdistmp'}  = ['diff_nitecmb','_ncdistmp','_nitecmb'];
  $parentHash{'diff_nc'}        = ['diff_ncdistmp','\_nc','_ncdistmp'];
  $parentHash{'diff_nccat'}     = ['diff_nc','_nccat.cat','_nc'];

  $parentHash{'shapelet_shpltall'} = ['red','_shpltall',''];
  $parentHash{'shapelet_shpltpsf'} = ['shapelet_shpltall','_shpltpsf','_shpltall'];
  $parentHash{'shapelet_psfmodel'} = ['shapelet_shpltall','_psfmodel','_shpltall'];
  $parentHash{'shapelet_shear'} = ['shapelet_shpltpsf','_shear','_shpltpsf'];

  $parentHash{'photoz_zcat'} = ['photoz_cat','_zcat','_cat'];

  $parentHash{'illumcor'}       = ['supersky','illumcor','supersky'];
  $parentHash{'fringecor'}      = ['supersky','fringecor','supersky'];

  $parentHash{'shapelet_mes'}   = ['coadd_cat','_mes','_cat'];

# Added by Ankit for SN Processing:

  $parentHash{'diff_nitecmb_diff'}   = ['red',$nullStr,$nullStr];
  $parentHash{'diff_nitecmb_srch'}   = ['red',$nullStr,$nullStr];
  $parentHash{'diff_nitecmb_temp'}   = ['red',$nullStr,$nullStr];

  my $parentType = $parentHash{$childType}->[0];
  my $fileName = $childName;

  if ( ($childType eq 'shapelet_shpltall' && $childClass eq 'wl') ||
       ($childType eq 'mask') ||
       ($childType eq 'norm') ||
       ($childType eq 'norm_psfcat')# ||
       #($childType eq 'norm_kern')
     ){

    my ($oldRun,$nite,$imageName,$tmp,$tmpFiletype) = split /_/,$fileName;

    #$currentRun = join '_',($oldRun,$nite) if ($childType ne 'norm_kern');
    $currentRun = join '_',($oldRun,$nite);

    $fileName =~ s/$currentRun\_// if ($childType eq 'mask' ||
                                       $childType =~ m/norm/ ||
                                       $childType eq 'shapelet_shpltall');

    $parentType = 'remap' if ($childClass eq 'coadd' && $childType eq 'mask');

  }

  if ($parentType ne 'raw_obj'){
    $fileName =~
     s/$parentHash{$childType}->[1]/$parentHash{$childType}->[2]/;
  }
# Added these elsifs to cater to parent ID identification for SN processing 
elsif($childType eq 'diff_nitecmb_diff')
{
	if( $childName =~ /SEARCH\-(.*)_(.*)_expos([0-9]{1,2})\+(.*)\.(.*)/ )
	{
		$fileName = "decam-25-42-".$1.'-'.$3.'_'.$2;
	} 
	elsif( $childName =~ /SEARCH\-(.*)_(.*)_nitecmb\+(.*)\.(.*)/ )
	{
		switch($1)
		{
			case 'g' {
				$fileName = "decam-25-42-".$1.'-1'.'_'.$2;
			}
			case 'r' {
				$fileName = "decam-25-42-".$1.'-3'.'_'.$2;
			}
			case 'i' {
				$fileName = "decam-25-42-".$1.'-7'.'_'.$2;
			}
			case 'z' {
				$fileName = "decam-25-42-".$1.'-13'.'_'.$2;
			}
		}
	} 
	else
	{
	    $eventStr =  "Cannot match filetype information to get the parent ID for file: $childName";
	    reportEvent(2,'STATUS',4,$eventStr);
	}
}
elsif( $childType eq 'diff_nitecmb_srch')
{
	if( $childName =~ /(.*)\-(.*)\-(.*)\_([0-9]{1,2})\+(.*)\.(.*)/ )
	{
		$fileName = "decam-25-42-".$2.'-'.$3.'_'.$4;
	} 
	else
	{
	    $eventStr =  "Cannot match filetype information to get the parent ID for file: $childName";
	    reportEvent(2,'STATUS',4,$eventStr);
	}
}
elsif( $childType eq 'diff_nitecmb_temp'){

	if( $childName =~ /(.*)-(.*)_(.*)\_expos([0-9]{1,2})\.(.*)/ )
	{
		$fileName = "decam-25-42-".$2.'-'.$4.'_'.$3;
	} 
	else
	{
	    $eventStr =  "Cannot match filetype information to get the parent ID for file: $childName";
	    reportEvent(2,'STATUS',4,$eventStr);
	}
}
 else {
    $fileName = $childName;
  }



  $fileName =~ s/\\\+/\+/g;
  $childName =~ s/\\\+/\+/g;
  $childName =~ s/\\/\+/g;
  $fileName =~ s/\\/\+/g;

  $fileName .= '.fits' if ($fileName !~ m/\.fits$/);
  $fileName =~ s/\.\./\./g;

  my $parentId = undef;
  
#
# past runs.  Throw level 4 error if not found after that.
#

  if ($childType !~ 'coadd'){
    $parentId = (defined($exposureName) && $exposureName ne q()) ?
      $ingestedFiles->{$parentType}->{$fileName}->{$currentRun}->{$exposureName}->{'id'} :
       $ingestedFiles->{$parentType}->{$fileName}->{$currentRun}->{'id'};
	
	if(defined $otherParentInfo)
	{
		foreach my $key (keys %$otherParentInfo)
		{
			$otherParentInfo->{$key} =  (defined($exposureName) && $exposureName ne q()) ?
      $ingestedFiles->{$parentType}->{$fileName}->{$currentRun}->{$exposureName}->{$key} :
       $ingestedFiles->{$parentType}->{$fileName}->{$currentRun}->{$key};
		}
	}
	
  } else {
    $parentId = $ingestedFiles->{$parentType}->{$fileName}->{$currentRun}->{'id'};
	if(defined $otherParentInfo)
        {
                foreach my $key (keys %$otherParentInfo)
                {
                        $otherParentInfo->{$key} = $ingestedFiles->{$parentType}->{$fileName}->{$currentRun}->{$key}; 
                }
        }

  }

  if (not $parentId){
    $eventStr =  "Parent id for $childName not in current run $currentRun";
    reportEvent(2,'STATUS',1,$eventStr);
    foreach my $run (sort {$b cmp $a} keys %{$runIDS}){
      next if ($run eq $currentRun);
      $parentId =(defined($exposureName) && $exposureName ne q()) ?
                 $ingestedFiles->{$parentType}->{$fileName}->{$run}->{$exposureName}->{'id'} :
                 $ingestedFiles->{$parentType}->{$fileName}->{$run}->{'id'};
      $eventStr =  "Parent id for $childName found in run $run";
      reportEvent(2,'STATUS',1,$eventStr) if ($parentId);

	if(defined $otherParentInfo && defined $parentId)
        {
                foreach my $key (keys %$otherParentInfo)
                {
                        $otherParentInfo->{$key} = (defined($exposureName) && $exposureName ne q()) ?
                 $ingestedFiles->{$parentType}->{$fileName}->{$run}->{$exposureName}->{$key} :
                 $ingestedFiles->{$parentType}->{$fileName}->{$run}->{$key};
                }
        }

      last if ($parentId);
    }
  }

  if (not $parentId){
    $eventStr =  qq{ 
    WARNING:  
      The Parent Image of this file does not exist in the LOCATION Table
      childType:  $childType
      parentType: $parentType
      childName:  $childName
      parentName: $fileName
    };
    reportEvent(2,'STATUS',4,$eventStr);
  }

  return ($parentId);

}

sub verifyCoaddList{

  my ($files,$project) = @_;

  my ($haveG,$haveR,$haveI,$haveZ,$haveY) = 0;

  foreach my $file (@$files){
    my $catName = $file->{'localfilename'};
    $haveG++ if $catName =~ m/g_cat/;
    $haveR++ if $catName =~ m/r_cat/;
    $haveI++ if $catName =~ m/i_cat/;
    $haveZ++ if $catName =~ m/z_cat/;
    $haveY++ if $catName =~ m/Y_cat/;
  }

#
# Exit if number of catalogs is greater than 5
#
  if ( ($haveG + $haveR + $haveI + $haveZ) > 5 ){

    print "STATUS5BEG Number of coadds is greater than 5, exiting. STATUS5END\n";
    exit(1);

  }

  if ($project eq 'BCS' || $project eq 'SCS' || $project eq 'SPT' ){
    return ($haveG && $haveR && $haveI && $haveZ) ? 1 : 0;
  } else {
    return ($haveG && $haveR && $haveI && $haveZ && $haveY) ? 1 : 0;
  }

}

sub getWLinfo {
    
  my ($ingestedFiles, $fileInfoHashRef, $runIDs) = @_;
  #my $run = $fileInfoHashRef->{'OLDRUN'};
  my $run = $fileInfoHashRef->{'RUN'};
  my $fileName = $fileInfoHashRef->{'FILENAME'};
  my $fileType = $fileInfoHashRef->{'FILETYPE'};
  my $exposureName = $fileInfoHashRef->{'EXPOSURENAME'};
  my $eventStr;
  my $wlInfo;
  my $imageType;
  my $catalogType;

  my $imageName = $fileName;
  my $isCoaddShear = ($imageName =~ m/\_mes/) ? 1 : 0 ;
  #$imageName =~ s/$run\_//;
  my $catalogName = $imageName;
  $imageName =~ s/\_(psfmodel|shpltpsf|shpltall|shear|mes)//;
  $catalogName =~ s/\_(psfmodel|shpltpsf|shpltall|shear|mes)/\_cat/;

  if ($isCoaddShear){

    $imageType = 'coadd';
    $catalogType = 'coadd_cat';

  } else {

    $imageType = 'red';
    $catalogType = 'red_cat';

  }

  $wlInfo->{'imageid'}  = 
     defined $exposureName ?
     $ingestedFiles->{$imageType}->{$imageName}->{$run}->{$exposureName}->{'id'} :
     $ingestedFiles->{$imageType}->{$imageName}->{$run}->{'id'};

  $wlInfo->{'catalogid'} = 
     defined $exposureName ?
     $ingestedFiles->{$catalogType}->{$catalogName}->{$run}->{$exposureName}->{'id'} :
     $ingestedFiles->{$catalogType}->{$catalogName}->{$run}->{'id'};

  $wlInfo->{'ccd'} = 
     defined $exposureName ?
     $ingestedFiles->{$imageType}->{$imageName}->{$run}->{$exposureName}->{'ccd'} :
     $ingestedFiles->{$imageType}->{$imageName}->{$run}->{'ccd'} 
     if ($fileType ne 'shapelet_mes');

  $wlInfo->{'band'} = 
     defined $exposureName ?
     $ingestedFiles->{$imageType}->{$imageName}->{$run}->{$exposureName}->{'band'} :
     $ingestedFiles->{$imageType}->{$imageName}->{$run}->{'band'};

#
# Loop through all runs to look for the imageId
#
  foreach my $run (keys %$runIDs){

    my $imageId = 
       defined $exposureName ?
       $ingestedFiles->{$imageType}->{$imageName}->{$run}->{$exposureName}->{'id'} :
       $ingestedFiles->{$imageType}->{$imageName}->{$run}->{'id'};

    if ($imageId){

      $wlInfo->{'imageid'} = $imageId;
      $wlInfo->{'band'} = 
         defined $exposureName ?
         $ingestedFiles->{$imageType}->{$imageName}->{$run}->{$exposureName}->{'band'} :
         $ingestedFiles->{$imageType}->{$imageName}->{$run}->{'band'};
      $wlInfo->{'ccd'} = 
         defined $exposureName ?
         $ingestedFiles->{$imageType}->{$imageName}->{$run}->{$exposureName}->{'ccd'} :
         $ingestedFiles->{$imageType}->{$imageName}->{$run}->{'ccd'}
         if ($fileType ne 'shapelet_mes');
      last;

    }
  }


#
# Loop through all runs to find catalog id
#
  foreach my $run (keys %$runIDs){
    my $catalogId = 
       defined $exposureName ?
       $ingestedFiles->{$catalogType}->{$catalogName}->{$run}->{$exposureName}->{'id'} :
       $ingestedFiles->{$catalogType}->{$catalogName}->{$run}->{'id'};
    if ($catalogId){
      $wlInfo->{'catalogid'} = $catalogId;
      last;
    }
  }

  if (!$wlInfo->{'imageid'}) {
    my $eventStr = qq{WL imageid Not found for $fileName};
    reportEvent(2,'STATUS',4,$eventStr);
  }
  if (!$wlInfo->{'catalogid'}) {
    my $eventStr = qq{WL catalogid Not found for $fileName};
    reportEvent(2,'STATUS',4,$eventStr);
  }

  return $wlInfo;

}

sub sniffForCompressedFile{

  my ($fileName) = @_;
  my $gzFile = $fileName . '.gz';
  my $fzFile = $fileName . '.fz';

  if (-e $gzFile){ 
    return $gzFile;
  } elsif (-e $fzFile){
    return $fzFile;
  } else {
    return q();
  }

}


# colNames - array ref of strings  
# 
#
# (Was makeFitsBinaryTable from ArchivePortalUtils.pm)
sub writeFitsBinaryTable{
    my ($colNames, $data, $tableName, $tableInfoHashRef, $outFile) = @_;
    my $status=0;
    my $errstr="";

    my $numCols = scalar(@$colNames);
    my $numRows = scalar(@{$$data[0]});

    $outFile .= '.fits' if ($outFile !~ m/fits$/);

    # These are fits standard datatypes
    my $int32  = '1J';
    my $long   = '1K';
    my $float  = '1E';
    my $double = '1D';
    my $char   = '1A1';
    my $string = '';
    if ($tableName eq 'DC4_TRUTH'){
        $string = '60A';
    } else {
        $string = '25A';
    }

    my $tTypes = { $char => TSTRING, $string => TSTRING,
                   $int32 => TLONG, $long => TLONG,
                   $float => TFLOAT, $double => TDOUBLE };


    # convert DB column types into fits data types
    my @tForms = ();
    my @tUnits = ();
    for (my $c=0; $c < scalar(@$colNames); $c++) {
        my $cname = $$colNames[$c];
        $cname =~ s/\s+$//; #strip whitespace
        if (!defined($tableInfoHashRef->{$cname})) {
            print "Error: could not find DB info for column name $cname\n";
            exit 1;
        }
        my $tableType = $tableInfoHashRef->{$cname}->{'type'};
        if ($tableType eq 'VARCHAR2'){
            $tForms[$c]  = $string; 
        } elsif ($tableType eq 'CHAR') {
            $tForms[$c]  = $char; 
        } elsif ($tableType eq 'NUMBER') {
            $tForms[$c] = $long;
            my $tableScale = 0;
            if (defined($tableInfoHashRef->{$cname}->{'scale'})) {
                $tableScale = $tableInfoHashRef->{$cname}->{'scale'};
                if ($tableScale > 0){
                    $tForms[$c] = $double;
                } 
            }
        } elsif ($tableType eq 'BINARY_FLOAT') {
            $tForms[$c] = $float;
        } elsif ($tableType eq 'BINARY_DOUBLE') {
            $tForms[$c] = $double;
        }
        
        $tUnits[$c] = q{};
    }

    my $fptr = Astro::FITS::CFITSIO::create_file("!$outFile",$status);
    if ($status != 0) {
        print "Could not create file $outFile ($status)\n";
        Astro::FITS::CFITSIO::fits_get_errstatus($status, $errstr);
        print "$errstr\n";
        exit 1;
    }
    $fptr->create_tbl(BINARY_TBL,$numRows,$numCols,$colNames,
                      \@tForms,\@tUnits,'DESDM',$status);
    if ($status != 0) {
        print "Could not create tbl ($status)\n";
        Astro::FITS::CFITSIO::fits_get_errstatus($status, $errstr);
        print "$errstr\n";
        exit 1;
    }

    # Insert the columns into the binary table
    for (my $c = 0; $c < scalar(@$colNames); $c++){
        my $cname = $$colNames[$c];
        my $tForm = $tForms[$c];
        my $dataType = $tTypes->{$tForm};
        my $colData = $$data[$c];

#print Dumper($colData),"\n";

        print "writing col ",$c+1,"\n";
        print "   datatype = $dataType\n";
        print "   number of rows = $numRows\n";
        print "   number in array = ", scalar(@$colData), "\n";
        $status = 0;
        Astro::FITS::CFITSIO::fits_write_col($fptr,$dataType,$c+1,1,1,
                                            $numRows,$colData,$status);
        if ($status != 0) {
            Astro::FITS::CFITSIO::fits_get_errstatus($status, $errstr);
            print "Error writing column #$c  ($dataType, $numRows, $status)\n";
            print "$errstr\n";
            exit 1;
        }
    }

    $fptr->close_file($status);

    return;
} # end writeFitsBinaryTable


# assumes all rows have same number of columns 
# i.e. no-value spots have a placeholder like 0 or ""
sub rows2cols {
    my ($rows) = @_;
    my $colsarr = [];

    for (my $r = 0; $r < scalar(@$rows); $r++) {
        my $row = $$rows[$r];
        for (my $c = 0; $c < scalar(@$row); $c++) {
            push @{$$colsarr[$c]}, $$row[$c];
        }
    }
    return $colsarr;
}

1;
