#
# EventUtils.pm
#
# DESCRIPTION:
#
# AUTHOR:  Tony Darnell (tdarnell@uiuc.edu)
#
# $Rev: 6149 $
# $LastChangedBy: ankitc $
# $LastChangedDate: 2010-11-16 14:16:47 -0600 (Tue, 16 Nov 2010) $
#

package DB::EventUtils;

use strict;
require Exporter;
our @ISA = qw(Exporter);

our @EXPORT = qw{
  reportEvent
};

sub reportEvent {

  my ($verboseLevel,$type,$level,$event) = @_;

  if ($verboseLevel > 1) {
    printEvent($type,$level,$event);
  } else {
    if ($level == 5){
      print qq{** $event **\n};
    } else {
      print qq{$event\n};
    }
  }

}

sub printEvent {

  my ($type,$level,$event) = @_;

  if ($type eq 'STATUS'){
    print  " STATUS",$level,"BEG ",$event," STATUS",$level,"END\n";
    #print " Thread: ",threads->tid() , " STATUS",$level,"BEG ",$event," STATUS",$level,"END\n";
  } elsif ($type eq "QA"){
    print "QA",$level,"BEG ",$event," QA",$level,"END\n";
  } else {
    print "STATUS5BEG Unknown event type: ",$event," STATUS5END\n";
    exit(1);
  }

}

1;
