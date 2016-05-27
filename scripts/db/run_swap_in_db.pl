#!/usr/bin/env perl

use strict;
use warnings;
use autodie;
use Getopt::Long;
use ReseqTrack::DBSQL::DBAdaptor;


my $dbhost;
my $dbuser;
my $dbpass;
my $dbport;
my $dbname;
my $exp_list;
my $save_changes = 0;


GetOptions( 'dbhost=s'          => \$dbhost,
            'dbport=s'          => \$dbport,
            'dbname=s'          => \$dbname,
            'dbuser=s'          => \$dbuser,
            'dbpass=s'          => \$dbpass,
            'exp_list=s'        => \$exp_list,
            'save_changes'      => \$save_changes,
          );

die "Experiment list is required for swap",$/ unless $exp_list; 

my $exp_hash = get_hash( $exp_list );

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
  -host   => $dbhost,
  -user   => $dbuser,
  -port   => $dbport,
  -dbname => $dbname,
  -pass   => $dbpass,
);

$db->dbc->disconnect_when_inactive(1);

my $ea = $db->get_ExperimentAdaptor;
my $ra = $db->get_RunAdaptor;

foreach my $old_exp( keys %{$exp_hash}){
  my $new_exp       = $$exp_hash{$old_exp};
  my $new_exp_entry = $ea->fetch_by_source_id($new_exp);
  my $new_exp_id    = $new_exp_entry->dbID;
  die unless $new_exp_id;

  my $old_exp_entry = $ea->fetch_by_source_id($old_exp);
  my $old_exp_id    = $old_exp_entry->dbID;
  die unless $old_exp_id;

  my $old_runs      = $ra->fetch_by_experiment_id($old_exp_id);  
  
  foreach my $old_run ( @$old_runs ){
    my $run_source_id = $old_run->source_id;
    die unless $run_source_id;
    print "changing experiment details for Run: $run_source_id, from $old_exp:$old_exp_id to $new_exp:$new_exp_id",$/;

    if( $save_changes ){

      my $history = ReseqTrack::History->new(
            -other_id => $old_run->dbID, -table_name => 'run',
            -comment => "changed exp from $old_exp to $new_exp",
          );

      eval{
        $old_run->experiment_id($new_exp_id);
      };

      die "Errors $@",$/
         if $@;
      $old_run->history($history);
      $ra->update($old_run);
    }
  }  
}



sub get_hash {
  my ( $exp_list ) = @_;
  my %hash;
  
  open my $fh, '<', $exp_list;
  while( <$fh> ){
    chomp;
    next  if /^#/;
  
    my @vals = split "\t";
    $hash{$vals[0]} = $vals[1];
  } 
  close($fh);
  return \%hash;
}

=head1 Description
Sript for changing experiment details for the runs in database 

Options:

  -exp_list :  tab-delimited file listing the old and new experiment ids for the change
                e.g OLD_EXPERIMENT_ID <TAB> NEW_EXPERIMENT_ID
  -dbhost   : Database host name
  -dbport   : Database port
  -dbuser   : Database user name
  -dbpass   : Database password
  -dbname   : Database name
