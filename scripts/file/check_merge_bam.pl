#!/usr/bin/env perl
use strict;
use warnings;
use ReseqTrack::DBSQL::DBAdaptor;
use IPC::System::Simple qw(capture);
use Getopt::Long;

my $dbhost;
my $dbuser;
my $dbpass;
my $dbport;
my $dbname;
my $lib_type = 'ChIP-Seq';
my $merge_file_type;
my $final_bam;
my $check_bam = 1;
my $tag_name = 'ID';
my $samtools = '/nfs/1000g-work/G1K/work/bin/samtools/samtools';

GetOptions( 'dbhost=s'     => \$dbhost,
            'dbuser=s'     => \$dbuser,
            'dbpass=s'     => \$dbpass,
            'dbport=s'     => \$dbport,
            'dbname=s'     => \$dbname, 
            'lib_type=s'   => \$lib_type,
            'merge_type=s' => \$merge_file_type,
            'final_bam=s'  => \$final_bam,
            'check_bam!'   => \$check_bam,
            'tag_name=s'   => \$tag_name,
            'samtools=s'   => \$samtools,
);

die `perldoc -t $0`
  if ( !$merge_file_type || !$final_bam );

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
  -host   => $dbhost,
  -user   => $dbuser,
  -port   => $dbport,
  -dbname => $dbname,
  -pass   => $dbpass,
);

$db->dbc->disconnect_when_inactive(1);

my $ra = $db->get_RunAdaptor;
my $ea = $db->get_ExperimentAdaptor;
my $ca = $db->get_CollectionAdaptor;
my $fa = $db->get_FileAdaptor;

my $all_exps = $ea->fetch_all();


while ( my $exp_obj = shift @$all_exps ){
  my $lib_strategy = $exp_obj->library_strategy;
  next unless $lib_strategy eq $lib_type;
 
  my $exp_id   = $exp_obj->dbID;
  my $exp_name = $exp_obj->source_id;
  die unless $exp_id;

  my $runs      = $ra->fetch_by_experiment_id($exp_id);
  my $merge_bam = $ca->fetch_by_name_and_type($exp_obj->source_id, $merge_file_type);
  next unless $merge_bam;

  my $merge_runs = $merge_bam->other_ids;

  warn $exp_name,"\n"
     unless @$runs == @$merge_runs;

  if ( @$merge_runs > 1 && $check_bam && @$runs == @$merge_runs ){
    my $c = $ca->fetch_by_name_and_type( $exp_name, $final_bam );
    die "missing: $exp_name, $final_bam",$/ 
      unless $c;                               ## check for the final bam when the merge bam is present

    my @run_ids = map{ $_->source_id } @$runs;
    my $file_ids = $c->other_ids;
    
    foreach my $file_id ( @$file_ids ){
      my $file = $fa->fetch_by_dbID($file_id);
      my $path = $file->name;
      die unless $path;

      my $rg_tag = get_rg_tags( $path, $tag_name, $samtools );
      my $lc = List::Compare->new( \@run_ids, $rg_tag );
      warn "RG check: $exp_name\n"
        unless $lc->is_LequivalentR();
    }
  }
}

sub get_rg_tags{
  my ( $bam, $tag_name, $samtools ) = @_;
  my $sam_rg_cmd = "$samtools view -H ";
  $sam_rg_cmd .=  $bam;

  my @header_lines = capture( $sam_rg_cmd );
  my @rg_lines = grep{ /^\@RG/ } @header_lines;

  die "no RG tag found in $bam"
     unless scalar @rg_lines > 0;

  my @tag_lists;

  foreach my $rg_line ( @rg_lines ){
    my @rg_values = split '\s+', $rg_line;
    my @rg_tags = grep{ /^$tag_name/ } @rg_values;

    foreach my $rg_tag ( @rg_tags ){
      my ($tag, $value) =  split ':', $rg_tag;
      die "$tag_name not found in the RG tag of $bam"
         unless $value;

      push( @tag_lists, $value);
    }
  }
  return \@tag_lists;
}

=head1

   Script for checking merged BAMs which are prepared from multiple runs

=head2
  Usage:
  
      perl check_merge_bam.pl $DB_PARAM --final_bam CHIP_DEDUP_BAM --merge_type  CHIP_MERGE_RUN_BAM --lib_type ChIP-Seq

=head2

  Options:

  dbhost     : MySQL host 
  dbuser     : MySQL user
  dbpass     : MySQL pass    
  dbport     : MySQL port    
  dbname     : MySQL db   
  samtools   : samtools path ( default: /nfs/1000g-work/G1K/work/bin/samtools/samtools )
  lib_type   : Library type  ( default: ChIP-Seq )   
  tag_name   : RG tag for comparison,    ( default: ID )   
  check_bam  : Check BAM RG tags if set, ( default: True )   
  final_bam  : Filetype of final BAM
  merge_type : Filetype of merge BAM collection

=cut
