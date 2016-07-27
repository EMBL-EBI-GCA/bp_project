#!/usr/bin/env perl
use strict;
use warnings;
use XML::Twig;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use Getopt::Long;
use File::Basename qw( basename );

my ( $in_file,  $era_user, $era_pass );
my $center_name = 'BLUEPRINT';
my $ega_box = undef;

GetOptions( 'infile=s'   => \$in_file,
            'era_user=s' => \$era_user,
            'era_pass=s' => \$era_pass,
            'ega_box=s'  => \$ega_box, 
);


die `perldoc -t $0` if !$in_file || !$ega_box || !$era_user || !$era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $index_hash = get_index_hash( $in_file, 'FILE' );

my $db_string = 'select analysis_alias, analysis_id, ega_id from analysis where analysis_alias = ? and center_name = ?' ; 
my $sth = $era->dbc->prepare( $db_string );

my $ega_string = 'select file_name from ega_submission_file where dir_name = ? and file_name like ?' ; 
my $sth_ega = $era->dbc->prepare( $ega_string );

foreach my $path ( keys %$index_hash ) {
  my $exp_id = $$index_hash{$path}{EXPERIMENT_ID};
  my $filename = basename($path); 
  $sth->execute( $filename, $center_name );
  my $row = $sth->fetchrow_arrayref();
  if ($row){
    print join("\t", $exp_id,@$row),$/;
  }
  else{
     $sth_ega->execute( $ega-box, '%'.$filename);
     my $ega_file = undef;
     ( $ega_file ) = $sth_ega->fetchrow_array();
     $ega_file = $ega_file // 'None';
     print $exp_id, "\t", $filename,"\t",$ega_file,$/;
  }
}
 
sub get_index_hash {
my ( $file, $key_string ) = @_;
  die "Key string not found to look in $file",$/
    unless $key_string;

  open my $fh, '<', $file;
  my @header;
  my %data;
  my $key_index = undef;

  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;

    if ( @header ) {
      $data { $vals[$key_index] }{ $header[$_] } = $vals[$_] for 0..$#header;
    }
    else {
      @header = map { uc($_) } @vals;
      my @key_index_array = grep{ $header[$_] eq $key_string } 0..$#header;

      die "$key_string not found in $file",$/
         if @key_index_array == 0;

      $key_index = $key_index_array[0];
    }
  }
  return \%data;
  close( $fh );
}

=head1
 
  Script for checking existing analysis entry in EGA, for Blueprint project

=head2 Usage

   perl check_ega_analysis.pl --infile alignments.index --ega_box ega_box_xy --era_user $DB_USER --era_pass $DB_PASS 
    
=head2  Options

  infile   : Alignment index file
  ega_box  : EGA submission account
  era_user : ERAPRO user name
  era_pass : ERAPRO password 
=cut
