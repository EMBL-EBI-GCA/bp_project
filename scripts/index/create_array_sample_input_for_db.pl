#!/usr/bin/env perl
use strict;
use warnings;
use autodie;
use ReseqTrack::Tools::ERAUtils;
use Getopt::Long;

my $era_user;
my $era_pass;
my $infile;
my $out_index = 'array_sample_infile.out';
my $file_map  = 'sample_file_map.out';
my $data_dir;
my $species_name = 'Homo sapiens';

GetOptions( 'infile=s'   => \$infile,
            'data_dir=s' => \$data_dir,
            'outfile=s'  => \$out_index,
            'file_map=s' => \$file_map,
            'species=s'  => \$species_name,
            'era_user=s' => \$era_user,
            'era_pass=s' => \$era_pass,
          );

die `perldoc -t $0` if !$infile || !$data_dir;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;


my @db_header = qw/ STUDY_SOURCE_ID      SAMPLE_NAME         SCIENTIFIC_NAME 
                    BIOMATERIAL_PROVIDER CELL_TYPE           TISSUE_TYPE 
                    MOLECULE             DISEASE             SAMPLE_ONTOLOGY_URI 
                    DISEASE_ONTOLOGY_URI DONOR_ID            DONOR_AGE 
                    DONOR_SEX            DONOR_HEALTH_STATUS RUN_CENTER 
                 /;

my ($data, $file) = get_data( $infile );
my $study_id_map  = write_file( $data, $file, \@db_header, $era, $out_index, $species_name );

open my $OUT_MAP, '>', $file_map;
foreach my $file_path (keys %{$file}){
  my $study_id    = $file->{$file_path}{study};
  my $sample_name = $file->{$file_path}{sample};

  my $ena_id = $study_id_map->{$study_id};
  $file_path = $data_dir.'/'.$file_path; 
  $file_path =~ s{//}{/}g;
  print $OUT_MAP join("\t",$ena_id, $sample_name, $file_path),$/;
}
close($OUT_MAP);

sub write_file {
  my ( $data, $file, $db_header, $era, $out_index, $species_name ) = @_;
  my %study_id_map;

  open my $OUT_INDEX,'>', $out_index;

  foreach my $study (keys %{$data} ){
    my $study_id = get_study_name( $study, $era );
    $study_id_map{$study}=$study_id;
    foreach my $sample( keys %{$$data{$study}}){
      my %array_sample;
      my $sample_entry = $$data{$study}{$sample};
      foreach my $field (@$db_header){
        if ( $field eq 'STUDY_SOURCE_ID'){
          $array_sample{$field} = $study_id;
        }
        elsif ( $field eq 'SCIENTIFIC_NAME' ){
          $array_sample{$field} = $species_name;
        }
       elsif ( $field eq 'RUN_CENTER' ){
          $array_sample{$field} = $sample_entry->{CENTER_NAME};
        }
        else {
           die "missing $field for $study, $sample",$/ 
                unless exists $sample_entry->{$field};
           $array_sample{$field} = $sample_entry->{$field};
        }
      }
      print $OUT_INDEX join ("\t", @array_sample{@$db_header}),$/;
    }
  }
  close($OUT_INDEX);
  return \%study_id_map;
}

sub get_data {
  my ( $infile ) = @_;
  my @header;
  my %data_hash;
  my %file_hash;

  open my $fh,'<', $infile;
  while ( <$fh> ) {
    chomp;
    next if /^#/;
    if ( @header ) {
      my @lines = split "\t";
      my %hash;
      @hash{@header}  = @lines;
      my $study_id    = $hash{STUDY_ID};
      my $sample_name = $hash{SAMPLE_NAME};
      my $file        = $hash{FILE};
      $data_hash{$study_id}{$sample_name}=\%hash;
      $file_hash{$file}{study}=$study_id;      
      $file_hash{$file}{sample}=$sample_name;      
    }
    else {
      @header = split "\t";
    }
  }
  close($fh);
  return \%data_hash, \%file_hash;
}

sub get_study_name {
  my ( $study_id, $era ) = @_;
  my  $id;
  my $sth = $era->dbc->prepare("select study_id from study where ega_id =?");
  
  $sth->execute( $study_id );
  while( my $row=$sth->fetchrow_hashref()){
    $id = $row->{STUDY_ID};
  }
  return $id;
}

=head1 Description

  Script for preparing input for array_sample table

=head2 Example 

  perl create_array_sample_input_for_db.pl -infile <array_data.index> -data_dir </dir_location/array_files/> -era_user <user> -era_pass <pass>

=cut
