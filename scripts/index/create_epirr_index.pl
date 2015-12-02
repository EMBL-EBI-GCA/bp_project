#!/usr/bin/env perl
use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;
use autodie;
use JSON;
use Getopt::Long;
use Data::Dumper;

my $era_user;
my $era_pass;
my $production_dir;


GetOptions( "era_user=s" => \$era_user,
            "era_pass=s" => \$era_pass,
            "json_dir=s" => \$production_dir,
          );

die `perldoc -t $0` if !$era_user || !$era_pass || !$production_dir;
my @era_conn = ( $era_user, $era_pass );


my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;


my $epirr_file_list = get_file_list( $production_dir );
my $epirr_hash      = prepare_epirr_id_mapping( $epirr_file_list );

prepare_index( $epirr_hash, $era );


sub prepare_index{
  my ( $epirr_hash, $xml_sth ) = @_;
  my @header = qw/ EPIRR_ID	
                   DESCRIPTION
                   STATUS	
                   DONOR_ID
                   TISSUE_TYPE
                   CELL_TYPE
                   DISEASE
                   SAMPLE_IDS
                   ENA_EXPERIMENT_IDS
                   EGA_EXPERIMENT_IDS
                   EGA_DATASET_IDS
                /;

  print join ( "\t", @header ),$/;


  foreach my $epirr_id ( keys %{$epirr_hash} ){
    my $description = $$epirr_hash{ $epirr_id }{DESCRIPTION};
    my $status      = $$epirr_hash{ $epirr_id }{STATUS}      ? $$epirr_hash{ $epirr_id }{STATUS} : 'NA';
    my $donor_id    = $$epirr_hash{ $epirr_id }{DONOR_ID}    ? $$epirr_hash{ $epirr_id }{DONOR_ID} : 'NA';
    my $disease     = $$epirr_hash{ $epirr_id }{DISEASE}     ? $$epirr_hash{ $epirr_id }{DISEASE} : 'NA';
    my $cell_type   = $$epirr_hash{ $epirr_id }{CELL_TYPE}   ? $$epirr_hash{ $epirr_id }{CELL_TYPE} : 'NA';
    my $tissue_type = $$epirr_hash{ $epirr_id }{TISSUE_TYPE} ? $$epirr_hash{ $epirr_id }{TISSUE_TYPE} : 'NA';
   
    my @line_arr = ($epirr_id, $description, $status, $donor_id, $tissue_type, $cell_type, $disease);
    my @sample_arr;
    my @ena_exp_arr;
    my @ega_exp_arr;

    foreach my $primary_id ( @{$$epirr_hash{ $epirr_id }{RAW_DATA}} ){
      my ( $experiment_id, $ega_id, $sample_id );

      $primary_id    =~ /^EGA/ ? $ega_id = $primary_id : $experiment_id = $primary_id;
      $experiment_id = get_ena_id( $ega_id, $era ) if ( $ega_id && !$experiment_id );
      $sample_id     = get_sample_id( $experiment_id, $era );
      $ega_id        = undef if !$ega_id;

      push @ena_exp_arr, $experiment_id;
      push ( @ega_exp_arr, $ega_id ) if $ega_id; 
      push @sample_arr, $sample_id;
    }

    my $sample_list  = join ( ";", @sample_arr);
    my $ena_exp_list = join ( ";", @ena_exp_arr );
    my $ega_exp_list;

    if ( @ega_exp_arr ){
      $ega_exp_list = join ( ";", @ega_exp_arr ) if @ega_exp_arr;
    }
    else {
      $ega_exp_list = '';
    }

    my $ega_dataset_list;
    if ( exists ( $$epirr_hash{ $epirr_id }{ SECONDARY_DATA })){
      $ega_dataset_list = join ( ";", @{$$epirr_hash{ $epirr_id }{ SECONDARY_DATA }} );
    }
    else {
      $ega_dataset_list = '';
    } 

    push @line_arr, $sample_list, $ena_exp_list, $ega_exp_list, $ega_dataset_list;
    print join ( "\t", @line_arr ),$/;
  }
}

sub get_file_list {
  my ( $old_epirr_dir ) = @_;
  my @file_list;

  opendir ( my $dh, $old_epirr_dir );
  while ( readdir $dh ) {
    my $file_path = $old_epirr_dir.'/'. $_;

    push @file_list, $file_path
         if $file_path =~ /\S+\.refepi\.out\.json$/;
  }
  closedir $dh;
  return \@file_list;
}

sub prepare_epirr_id_mapping {
  my ( $epirr_file_list ) = @_;
  my %full_epirr_hash;
  
  foreach my $file ( @$epirr_file_list ) {
    my ( $epirr_id_hash ) = get_ega_ids( $file );
    foreach my $experiment_id ( keys %{$epirr_id_hash} ) {
      die if exists $full_epirr_hash{ $experiment_id };
      $full_epirr_hash{ $experiment_id } = $$epirr_id_hash{ $experiment_id };
    }
  }
  return \%full_epirr_hash;
}

sub get_ega_ids {
  my ( $file ) = @_;
  my %epirr_id_hash;

  open my $fh, '<', $file;
  my $json = do{ local $/; <$fh> };
  close( $fh );

  my $data        = decode_json( $json );
  my $epirr_id    = $$data{ accession };
  my $description = $$data{ description };
  my $status      = $$data{ status };
  my $donor_id    = $$data{ meta_data }{ donor_id };
  my $tissue_type = $$data{ meta_data }{ tissue_type };
  my $cell_type   = $$data{ meta_data }{ cell_type };
  my $disease     = $$data{ meta_data }{ disease };

  $epirr_id_hash{$epirr_id}{DESCRIPTION} = $description;
  $epirr_id_hash{$epirr_id}{STATUS}      = $status;
  $epirr_id_hash{$epirr_id}{DONOR_ID}    = $donor_id;
  $epirr_id_hash{$epirr_id}{TISSUE_TYPE} = $tissue_type;
  $epirr_id_hash{$epirr_id}{CELL_TYPE}   = $cell_type;
  $epirr_id_hash{$epirr_id}{DISEASE}     = $disease;

  my @raw_data_id_list;
  foreach my $raw_data( @{$$data{raw_data}} ){
    my $raw_data_id    = $$raw_data{primary_id};
    my $secondary_data = $$raw_data{secondary_id} ? $$raw_data{secondary_id} : undef;
    push @{$epirr_id_hash{$epirr_id}{RAW_DATA}},$raw_data_id; 
    push @{$epirr_id_hash{$epirr_id}{SECONDARY_DATA}},$secondary_data if $secondary_data;
  }
 return \%epirr_id_hash;
}


sub get_ena_id {
  my ( $ega_id, $era ) = @_;
  my $xml_sth = $era->dbc->prepare( "select e.experiment_id,e.ega_id from experiment e where e.ega_id = ?" );

  $xml_sth->execute( $ega_id );
  my $exp_id;

  while ( my $xr = $xml_sth->fetchrow_arrayref()){
    $exp_id      = $$xr[0];
    my $ega_id_test = $$xr[1];
    die unless $ega_id_test eq $ega_id;
  }
  return $exp_id;
}

sub get_sample_id {
  my ( $exp_id, $era ) = @_;
  my $xml_sth = $era->dbc->prepare( "select e.experiment_id, ex.sample_name from experiment e,
xmltable( '/EXPERIMENT_SET/EXPERIMENT' passing e.experiment_xml
columns
       sample_name varchar2(512) PATH '//DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS//SUBMITTER_ID'
) ex
 where e.experiment_id = ?" );
  
  $xml_sth->execute( $exp_id );
  my $sample_name;

  while ( my $xr = $xml_sth->fetchrow_arrayref()){
    my $exp      = $$xr[0];
    $sample_name = $$xr[1];
    die unless $exp eq $exp_id;
  }
  return $sample_name;  
}

=head1   EpiRR index file generation script

=head2
  
  Usage:
         perl create_epirr_index.pl -era_user <era username> -era_pass <era_pass> -json_dir <EpiRR json output dir>

=cut

