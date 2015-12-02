#!/usr/bin/env perl
use strict;
use warnings;
use autodie;
use Getopt::Long;
use Data::Dump qw(dump);


my $in_progress_matrix;
my $in_ncmls_list;
my $in_ihec_list;
my $epirr_data;

die `perldoc -t $0` if !$in_progress_matrix || !$in_ncmls_list;

GetOptions(  "progress_matrix=s" => \$in_progress_matrix,
             "ncmls_list=s"      => \$in_ncmls_list,
             "ihec_list=s"       => \$in_ihec_list,
          );

my $ebi_data   = read_progress_matrix( $in_progress_matrix );
$epirr_data    = read_epirr_index( $in_ihec_list );
my $ncmls_data = read_progress_matrix( $in_ncmls_list );

my %comp_donor_list = ();

my @header = qw/ IHEC_Reference_Epigenome_Identifier
                 DONOR_ID
                 Description
                 Type
               /;

my @assays = ( 'Bisulfite-Seq', 'RNA-Seq', 'ChIP-Seq Input', 'H3K4me3',
               'H3K4me1', 'H3K9me3', 'H3K27ac', 'H3K27me3', 'H3K36me3',
               'H2A.Zac', 'H3K9/14ac');

print join ("\t",@header, @assays),$/;

combine_data( $ebi_data, $epirr_data, $ncmls_data, $mpimg_data, $cnag_data );

sub combine_data {
  my ( $ebi_data, $epirr_data, $ncmls_data, $mpimg_data, $cnag_data ) = @_;

  foreach my $ebi_entry ( @$ebi_data ){
    my $epirr_id    = get_epirr_entry( $ebi_entry, $epirr_data );
    my $ebi_assay   = get_ebi_assay( $ebi_entry, \@assays);  
    my ( $updated_assay, $new_entry )      
                    = update_from_ncmls( $ebi_entry, $ebi_assay, \@assays, $ncmls_data );

    my $donor_id    = $$ebi_entry{ID};
    my $description;
    my $cell_type   = $$ebi_entry{'Cell type'};
    my $tissue      = $$ebi_entry{Tissue};
    my $disease     = $$ebi_entry{Disease};
    $description    = $cell_type if $cell_type;
    $description    = $tissue    if !$cell_type && $tissue;
    $description   .= ' ,'.$disease
                      if $disease && $disease ne 'None';
 
    my $donor_sex   = $$ebi_entry{Sex};

    my $type = 'SD';
    $type = 'COMP' if $donor_id =~ /pool/i;
    $type = 'COMP' if $donor_sex =~ /Mixed/i;
    $type = 'COMP' if exists $comp_donor_list{$donor_id};

    my $assay_string;
    $assay_string  = join ("\t", @$updated_assay{@assays})
                       if ($updated_assay && ref $updated_assay eq 'HASH'); 

    my $assay_count = 0;
    foreach my $assay_name (@assays){
      $assay_count++ if exists $$updated_assay{$assay_name} && $$updated_assay{$assay_name} ne '';
    }

    if ( $assay_count >0 ){
      print join ("\t", $epirr_id, $donor_id, $description, $type, $assay_string),$/
            if ($updated_assay && ref $updated_assay eq 'HASH');   

      print join ("\t", @$new_entry),$/ 
            if $new_entry  && ref $new_entry eq 'ARRAY';
    }
  }
  my $unmapped_ncmls_data = get_new_ncmls_entry( \@assays, $ncmls_data );
  print join("\n",@$unmapped_ncmls_data),$/;

}

sub get_new_ncmls_entry {
  my ( $assays, $ncmls_data ) = @_;
  my @unmapped_ncmls_data;
  
  foreach my $ncmls_entry ( @$ncmls_data ){
    my @row = ();
    
    unless ( exists ($$ncmls_entry{MAPPED})){
      my $assay_count = 0; 
      my $sample_id   = $$ncmls_entry{SAMPLE};
      my $donor_id    = substr($sample_id,0, 6);
      my $description = $$ncmls_entry{DESCRIPTION};
      my $type        = $$ncmls_entry{TYPE};
     
      next if !$sample_id || $sample_id eq '';

      @row = ('-', $donor_id, $description . " ( $sample_id )", $type );
      
      foreach my $assay_name ( @$assays ){

        if ( exists ($$ncmls_entry{$assay_name} )){
          push @row, $$ncmls_entry{$assay_name};
          $assay_count++ if $$ncmls_entry{$assay_name} ne '';
        }

        push @row,''
             unless exists $$ncmls_entry{$assay_name};
      } 
    
      unless ( $assay_count == 0 ){                                ## add entry if any assay is present
        my $line = join("\t", @row);
        push @unmapped_ncmls_data, $line;            
      }
    }
  }
  return \@unmapped_ncmls_data;
}

sub update_from_ncmls {
  my ( $ebi_entry, $ebi_assay, $assays, $ncmls_data ) = @_;

  my ( $updated_assay, $new_entry );

  my $ebi_samples = $$ebi_entry{Samples};
   
  my @sample_list;
  if ( $ebi_samples =~ /;/ ){
    @sample_list = split ";", $ebi_samples;
  }
  else {
    push @sample_list, $ebi_samples;
  }

  die $ebi_samples, $/ unless @sample_list > 0;
  my %ebi_samples_list = map{ $_ =>1 } @sample_list;
  
  foreach my $ncmls_entry( @$ncmls_data ){

    my $ncmls_sample = $$ncmls_entry{SAMPLE};
    die "no ncmls sample" if !$ncmls_sample;                                   ## ncmls entries should have sample name

    if ( exists ( $ebi_samples_list{$ncmls_sample})){                          ## check exact match of sample
      $updated_assay = add_ncmls_assay( $ebi_assay, $assays, $ncmls_entry, $ncmls_sample);
      $$ncmls_entry{MAPPED} = 1;
    }
    elsif ( $ncmls_sample =~ /_new/i ){                                        ## check repeat samples
      $ncmls_sample =~ s/_new//;
      if ( exists ( $ebi_samples_list{$ncmls_sample})){
         $new_entry = add_new_entry( $ebi_entry, $assays, $ncmls_entry );
         $$ncmls_entry{MAPPED} = 1;
      }
    }
    else {                                                                     ## keep ebi record if no ncmls entry prtesent
      $updated_assay = $ebi_assay;
    }
  }
  return ( $updated_assay, $new_entry );
}

sub add_ncmls_assay {
  my ( $ebi_assay, $assays, $ncmls_entry, $ncmls_sample ) = @_;
  my %updated_assay;

  foreach my $assay_name ( @$assays ){
    if ( exists ( $$ebi_assay{$assay_name} )){
      if ( exists ( $$ncmls_entry{$assay_name} )){
        my $ebi_status   = $$ebi_assay{$assay_name};
        my $ncmls_status = $$ncmls_entry{$assay_name};
        unless ( $ncmls_status eq '' ){
          $updated_assay{$assay_name} = $ncmls_status;
          warn "changing status of $ncmls_sample for $assay_name from '$ebi_status' to '$ncmls_status'",$/            ## using ncmls sample status
             unless $ncmls_status eq $ebi_status;
        }
      }
      else {
        $updated_assay{$assay_name} = $$ebi_assay{$assay_name};
      }
    }
    else {
      $updated_assay{$assay_name} = '';
    }
  }
  return \%updated_assay;
}

sub add_new_entry {
  my ( $ebi_entry, $assays, $ncmls_entry ) = @_;

  my $donor_id    = $$ebi_entry{ID};
  my $description = $$ncmls_entry{DESCRIPTION};
  my $sample      = $$ncmls_entry{SAMPLE};
  my $type        = $$ncmls_entry{TYPE}; 
  my @new_entry   = ( '-', $donor_id, $description . " ( $sample )", $type );
  
   
  foreach my $assay_name ( @$assays ){
    if ( exists ( $$ncmls_entry{$assay_name})){
      push @new_entry, $$ncmls_entry{$assay_name};
    }
    else {
      push @new_entry,'';
    }  
  }
  return \@new_entry;
}

sub get_ebi_assay{
  my ( $ebi_entry, $assays) = @_;
  my %ebi_assay;

  foreach my $assay_name( @$assays ){ 
    if ( exists ( $$ebi_entry{$assay_name} )){  
      $ebi_assay{$assay_name} = 'C'
        if $$ebi_entry{$assay_name} &&  $$ebi_entry{$assay_name} ne ''; 
    }

    if ( exists ( $$ebi_entry{'RNA-Seq (total)'})){
      $ebi_assay{'RNA-Seq'} = 'C' 
        if $$ebi_entry{'RNA-Seq (total)'} &&  $$ebi_entry{'RNA-Seq (total)'} ne '';
    }

    if( exists ($$ebi_entry{'RNA-Seq (polyA)'})){
      $ebi_assay{'RNA-Seq'} = 'C'
         if $$ebi_entry{'RNA-Seq (polyA)'} && $$ebi_entry{'RNA-Seq (polyA)'} ne '';
    }

    unless (exists ( $$ebi_entry{$assay_name})){
      $ebi_assay{$assay_name} = '';
    }

  }
  return \%ebi_assay;
}

sub get_epirr_entry {
  my ( $ebi_entry, $epirr_data ) = @_;

  my $epirr_id;
 
  my $donor_id      = $$ebi_entry{ID};
  my $samples       = $$ebi_entry{Samples}; 
  my $disease       = $$ebi_entry{Disease};
  my $tissue        = $$ebi_entry{Tissue};

  my @sample_list;
  if ( $samples =~ /;/ ){
    @sample_list = split ";", $samples;
  }
  else {
    push @sample_list, $samples;
  }

  my $match_count   = 0;

  foreach my $epirr_line( @$epirr_data ){
    $epirr_id         = $$epirr_line{EPIRR_ID}; 
    my $epirr_donor   = $$epirr_line{DONOR_ID};
    my $epirr_samples = $$epirr_line{SAMPLE_IDS};
    my $epirr_tissue  = $$epirr_line{TISSUE_TYPE};
    my $epirr_cell    = $$epirr_line{CELL_TYPE};
    my $epirr_disease = $$epirr_line{DISEASE};
 
    foreach my $sample_id ( @sample_list ) {        ## EPIRR id by sample name match
      $match_count++
      if( exists ( $$epirr_samples{$sample_id}));
    }

    last if $match_count > 0;
  }

  $epirr_id = '-' if $match_count == 0;             ## reset counter for non epirr entries
  return $epirr_id;
}

sub read_progress_matrix{
  my ( $in_file ) = @_;

  open my $fh, '<', $in_file;
  my @header;
  my @data;
  while (<$fh>) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;
    if (@header) {
      my %row;
      @row{@header} = @vals;
      push @data, \%row;
    }
    else {
      @header = @vals;
    }
  }
  close( $fh );
  return \@data;  
}

sub read_epirr_index{
  my ( $in_file ) = @_;
  
  open my $fh, '<', $in_file;
  my @header;
  my @data;
  while (<$fh>) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;
    if (@header) {
      my %row;
      @row{@header}    = @vals;
      my $sample_str   = $row{SAMPLE_IDS}; 
      $row{SAMPLE_IDS} = str_to_hash( $sample_str ) 
                         if $sample_str;
      push @data, \%row;
    }
    else {
      @header = map { uc($_) } @vals;
    }
  }
  close( $fh );
  return \@data;
}

sub str_to_hash {
  my ( $str ) = @_;
  die 'No input' unless $str;
  my @array = split ";", $str;
  my %hash = map{ $_ => 1 } @array;
  return \%hash;
}


=head1
 IHEC sample tracking report generation script

=head2
 Usage:
        perl ihec_tracking_report.pl -progress_matrix <progress_matrix> -ncmls_list <ncmls_list> 

=head2

 Options:
      
 -ihec_list : EPIRR index file
   
=cut
