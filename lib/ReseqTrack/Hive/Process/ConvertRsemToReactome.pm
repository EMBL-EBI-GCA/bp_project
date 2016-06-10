package ReseqTrack::Hive::Process::ConvertRsemToReactome;

use strict;

use base ('ReseqTrack::Hive::Process::BaseProcess');
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::Tools::Exception qw(throw);
use ReseqTrack::Tools::FileSystemUtils qw(check_file_exists delete_file);
use ReseqTrack::Tools::GeneralUtils   qw(get_open_file_handle execute_system_command);
use Math::Trig;
use Scalar::Util::Numeric qw(isint isnum);
use PerlIO::gzip;
use File::Slurp;
use File::Copy;

sub param_defaults {
  return {
  };
}


sub run {
  my $self = shift @_;

  $self->param_required( 'file' );
  my $annotation_file     = $self->param_required( 'annotation_file' );
  my $reactome_gene_file  = $self->param_required( 'reactome_gene_file' );
  my $transform_name      = $self->param_required( 'transform_name' );
  my $files               = $self->param_as_array( 'file' );

  foreach my $file ( @$files ) {
    check_file_exists( $file );
  }
  
  throw('expecting single input file') if scalar @$files > 1;
  check_file_exists( $annotation_file );
  check_file_exists( $reactome_gene_file );
 
  throw( "couldn't identify transform_name: $transform_name" )
       unless $transform_name eq 'asinh'  or 
              $transform_name eq 'log';

  my $input    = $$files[0];
  my $out_file = $input;
  $out_file =~ s/\.results(?:\.gz)?$/\.reactome/;
  throw( "$out_file exists" ) if -e $out_file; 
 
  my $tmp_file_name  = $out_file . '.tmp';
  my $output_name    = $tmp_file_name;
  my $reactome_genes = _read_reactome_genes( $reactome_gene_file );
  my $annotation     = _read_annotation( $annotation_file );
  my $input_fh       = _opener( $input );
  my $quants         = _read_quant_file( $input_fh, $annotation );
  close( $input_fh );

  my $log;
  open my $output_fh, '>', $tmp_file_name;
  _write_reactome_quants( $output_fh, $reactome_genes, $quants, $log, $transform_name );
  close( $output_fh );

  move( $tmp_file_name, $out_file ); 

  $self->output_param( 'reactome', $out_file );
}

sub _read_reactome_genes {
  my ($reactome_genes_file) = @_;
  my @reactome_genes =
    sort { $a cmp $b } read_file( $reactome_genes_file, chomp => 1 );

  return \@reactome_genes;
}

sub _read_annotation {
  my ( $annotation_file_name ) = @_;

  my $annotation_fh = _opener( $annotation_file_name );

  my %annotation;
  LINE:
  while ( <$annotation_fh> ) {
    next unless $_ =~ m/^\S+\t\S+\tgene/ ; #huge performance improvement by checking type early 

    chomp;

    my $feature = _parse_gff( $_ );
    next LINE if $feature->{type} ne 'gene';
    $annotation{ $feature->{attrib}{gene_id} } = $feature;
  }
  close( $annotation_fh );
  return \%annotation;
}

sub _parse_gff {
  my ($line) = @_;
  my %f = ( attrib => {} );
  my $attributes_text;
  ( 
    $f{seq},    $f{src},   $f{type},
    $f{start},  $f{end},   $f{score},
    $f{strand}, $f{phase}, $attributes_text
  ) = split /\t/, $line;

  for my $a ( split /; */, $attributes_text ) {
    my ( $key, $value ) = split / /, $a;
    $value =~ s/"//g if $value;

    $f{attrib}{$key} = $value;
  }
  return \%f;
}

sub _opener {
  my ($file) = @_;
  my $fh;

  if ( $file =~ m/\.gz/ ) {
    open $fh, '<:gzip', $file;
  }
  else {
    open $fh, '<', $file;
  }
  return $fh;
}

sub _read_quant_file {
  my ( $input_fh, $annotation ) = @_;
  my %quants;

  LINE:
  while ( <$input_fh> ) {
    chomp;
    next if !$_ or m/^#/;
    next if /^(gene|transcript)/;

    my $feature = _parse_rsem( $_ );
    throw( "No feature parsed from line: $_" ) unless $feature;

    my $tpm = $feature->{TPM};
    throw( "No RPKM found in line: $_" ) unless defined $tpm;

    my $gene_id = $feature->{gene_id};
    throw( "No gene id found in line: $_" ) unless $gene_id;

    my $annotation_feature = $annotation->{$gene_id};
    throw( "No feature found for $gene_id" ) unless $annotation_feature;

    my $gene_name = $annotation_feature->{attrib}{gene_name};
    throw( "No gene_name found for $gene_id" ) unless $gene_name;

    $quants{$gene_name} = $tpm;
  }
  return \%quants;
}

sub _parse_rsem {
  my ($line) = @_;
  my %f;
  my $attributes_text;

  ( 
    $f{gene_id},          $f{transcript_id},  $f{length},
    $f{effective_length}, $f{expected_count}, $f{TPM},
    $f{FPKM},             $attributes_text,
  ) = split /\t/, $line;

  return \%f;
}

sub _write_reactome_quants {
  my ( $output_fh, $reactome_genes, $quants, $log , $transform_name ) = @_;

  my $scale = 'TPM';
  $scale    = 'log(TPM)'   if ( $transform_name eq 'log' );
  $scale    = 'asinh(TPM)' if ( $transform_name eq 'asinh' );

  print $output_fh "#Gene expression\t ${scale}$/";

  for (@$reactome_genes) {
    my $q = $quants->{$_};
    if ( $transform_name eq 'log' ){
      if ($q) {
        $q = log10($q);
      }
      else{
        $q = '';
      }
    }
    if (defined $q && $transform_name eq 'asinh' ){
      $q = asinh($q);
    }
    print $output_fh "$_\t$q$/"
      if ( defined $q );
  }
}


1;
