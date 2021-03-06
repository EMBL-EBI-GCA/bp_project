package ReseqTrack::Hive::PipeSeed::RunWigglerSeed;

use strict;
use warnings;
use base ('ReseqTrack::Hive::PipeSeed::BasePipeSeed');
use ReseqTrack::Tools::Exception qw(throw warning);
use autodie;
use Data::Dump qw( dump );
use Exporter qw( import );

our @EXPORT_OK = qw( _check_exp_type );

sub create_seed_params {
  my ($self) = @_;
  my $options = $self->options;
  
  my $require_experiment_type = $options->{'require_experiment_type'} || undef;    
 
  my $metadata_file = $options->{'metadata_file'};
  throw('require metadata file') unless $metadata_file;

  my $path_names_array = $options->{'path_names_array'} ? $options->{'path_names_array'} : undef;  
  my $metadata_hash    = _get_metadata_hash( $metadata_file, 'EXPERIMENT_ID' );


  throw('this module will only accept pipelines that work on the collection table')
      if $self->table_name ne 'collection';
      
  my $db = $self->db();
  my $sa = $db->get_SampleAdaptor;
  my $ea = $db->get_ExperimentAdaptor;
  my $ra = $db->get_RunAdaptor;
  my $ca = $db->get_CollectionAdaptor;

  my $exp_type_attribute_name = 'EXPERIMENT_TYPE';

  $self->SUPER::create_seed_params();
  my @new_seed_params;
  
  SEED:
  foreach my $seed_params (@{$self->seed_params}) {
      my ($collection, $output_hash) = @$seed_params;

      my $collection_attributes = $collection->attributes;

      foreach my $collection_attribute (@$collection_attributes) {
        $output_hash->{ $collection_attribute->attribute_name } = $collection_attribute->attribute_value;    ## adding all collection attributes to pipeline
      }
      my $experiment_name = $collection->name;

      throw("$experiment_name not present in $metadata_file") unless exists ( $$metadata_hash{$experiment_name} );
      my $metadata_path_hash   = _get_path_hash( $experiment_name, $metadata_hash, $path_names_array );

      foreach my $path_name ( keys %{$metadata_path_hash} ){
        my $path_value = $$metadata_path_hash{$path_name};
        $output_hash->{$path_name} = $path_value;
      }
    

      my $experiment = $ea->fetch_by_source_id( $experiment_name );
      my $experiment_attributes = $experiment->attributes;

      my $exp_type_check = _check_exp_type( $experiment_attributes, $exp_type_attribute_name, $output_hash);
      next SEED unless $exp_type_check;

      $output_hash->{'experiment_source_id'} = $experiment_name;    ## adding attribute only for selected seeds
      my $runs      = $ra->fetch_by_experiment_id($experiment->dbID);
      my $sample_id = $$runs[0]->experiment->sample_id;             ## assuming all runs of an experiment are from same sample
      my $sample    = $sa->fetch_by_dbID( $sample_id );
      $output_hash->{'sample_alias'} = $sample->sample_alias;

      push ( @new_seed_params, $seed_params )
   }
  $self->seed_params(\@new_seed_params);                            ## updating the seed param
  $db->dbc->disconnect_when_inactive(1);
}

=head1

  Check for Experiment attribute and modify it as required. Returns true if required attributes are present.

=cut

sub  _check_exp_type {
  my ( $experiment_attributes, $exp_type_attribute_name, $output_hash) = @_;
  my $exp_check_flag = 1;                                           ## Default allow all

  my ($attribute) = grep {$_->attribute_name eq $exp_type_attribute_name} @$experiment_attributes;
  $exp_check_flag = 0 unless $attribute;

  if ( $attribute ){
    my $attribute_name  = $attribute->attribute_name;
    my $attribute_value = $attribute->attribute_value;

    $attribute_value=~ s/Histone\s+//g
      if( $attribute_name eq 'EXPERIMENT_TYPE' );                ## fix for blueprint ChIP file name

    $attribute_value=~ s/\//_/g
      if( $attribute_name eq 'EXPERIMENT_TYPE' );                ## fix for blueprint ChIP file name for H3k9/14ac

    $attribute_value=~ s/ChIP-Seq\s+//g
      if( $attribute_name eq 'EXPERIMENT_TYPE' );                ## fix for blueprint ChIP file name

    $attribute_value=~ s/Chromatin\sAccessibility/Dnase/
      if( $attribute_name eq 'EXPERIMENT_TYPE' );                ## fix for blueprint Dnase file name 

    $output_hash->{$attribute_name} = $attribute_value;
  }
  return $exp_check_flag;
}

=head1 _get_path_hash

Returns a metadata hash from metadat hash. Inputs are key metadata id, metadata hash and an array of parameters (optional).

=cut

sub _get_path_hash {
  my ( $key_id, $metadata_hash, $path_names_array ) = @_;
  my $path_hash;

  throw("$key_id not found in metadata file") unless exists $$metadata_hash{ $key_id };

  $$metadata_hash{ $key_id }{SAMPLE_DESC_1} = "NO_TISSUE" 
    if ( $$metadata_hash{ $key_id }{SAMPLE_DESC_1} eq "-" );
 
  $$metadata_hash{ $key_id }{SAMPLE_DESC_2} = "NO_SOURCE" 
    if ( $$metadata_hash{ $key_id }{SAMPLE_DESC_2} eq "-" );
  
  $$metadata_hash{ $key_id }{SAMPLE_DESC_3} = "NO_CELL_TYPE" 
    if ( $$metadata_hash{ $key_id }{SAMPLE_DESC_3} eq "-" );

  if ( scalar @$path_names_array >= 1 ){
    my @uc_path_names_array = map{ uc($_) } @$path_names_array;
    my $key_metadata_hash   = $$metadata_hash{ $key_id };

    foreach my $key( @uc_path_names_array ){
      throw("$key in not present in metadata") unless exists $$key_metadata_hash{ $key };
    }

    my @path_name_values    = @$key_metadata_hash{ @uc_path_names_array };
    @path_name_values       = map{ s/[\s=\/\\;,'"()]/_/g; $_; }@path_name_values;
    @path_name_values       = map{ s/_+/_/g; $_; }@path_name_values;
    @path_name_values       = map{ s/_$//g; $_; }@path_name_values;
    @path_name_values       = map{ s/^_//g; $_; }@path_name_values;

    @$path_hash{ @$path_names_array } = @path_name_values;
  }
  else {
    $path_hash = $$metadata_hash{ $key_id };
  }
  return $path_hash;
}

=head1 _get_metadata_hash

Returns metadata hash from an index file keyed by any selected field.

=cut

sub _get_metadata_hash {
my ( $file, $key_string ) = @_;
  open my $fh, '<', $file;
  my @header;
  my %data;
  my $key_index = undef;

  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;

    if ( @header ) {
      throw("$key_string not found in $file") unless $key_index >= 0;
      $data { $vals[$key_index] }{ $header[$_] } = $vals[$_] for 0..$#header;
    }
    else {
      @header = map { uc($_) } @vals;
      my @key_index_array = grep{ $header[$_] eq $key_string } 0..$#header;
      $key_index = $key_index_array[0];
    }
  }
  return \%data;
  close( $fh );
}

1;


