package ReseqTrack::Hive::PipeSeed::RunMergeSeed;

use strict;
use warnings;
use base ('ReseqTrack::Hive::PipeSeed::BasePipeSeed');
use ReseqTrack::Tools::Exception qw(throw);
use Exporter qw( import );

our @EXPORT_OK = qw( _check_exp_attribute );

sub create_seed_params {
  my ($self) = @_;
  my $options = $self->options;

  my $metadata_file = $options->{'metadata_file'};
  throw('require metadata file') unless $metadata_file;

  my $path_names_array = $options->{'path_names_array'} ? $options->{'path_names_array'} : undef;
  my $metadata_hash    = _get_metadata_hash( $metadata_file, 'EXPERIMENT_ID' );
  
  my $experiment_collection_type = $options->{'experiment_collection_type'} ? 
                                   $options->{'experiment_collection_type'} : undef;

  my $experiment_merge_type = $options->{'experiment_merge_type'} ? 
                              $options->{'experiment_merge_type'} : undef;
  
  my $run_collection_type = $options->{'run_collection_type'} ? 
                            $options->{'run_collection_type'} : undef;

  my $output_experiment_columns = ref($options->{'output_experiment_columns'}) eq 'ARRAY' ? $options->{'output_experiment_columns'}
                      : defined $options->{'output_experiment_columns'} ? [$options->{'output_experiment_columns'}]
                      : [];
  my $output_experiment_attributes = ref($options->{'output_experiment_attributes'}) eq 'ARRAY' ? $options->{'output_experiment_attributes'}
                      : defined $options->{'output_experiment_attributes'} ? [$options->{'output_experiment_attributes'}]
                      : [];
  my $output_sample_columns = ref($options->{'output_sample_columns'}) eq 'ARRAY' ? $options->{'output_sample_columns'}
                      : defined $options->{'output_sample_columns'} ? [$options->{'output_sample_columns'}]
                      : [];
  my $output_sample_attributes = ref($options->{'output_sample_attributes'}) eq 'ARRAY' ? $options->{'output_sample_attributes'}
                      : defined $options->{'output_sample_attributes'} ? [$options->{'output_sample_attributes'}]
                      : [];
  my $output_study_columns = ref($options->{'output_study_columns'}) eq 'ARRAY' ? $options->{'output_study_columns'}
                      : defined $options->{'output_study_columns'} ? [$options->{'output_study_columns'}]
                      : [];
  my $output_study_attributes = ref($options->{'output_study_attributes'}) eq 'ARRAY' ? $options->{'output_study_attributes'}
                      : defined $options->{'output_study_attributes'} ? [$options->{'output_study_attributes'}]
                      : [];  

  throw('this module will only accept pipelines that work on the experiment table')
      if $self->table_name ne 'experiment';

  throw('this module require both experiment_collection_type and run_collection_type') 
      unless ( $experiment_collection_type && $experiment_merge_type && $run_collection_type );

  my $file_table = 'file';
  my $db  = $self->db();
  my $ea  = $db->get_ExperimentAdaptor;
  my $ca  = $db->get_CollectionAdaptor;
  my $ra  = $db->get_RunAdaptor;
  my $sta = $db->get_StudyAdaptor;
  my $sa  = $db->get_SampleAdaptor;

  $self->SUPER::create_seed_params();
  my @new_seed_params;
  
  SEED:
  foreach my $seed_params (@{$self->seed_params}) {
    my ($experiment, $output_hash) = @$seed_params;
    my $experiment_source_id = $experiment->experiment_source_id;
    my $experiment_id        = $experiment->dbID;

    my $runs = $ra->fetch_by_experiment_id( $experiment_id );
    my $run = $$runs[0];
    next SEED if !$run;

    throw("$experiment_source_id not present in $metadata_file") unless exists ( $$metadata_hash{$experiment_source_id} );
    my $metadata_path_hash   = _get_path_hash( $experiment_source_id, $metadata_hash, $path_names_array );

    foreach my $path_name ( keys %{$metadata_path_hash} ){
      my $path_value = $$metadata_path_hash{$path_name};
      $output_hash->{$path_name} = $path_value;
    }
    

    if (scalar @$output_sample_columns || scalar @$output_sample_attributes) {
      
      my $sample = $run->sample;
      throw('did not get a sample for run with id '.$run->name) if !$sample;
      foreach my $column_name (@$output_sample_columns) {
        $output_hash->{$column_name} = &{$sa->column_mappings($sample)->{$column_name}}();
      }
      if (@$output_sample_attributes) {
        my $sample_attributes = $sample->attributes;
        ATTRIBUTE:
        foreach my $attribute_name (@$output_sample_attributes) {
          my ($attribute) = grep {$_->attribute_name eq $attribute_name} @$sample_attributes;
          next ATTRIBUTE if !$attribute;
          $output_hash->{$attribute_name} = $attribute->attribute_value;
        }
      }
    }
   
    foreach my $column_name (@$output_experiment_columns) {
        $output_hash->{$column_name} = &{$ea->column_mappings($experiment)->{$column_name}}();
    }
    my $experiment_attributes = $experiment->attributes;
    my $exp_type_check = _check_exp_attribute( $experiment_attributes, $output_hash, $output_experiment_attributes );
    next SEED unless $exp_type_check;
  
    if (scalar @$output_study_columns || scalar @$output_study_attributes) {
        my $study = $sta->fetch_by_dbID($experiment->study_id);
        throw('did not get a study with id '.$experiment->study_id) if !$study;
        foreach my $column_name (@$output_study_columns) {
          $output_hash->{$column_name} = &{$sta->column_mappings($study)->{$column_name}}();
        }
        if (@$output_study_attributes) {
          my $study_attributes = $study->attributes;
          ATTRIBUTE:
          foreach my $attribute_name (@$output_study_attributes) {
            my ($attribute) = grep {$_->attribute_name eq $attribute_name} @$study_attributes;
            next ATTRIBUTE if !$attribute;
            $output_hash->{$attribute_name} = $attribute->attribute_value;
          }
        }
     }

    
    my $seed_experiment = 0;
   
    if( $ca->fetch_by_name_and_table_name( $experiment_source_id, $file_table ) ) {  ## update an existing merge bam
      my $experiment_collections = $ca->fetch_by_name_and_table_name( $experiment_source_id, $file_table );
      my $experiment_exists = 0;

      foreach my $experiment_collection ( @$experiment_collections ){
        my $collection_name = $experiment_collection->name; 
        $experiment_exists++ if $collection_name eq $experiment_collection_type 
      }

      next SEED if $experiment_exists;   ## implement method for updating merge

      my $runs = $ra->fetch_by_experiment_id( $experiment_id );
 
      RUN:
      foreach my $run ( @$runs ){
        my $run_source_id = $run->run_source_id;
        next RUN unless ( $ca->fetch_by_name_and_table_name( $run_source_id, $file_table ) ); ## no file exists for run
        my $run_collections = $ca->fetch_by_name_and_table_name( $run_source_id, $file_table );
 
        RUN_COLLECTION:
        foreach my $run_collection ( @$run_collections ){
          my $collection_type = $run_collection->type;
          next RUN_COLLECTION unless $collection_type eq $run_collection_type;  ## look for only specific collections
           $seed_experiment ++;               ## merger where existing merged bam not present
        }
      }
    }
    else {                               ## create a new merge bam
      my $runs = $ra->fetch_by_experiment_id( $experiment_id );
      
      RUN:
      foreach my $run ( @$runs ){
        my $run_source_id = $run->run_source_id;
        next RUN unless ( $ca->fetch_by_name_and_table_name( $run_source_id, $file_table ) ); ## no file exists for run
    
        my $run_collections = $ca->fetch_by_name_and_table_name( $run_source_id, $file_table );

        RUN_COLLECTION:
        foreach my $run_collection ( @$run_collections ){
          my $collection_type = $run_collection->type;
          next RUN_COLLECTION unless $collection_type eq $run_collection_type;  ## look for only specific collections
          $seed_experiment++;
        }
      }
    }
    push ( @new_seed_params, $seed_params ) if $seed_experiment;  ## creating new seed param list based on criteria
  }
  $self->seed_params(\@new_seed_params);  ## updating the seed param
}
=head1

=cut

sub _check_exp_attribute {
  my ( $experiment_attributes, $output_hash, $output_experiment_attributes ) = @_;
  my $exp_type_check_flag = 1;                                                      ## default pass all 
  my ($exp_type_attribute) = grep {$_->attribute_name eq 'EXPERIMENT_TYPE'} @$experiment_attributes;
  $exp_type_check_flag = 0 unless $exp_type_attribute;

  if (@$output_experiment_attributes) {
    ATTRIBUTE:
    foreach my $attribute_name (@$output_experiment_attributes) {
      my ($attribute) = grep {$_->attribute_name eq $attribute_name} @$experiment_attributes;
      next ATTRIBUTE if !$attribute;
      my $attribute_value = $attribute->attribute_value;

      $attribute_value=~ s/Histone\s+//g
        if( $attribute_name eq 'EXPERIMENT_TYPE' );    ## fix for blueprint ChIP file name

      $attribute_value=~ s/\//_/g
        if( $attribute_name eq 'EXPERIMENT_TYPE' );    ## fix for blueprint ChIP file name for H3k9/14ac

      $attribute_value=~ s/ChIP-Seq\s+//g
        if( $attribute_name eq 'EXPERIMENT_TYPE' );    ## fix for blueprint ChIP file name

      $attribute_value=~ s/Chromatin\sAccessibility/Dnase/
        if( $attribute_name eq 'EXPERIMENT_TYPE' );    ## fix for blueprint Dnase file name 
  
      $output_hash->{$attribute_name} = $attribute_value;
    }
  }
  return $exp_type_check_flag;
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
