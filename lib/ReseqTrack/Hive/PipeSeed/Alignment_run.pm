
package ReseqTrack::Hive::PipeSeed::Alignment_run;

use strict;
use warnings;
use autodie;
use base ('ReseqTrack::Hive::PipeSeed::BasePipeSeed');
use ReseqTrack::Tools::Exception qw(throw);


sub create_seed_params {
  my ($self) = @_;
  my $options = $self->options;

  my $metadata_file = $options->{'metadata_file'};
  throw('require metadata file') unless $metadata_file;

  my $collection_type = $options->{'collection_type'};
  throw('require collection_type') unless $collection_type;

  my $path_names_array = $options->{'path_names_array'} ? $options->{'path_names_array'} : undef;
 
  my $metadata_hash = _get_metadata_hash( $metadata_file, 'RUN_ID' );

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
  
  my $require_experiment_columns = $options->{'require_experiment_columns'} || {};
  my $require_study_columns      = $options->{'require_study_columns'} || {};
  my $require_sample_columns     = $options->{'require_sample_columns'} || {};

  my $exclude_experiment_columns = $options->{'exclude_experiment_columns'} || {}; 
  my $exclude_study_columns      = $options->{'exclude_study_columns'} || {}; 
  my $exclude_sample_columns     = $options->{'exclude_sample_columns'} || {}; 
 
  my $require_experiment_attributes = $options->{'require_experiment_attributes'} || {};
  my $require_study_attributes      = $options->{'require_study_attributes'} || {};
  my $require_sample_attributes     = $options->{'require_sample_attributes'} || {};

  my $exclude_experiment_attributes = $options->{'exclude_experiment_attributes'} || {};
  my $exclude_sample_attributes     = $options->{'exclude_sample_attributes'} || {};
  my $exclude_study_attributes      = $options->{'exclude_study_attributes'} || {};

  throw('this module will only accept pipelines that work on the run table')
      if $self->table_name ne 'run';

  my $db  = $self->db();
  my $sa  = $db->get_SampleAdaptor;
  my $ca  = $db->get_CollectionAdaptor;
  my $ea  = $db->get_ExperimentAdaptor;
  my $sta = $db->get_StudyAdaptor;

  $self->SUPER::create_seed_params();
  
  my @final_seed;

  SEED:
  foreach my $seed_params (@{$self->seed_params}) {
    my ($run, $output_hash) = @$seed_params;

    my $run_id = $run->source_id;

    next SEED if !$ca->fetch_by_name_and_type($run_id, $collection_type);  ## no file present for following run
    next SEED unless exists( $$metadata_hash{ $run_id } );                 ## allow run for test
    throw("$run_id not present in $metadata_file") unless exists( $$metadata_hash{ $run_id } );
    my $metadata_path_hash = _get_path_hash( $run->source_id, $metadata_hash, $path_names_array );
                            
    foreach my $path_name ( keys %{$metadata_path_hash} ){
      my $path_value = $$metadata_path_hash{$path_name};
      $output_hash->{$path_name} = $path_value;
    }

    if (scalar @$output_sample_columns || scalar @$output_sample_attributes) {

      my $sample = $run->sample;
      throw('did not get a sample for run with id '.$run->name) if !$sample;
    
      SAMPLE_COLUMN:
      foreach my $column_name (keys %{$sa->column_mappings($sample)}) {

        my $column_value         = &{$sa->column_mappings($sample)->{$column_name}}();
       
        next SAMPLE_COLUMN unless exists $require_sample_columns->{$column_name};
        my $check_require_sample = _check_hash( $column_name, $column_value, $require_sample_columns, 'require' );
        next SEED if $check_require_sample == 0;
      
        next SAMPLE_COLUMN unless exists $exclude_sample_columns->{$column_name};
        my $check_exclude_sample = _check_hash( $column_name, $column_value, $exclude_sample_columns, 'exclude' );
        next SEED if $check_exclude_sample > 0;        

        $output_hash->{$column_name} = &{$sa->column_mappings($sample)->{$column_name}}();
      }
      if (@$output_sample_attributes) {
        my $sample_attributes = $sample->attributes;

        my $require_sample_att_flag = _check_attributes( $sample_attributes, $require_sample_attributes, 'require' );
        next SEED if $require_sample_att_flag == 0;

        my $exclude_sample_att_flag = _check_attributes( $sample_attributes, $exclude_sample_attributes, 'exclude' );
        next SEED if $exclude_sample_att_flag > 0;

        ATTRIBUTE:
        foreach my $attribute_name (@$output_sample_attributes) {
          my ($attribute) = grep {$_->attribute_name eq $attribute_name} @$sample_attributes;
          next ATTRIBUTE if !$attribute;
          
          $output_hash->{$attribute_name} = $attribute->attribute_value;
        }
      }
    }

    if (scalar @$output_experiment_columns || scalar @$output_experiment_attributes
          || scalar @$output_study_columns || scalar @$output_study_attributes) {
      my $experiment = $ea->fetch_by_dbID($run->experiment_id);
      throw('did not get an experiment with id '.$run->experiment_id) if !$experiment;

      EXP_COLUMN:
      foreach my $column_name ( keys %{$ea->column_mappings($experiment)}) {
        
        my $column_value             = &{$ea->column_mappings($experiment)->{$column_name}}();

        next EXP_COLUMN unless exists $require_experiment_columns->{$column_name};
        my $check_require_experiment = _check_hash( $column_name, $column_value, $require_experiment_columns, 'require' );
        next SEED if $check_require_experiment == 0;

        next EXP_COLUMN unless exists $exclude_experiment_columns->{$column_name};
        my $check_exclude_experiment = _check_hash( $column_name, $column_value, $exclude_experiment_columns, 'exclude' );
        next SEED if $check_exclude_experiment > 0;
      }
 
      foreach my $column_name (@$output_experiment_columns){
        $output_hash->{$column_name} = &{$ea->column_mappings($experiment)->{$column_name}}();
      }

      if (@$output_experiment_attributes) {
        my $experiment_attributes = $experiment->attributes;

        my $require_experiment_att_flag = _check_attributes( $experiment_attributes, $require_experiment_attributes, 'require' );
        next SEED if $require_experiment_att_flag == 0;

        my $exclude_experiment_att_flag = _check_attributes( $experiment_attributes, $exclude_experiment_attributes, 'exclude' );
        next SEED if $exclude_experiment_att_flag > 0;       

        ATTRIBUTE:
        foreach my $attribute_name (@$output_experiment_attributes) {
          my ($attribute) = grep {$_->attribute_name eq $attribute_name} @$experiment_attributes;
          next ATTRIBUTE if !$attribute;
          $output_hash->{$attribute_name} = $attribute->attribute_value;
        }
      }
      if (scalar @$output_study_columns || scalar @$output_study_attributes) {
        my $study = $sta->fetch_by_dbID($experiment->study_id);
        throw('did not get a study with id '.$experiment->study_id) if !$study;

        STUDY_COLUMN:
        foreach my $column_name ( keys %{$sta->column_mappings($study)}) {

          my $column_value        = &{$sta->column_mappings($study)->{$column_name}}();

          next STUDY_COLUMN unless exists $require_study_columns->{$column_name};
          my $check_require_study = _check_hash( $column_name, $column_value, $require_study_columns, 'require' );
          next SEED if $check_require_study == 0;
  
          next STUDY_COLUMN unless exists $exclude_study_columns->{$column_name};
          my $check_exclude_study = _check_hash( $column_name, $column_value, $exclude_study_columns, 'exclude' );
          next SEED if $check_exclude_study > 0;
 
          $output_hash->{$column_name} = &{$sta->column_mappings($study)->{$column_name}}();
        }
        if (@$output_study_attributes) {
          my $study_attributes = $study->attributes;

          my $require_study_att_flag = _check_attributes( $study_attributes, $require_study_attributes, 'require' );
          next SEED if $require_study_att_flag == 0;

          my $exclude_study_att_flag = _check_attributes( $study_attributes, $exclude_study_attributes, 'exclude' );
          next SEED if $exclude_study_att_flag > 0;

          ATTRIBUTE:
          foreach my $attribute_name (@$output_study_attributes) {
            my ($attribute) = grep {$_->attribute_name eq $attribute_name} @$study_attributes;
            next ATTRIBUTE if !$attribute;
            $output_hash->{$attribute_name} = $attribute->attribute_value;
          }
        }
      }
    }
    push @final_seed, $seed_params;
  }
  $self->seed_params(\@final_seed);
}

=head1 _check_attributes

Check attbrites in seed attribute array and input attribute hash.
Return 1 for 'require' tag and 0 for 'exclude' tag if not matching attribute found.

=cut


sub _check_attributes {
  my ( $seed_attributes, $check_attributes, $tag ) = @_;
  my $check_flag  = 0;
  my $nohit_count = 0;

  ATTRIBUTE:
  foreach my $check_attribute_name ( keys %$check_attributes ){
    my $check_attribute_value = $$check_attributes{ $check_attribute_name };
    my ($attribute) = grep {$_->attribute_name eq $check_attribute_name} @$seed_attributes;

    if ( !$attribute ){
      $nohit_count++;
      next ATTRIBUTE;
    }
    $check_flag++ if $attribute->attribute_value eq $check_attribute_value;  
  }
  
  if ( $nohit_count eq scalar keys %$check_attributes ) { ## if no attribute found for the seed
    $check_flag = 1 if $tag eq 'require';
    $check_flag = 0 if $tag eq 'exclude'; 
  }
  return $check_flag;
}

=head1 _check_hash

This subroutine checks for column values within a hash. It return 1 in its satisfying the check.
If the column name doesn't exist in hash, return 1 for 'require' tag and 0 for 'exclude tag'

=cut

sub _check_hash {
  my ( $column_name, $column_value, $require_hash, $tag  ) = @_;
  throw("Allowed tag require or exclude") 
       unless $tag eq 'require' or $tag eq 'exclude';

  my $check_flag = 0;
  if ( exists ( $$require_hash{ $column_name })) {
    if ( exists ( $$require_hash{ $column_name })) {
       my $require_value = $$require_hash{ $column_name };
       if (ref $require_value eq 'ARRAY' ){
         my %value_hash = map { $_ => 1 } @$require_value;
         $check_flag++ if exists $value_hash{ $column_value };
       }
       else {
         $check_flag++ if $column_value eq $require_value;
       }
    }
  }
  else {
    $check_flag = 1 if ( $tag eq 'require' );
    $check_flag = 0 if ( $tag eq 'exclude' );
  } 
  return $check_flag;
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
