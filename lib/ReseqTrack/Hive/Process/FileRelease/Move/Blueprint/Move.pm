package ReseqTrack::Hive::Process::FileRelease::Move::Blueprint::Move;

use strict;
use File::Basename qw(fileparse dirname);
use ReseqTrack::Tools::Exception qw(throw warning);
use ReseqTrack::Tools::GeneralUtils qw(current_date);
use BlueprintFileMovePath qw(cnag_path crg_path wtsi_path get_meta_data_from_index get_alt_sample_name_from_file);
use base ('ReseqTrack::Hive::Process::FileRelease::Move');

sub param_defaults {
  my ( $self ) = @_;
  return {
    %{$self->SUPER::param_defaults()},
  };
}

sub derive_path {
  my ( $self, $dropbox_path, $file_object ) = @_;	
  
  my ( $destination, $collection_name );
  
  my $derive_path_options = $self->param( 'derive_path_options' );
  
  my $aln_base_dir = $derive_path_options->{aln_base_dir};                     ## alignment files direcroty
  throw( "this module needs a aln_base_dir" ) 
    if ! defined $aln_base_dir;
    
 my $vcf_base_dir = $derive_path_options->{vcf_base_dir};                      ## vcf_base_dir direcroty
  throw( "this module needs a vcf_base_dir" ) 
    if ! defined $vcf_base_dir;
  
  my $results_base_dir = $derive_path_options->{results_base_dir};             ## result files directory
  throw( "this module needs a results_base_dir" ) 
    if ! defined $results_base_dir;
  
  my $run_meta_data_file =  $derive_path_options->{run_meta_data_file};        ## metadata file
  throw( "this module needs a run_meta_data_file" ) 
    if ! defined $run_meta_data_file;
  
  my $species = $derive_path_options->{species};                               ## species info
  throw( "this module needs a species" ) 
    if ! defined $species;
    
  my $genome_version = $derive_path_options->{genome_version};                 ## genome_version
  throw( "this module needs a genome_version" ) 
    if ! defined $genome_version;  
  
  my $alt_sample_name = $derive_path_options->{alt_sample_name};               ## alt_sample_name
  throw( "this module needs a alt_sample_name" ) 
    if ! defined $alt_sample_name;  
    
  my $incoming_type = $derive_path_options->{incoming_type};                   ## incoming_type
  throw( "this module needs a incoming_type" ) 
    if ! defined $incoming_type;  
    
  my $incoming_md5_type = $derive_path_options->{incoming_md5_type};           ## incoming_md5_type
  throw( "this module needs a incoming_md5_type" ) 
    if ! defined $incoming_md5_type; 
    
  my $internal_type = $derive_path_options->{internal_type};                   ## internal_type
  throw( "this module needs a internal_type" ) 
    if ! defined $internal_type; 
    
  my $freeze_date = $derive_path_options->{freeze_date};                       ## freeze date is required
  throw( "this module needs a freeze date" )
    if ! defined $freeze_date;
   
  my $file_host_name = $file_object->host->name;                               ## fetch host name
  throw( "this module needs a host name" )
    if ! defined $file_host_name;

  my $collection_tag = $derive_path_options->{collection_tag};                 ## collection tag, e.g. EXPERIMENT_ID
  throw( "this module needs a collection tag" )
    if ! defined $collection_tag;

  my $file_type = $file_object->type;
  
  throw("can't move $file_type") if ( $file_type eq $incoming_type ||
                                      $file_type eq $incoming_md5_type ||
                                      $file_type eq $internal_type );
  
  
  my $meta_data = get_meta_data_from_index($run_meta_data_file);                   ## get metadata hash from file

  my ( $filename, $incoming_dirname ) = fileparse( $dropbox_path );
  my $alt_sample_hash = {};
  $alt_sample_hash = get_alt_sample_name_from_file( $alt_sample_name ) if $alt_sample_name;
  
  my %path_options = ( filename         => $filename, 
                       aln_base_dir     => $aln_base_dir,
                       vcf_base_dir     => $vcf_base_dir,
                       results_base_dir => $results_base_dir,
                       meta_data        => $meta_data,
                       species          => $species,
                       genome_version   => $genome_version,
                       freeze_date      => $freeze_date,
                       alt_sample_hash  => $alt_sample_hash,
                       filetype         => $file_type,
                       collection_tag   => $collection_tag,
                     );
                     
  if ( $filename =~ m/^(ERR\d+)/ ) {                                    ## check for ENA id 
    throw( 'Expection sample name got ENA run id' );                    ## ENA is support is disabled
  }
  elsif ( $file_host_name eq 'CNAG'  ) {                               ## data from CNAG
  
   ( $destination, $collection_name ) = cnag_path( \%path_options ); 
  }
  elsif ( $file_host_name eq 'CRG' ) {                                   ## data from CRG
   
   ( $destination, $collection_name ) = crg_path( \%path_options );                                         
  }
  elsif ( $file_host_name eq 'WTSI' ) {                                  ## data from WTSI
  
   ( $destination, $collection_name ) = wtsi_path( \%path_options );  
  }
  else {
    throw( "Cannot find sample information from $filename and host id $file_host_name" );
  }
  
  throw("couldn't find collection name") if !$collection_name;
  
  return $destination, $collection_name; 
} 


1;
