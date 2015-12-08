package ReseqTrack::Hive::Process::FileRelease::Move::Blueprint::Move;

use strict;
use File::Basename qw(fileparse dirname);
use ReseqTrack::Tools::Exception qw(throw warning);
use ReseqTrack::Tools::GeneralUtils qw(current_date);

use base ('ReseqTrack::Hive::Process::FileRelease::Move');

sub param_defaults {
  my ( $self ) = @_;
  return {
    %{$self->SUPER::param_defaults()},
  };
}

sub derive_path {
  my ( $self, $dropbox_path, $file_object ) = @_;	
  
  my $destination;
  
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
   
  my $file_host_name = $file_object->host->name;                                     ## fetch host name
  throw( "this module needs a host name" )
    if ! defined $file_host_name;

  my $file_type = $file_object->type;
  
  throw("can't move $file_type") if ( $file_type eq $incoming_type ||
                                      $file_type eq $incoming_md5_type ||
                                      $file_type eq $internal_type );
  
  
  my $meta_data = _get_meta_data($run_meta_data_file);                   ## get metadata hash from file

  my ( $filename, $incoming_dirname ) = fileparse( $dropbox_path );
  my $alt_sample_hash = {};
  $alt_sample_hash = _get_hash_from_file( $alt_sample_name ) if $alt_sample_name;
  
  my %path_options = ( filename         => $filename, 
                       aln_base_dir     => $aln_base_dir,
                       vcf_base_dir     => $vcf_base_dir,
                       results_base_dir => $results_base_dir,
                       meta_data        => $meta_data,
                       species          => $species,
                       genome_version   => $genome_version,
                       freeze_date      => $freeze_date,
                       alt_sample_hash  => $alt_sample_hash,
                       file_object      => $file_object,
                     );
                     
  if ( $filename =~ m/^(ERR\d+)/ ) {                                    ## check for ENA id 
    throw( 'Expection sample name got ENA run id' );                    ## ENA is support is disabled
  }
  elsif ( $file_host_name eq 'CNAG'  ) {                               ## data from CNAG
  
    $destination = $self->_derive_CNAG_path( \%path_options ); 
  }
  elsif ( $file_host_name eq 'CRG' ) {                                   ## data from CRG
   
    $destination = $self->_derive_CRG_path( \%path_options );                                         
  }
  elsif ( $file_host_name eq 'NCMLS' )   {                              ## data from NCMLS
  
    $destination = $self->_derive_NCMLS_path( \%path_options );  
  }
  elsif ( $file_host_name eq 'WTSI' ) {                                  ## data from WTSI
  
    $destination = $self->_derive_WTSI_path( \%path_options );  
  }
  else {
    throw( "Cannot find sample information from $filename and host id $file_host_name" );
  }
  
 return $destination; 
 
} 

sub _get_hash_from_file {
  my ( $file ) = @_;
  my %file_info;

  open my $fh, '<', $file;
  while ( <$fh> ){
    chomp;
    my @vals = split'\t';
    throw("expecting 2 columns , got ".scalar @vals." in $file") unless scalar @vals == 2;
    $file_info{ $vals[0] } = $vals[1];
  }
  close( $fh );
  return \%file_info;
}
  

sub _get_meta_data {
  my ( $meta_data_file, ) = @_;
  my %meta_data;
  my @headers;

  open my $mdfh,'<', $meta_data_file or die "Could not open $meta_data_file: $!";

  while (<$mdfh>) {
    chomp;
    my @vals = split "\t", $_;
    if (@headers) {
      my %row;
      @row{@headers}                  = @vals;
      $meta_data{ $row{sample_name} } = \%row;
    }
    else {
      @headers = map { lc($_) } @vals;
    }
  }
  close $mdfh;
  return \%meta_data;
}

sub _derive_CNAG_path {
  my ( $self, $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( "missing filename" );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( "missing aln_base_dir" );
  my $vcf_base_dir     = $$options{vcf_base_dir}     or throw( "missing vcf_base_dir" );
  my $results_base_dir = $$options{results_base_dir} or throw( "missing results_base_dir" );
  my $meta_data        = $$options{meta_data}        or throw( "missing meta_data object" );
  my $file_object      = $$options{file_object}      or throw( "missing file object" );
  my $species          = $$options{species}          or throw( 'missing species name' );          ### species name
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );     
  my $alt_sample_hash  = $$options{alt_sample_hash}  or throw( 'missing alt_sample_hash' );   
  
  my ( $pipeline_name, $output_dir, $meta_data_entry );
  my ( $sample_id, $experiment_type, $pipeline, $date, $suffix );  
  
  if ( $file_object->type eq 'BS_BAM_CNAG' || $file_object->type eq 'BS_BAI_CNAG' ) {
   # ( $sample_id, $suffix ) = split '\.',  $filename;                           ## CNAG BAM ana analysis file name convensions are different
     my @file_fields = split '\.',  $filename;
     $sample_id = $file_fields[0];
     $suffix    = $file_fields[-1];
     
     $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
     $meta_data_entry = $meta_data->{$sample_id};

     throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
     $meta_data_entry = _get_experiment_names( $meta_data_entry );              ## reset experiment specific hacks
     
     $pipeline_name = 'gem_cnag_bs';
     throw("expecting suffix bam, got $suffix") unless ( $suffix =~ m/^bam$/i or $suffix =~ m/^bai$/i );   ### file suffix check
     
     $experiment_type = 'BS';
     $output_dir = $aln_base_dir;
  } 
  elsif ( $file_object->type eq 'BS_BCF_CNAG' ||
          $file_object->type eq 'BS_BCF_CSI_CNAG' ){

    my @file_fields = split '\.',  $filename;
    $sample_id = $file_fields[0];
    $suffix    = $file_fields[-1] eq 'gz' ? 'bcf.gz' : 'bcf';
    $suffix   .= '.csi' if $file_object->type eq 'BS_BCF_CSI_CNAG';
    
    $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
    $meta_data_entry = $meta_data->{$sample_id};
    throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
    $meta_data_entry = _get_experiment_names( $meta_data_entry );
   
    $pipeline_name   = 'gem_cnag_bs';
    $experiment_type = 'WGBS'; 
    $output_dir      = $vcf_base_dir; 
  }                                                                    
  else {
   my $file_freeze_date;
   ( $sample_id, $experiment_type, $pipeline_name, $file_freeze_date, $suffix ) = ( $filename =~ /(\S+?)\.(\S+?)\.(\S+?)\.(\S+?)\.(\S+)/ ); 
    
    $sample_id       = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
    $meta_data_entry = $meta_data->{$sample_id};
    throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
    $output_dir =  $results_base_dir;
  }

  $experiment_type = $meta_data_entry->{experiment_type} if !$experiment_type;
  $meta_data_entry->{experiment_type} = $experiment_type;

  my %options = ( meta_data_entry => $meta_data_entry,
                  output_base_dir => $output_dir,
                  filename        => $filename,
                  experiment_type => $experiment_type,
                  suffix          => $suffix,
                  pipeline_name   => $pipeline_name,
                  species         => $species,
                  genome_version  => $genome_version,
                  freeze_date     => $freeze_date,
                );
  my $destination = $self->_get_new_path( \%options )  or throw("couldn't get new file path");
                                       
  return $destination; 
}

sub _derive_CRG_path {
  my ( $self, $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( 'missing filename' );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( 'missing aln_base_dir' );
  my $results_base_dir = $$options{results_base_dir} or throw( 'missing results_base_dir' );
  my $meta_data        = $$options{meta_data}        or throw( 'missing meta_data object' );
  my $file_object      = $$options{file_object}      or throw( 'missing file object' );
  my $species          = $$options{species}          or throw( 'missing species name' );      
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );  ## species name
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  
  my @file_fields   = split '\.',  $filename;
  my $sample_id     = $file_fields[0];
  my $mark          = $file_fields[1];
  my $pipeline_name = $file_fields[2];
  my $date          = $file_fields[3];
  my $suffix        = $file_fields[-1];
  
  
  my ($run, $big_wig, $output_dir, $is_summary_file );
  
  my $meta_data_entry = $meta_data->{$sample_id};
  
  $meta_data_entry = _get_experiment_names( $meta_data_entry );                        ## reset experiment specific hacks
  throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
  
  if ( $file_object->type eq 'RNA_BAM_CRG' || $file_object->type eq 'RNA_BAI_CRG' ) {      
      $output_dir = $aln_base_dir;
      $pipeline_name = 'gem_grape_crg';
  }
  elsif ( $file_object->type eq 'RNA_BAM_STAR_CRG' || $file_object->type eq 'RNA_BAI_STAR_CRG' ) {
      $output_dir = $aln_base_dir;
      $pipeline_name = 'star_grape2_crg';
   }
   elsif ( $file_object->type =~ m/SIGNAL/ ) {
    ( $run, $mark, $big_wig ) = split '\.', $filename;
    
     if ( $file_object->type eq 'RNA_SIGNAL_CRG' ) {
       $pipeline_name = 'gem_grape_crg';    
     }
     elsif( $file_object->type eq  'RNA_SIGNAL_STAR_CRG' ){
       $pipeline_name = 'star_grape2_crg';
      }
      $suffix = '.bw' if ( $big_wig eq 'bigwig' ); 
      
      if ( $mark eq 'plusRaw' ) {                                                ### RNA-Seq strand info
        $mark = 'plusStrand';
      }
      if ( $mark eq 'minusRaw' ) {
        $mark = 'minusStrand';
      }

      $meta_data_entry->{experiment_type} = $mark;
      
      $output_dir = $results_base_dir;
    }
    elsif ( $file_object->type =~ m/CONTIGS/ ) {
       if( $file_object->type eq 'RNA_CONTIGS_CRG' ){
         $pipeline_name = 'gem_grape_crg'; 
       }
       elsif ( $file_object->type eq 'RNA_CONTIGS_STAR_CRG' ){  
         $pipeline_name = 'star_grape2_crg';
       }
      $meta_data_entry->{experiment_type} = 'contigs';      
      $output_dir = $results_base_dir;      
    }  
    elsif ( $file_object->type eq 'RNA_JUNCTIONS_CRG' ) {
      $meta_data_entry->{experiment_type} = 'splice_junctions';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $file_object->type eq 'RNA_EXON_QUANT_CRG' ) {
      $meta_data_entry->{experiment_type} = 'exon_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $file_object->type eq 'RNA_EXON_QUANT_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'exon_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    } 
    elsif ( $file_object->type eq 'RNA_TRANSCRIPT_QUANT_CRG' ) {
      $meta_data_entry->{experiment_type} = 'transcript_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $file_object->type eq 'RNA_TRANSCRIPT_QUANT_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'transcript_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    }
    elsif ( $file_object->type eq 'RNA_GENE_QUANT_CRG' ) {
      $meta_data_entry->{experiment_type} = 'gene_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $file_object->type eq 'RNA_GENE_QUANT_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'gene_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    }
    elsif ( $file_object->type eq 'RNA_SPLICING_RATIOS_CRG' ) {
      $meta_data_entry->{experiment_type} = 'splice_ratios';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $file_object->type eq 'RNA_SPLICING_RATIOS_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'splice_ratios';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    }
    elsif ( $file_object->type eq 'RNA_COSI_STAR_CRG' ) {
     $output_dir = $results_base_dir;
     $meta_data_entry->{experiment_type} = $mark;
    }
    else {
      throw( "Unsure how to label file $filename " . $file_object->type );
    }
  
    my %options = ( meta_data_entry => $meta_data_entry,
                    output_base_dir => $output_dir,
                    filename        => $filename,
                    suffix          => $suffix,
                    pipeline_name   => $pipeline_name,
                    species         => $species,
                    freeze_date     => $freeze_date,
                    genome_version  => $genome_version,
                  );  
    my $destination = $self->_get_new_path( \%options  ) or throw("couldn't get new file path");
    return $destination;                                     
}

sub _derive_NCMLS_path {
  my ( $self, $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( "missing filename" );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( "missing aln_base_dir" );
  my $results_base_dir = $$options{results_base_dir} or throw( "missing results_base_dir" );
  my $species          = $$options{species}          or throw( 'missing species name' );                   ## species name
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );
  my $file_object      = $$options{file_object}      or throw( 'missing file object' );
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  
  my ( $sample_id, $mark, $suffix ) = split /\.|_/, $filename;                                   ## NCMLS file name format: ??? ## FIX_IT
  my $meta_data = $$options{meta_data} or throw( "missing meta_data object" );
  my $meta_data_entry = $meta_data->{$sample_id};
  $meta_data_entry = _get_experiment_names( $meta_data_entry );                                  ## reset experiment specific hacks

  my $pipeline_name;
  my $output_dir; 
  
  if ( $file_object->type =~ m/DNASE/ ) {
      $pipeline_name = 'hotspot_ncmls';
  }
  
  if ( $file_object->type =~ m/BAM/ || $file_object->type =~ m/BAI/ ) {      
      $output_dir = $aln_base_dir;
  }
  
  my %options = ( meta_data_entry => $meta_data_entry,
                  output_base_dir => $output_dir,
                  filename        => $filename,
                  suffix          => $suffix,
                  pipeline_name   => $pipeline_name,
                  species         => $species,
                  genome_version  => $genome_version,
                  freeze_date     => $freeze_date
                );

  my $destination = $self->_get_new_path( \%options ) or throw("couldn't get new file path");                                           
  return $destination;
    
}


sub _derive_WTSI_path {
  my ( $self, $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( "missing filename" );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( "missing aln_base_dir" );
  my $results_base_dir = $$options{results_base_dir} or throw( "missing results_base_dir" );
  my $species          = $$options{species}          or throw( 'missing species name' );                    ## species name
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );
  my $file_object      = $$options{file_object}      or throw( 'missing file object' );
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  my $pipeline_name;
  my $output_dir; 
  
  my ( $sample_id, $type, $algo, $date, $suffix  ) = split '\.', $filename;                     ## WTSI proposed file format
  
  
  my $meta_data = $$options{meta_data} or throw( "missing meta_data object" );
  my $meta_data_entry = $meta_data->{$sample_id};
  $meta_data_entry = _get_experiment_names( $meta_data_entry );                                  ## reset experiment specific hacks
  
  if ( $file_object->type =~ m/BAM/ || $file_object->type =~ m/BAI/ ) {      
      $output_dir = $aln_base_dir;
      $meta_data_entry->{experiment_type} = $type;
      $pipeline_name = $algo;
  }
  
  my %options = ( meta_data_entry => $meta_data_entry,
                  output_base_dir => $output_dir,
                  filename        => $filename,
                  suffix          => $suffix,
                  pipeline_name   => $pipeline_name,
                  species         => $species,
                  genome_version  => $genome_version,
                  freeze_date     => $freeze_date
               );

  my $destination = $self->_get_new_path( \%options ) or throw("couldn't get new file path");                                           
  return $destination;
}

sub _get_new_path {
 my ( $self, $options ) = @_;
 my $destination;

 my $meta_data_entry = $$options{meta_data_entry} or throw( 'No meta_data_entry' );
 my $output_base_dir = $$options{output_base_dir} or throw( 'No output_base_dir' );
 my $filename        = $$options{filename}        or throw( 'No filename' );
 my $species         = $$options{species}         or throw( 'missing species name' );                     ## species name
 my $genome_version  = $$options{genome_version}  or throw( 'missing genome version' );
 my $freeze_date     = $$options{freeze_date}     or throw( 'missing freeze date' );
 my $suffix          = $$options{suffix}          or throw( 'missing suffix' );
 my $pipeline_name   = $$options{pipeline_name}   or throw( 'missing pipeline_name' );
 
 my $alt_sample_hash = {};
 $alt_sample_hash    = $$options{alt_sample_hash};

 my $experiment_type; 
 $experiment_type = $meta_data_entry->{experiment_type} unless $experiment_type;
 
 $species = lc( $species );
 $species =~ s{\s+}{_}g;

 throw("species information is required") if !$species;
 
 my $sample_name = $meta_data_entry->{sample_name};
 $sample_name = $$alt_sample_hash{ $sample_name } if exists $$alt_sample_hash{ $sample_name };
 
 my @file_tokens = (   $sample_name,
                       $experiment_type,
                       $pipeline_name,
                       $genome_version,
                       $freeze_date,
                       $suffix
                   );

 my $new_file_name = join( '.', @file_tokens );
 $new_file_name =~ s!/!_!g;
 $new_file_name =~ s/ /_/g;
                                                       
 
 if ( $filename eq $new_file_name ) {
  warning ( "RETAINED file name:",$filename," : ",$new_file_name );
 }
 else {
  warning ( "CHANGED file name:",$filename," : ",$new_file_name );
 }

 $$meta_data_entry{sample_desc_1} = "NO_TISSUE" 
                   if $meta_data_entry->{sample_desc_1} eq "-";
  
 $$meta_data_entry{sample_desc_2} = "NO_SOURCE" 
                  if $meta_data_entry->{sample_desc_2} eq "-";

 $$meta_data_entry{sample_desc_3} = "NO_CELL_TYPE" 
                  if $meta_data_entry->{sample_desc_3} eq "-";
                  
 my $sample_desc_1 = $$meta_data_entry{sample_desc_1};
 $sample_desc_1 =~ s!/!_!g;

 my $sample_desc_2 = $$meta_data_entry{sample_desc_2};
 $sample_desc_2 =~ s!/!_!g;

 my $sample_desc_3 = $$meta_data_entry{sample_desc_3};
 $sample_desc_3 =~ s!/!_!g;
                        
 my @dir_tokens = (  $output_base_dir,                 
                     $species,
                     $genome_version,
                     $sample_desc_1,
                     $sample_desc_2,
                     $sample_desc_3,
                     $meta_data_entry->{library_strategy},
                     $meta_data_entry->{center_name} 
                  );
 
  @dir_tokens = map{ ( my $substr = $_ ) =~ s!//!_!g; $substr; } @dir_tokens;    ## not  allowing "/" in token
 
  my $dir = join( '/', @dir_tokens );
  
  $destination = $dir . '/' . $new_file_name;
  
  $destination =~ s!//!/!g;
  $destination =~ s/ /_/g;
  $destination =~ s/[ '",;()=\[\]]/_/g;
  $destination =~ s/_\//\//g;                                ## not  allowing "abc_/def"
  $destination =~ s/\/_/\//g;                                ## not  allowing "abc/_def"
  $destination =~ s/_+/_/g;
  
  return $destination;
}

sub _get_experiment_names {
  my ( $meta_data_entry ) = @_;
 
  if ( $meta_data_entry->{library_strategy} eq 'RNA-Seq'
    || $meta_data_entry->{library_strategy} eq 'DNase-Hypersensitivity' )  {
    $meta_data_entry->{experiment_type} = $meta_data_entry->{library_strategy};
  }
  
  $meta_data_entry->{experiment_type} =~ s/\QDNase-Hypersensitivity\E/DNase/;
  $meta_data_entry->{experiment_type} =~ s/\QDNA Methylation\E/BS-Seq/;
  
  return $meta_data_entry;
}

1;
