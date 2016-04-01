package BlueprintFileMovePath;;

use strict;
use warnings;
use Exporter qw( import );

our @EXPORT_OK = qw( cnag_path crg_PATH wtsi_path get_meta_data_from_index );

sub cnag_path {
  my( $options ) = @_;

  my $filename        = $options->{filename}        or die 'no filename';
  my $filetype        = $options->{filetype}        or die 'no filetype';
  my $alt_sample_hash = $options->{alt_sample_hash} or die 'no alt_sample_hash';
  my $meta_data       = $options->{meta_data}       or die 'no meta_data';
  my $collection_tag  = $options->{collection_tag}  or die 'no collection_tag';
  my $aln_base_dir    = $options->{aln_base_dir}    or die 'no aln_base_dir'; 
  my $vcf_base_dir    = $options->{vcf_base_dir}    or die 'no vcf_base_dir';
  my $results_base_dir= $options->{results_base_dir}or die 'no results_base_dir';
  my $species         = $options->{species}         or die 'no species';
  my $genome_version  = $options->{genome_version}  or die 'no genome_version';
  my $freeze_date     = $options->{freeze_date}     or die 'no freeze_date';
  $collection_tag     = lc( $collection_tag );

  my ( $pipeline_name, $output_dir, $meta_data_entry, $collection_name );
  my ( $sample_id, $experiment_type, $pipeline, $date, $suffix ); 
  
  if ( $filetype eq 'BS_BAM_CNAG' || $filetype eq 'BS_BAI_CNAG' ) {
    my @file_fields = split '\.', $filename;
    $sample_id = $file_fields[0];
    $suffix    = $file_fields[-1];
    
    $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
    $meta_data_entry = $meta_data->{$sample_id};
    die "No metadata for sample $sample_id" if ( $sample_id && !$meta_data_entry );

    $meta_data_entry = _get_experiment_names( $meta_data_entry ); 
    
    $collection_name = $meta_data_entry->{$collection_tag};
    die "no collection name for sample $sample_id"  unless $collection_name;  
    
    $pipeline_name = 'gem_cnag_bs';
    die "expecting suffix bam, got $suffix" unless ( $suffix =~ m/^bam$/i or $suffix =~ m/^bai$/i ); 
 
    $experiment_type = 'BS';
    $output_dir = $aln_base_dir;
   
    $meta_data_entry->{experiment_type} = $experiment_type; 
  }
  elsif ( $filetype eq 'BS_BCF_CNAG' ||
          $filetype eq 'BS_BCF_CSI_CNAG' ){

    my @file_fields = split '\.', $filename;
    $sample_id = $file_fields[0];
    $suffix = $file_fields[-1] eq 'gz' ? 'bcf.gz' : 'bcf';
    $suffix .= '.csi' if $filetype eq 'BS_BCF_CSI_CNAG';

    $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
    $meta_data_entry = $meta_data->{$sample_id};
    die "No metadata for sample $sample_id" if ( $sample_id && !$meta_data_entry );
    $meta_data_entry = _get_experiment_names( $meta_data_entry );
    
    $collection_name = $meta_data_entry->{$collection_tag};
  
    die "no collection name for sample $sample_id"  unless $collection_name;
    $pipeline_name   = 'gem_cnag_bs';
    $experiment_type = 'WGBS'; 
    $output_dir      = $vcf_base_dir; 
    $meta_data_entry->{experiment_type} = $experiment_type;  
  }
  else {
    my $file_freeze_date;
    ( $sample_id, $experiment_type, $pipeline_name, $file_freeze_date, $suffix ) = ( $filename =~ /(\S+?)\.(\S+?)\.(\S+?)\.(\S+?)\.(\S+)/ );
    
    $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
    $meta_data_entry = $meta_data->{$sample_id};
    die "No metadata for sample $sample_id"  if ( $sample_id && !$meta_data_entry );

    $output_dir = $results_base_dir;
    $collection_name = $meta_data_entry->{$collection_tag};
    die "no collection name for sample $sample_id" unless $collection_name;
    $meta_data_entry->{experiment_type} = $experiment_type;
  }
  my %path_options = ( meta_data_entry => $meta_data_entry,
                       output_base_dir => $output_dir,
                       filename        => $filename,
                       experiment_type => $experiment_type,
                       suffix          => $suffix,
                       pipeline_name   => $pipeline_name,
                       species         => $species,
                       genome_version  => $genome_version,
                       freeze_date     => $freeze_date,
                    );

  my $destination = _get_new_path( \%path_options )  or die "couldn't get new file path";                                       
  return $destination, $collection_name;
}

sub crg_path {
  my ( $options ) = @_;
  
  my $filename         = $options->{filename}          or die 'missing filename';
  my $aln_base_dir     = $options->{aln_base_dir}      or die 'missing aln_base_dir';
  my $results_base_dir = $options->{results_base_dir}  or die 'missing results_base_dir';
  my $meta_data        = $options->{meta_data}         or die 'missing meta_data object';
  my $filetype         = $options->{filetype}          or die 'missing file object';
  my $species          = $options->{species}           or die 'missing species name';      
  my $freeze_date      = $options->{freeze_date}       or die 'missing freeze date';  ## species name
  my $genome_version   = $options->{genome_version}    or die 'missing genome version';
  my $collection_tag   = $options->{collection_tag}   or die 'missing collection name tag';
  $collection_tag      = lc( $collection_tag );

  my @file_fields   = split '\.',  $filename;
  my $sample_id     = $file_fields[0];
  my $mark          = $file_fields[1];
  my $pipeline_name = $file_fields[2];
  my $date          = $file_fields[3];
  my $suffix        = $file_fields[-1];
 
  my ($run, $big_wig, $output_dir, $is_summary_file );
  
  my $meta_data_entry = $meta_data->{$sample_id};
  
  $meta_data_entry = _get_experiment_names( $meta_data_entry );

  die "No metadata for sample $sample_id"  if ( $sample_id && !$meta_data_entry );

  my $collection_name = $meta_data_entry->{$collection_tag};
  die "no collection name for sample $sample_id"  unless $collection_name; 

  if ( $filetype eq 'RNA_BAM_CRG' || $filetype eq 'RNA_BAI_CRG' ) {      
      $output_dir = $aln_base_dir;
      $pipeline_name = 'gem_grape_crg';
  }
  elsif ( $filetype eq 'RNA_BAM_STAR_CRG' || $filetype eq 'RNA_BAI_STAR_CRG' ) {
      $output_dir = $aln_base_dir;
      $pipeline_name = 'star_grape2_crg';
   }
   elsif ( $filetype =~ m/SIGNAL/ ) {
    ( $run, $mark, $big_wig ) = split '\.', $filename;
    
     if ( $filetype eq 'RNA_SIGNAL_CRG' ) {
       $pipeline_name = 'gem_grape_crg';    
     }
     elsif( $filetype eq  'RNA_SIGNAL_STAR_CRG' ){
       $pipeline_name = 'star_grape2_crg';
      }
      $suffix = '.bw' if ( $big_wig eq 'bigwig' ); 

      if ( $mark eq 'plusRaw' ) {
        $mark = 'plusStrand';
      }
      if ( $mark eq 'minusRaw' ) {
        $mark = 'minusStrand';
      }

      $meta_data_entry->{experiment_type} = $mark;
      
      $output_dir = $results_base_dir;
    }
    elsif ( $filetype =~ m/CONTIGS/ ) {
       if( $filetype eq 'RNA_CONTIGS_CRG' ){
         $pipeline_name = 'gem_grape_crg'; 
       }
       elsif ( $filetype eq 'RNA_CONTIGS_STAR_CRG' ){  
         $pipeline_name = 'star_grape2_crg';
       }
      $meta_data_entry->{experiment_type} = 'contigs';      
      $output_dir = $results_base_dir;      
    }  
    elsif ( $filetype eq 'RNA_JUNCTIONS_CRG' ) {
      $meta_data_entry->{experiment_type} = 'splice_junctions';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $filetype eq 'RNA_EXON_QUANT_CRG' ) {
      $meta_data_entry->{experiment_type} = 'exon_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $filetype eq 'RNA_EXON_QUANT_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'exon_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    } 
    elsif ( $filetype eq 'RNA_TRANSCRIPT_QUANT_CRG' ) {
      $meta_data_entry->{experiment_type} = 'transcript_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $filetype eq 'RNA_TRANSCRIPT_QUANT_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'transcript_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    }
    elsif ( $filetype eq 'RNA_GENE_QUANT_CRG' ) {
      $meta_data_entry->{experiment_type} = 'gene_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $filetype eq 'RNA_GENE_QUANT_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'gene_quantification';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    }
    elsif ( $filetype eq 'RNA_SPLICING_RATIOS_CRG' ) {
      $meta_data_entry->{experiment_type} = 'splice_ratios';
      $output_dir = $results_base_dir;
      $pipeline_name = 'gem_grape_crg';
    }
    elsif ( $filetype eq 'RNA_SPLICING_RATIOS_STAR_CRG' ) {
      $meta_data_entry->{experiment_type} = 'splice_ratios';
      $output_dir = $results_base_dir;
      $pipeline_name = 'rsem_grape2_crg';
    }
    elsif ( $filetype eq 'RNA_COSI_STAR_CRG' ) {
     $output_dir = $results_base_dir;
     $meta_data_entry->{experiment_type} = $mark;
    }
    else {
      die "Unsure how to label file $filename " . $filetype ;
    }
  
    my %path_options = ( meta_data_entry => $meta_data_entry,
                         output_base_dir => $output_dir,
                         filename        => $filename,
                         suffix          => $suffix,
                         pipeline_name   => $pipeline_name,
                         species         => $species,
                         freeze_date     => $freeze_date,
                         genome_version  => $genome_version,
                       );  
    my $destination = _get_new_path( \%path_options  ) or die "couldn't get new file path";
    return $destination, $collection_name;                                     
}

sub wtsi_path {
  my ( $options ) = @_;
  
  my $filename         = $options->{filename}         or die "missing filename";
  my $aln_base_dir     = $options->{aln_base_dir}     or die "missing aln_base_dir";
  my $results_base_dir = $options->{results_base_dir} or die "missing results_base_dir";
  my $species          = $options->{species}          or die 'missing species name';                    ## species name
  my $freeze_date      = $options->{freeze_date}      or die 'missing freeze date';
  my $filetype         = $options->{filetype}         or die 'missing file object';
  my $genome_version   = $options->{genome_version}   or die 'missing genome version';
  my $collection_tag   = $options->{collection_tag}   or die 'missing collection name tag';
  $collection_tag      = lc( $collection_tag );

  my $pipeline_name;
  my $output_dir; 
  
  my ( $sample_id, $type, $algo, $date, $suffix  ) = split '\.', $filename;                     ## WTSI proposed file format
  
  
  my $meta_data = $$options{meta_data} or throw( "missing meta_data object" );
  my $meta_data_entry = $meta_data->{$sample_id};
  $meta_data_entry = _get_experiment_names( $meta_data_entry );                                  ## reset experiment specific hacks
  die "no metadata entry for $sample_id" if !$meta_data_entry;

  my $collection_name = $meta_data_entry->{$collection_tag};
  die "no collection name for sample $sample_id"  unless $collection_name;
  
  if ( $filetype =~ m/BAM/ || $filetype =~ m/BAI/ ) {      
      $output_dir = $aln_base_dir;
      $meta_data_entry->{experiment_type} = $type;
      $pipeline_name = $algo;
  }
  
  my %path_options = ( meta_data_entry => $meta_data_entry,
                       output_base_dir => $output_dir,
                       filename        => $filename,
                       suffix          => $suffix,
                       pipeline_name   => $pipeline_name,
                       species         => $species,
                       genome_version  => $genome_version,
                       freeze_date     => $freeze_date
                    );

  my $destination = _get_new_path( \%path_options ) or die "couldn't get new file path",$/;                                           
  return $destination, $collection_name;
}


sub get_meta_data_from_index {
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

      die "EXPERIMENT_STATUS not found",$/ unless $row{experiment_status};
      die "SAMPLE_STATUS not found",$/     unless $row{sample_status};

      next unless $row{experiment_status} eq 'private' or
                  $row{experiment_status} eq 'public';
      next unless $row{sample_status} eq 'private' or
                  $row{sample_status} eq 'public';

      $meta_data{ $row{sample_name} } = \%row;
    }
    else {
      @headers = map { lc($_) } @vals;
    }
  }
  close $mdfh;
  return \%meta_data;
}




sub _get_new_path {
 my ( $options ) = @_;
 my $destination;

 my $meta_data_entry = $options->{meta_data_entry} or throw( 'No meta_data_entry' );
 my $output_base_dir = $options->{output_base_dir} or throw( 'No output_base_dir' );
 my $filename        = $options->{filename}        or throw( 'No filename' );
 my $species         = $options->{species}         or throw( 'missing species name' );                     ## species name
 
 my $genome_version  = $options->{genome_version}  or throw( 'missing genome version' );
 my $freeze_date     = $options->{freeze_date}     or throw( 'missing freeze date' );
 my $suffix          = $options->{suffix}          or throw( 'missing suffix' );
 my $pipeline_name   = $options->{pipeline_name}   or throw( 'missing pipeline_name' );
 
 my $alt_sample_hash = {};
 $alt_sample_hash    = $options->{alt_sample_hash};

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
  warn "RETAINED file name: $filename : $new_file_name",$/;
 }
 else {
  warn "CHANGED file name: $filename : $new_file_name",$/;
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

 @dir_tokens = map{ ( my $substr = $_ ) =~ s!//!_!g; $substr; } @dir_tokens; ## not  allowing "/" in token
 
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
