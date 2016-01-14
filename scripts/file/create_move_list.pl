#!/usr/bin/env perl
use strict;
use warnings;
use ReseqTrack::Tools::Exception;
use ReseqTrack::DBSQL::DBAdaptor;
use File::Basename;
use Getopt::Long;
use ReseqTrack::File;
use ReseqTrack::Host;
use autodie;

$| = 1;

my $dbhost;
my $dbuser;
my $dbpass;
my $dbport;
my $dbname;
my $run_meta;
my $exp_meta;
my $run_keyword    = 'RUN_ID';
my $exp_keyword    = 'EXPERIMENT_ID';
my $type_list_file;
my $aln_dir;
my $result_dir;
my $vcf_dir;
my $species        = 'homo_sapiens';
my $genome_version;

GetOptions(
        'dbhost=s'          => \$dbhost,
        'dbname=s'          => \$dbname,
        'dbuser=s'          => \$dbuser,
        'dbpass=s'          => \$dbpass,
        'dbport=s'          => \$dbport,
        'run_meta=s'        => \$run_meta,
        'exp_meta=s'        => \$exp_meta,
        'run_keyword=s'     => \$run_keyword,
        'exp_keyword=s'     => \$exp_keyword,
        'type_list=s'       => \$type_list_file,
        'aln_dir=s'         => \$aln_dir,
        'vcf_dir=s'         => \$vcf_dir,
        'result_dir=s'      => \$result_dir,
        'species=s'         => \$species,
        'genome_version=s'  => \$genome_version,
      );

die `perldoc -t $0` if !$run_meta || !$exp_meta || !$run_keyword || !$exp_keyword || !$type_list_file ||
                       !$aln_dir || !$vcf_dir || !$result_dir || !$species || !$genome_version;

my %file_path_hash = ( aln_dir        => $aln_dir,
                       vcf_dir        => $vcf_dir,
                       result_dir     => $result_dir,
                       species        => $species,
                       genome_version => $genome_version,
                     );

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
          -host   => $dbhost,
          -user   => $dbuser,
          -port   => $dbport,
          -dbname => $dbname,
          -pass   => $dbpass,
        );

my $ca = $db->get_CollectionAdaptor;
my $fa = $db->get_FileAdaptor;
my $ha = $db->get_HostAdaptor;

my $file_types    = get_hash( $type_list_file );
my $run_meta_hash = get_index_hash( $run_meta, $run_keyword );
my $exp_meta_hash = get_index_hash( $exp_meta, $exp_keyword );


TYPE:
foreach my $type ( keys %$file_types ){ 
  my $cs = $ca->fetch_by_type( $type );
  next TYPE if !$cs;

  COLLECTION:
  foreach my $c ( @{$cs} ){
    next COLLECTION if $c->table_name ne 'file';
    my $collection_name = $c->name;

    for my $f ( @{ $c->others } ) {
      my $file_path = $f->name;
      my $file_size = $f->size;
      my $file_type = $f->type;
      my $file_md5  = $f->md5;
      my $host_id   = $f->host_id;
      
      next COLLECTION if $f->withdrawn eq 1;        ## not moving withdrawn files

      my $host = $ha->fetch_by_dbID($host_id);
      next COLLECTION if $host->remote eq 1;        ## not moving remote files

      my $new_path;

      if ( $collection_name =~ /^ERR\d+/ ){         ## collections with ERR ids
        $new_path = get_new_path( \%file_path_hash, $run_meta_hash, $collection_name, $file_path, $type );
      }
      elsif ( $collection_name =~ /^ERX\d+/ ){      ## collections with ERX ids
        $new_path = get_new_path( \%file_path_hash, $exp_meta_hash, $collection_name, $file_path, $type );
      }
      throw("check collection name: $collection_name ,type: $file_type ,file: $file_path") if !$new_path;

      print join("\t", $file_path, $new_path),$/
          if $file_path ne $new_path;
    }
  }
}


sub get_hash {
  my ( $file ) = @_;
  my %hash;

  open my $fh, '<', $file;
  while( <$fh> ){
    chomp;
    $hash{ $_ }++;
  } 
  return \%hash;
}

sub get_index_hash {
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
      die "$key_string not found in $file\n" unless $key_index >= 0;
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

sub get_new_path{
  my ( $file_path_hash, $index_hash, $collection_name, $file_path, $file_type ) = @_;
  
  my $aln_dir        = $file_path_hash->{aln_dir};
  my $vcf_dir        = $file_path_hash->{vcf_dir};
  my $result_dir     = $file_path_hash->{result_dir};
  my $species        = $file_path_hash->{species};
  my $genome_version = $file_path_hash->{genome_version};

  my ( $file_name ) = fileparse( $file_path );
  my $meta_data_entry = $$index_hash{ $collection_name };


  my $output_base_dir;

  if ( $file_path =~ /^$aln_dir/ ){
    $output_base_dir = $aln_dir;
  }
  elsif ( $file_path =~ /^$result_dir/ ){
    $output_base_dir = $result_dir;
  }
  elsif ( $file_path =~ /^$vcf_dir/ ){
    $output_base_dir = $vcf_dir;
  }
  
  throw("check file path: $file_path") if !$output_base_dir;
      
  $$meta_data_entry{SAMPLE_DESC_1} = "NO_TISSUE"
                   if $meta_data_entry->{SAMPLE_DESC_1} eq "-";

  $$meta_data_entry{SAMPLE_DESC_2} = "NO_SOURCE"
                  if $meta_data_entry->{SAMPLE_DESC_2} eq "-";

  $$meta_data_entry{SAMPLE_DESC_3} = "NO_CELL_TYPE"
                  if $meta_data_entry->{SAMPLE_DESC_3} eq "-";

  
  my $metadata_SAMPLE_DESC_1 = $meta_data_entry->{SAMPLE_DESC_1};
  $metadata_SAMPLE_DESC_1 =~ s!/!_!g;
  
  my $metadata_SAMPLE_DESC_2 = $meta_data_entry->{SAMPLE_DESC_2};
  $metadata_SAMPLE_DESC_2 =~ s!/!_!g;

  my $metadata_SAMPLE_DESC_3 = $meta_data_entry->{SAMPLE_DESC_3};
  $metadata_SAMPLE_DESC_3 =~ s!/!_!g; 
  
  my @dir_tokens = ( $output_base_dir,
                     $species,
                     $genome_version,
                     $metadata_SAMPLE_DESC_1,
                     $metadata_SAMPLE_DESC_2,
                     $metadata_SAMPLE_DESC_3,
                     $meta_data_entry->{LIBRARY_STRATEGY},
                     $meta_data_entry->{CENTER_NAME}
                  );

  
  @dir_tokens = map{ ( my $substr = $_ ) =~ s!//!_!g; $substr; } @dir_tokens;    ## not  allowing "/" in token
  
  my $dir = join( '/', @dir_tokens );

  my $destination = $dir . '/' . $file_name;

  $destination =~ s!//!/!g;

  $destination =~ s/ /_/g;
  $destination =~ s/[ ,;()'"=]/_/g;
  $destination =~ s/_\//\//g; ## not  allowing "abc_/def"
  $destination =~ s/\/_/\//g; ## not  allowing "abc/_def"
  $destination =~ s/_+/_/g;                                    

 warn "$collection_name has following path: $destination\n" if $destination =~ /\/NO_.*\//;

 return $destination;
}

=head1
  Move list generation script based on sample metadata change

=head2
  Usage:

  perl create move_list.pl 

=head2
  Options:

   -dbhost          Mysql db host name
   -dbport          Mysql db port
   -dbname          Mysql db name
   -dbuser          Mysql db user
   -dbpass          Mysql db pass
   -run_meta        Run metadata index file
   -exp_meta        Experiment metadata index file
   -run_keyword     Keyword for building run level paths, default 'RUN_ID'
   -exp_keyword     Keyword for building experiment level paths, default 'EXPERIMENT_ID'
   -type_list       Lists of file types to build paths
   -aln_dir         Alignment directory prefix
   -result_dir      Results directory Prefix
   -vcf_dir         VCF directory prefix
   -species         Species name, default 'homo_sapiens'
   -genome_version  Genome build name

=cut

