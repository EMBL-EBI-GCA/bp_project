#!/usr/bin/env perl
use strict;
use warnings;

use ReseqTrack::Tools::Exception qw( throw warning );
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::Tools::FileSystemUtils qw(check_file_exists check_directory_exists);
use ReseqTrack::Tools::FileUtils;
use ReseqTrack::Tools::GeneralUtils qw( get_time_stamps );
use ReseqTrack::Tools::Exception qw(throw warning);
use ReseqTrack::Tools::AttributeUtils;
use File::Copy;
use File::Path;
use File::Basename;
use Getopt::Long;
use autodie;

my $dbhost = 'mysql-blueprint';
my $dbuser = 'g1krw';
my $dbpass;
my $dbport = 4360;
my $dbname = 'ihec_benchmark';

my $host_name            = '1000genomes.ebi.ac.uk';
my $withdrawn_dir        = '/nfs/1000g-work/G1K/work/avikd/test_dir/file_release_bp_test/withdrawn_dir';
my $work_dir             = '/nfs/1000g-work/G1K/work/avikd/test_dir/test_file_release_pipe/run';
my $incoming_file_type   = 'INCOMING';
my $incoming_md5_type    = 'INCOMING_MD5';
my $incoming_md5_tag     = 'md5';
my $internal_file_type   = 'INTERNAL';
my $aln_base_dir         = '/nfs/1000g-work/G1K/work/avikd/test_dir/test_file_release_pipe//base_dir/alignment';
my $vcf_base_dir         = '/nfs/1000g-work/G1K/work/avikd/test_dir/test_file_release_pipe//base_dir/vcf';
my $results_base_dir     = '/nfs/1000g-work/G1K/work/avikd/test_dir/test_file_release_pipe//base_dir/results';
my $species              = 'homo sapiens';
my $freeze_date          = undef;
my $metadata_file        = '/nfs/1000g-work/ihec/work/davidr/cron/meta_data.tab';
my $validate_file        = 0;
my $assign_type          = 0;
my $check_path           = 0;
my $update_from_manifest = 0;
my $genome_version       = undef;              ## set genome version for all files
my $genome_version_file;                       ## use a file to set individual genome version
my $attribute_name       = 'genome_version';
my $collection_tag       = 'experiment_id';
my $move_file            = 0;
my $strict_check         = 0;
my $alt_sample_name      = undef;

&GetOptions(
        'dbhost=s'              => \$dbhost,
        'dbname=s'              => \$dbname,
        'dbuser=s'              => \$dbuser,
        'dbpass=s'              => \$dbpass,
        'dbport=s'              => \$dbport,
        'withdrawn_dir=s'       => \$withdrawn_dir,
        'work_dir=s'            => \$work_dir,
        'incoming_file_type=s'  => \$incoming_file_type,
        'incoming_md5_type=s'   => \$incoming_md5_type,
        'md5_manifest_tag=s'    => \$incoming_md5_tag,
        'metadata_file=s'       => \$metadata_file,
        'validate_file!'        => \$validate_file,
        'assign_type!'          => \$assign_type,
        'check_path!'           => \$check_path,
        'update_from_manifest!' => \$update_from_manifest,
        'genome_version=s'      => \$genome_version,
        'genome_version_file=s' => \$genome_version_file,
        'genome_attribute=s'    => \$attribute_name,
        'collection_tag=s'      => \$collection_tag,       
        'aln_base_dir=s'        => \$aln_base_dir,
        'vcf_base_dir=s'        => \$vcf_base_dir,
        'results_base_dir=s'    => \$results_base_dir,
        'move_file!'            => \$move_file,
        'strict_check!'         => \$strict_check,
        'freeze_date=s'         => \$freeze_date,
        'alt_sample_name=s'     => \$alt_sample_name,
       );

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
                        -host   => $dbhost,
                        -user   => $dbuser,
                        -port   => $dbport,
                        -dbname => $dbname,
                        -pass   => $dbpass,
                       );
throw( "No DB connection established" ) if ( ! $db );

$db->dbc->disconnect_when_inactive(1);

if ( $validate_file ) {
  throw("genome version is required") if !$genome_version && !$genome_version_file;

  if ( $strict_check ){ 
    validate_foreign_files( $db,            $incoming_file_type, $incoming_md5_type,  $incoming_md5_tag, 
                            $withdrawn_dir, $host_name,          $internal_file_type, $update_from_manifest  );
  }
  else {
   my $ha = $db->get_HostAdaptor;
   my $foreign_hosts = $ha->fetch_all_remote;
   
   foreach my $f_host ( @{ $foreign_hosts } ){
     my $f_host_name = $f_host->name;
     add_dropbox_path( $db, $f_host_name, $incoming_file_type ); 
   }
 }
  
  add_genome_version( $db, $incoming_file_type, $genome_version, $attribute_name, $genome_version_file );
}

assign_file_types( $db, $incoming_file_type ) if $assign_type;

#my %derive_path_options = ( aln_base_dir     => $aln_base_dir,
#                            vcf_base_dir     => $vcf_base_dir,
#                            results_base_dir => $results_base_dir,
#                            species          => $species,
#                            freeze_date      => $freeze_date,
#                            genome_attribute => $attribute_name,
#                            collection_tag   => $collection_tag,
#                            move_file        => $move_file,
#                            local_host_name  => $host_name,
#                            alt_sample_name => $alt_sample_name,
#                          );

if ( $check_path ){
  throw("freeze_date required") unless $freeze_date;

  my %derive_path_options = ( aln_base_dir      => $aln_base_dir,
                              vcf_base_dir      => $vcf_base_dir,
                              results_base_dir  => $results_base_dir,
                              species           => $species,
                              freeze_date       => $freeze_date,
                              genome_attribute  => $attribute_name,
                              collection_tag    => $collection_tag,
                              move_file         => $move_file,
                              local_host_name   => $host_name,
                              alt_sample_name   => $alt_sample_name,
                              incoming_type     => $incoming_file_type,
                              incoming_md5_type => $incoming_md5_type,
                              internal_type     => $internal_file_type,
                            );  


  build_dest_path( $db, $metadata_file, \%derive_path_options );

}


sub validate_foreign_files {
  my ( $db,            $incoming_file_type, $incoming_md5_type,  $incoming_md5_tag, 
       $withdrawn_dir, $host_name,          $internal_file_type, $update_from_manifest )= @_;

  my $ha = $db->get_HostAdaptor;
  my $foreign_hosts = $ha->fetch_all_remote;
  
  my $fa = $db->get_FileAdaptor;

  foreach my $f_host ( @{ $foreign_hosts } ){
    my $f_host_dbID = $f_host->{dbID};
    my $f_host_name = $f_host->name;
    my $f_files     = $fa->fetch_by_host( $f_host_dbID );
    my $f_host_dir  = $f_host->dropbox_dir;
 
    my $manifest_file;
    my $manifest_path;
    my $files_md5_hash;
    my $files_size_hash;
    my $matched_files_with_md5;
    my $manifest_check_flag;
    my $missing_md5_check = 0;

    my %incoming_files_no_md5;
    
    foreach my $f_file ( @{ $f_files } ) {
      my $f_file_name = $f_file->name;
      my $f_file_type = $f_file->type;
      my $f_file_md   = $f_file->{md5};

      if ( $f_file_type eq $incoming_md5_type ) {

        thorw( "multiple manifest files found for host $f_host_name" ) if $manifest_file;        ## if manifest is already defined and their is another md5 manifest for same host present

        $manifest_file = $f_file_name;
        $manifest_path = $f_file_name;

        throw( "no host dir for host $f_host_name" ) unless $f_host_dir;

        $manifest_path = $f_host_dir .'/'. $manifest_path;                                       ## get md5 manifest file and trim path
        check_file_exists( $manifest_path );

        ( $files_md5_hash, $files_size_hash ) = get_manifest_md5_and_size( $manifest_path, $f_host_name );     ## get md5 and size hash
      }
      else {
        $missing_md5_check++ unless $f_file_md;

        push @{$incoming_files_no_md5{ $f_host_name }}, $f_file_name  unless $f_file_md; 
      }
    }  
    throw("no manifest file found for host $f_host_name") 
         if( !$manifest_file && $missing_md5_check );                                             ## no manifest file present and files missing md5
    ( $matched_files_with_md5, $manifest_check_flag ) = 
           check_incoming_and_manifest_files( $db, $incoming_files_no_md5{ $f_host_name }, 
                                              $files_md5_hash );                                  ## check consistency of incoming and manifest files   
    
    if ( $manifest_file ) {
      if ( $manifest_check_flag > 0 ) {
        warning( "unknown files present in manifest file $manifest_path, not withdrawing" );
      }
      else {
        update_db_files( $db, $matched_files_with_md5, $files_size_hash ) 
                    if ( keys %{ $matched_files_with_md5 } && $update_from_manifest );            ## update files in db with md5 and size from manifest file

        warning( "withdrawing manifest file $manifest_file for host $f_host_name" );
        withdraw_file( $db, $manifest_file, $manifest_path, $withdrawn_dir, $host_name, $internal_file_type  );
        add_dropbox_path( $db, $f_host_name, $incoming_file_type );
      }        
    }
  }
}

sub get_manifest_md5_and_size {
  my ( $manifest_path, $host_name ) = @_;
  my ( %files_md5_hash, %files_size_hash );
  open my $fh, '<', $manifest_path;
  while( <$fh> ){
    chomp;
    next if /^$/;                                                                         ## skip empty lines

    my @values = split '\t';
    throw( "expecting 3 columns got $#values+1 for $manifest_path" ) if $#values != 2;

    my @file_path = ( $values[0] =~ /(\/)?(incoming\/)?($host_name\/)?(\S+)$/ );          ## get relative filename under the host directory
    my $file_name = $file_path[ $#file_path ];

    my $file_size = $values[1];
    my $file_md5  = $values[2];

    $files_md5_hash{ $file_name }  = $file_md5;
    $files_size_hash{ $file_name } = $file_size;
  }
  close ($fh);
  return ( \%files_md5_hash, \%files_size_hash );
}

sub check_incoming_and_manifest_files {
  my ( $db, $host_files, $files_md5_hash ) = @_;
  my %incoming_hash;                                                                      ## filename hash for incoming files
  my %matched_files_with_md5;
  my $manifest_check_flag = 0;

  my $fa = $db->get_FileAdaptor;

  foreach my $incoming_file ( @{ $host_files } ){
    if ( exists $$files_md5_hash{ $incoming_file }) {                                     ## look for manifest entries for incoming files without md5
      $matched_files_with_md5{ $incoming_file } = $$files_md5_hash{ $incoming_file };     ## get md5 if an entry found
    }
    else {
      warning( "$incoming_file is missing in md5 manifest file" );
    }
  }  
  
  foreach my $manifest_file ( keys %{ $files_md5_hash } ){
    my @file_path = ( $manifest_file =~ /(\/)?(incoming\/)?($host_name\/)?(\S+)$/ );          ## get relative filename under the host directory
    my $manifest_file_basename = $file_path[ $#file_path ];
    my $existing = $fa->fetch_by_name( $manifest_file_basename );                           ## check in db for already existing files
    
    unless ( $existing ) {
      warning( "$manifest_file present in manifest but not in db" );
      $manifest_check_flag++;                                                                 ## count entries in manifest file without any database entry
    }
  }
  
  return \%incoming_hash, $manifest_check_flag; 
}

sub withdraw_file {
  my ( $db, $file_name, $file_path, $withdrawn_dir, $host_name, $internal_file_type  ) = @_;
  
  my $fa = $db->get_FileAdaptor;
  my $ha = $db->get_HostAdaptor;
  my $host_object = $ha->fetch_by_name($host_name);

  my ( $current_time_stamp ) = get_time_stamps;
  $current_time_stamp =~ s{[ -]}{_}g;
 
  if ( my $existing_file = $fa->fetch_by_name( $file_name )) {

    my $old_type = $existing_file->type;
    my $old_host = $existing_file->host;

    my $new_dir_path = $withdrawn_dir .'/'.$old_host->name.'/'. $current_time_stamp;
    $new_dir_path  =~ s{//}{/}g;
    my $new_file_path = $new_dir_path .'/'. basename( $file_path );

    $existing_file->name( $file_path );
    $existing_file->type( $internal_file_type );
    $existing_file->host( $host_object );
    $existing_file->withdrawn("1");
    my $file_db_id = $existing_file->{dbID};

    my $history = ReseqTrack::History->new(
                  -other_id   => $file_db_id,
                  -table_name => 'file',
                  -comment    => "file path changed from $file_name to $file_path;type changed from $old_type to $internal_file_type;file marked as withdrawn",
                 );
  $existing_file->history( $history );
  $fa->update( $existing_file,1 );
  
  move_file_in_db_and_dir( [$existing_file], $new_dir_path, $internal_file_type, $db );
  throw( "Failed to move $file_path to $new_file_path") unless( -e $new_file_path );

  }
  else {
    throw( "couldn't find $file_name in database" );
  }
}

sub add_dropbox_path {
  my ( $db, $host_name, $internal_file_type ) = @_;

  my $ha = $db->get_HostAdaptor;
  my $fa = $db->get_FileAdaptor;
  my $host = $ha->fetch_by_name( $host_name );
  my $host_dir  = $host->dropbox_dir;
  throw( "no dir found for host $host_name" ) unless $host_dir;

  my $files = $fa->fetch_by_host( $host->dbID );

  foreach my $file ( @{$files} ){
    my $file_type = $file->type;
    my $file_path = $file->name;
    my $file_md5  = $file->md5;

    next unless $file_type eq $internal_file_type;  ## adding path to selective file types
    next unless $file_md5;                          ## skipping for files loaded in database without md5    

    if ( $file_path =~ /^\// ){
      my $dirname = dirname( $file_path );
      next if -d $dirname;                          ## skip adding dropbox path if its already absolute paths
      warning("Skipping file $file_path");    
    }

    my $new_file_path = $host_dir. '/'. $file_path;
    $new_file_path =~ s{//}{/}g;

    check_file_exists( $new_file_path );

  
    my $history = ReseqTrack::History->new(
                    -other_id  => $file->dbID,
                    -table_name => 'file',
                    -comment => "change path: $file_path to $new_file_path",
                  );

    $file->name( $new_file_path );
    $file->history( $history );
    $fa->update( $file );  
  } 
}

sub update_db_files {
  my ( $db, $matched_files_with_md5, $files_size_hash ) = @_;

  foreach my $file_path ( %{$matched_files_with_md5} ) {
    my $file_size = $$files_size_hash{ $file_path };
    my $file_md5  = $$matched_files_with_md5{ $file_path };

    throw("coundn't found md5 for $file_path" ) unless $file_md5;
    throw("coundn't found size for $file_path" ) unless $file_size;

    my $fa = $db->get_FileAdaptor;
    if ( my $existing_file = $fa->fetch_by_name( $file_path )) {
      my $existing_md5  = $existing_file->md5;
      my $existing_size = $existing_file->size;

      throw("MD5 already exists for $file_path") if $existing_md5;
     
      $existing_file->md5($file_md5);
      $existing_file->size($file_size);
      my $file_db_id = $existing_file->{dbID};

      my $history = ReseqTrack::History->new(
                     -other_id  => $file_db_id,
                     -table_name => 'file',
                     -comment => "md5 changed from $existing_md5 to $file_md5;file size changed from $existing_size to $file_size",
                   );

     $existing_file->history($history);

      $fa->update($existing_file);
    }  
    else {
      throw("file $file_path not found in db");
    }
  }
}

sub add_genome_version {
  my ( $db, $incoming_file_type, $genome_version, $attribute_name, $genome_version_file ) = @_;
  my $fa = $db->get_FileAdaptor;
  my $files = $fa->fetch_by_type( $incoming_file_type );  

  my $file_info = get_hash_from_file( $genome_version_file )
                                          if $genome_version_file;

  my @statistics;

  foreach my $file ( @{$files} ){   
    my $file_name = $file->name;

    if( $genome_version ){
      push @statistics, 
            create_attribute_for_object( $file, $attribute_name, $genome_version );
    }
    elsif ( $genome_version_file ){
      my $genome_name = $$file_info{ $file_name };
      throw( "genome version not found for $file_name" ) unless $genome_name;
      push @statistics,
            create_attribute_for_object( $file, $attribute_name, $genome_version );
    }
    else {
      warning( "not adding attribute $attribute_name for incoming file: $file_name" );
    }
    my $attributes = $file->uniquify_attributes( \@statistics );
    $file->attributes( $attributes );
    $fa->store_attributes( $file );
  }
}

#sub read_genome_version_file {
sub get_hash_from_file {
  my ( $file ) = @_;
  my %file_info;

  open my $fh, '<', $file;
  while ( <$fh> ){
    chomp;
    my @vals = split'\t';
    throw("expecting 2 columns , got ".scalar @vals." in $file") unless scalar @vals == 2;
    $file_info{ $vals[0] }= $vals[1];
  }
  close( $fh );
  return \%file_info;
}

sub assign_file_types {
  my ( $db, $incoming_file_type ) = @_;
  my $fa = $db->get_FileAdaptor;
  my $incoming_files = $fa->fetch_by_type($incoming_file_type);

  foreach my $new_file ( @$incoming_files ){
    my $file_md5  = $new_file->md5;
    my $file_size = $new_file->size;
    my $old_type  = $new_file->type;
    my $file_dbID = $new_file->{dbID};  
  
    next unless $file_md5 && $file_size;  ## skip if no md5 and size present
    my $updated_files = assign_type([$new_file],$db);               ## assign file_type
    my $new_type = $new_file->type;

    my $history = ReseqTrack::History->new(
                  -other_id  => $file_dbID,
                  -table_name => 'file',
                  -comment => "type changed from $old_type to $new_type",
                 );

   $new_file->history($history);
   $fa->update($new_file);
  }
}

sub build_dest_path {
  my ( $db, $metadata_file, $derive_path_options ) = @_;
  
  my $destination;
  my $collection_name;
 
  my $aln_base_dir      = $$derive_path_options{aln_base_dir}; 
  my $vcf_base_dir      = $$derive_path_options{vcf_base_dir}; 
  my $results_base_dir  = $$derive_path_options{results_base_dir};
  my $meta_data         = get_meta_data( $metadata_file );
  my $species           = $$derive_path_options{species};
  my $freeze_date       = $$derive_path_options{freeze_date};
  my $genome_attribute  = $$derive_path_options{genome_attribute};
  my $collection_tag    = $$derive_path_options{collection_tag};
  my $move_file         = $$derive_path_options{move_file};
  my $local_host_name   = $$derive_path_options{local_host_name};
  my $alt_sample_name   = $$derive_path_options{alt_sample_name};
  my $incoming_type     = $$derive_path_options{incoming_type};
  my $incoming_md5_type = $$derive_path_options{incoming_md5_type};
  my $internal_type     = $$derive_path_options{internal_type};
  my $genome_version;

  my $alt_sample_hash = {};
  $alt_sample_hash = get_hash_from_file( $alt_sample_name ) if $alt_sample_name;

  warn ( "files will be moved to destination directory") if $move_file;

  my $fa = $db->get_FileAdaptor;
  my $ha = $db->get_HostAdaptor;
  my $ca = $db->get_CollectionAdaptor;
  my $foreign_hosts = $ha->fetch_all_remote;
  my $local_host = $ha->fetch_by_name($local_host_name);
  throw("no host for $local_host_name") unless $local_host;
  
  foreach my $f_host ( @{ $foreign_hosts } ){
     my $f_host_dbID = $f_host->{dbID};
     my $f_host_name = $f_host->name;
     my $f_files     = $fa->fetch_by_host( $f_host_dbID );

     foreach my $file_object ( @{ $f_files } ) {
       my $f_file_name = $file_object->name;
       my $f_file_type = $file_object->type;
       my $f_file_md   = $file_object->{md5};
       next unless $f_file_md;
       next if $f_file_type eq $incoming_type;
       next if $f_file_type eq $incoming_md5_type;
       next if $f_file_type eq $internal_type;         ## skipping incoming and internal files 
     
       my $file_attributes = $file_object->attributes;
       my ( $attribute ) = grep { $_->attribute_name eq $genome_attribute } @$file_attributes;

       throw( "genome version not found for $f_file_name with attribute name $genome_attribute" ) unless $attribute;

       $genome_version = $attribute->attribute_value;

       throw( "no genome version found for $f_file_name" ) unless $genome_version;

       my ( $filename ) = fileparse( $f_file_name );

       my %option = (  'filename'         => $filename,
                       'aln_base_dir'     => $aln_base_dir,
                       'vcf_base_dir'     => $vcf_base_dir,
                       'results_base_dir' => $results_base_dir,
                       'meta_data'        => $meta_data,
                       'file_object'      => $file_object,
                       'species'          => $species,
                       'genome_version'   => $genome_version,
                       'freeze_date'      => $freeze_date,
                       'collection_tag'   => $collection_tag,
                       alt_sample_hash    => $alt_sample_hash,
                    );

       if ( $f_host_name eq 'CNAG' ) { 
         ( $destination,$collection_name ) = derive_CNAG_path( \%option  ); 
       }
       elsif ( $f_host_name eq 'CRG' ) {
        ( $destination, $collection_name ) = derive_CRG_path(  \%option  );
       }
       elsif( $f_host_name eq 'WTSI' ) {
        ( $destination, $collection_name ) = derive_WTSI_path( \%option );
       }
       elsif( $f_host_name eq 'NCMLS' ){
        ( $destination, $collection_name ) = derive_NCMLS_path( \%option );
       }
       else {
         throw("Unknown host $f_host_name");;
       }
  
       throw("no destination found for $f_file_name type $f_file_type") unless $destination;

       print "FILE: $f_file_name\nTYPE: $f_file_type\nNEW_PATH: $destination\ncollection_name:$collection_name\n\n";


      if( $move_file ) {
        $file_object->host($local_host);
        $file_object->host_id( $local_host->dbID ); 
        $file_object->name( $destination );
        
        my ( $name, $dir, $suffix ) = fileparse( $destination );
        mkpath($dir);
        move( $f_file_name, $destination );

        my $comment = calculate_comment( $file_object, $fa->fetch_by_dbID( $file_object->dbID ) ); 
     
        my $history = ReseqTrack::History->new(
                       -other_id   => $file_object->dbID,
                       -table_name => 'file',
                       -comment    => $comment,
                      );

        $file_object->history( $history );
        $fa->update( $file_object, 1, 1 );
 
        my$collection = ReseqTrack::Collection->new(
                         -name       => $collection_name,
                         -others     => [$file_object],
                         -type       => $file_object->type,
                         -table_name => 'file',
                       );

        $ca->store( $collection, 1 );
       }
     }
  }
}

sub get_meta_data {
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

sub derive_CNAG_path {
  my ( $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( "missing filename" );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( "missing aln_base_dir" );
  my $vcf_base_dir     = $$options{vcf_base_dir}     or throw( "missing vcf_base_dir" );
  my $results_base_dir = $$options{results_base_dir} or throw( "missing results_base_dir" );
  my $meta_data        = $$options{meta_data}        or throw( "missing meta_data object" );
  my $file_object      = $$options{file_object}      or throw( "missing file object" );
  my $species          = $$options{species}          or throw( 'missing species name' );          ### species name
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );       
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  my $collection_tag   = $$options{collection_tag}   or throw( 'missing collection tag' );
  my $alt_sample_hash  = $$options{alt_sample_hash};
  my $pipeline_name;
  my $output_dir;
  my $experiment_type;
  my $meta_data_entry;
  my $suffix;
  my $sample_id; 
  
  if ( $file_object->type eq 'BS_BAM_CNAG' or $file_object->type eq 'BS_BAI_CNAG' ) {
     #  ( $sample_id, $suffix ) = split '\.',  $filename;
        my @file_fields = split '\.',  $filename;
        $sample_id = $file_fields[0];
        $suffix    = $file_fields[-1];
          
       $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
       $meta_data_entry = $meta_data->{$sample_id};

       throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
       $meta_data_entry = get_experiment_names( $meta_data_entry );              ## reset experiment specific hacks

       $pipeline_name = 'gem_cnag_bs';                                           ### CNAG pipeline name 
       throw("expecting suffix bam, got $suffix") unless ( $suffix =~ m/^bam$/i or $suffix =~ m/^bai$/i );   ### file suffix check
       $experiment_type = 'BS';
       $output_dir = $aln_base_dir;                                              ### reset CNAG experiment type    
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
    $meta_data_entry = get_experiment_names( $meta_data_entry );
   
    $pipeline_name = 'gem_cnag_bs';
    $experiment_type = 'WGBS'; 
    $output_dir = $vcf_base_dir; 
  }
  else {
    my $file_freeze_date;
    ( $sample_id, $experiment_type, $pipeline_name, $file_freeze_date, $suffix ) = ( $filename =~ /(\S+?)\.(\S+?)\.(\S+?)\.(\S+?)\.(\S+)/ ); 
       
    $sample_id = $$alt_sample_hash{ $sample_id } if exists $$alt_sample_hash{ $sample_id };
    $meta_data_entry = $meta_data->{$sample_id};
    throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
    $output_dir =  $results_base_dir;
  }
  my $collection_name = $meta_data_entry->{$collection_tag};
  throw( "no collection name for sample $sample_id" ) unless $collection_name;

  my %path_option = (  meta_data_entry => $meta_data_entry,
                       output_base_dir => $output_dir,
                       filename        => $filename,
                       experiment_type => $experiment_type,
                       suffix          => $suffix,
                       pipeline_name   => $pipeline_name,
                       species         => $species,
                       freeze_date     => $freeze_date,
                       genome_version  => $genome_version,
                       alt_sample_hash => $alt_sample_hash,
                    ); 
  
  my $destination = get_new_path( \%path_option );

  return $destination, $collection_name; 
}

sub derive_CRG_path {
  my ( $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( 'missing filename' );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( 'missing aln_base_dir' );
  my $results_base_dir = $$options{results_base_dir} or throw( 'missing results_base_dir' );
  my $meta_data        = $$options{meta_data}        or throw( 'missing meta_data object' );
  my $file_object      = $$options{file_object}      or throw( 'missing file object' );
  my $species          = $$options{species}          or throw( 'missing species name' );      
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );                           ### species name
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  my $collection_tag   = $$options{collection_tag}   or throw( 'missing collection tag' ); 
  
  #my ( $sample_id, $mark, $pipeline_name, $date, $suffix ) = split '\.',  $filename;
  my @file_fields   = split '\.',  $filename;
  my $sample_id     = $file_fields[0];
  my $mark          = $file_fields[1];
  my $pipeline_name = $file_fields[2];
  my $date          = $file_fields[3];
  my $suffix        = $file_fields[-1];
   
  my ($run, $big_wig, $output_dir, $is_summary_file );
  
  my $meta_data_entry = $meta_data->{$sample_id};
  my $collection_name = $meta_data_entry->{$collection_tag};
  throw( "no collection name for sample $sample_id" ) unless $collection_name;  

  $meta_data_entry = get_experiment_names( $meta_data_entry ); ## reset experiment specific hacks
  throw( "No metadata for sample $sample_id" ) if ( $sample_id && !$meta_data_entry );
  
 ## $pipeline_name = 'gem_grape_crg';                                                          ### CRG pipeline name
  
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
    
      if( $file_object->type eq 'RNA_SIGNAL_CRG' ){
        $pipeline_name = 'gem_grape_crg';            
      } 
      elsif( $file_object->type eq  'RNA_SIGNAL_STAR_CRG' ){
        $pipeline_name = 'star_grape2_crg';
      }
                             
      $suffix = '.bw' if ( $big_wig eq 'bigwig' );                               ### suffix
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
      throw( "Couldn't build path for file $filename with " . $file_object->type );
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

    my $destination = get_new_path( \%path_options );
    return $destination, $collection_name;                                     
}

sub derive_NCMLS_path {
  my ( $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( "missing filename" );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( "missing aln_base_dir" );
  my $results_base_dir = $$options{results_base_dir} or throw( "missing results_base_dir" );
  my $species          = $$options{species}          or throw( 'missing species name' );                                 ### species name
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );
  my $file_object      = $$options{file_object}      or throw( 'missing file object' );
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  my $collection_tag   = $$options{collection_tag}   or throw( 'missing collection tag' );
 
  my ( $sample_id, $mark, $suffix ) = split /\.|_/, $filename;                   ### NCMLS file name format: ??? ## FIX_IT
  my $meta_data = $$options{meta_data} or throw( "missing meta_data object" );
  my $meta_data_entry = $meta_data->{$sample_id};
  $meta_data_entry = get_experiment_names( $meta_data_entry ); ## reset experiment specific hacks
  my $collection_name = $meta_data_entry->{$collection_tag};
  throw( "no collection name for sample $sample_id" ) unless $collection_name;

  my $pipeline_name;
  my $output_dir; 
  
  if ( $file_object->type =~ m/DNASE/ ) {
      $pipeline_name = 'hotspot_ncmls';
  }
  
  if ( $file_object->type =~ m/BAM/ || $file_object->type =~ m/BAI/ ) {      
      $output_dir = $aln_base_dir;
  }
  
  my %path_options = ( meta_data_entry => $meta_data_entry,
                       output_base_dir => $output_dir,
                       filename        => $filename,
                       suffix          => $suffix,
                       pipeline_name   => $pipeline_name,
                       species         => $species,
                       freeze_date     => $freeze_date,
                       genome_version  => $genome_version
                      ); 
 
  my $destination = get_new_path( \%path_options );

  return $destination, $collection_name;
    
}


sub derive_WTSI_path {
  my ( $options ) = @_;
  
  my $filename         = $$options{filename}         or throw( "missing filename" );
  my $aln_base_dir     = $$options{aln_base_dir}     or throw( "missing aln_base_dir" );
  my $results_base_dir = $$options{results_base_dir} or throw( "missing results_base_dir" );
  my $species          = $$options{species}          or throw( 'missing species name' );                                 ### species name
  my $freeze_date      = $$options{freeze_date}      or throw( 'missing freeze date' );
  my $file_object      = $$options{file_object}      or throw( 'missing file object' );
  my $genome_version   = $$options{genome_version}   or throw( 'missing genome version' );
  my $collection_tag   = $$options{collection_tag}   or throw( 'missing collection tag' );

  my $pipeline_name;
  my $output_dir; 
  
  my ( $sample_id, $type, $algo, $date, $suffix  ) = split '\.', $filename;              ### WTSI_proposed file format
  
  
  my $meta_data = $$options{meta_data} or throw( "missing meta_data object" );
  my $meta_data_entry = $meta_data->{$sample_id};
  $meta_data_entry = get_experiment_names( $meta_data_entry ); ## reset experiment specific hacks
  my $collection_name = $meta_data_entry->{$collection_tag};
  throw( "no collection name for sample $sample_id: $filename" ) unless $collection_name;
    
  if ( $file_object->type =~ m/BAM/ || $file_object->type =~ m/BAI/ ) {      
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
                       freeze_date     => $freeze_date,
                       genome_version  => $genome_version
                     );

  my $destination = get_new_path( \%path_options  );
  
  return $destination, $collection_name;
}

sub get_new_path {
 my ( $options ) = @_;
 my $destination;
 my $meta_data_entry = $$options{meta_data_entry} or throw( "No meta_data_entry");
 my $output_base_dir = $$options{output_base_dir} or throw( "No output_base_dir");
 my $filename        = $$options{filename}        or throw( "No filename");
 my $species         = $$options{species}         or throw( "missing species name" );                                 ### species name
 my $freeze_date     = $$options{freeze_date}     or throw( "missing freeze date" );
 my $suffix          = $$options{suffix}          or throw( "missing suffix for $filename" );
 my $pipeline_name   = $$options{pipeline_name}   or throw( "missing pipeline_name" );
 my $genome_version  = $$options{genome_version}  or throw( 'missing genome version' );
 my $experiment_type = $$options{experiment_type};

 my $alt_sample_hash = {};
 $alt_sample_hash    = $$options{alt_sample_hash};
 
 $experiment_type = $meta_data_entry->{experiment_type} unless $experiment_type;
 $species = lc( $species );
 $species =~ s/\s+/_/g;
 
 #my @file_tokens = (   $meta_data_entry->{sample_name},
 #                      $experiment_type,
 #                      $pipeline_name, $freeze_date,  $suffix
 #                  );
 
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
  warning ( "RETAINED file name: $filename : $new_file_name" );
 }
 else {
  warning ( "CHANGED file name: $filename : $new_file_name" );
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
  $destination =~ s/[ ,;()=]/_/g;
  $destination =~ s/_\//\//g; ## not  allowing "abc_/def"
  $destination =~ s/_+/_/g;
  
  return $destination;
}

sub get_experiment_names {
  my ( $meta_data_entry ) = @_;
 
  if ( $meta_data_entry->{library_strategy} eq 'RNA-Seq'
    || $meta_data_entry->{library_strategy} eq 'DNase-Hypersensitivity' )  {
    $meta_data_entry->{experiment_type} = $meta_data_entry->{library_strategy};
  }
  
  $meta_data_entry->{experiment_type} =~ s/\QDNase-Hypersensitivity\E/DNase/;
  $meta_data_entry->{experiment_type} =~ s/\QDNA Methylation\E/BS-Seq/;
  
  return $meta_data_entry;
}


=pod

=head1 NAME

reseq-personal/avikd/validate_incoming_files.pl

=head1 SYNPOSIS

This script should check files from remote hosts loaded in databses with specific file types. Read md5 manifest and assign md5 to files ( if its not done already). Withdraw md5 manifest and move it to a separate directory. Also it would generate projected destination path for each incoming files.


=head1 OPTIONS

Database options

These set the parameters for the necessary database connection

 -dbhost, the name of the mysql-host
 -dbname, the name of the mysql database
 -dbuser, the name of the mysql user
 -dbpass, the database password if appropriate
 -dbport, the port the mysql instance is running on, this defaults to 4175
          the standard port for mysql-g1k

Standard options other than db paramters

 -work_dir              work directory
 -withdrawn_dir         withdrawn files directory
 -incoming_file_type    file type assigned to incoming files
 -incoming_md5_type     file type assigned to incoming md5 manifest 
 -md5_manifest_tag      manifest file tag
 -metadata_file         metadata index
 -validate_file         validate files based on manifest (default: off)
 -assign_type           assign file types based on file_type_rules_table (default: off)
 -check_path            generate destination path (default: off)
 -move_file             Move file to destination path is provided with -check_path option
 -genome_version        genome version name for all the files in database
 -genome_version_file   genome version for each files in the database in a tab-delimited file 
                        (column 1: file name, column 2: genome version)
 -genome_attribute      genome version will be added to database with the specified attribute name (default: genome_version)
 -collection_tag        tag name for building collection (default: experiment_id )
 -aln_base_dir          Alignment base directory
 -results_base_dir      Results base directory
 
=head1 Examples


 $DB_OPTS= '-dbhost a_dbhost -dbport 4360 -dbuser a_user -dbpass ???? -dbname a_db'
 
 Run it like this for the blueprint project:
 perl reseq-personal/avikd/validate_incoming_files.pl  $DB_OPTS  -validate_file -genome_version GRCh38 -assign_type -check_path


 Its possible to run individual steps of this script.

 Validating files:

 perl reseq-personal/avikd/validate_incoming_files.pl  $DB_OPTS  -validate_file -genome_version GRCh38 


 Assigning file types:

 perl reseq-personal/avikd/validate_incoming_files.pl  $DB_OPTS -assign_type

 
 Checking destination file paths:

 perl reseq-personal/avikd/validate_incoming_files.pl  $DB_OPTS -check_path

=cut
