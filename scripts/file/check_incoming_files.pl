#!/usr/bin/env perl

use strict;
use warnings;

use ReseqTrack::Tools::Exception qw( throw warning );
use ReseqTrack::DBSQL::DBAdaptor; 
use ReseqTrack::Tools::FileSystemUtils qw(check_file_exists check_directory_exists);
use ReseqTrack::Tools::GeneralUtils qw( get_time_stamps );
use ReseqTrack::Tools::FileUtils qw(create_object_from_path assign_type assign_type_by_filename);
use ReseqTrack::Tools::CurrentTreeMaker;
use ReseqTrack::Tools::CurrentTreeDiffer;
use File::Temp;
use File::Copy;
use File::Path;
use File::Basename qw(fileparse basename);
use Getopt::Long;
use autodie;

local $| = 1;

my $dbhost;
my $dbuser;
my $dbpass;
my $dbport;
my $dbname;


my $file_tree_name           = 'current.tree';
my $type                     = 'INCOMING';                ## file type for any incoming files from foreign host
my $manifest_type            = 'INCOMING_MD5';            ## if check_md5_file_and_load = 0, md5 manifest files will be added to db with this tag   
my $check_md5_file_and_load  = 1;                         ## check md5 manifest for format and file if its present and load md5 to db, if 0 then skip file format check and md5 loading
my $md5_manifest_tag         = 'md5';
my $withdraw_manifest        = 1;
my $withdrawn_dir;
my $dir_to_tree;
my $staging_dir;
my %options;
my $trim_dir;
my $old_tree_dir;

&GetOptions( 
        'dbhost=s'                  => \$dbhost,
        'dbname=s'                  => \$dbname,
        'dbuser=s'                  => \$dbuser,
        'dbpass=s'                  => \$dbpass,
        'dbport=s'                  => \$dbport,
        'dir_to_tree=s'             => \$dir_to_tree,
        'work_dir=s'                => \$staging_dir,
        'old_tree_dir=s'            => \$old_tree_dir,
        'file_tree_name=s'          => \$file_tree_name,
        'check_md5_file_and_load!'  => \$check_md5_file_and_load,
        'options=s'                 => \%options,
        'incoming_file_type=s'      => \$type,
        'incoming_md5_type=s'       => \$manifest_type,
        'md5_manifest_tag=s'        => \$md5_manifest_tag,
        'withdraw_manifest!'        => \$withdraw_manifest,
        'withdrawn_dir=s'           => \$withdrawn_dir,
       );

throw( 'Required option -dir_to_tree & -staging_dir' ) if ( !$dir_to_tree or !$staging_dir );
throw( "withdrawn_dir required for withdrawing manifest files" ) if ( $withdraw_manifest && !$withdrawn_dir );

$old_tree_dir //= $dir_to_tree;

my $old_tree_path = $old_tree_dir . '/' . $file_tree_name;
$old_tree_path    =~ s{//}{/}g;

check_file_exists( $old_tree_path );

# create the tree diffs object early in the script to make sure date is correct
# because the cron job is run close to midnight.

my $tree_diffs = ReseqTrack::Tools::CurrentTreeDiffer->new(
                  -old_tree               => $old_tree_path,
                  -output_dir             => $staging_dir,
                  -old_changelog_dir      => $staging_dir
                 );

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
                        -host   => $dbhost,
                        -user   => $dbuser,
                        -port   => $dbport,
                        -dbname => $dbname,
                        -pass   => $dbpass,
                       );

throw( "No DB connection established" ) if ( !$db );

my $fa = $db->get_FileAdaptor;

my ( $current_time_stamp ) = get_time_stamps;

my $tmp_current_tree = File::Temp->new( TEMPLATE => "temp.current.$current_time_stamp.XXXX",
                                        UNLINK   => 0, 
                                        DIR      => $staging_dir,
                                        SUFFIX   => '.tree'
                                      );

my $new_tree_path = $tmp_current_tree->filename;                                         ## creating new temp current.tree file

my $tree_maker = ReseqTrack::Tools::CurrentTreeMaker->new(
  -skip_regexes => $file_tree_name,
  -dir_to_tree  => $dir_to_tree,
  -file_adaptor => $fa,
  -trim_dir     => $trim_dir,
  -options      => \%options,
);

$tree_maker->run;
$tree_maker->print_to_file( $new_tree_path );
$tree_diffs->new_tree( $new_tree_path );

my $ftra = $db->get_FileTypeRuleAdaptor;
my $file_type_rules = $ftra->fetch_all_in_order;

$tree_diffs->file_type_rules( $file_type_rules );
$tree_diffs->run;

my $logged_changes = $tree_diffs->log_change;
 
if ( !$tree_diffs->quick_diff() ) {                                                         ## check for changes in current.tree
  warning( "No new files found" );
  unlink( $new_tree_path );
  exit(0);
}
else {
  warning( "Skipping MD5 and file size information as -check_md5_file_and_load is false" ) 
         if !$check_md5_file_and_load;                                                      ## -check_md5_file_and_load 0 will skip checking manifest file 

  process_files( $db, $logged_changes, $type, $manifest_type, $check_md5_file_and_load,  
                 $md5_manifest_tag,    $withdraw_manifest,    $withdrawn_dir           );   ## check for md5 manifest and load files

  unlink( $old_tree_path );                                                                 ## remove old current.tree
  move( $new_tree_path, $old_tree_path );                                                   ## move temp current.tree to curren.tree
}

$db->dbc->disconnect_when_inactive(1);



sub process_files {
  my ( $db, $logged_changes, $type, $manifest_type, $check_md5_file_and_load,  
       $md5_manifest_tag,    $withdraw_manifest,    $withdrawn_dir           ) = @_;

  my $host_adaptor = $db->get_HostAdaptor;
  my %host_files;

  while (my ( $change, $details_list ) = each %$logged_changes ) {
    if ( $change eq 'new' ) {
    foreach my $details ( @{ $details_list } ) {
      my $new_file_name = $$details[0];                                                    ## get new file names from current.tree diff hash
      my $basename = fileparse( $new_file_name );
      my $existing = $fa->fetch_by_filename( $basename );                                  ## check in db for already existing files
      if ( @$existing ) {
        throw( "File with name $basename already exists in the database: " .  
              join(',', map {$_->name} @$existing) ); 
      } 
      else {
        my ( $host_name, $file_base_name ) = 
                       ( $new_file_name =~ /incoming\/(\w+)\/(\S+)$/ );                    ## get host name for files 

        my $host = $host_adaptor->fetch_by_name( $host_name );                             ## check host
        throw( "No host name found for $new_file_name" ) unless $host;            

        push ( @{$host_files { $host_name }}, $file_base_name );                           ## make files array per host, add relative file path
      } 
    }
   }
  }

  ## strict checking of manifest before processing the files

  foreach my $host_name ( keys %host_files ){
    my ( $manifest_file, $host_file_type_hash ) = assign_incoming_type( $host_name,     \%host_files, $type, 
                                                                        $manifest_type, $md5_manifest_tag   );
    my $file_count = scalar @{$host_files{ $host_name }};
    warning( "received $file_count files for host $host_name");
 
    unless ( $manifest_file ) {
      warning ( "Couldn't find any MD5 manifest file for host $host_name" );                         ## warn that no manifest is available
      throw( "No md5 manifest found, set --nocheck_md5_file_and_load  to skip manifest file check" )
          if $check_md5_file_and_load;                                                               ## stop if no md5 manifest is present without load_without_md5_file option
    }
  }

 ## loading files to database
 
  foreach my $host_name ( keys %host_files ){
    my ( $manifest_file, $host_file_type_hash ) = assign_incoming_type( $host_name,     \%host_files, $type, 
                                                                        $manifest_type, $md5_manifest_tag   );

    my ( $files_md5_hash, $files_size_hash ) =  check_md5_manifest_file ( $db, $host_name,      \%host_files, 
                                                                          $host_file_type_hash, $manifest_type ) 
                                                                      if  $check_md5_file_and_load ;                ## check md5 format and incoming files
   
  
    foreach my $file_path ( @{$host_files{ $host_name }} ) {

      my $file_type = $$host_file_type_hash{ $host_name }{ $file_path }{ 'type' };
      my @host_file_paths;
      push @host_file_paths, $file_path;

      if ( $check_md5_file_and_load && $file_type eq $manifest_type ) { 
        load_new_files( $db, $host_name, $file_type, \@host_file_paths ) 
                      unless $withdraw_manifest;                                        ## load manifest files in db
       
        withdraw_manifest_file( $db, \@host_file_paths, $host_name ,$withdrawn_dir )
                      if $withdraw_manifest;                                            ## withdrawing manifest file

      }
      elsif ( $check_md5_file_and_load && $file_type ne $manifest_type ) {
        load_new_files( $db, $host_name, $file_type, \@host_file_paths, 
                        $files_md5_hash, $files_size_hash );                            ## load files with md5 ( how to load size ?? )
      }
      else {
        load_new_files( $db, $host_name, $file_type, \@host_file_paths );
      }
    }
  }
}


sub check_md5_manifest_file {
  my ( $db, $host_name, $host_files, $host_file_type_hash, $manifest_type ) = @_;
  
  my $host_adaptor = $db->get_HostAdaptor;
  my $host         = $host_adaptor->fetch_by_name( $host_name );
  my $host_dir     = $host->dropbox_dir;
  my %new_host_files;

  my @manifest_path_array;

  foreach my $file_path ( @{$$host_files{ $host_name }} ) {
    my $file_type = $$host_file_type_hash{ $host_name }{ $file_path }{ 'type' };                ## get file type

    push @manifest_path_array, $file_path 
         if  $file_type eq $manifest_type;
      
  }

  throw( "manifest file not found for $host_name" ) if scalar @manifest_path_array == 0;
  #throw( "multiple manifest file found for $host_name" ) if ( scalar @manifest_path_array > 1 );  


  my %manifest_enrty = map{ $_ => 1 } @manifest_path_array;
  @manifest_path_array = map{ $host_dir .'/'. $_ } @manifest_path_array;  

  my ( $files_md5_hash, $files_size_hash ) = 
       get_manifest_md5_and_size( \@manifest_path_array, $host_name );                            ## get md5 and size hash for files from each hosts
  
  my $host_files_array = $$host_files{ $host_name };
  my @new_host_files_array;

  foreach my $file( @$host_files_array ){
    push @new_host_files_array, $file unless exists $manifest_enrty{ $file };                     ## removing md5 manifest file from incoming list 
  }
 
  $new_host_files{ $host_name } = \@new_host_files_array;                                         ## reseting incoming files list

  check_incoming_and_manifest_files( $new_host_files{ $host_name }, $files_md5_hash );            ## check consistency of incoming and manifest files 

  return $files_md5_hash, $files_size_hash;                                              
}

sub withdraw_manifest_file {
  my ( $db, $host_files, $host_name ,$withdrawn_dir ) = @_;
  my $ha   = $db->get_HostAdaptor;
  my $host = $ha->fetch_by_name( $host_name );
  my $host_dir     = $host->dropbox_dir;

  my ( $current_time_stamp ) = get_time_stamps;
  $current_time_stamp =~ s{[ -]}{_}g;

  my $new_dir_path = $withdrawn_dir .'/'.$host_name.'/'. $current_time_stamp;
  $new_dir_path  =~ s{//}{/}g;

  foreach my $file ( @$host_files ){
    my $source_file_path = $host_dir.'/'.$file;
    $source_file_path =~ s{//}{/}g;
    check_file_exists( $source_file_path );

    my $new_file_path = $new_dir_path .'/'. basename( $file );

    warning( "moving $source_file_path to $new_file_path" );
    mkpath( $new_dir_path );
    move( $source_file_path, $new_file_path );
  }
}

sub check_incoming_and_manifest_files {
  my ( $host_files, $files_md5_hash ) = @_;

  my $manifest_files_count = scalar keys %{ $files_md5_hash };
  my $incoming_files_count = scalar @{ $host_files }; 

  throw( "File numbers are not matching for md5 manifest: $manifest_files_count and incoming dir: $incoming_files_count" ) 
          unless $manifest_files_count eq $incoming_files_count;

  my %incoming_hash;                                                                      ## filename hash for incoming files

  foreach my $incoming_file ( @{ $host_files } ){
     my $incoming_file_base = basename( $incoming_file );
     $incoming_hash{ $incoming_file_base }++;

     throw( "$incoming_file is missing in md5 manifest file" )
           unless exists $$files_md5_hash{ $incoming_file_base };                         ## check for incoming files basename in md5 manifest for each host
  }  
  
  foreach my $manifest_file ( keys %{ $files_md5_hash } ){
    throw( "$manifest_file not present in incoming directory" )  
           unless exists $incoming_hash{ $manifest_file };                                 ## check for manifest files in incoming dir
  } 
}

sub get_manifest_md5_and_size {
  my ( $manifest_path, $host_name ) = @_;
  my ( %files_md5_hash, %files_size_hash );

  foreach my $manifest_file( @$manifest_path ){
    check_file_exists( $manifest_file );

    open my $fh, '<', $manifest_file;
    while( <$fh> ){
      chomp;
      next if /^$/;                                                                         ## skip empty lines
      next if /^#/;                                                                         ## skip commented lines 

      my @values = split '\t';
      throw( "expecting 3 columns got $#values+1 for $manifest_file" ) if $#values != 2;

      my @file_path = ( $values[0] =~ /(\/)?(incoming\/)?($host_name\/)?(\S+)$/ );          ## get relative filename under the host directory
      my $file_name = $file_path[ $#file_path ];
      my $file_base = basename( $file_name );
     
      my $file_size = $values[1];
      my $file_md5  = $values[2];
      throw("md5 is not valid: $file_name: $file_md5") if $file_md5 !~ /^[a-f0-9]{32}$/;

      $files_md5_hash{ $file_base }  = $file_md5;
      $files_size_hash{ $file_base } = $file_size;
    }
    close ($fh);
  }
  return ( \%files_md5_hash, \%files_size_hash );
}

sub assign_incoming_type { 
  my ( $host_name, $host_files, $type, $manifest_type, $md5_manifest_tag ) = @_;

  my %host_file_type_hash;
  my $manifest_md5 = 0;

  foreach my $file_path ( @{$$host_files{ $host_name }} ) {
    if ( $file_path =~ /$md5_manifest_tag/i ){
       $manifest_md5 = 1;
       $host_file_type_hash{ $host_name }{ $file_path }{ 'type' } = $manifest_type;  
    }
    else {
      $host_file_type_hash{ $host_name }{ $file_path }{ 'type' } = $type;
    }
  }
  return $manifest_md5, \%host_file_type_hash;
}

sub load_new_files {
  my ( $db, $host_name, $type ,$host_files , $md5_hash , $size_hash )  = @_;

  my $fa   = $db->get_FileAdaptor;
  my $ha   = $db->get_HostAdaptor;
  my $host = $ha->fetch_by_name( $host_name );

  foreach my $load_path( @{$host_files} ){
    my $file = create_object_from_path( $load_path, $type, $host );

    if( $md5_hash && $size_hash ){
    
       my $load_path_base = basename( $load_path );
       throw("md5 not found for $load_path in host $host_name") 
            unless exists $$md5_hash{ $load_path_base };

       throw("size not found for $load_path in host $host_name")
           unless exists $$size_hash{ $load_path_base };

       my $md5 = $$md5_hash{ $load_path_base };
       $file->md5( $md5 );
    
       my $size = $$size_hash{ $load_path_base };
       $file->size( $size );
    }
    $fa->store( $file );
  }  
}


=pod

=head1 NAME

file/check_incoming_files.pl

=head1 SYNPOSIS

 This script should read a 'current.tree' file from the dropbox directory
 then, create a new 'current.tree' file and compare both for the new incoming files,
 get foreign hosts name for each files, check in database for defined hosts, look for a
 md5 manifest file, read its content and compare incoming files. If the mad5 manifest has
 entries for all the files then it will load the files to database with their md5 and size 
 and assigned to foreign hosts.

 Overriding default behaviour:
 Its possible to skip the check for md5 manifest file by setting 'check_md5_file_and_load'
 as false. Then it will load all the file to database with their foreign hosts information
 skipping md5 and size
 
 Manifest file can be created using file-manifest tools
 ( https://github.com/EMBL-EBI-GCA/gca-tools/blob/master/submissions/file-manifest ) 

=head1 OPTIONS

Database options

These set the parameters for the necessary database connection

 -dbhost, the name of the mysql-host
 -dbname, the name of the mysql database
 -dbuser, the name of the mysql user
 -dbpass, the database password if appropriate
 -dbport, the port the mysql instance

Standard options other than db paramters


 -options                    for constructing a hash of options for the CurrentTreeMaker module
                             e.g. -options skip_base_directories=1

 -dir_to_tree                Directory to create current.tree file.
                             default: null

 -work_dir                   Name of the working dir should be provided
                             default: null


 -file_tree_name             The output file has this name
                             This file must also be present in the dir_to_tree
                             default: current.tree


 -check_md5_file_and_load    Look for md5 manifest files and load md5 and size information
                             default: true

 -withdraw_manifest          Withdraw manifest file after checking, require '-withdrawn_dir' 
                             default: true

 -withdrawn_dir              Manifest files withdrawn directory


=head1 Examples

 $DB_OPTS= '-dbhost MYSQL_HOST -dbport PORT -dbuser USER -dbpass PASSWORD -dbname DBNAME'
 
 Run it like this for the Blueprint project (with default parameters):

   perl file/check_incoming_files.pl  $DB_OPTS  -dir_to_tree  <dir_path> -work_dir <work_dir_path> -withdrawn_dir <withdrawn_dir_path>

 To load incoming files without md5 information
  
   perl file/check_incoming_files.pl  $DB_OPTS  -dir_to_tree  <dir_path> -work_dir <work_dir> -withdrawn_dir <withdrawn_dir_path> --nocheck_md5_file_and_load

 To skip withdrawing manifest files and loading them in database:

   perl file/check_incoming_files.pl  $DB_OPTS  -dir_to_tree  <dir_path> -work_dir <work_dir> --nowithdraw_manifest

=cut




