#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use ReseqTrack::Tools::Exception qw(throw);
use ReseqTrack::Tools::FileSystemUtils qw(check_file_exists );
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::File;
use ReseqTrack::Collection;
use autodie;

$| = 1;

my $dbhost    = $ENV{'DB_HOST'};
my $dbuser    = $ENV{'DB_USER'};
my $dbpass    = $ENV{'DB_PASS'};
my $dbport    = $ENV{'DB_PORT'};
my $dbname    = 'blueprint_GRCh38';
my $list_file;
my $host_name = '1000genomes.ebi.ac.uk' ;
my $type;

&GetOptions(
  'dbhost=s'       => \$dbhost,
  'dbname=s'       => \$dbname,
  'dbuser=s'       => \$dbuser,
  'dbpass=s'       => \$dbpass,
  'dbport=s'       => \$dbport,
  'list_file=s'    => \$list_file,
  'type=s'         => \$type,
  );

die 'file list not found' unless $list_file;

my $file_data = read_file_list( $list_file );

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
    -host   => $dbhost,
    -user   => $dbuser,
    -port   => $dbport,
    -dbname => $dbname,
    -pass   => $dbpass,
);


my $fa = $db->get_FileAdaptor;
my $ca = $db->get_CollectionAdaptor;
my $ha = $db->get_HostAdaptor;

my $host = $ha->fetch_by_name( $host_name );

foreach my $line( @{$file_data} ){
  my $file_path  = $line->{path};
  my $collection = $line->{collection}; 
  my $file_size  = $line->{size};
  my $file_md5   = $line->{md5}; 
 
  check_file_exists( $file_path );

  my $basename = fileparse( $file_path );
  my $exists = $fa->fetch_by_filename( $basename );

  throw( "file already exists and not set to update: $file_path" ) if @$exists;
  
  $file_size =~ s/[\s,"]//g;
  throw( "did not recognise size of $file_path: $file_size" ) if $file_size !~ /^\d+$/; 
  
  $file_md5 =~ s/\s//g;
  throw( "md5 is wrong length $file_path: $file_md5" ) if length( $file_md5 ) != 32;

  my $file = ReseqTrack::File->new(
        -name => $file_path, 
        -size => $file_size,
        -md5  => $file_md5, 
        -host => $host,
        -type => $type,
        );

  $fa->store( $file );

  my $collection_obj = ReseqTrack::Collection->new(
          -name => $collection, 
          -type => $type,
          -others => $file, 
          -table_name =>'file',
  );

  $ca->store( $collection_obj );
}


sub read_file_list {
  my ( $file ) = @_;
  my @input_data;
  my @header = qw/collection path md5 size/;

  open my $fh, '<', $file; 
  while ( <$fh> ){
    chomp;
    my @values = split "\t";
    my %row;
    @row{@header}=@values;  
    push @input_data, \%row;  
  }
  close( $fh );
  return \@input_data;
}
