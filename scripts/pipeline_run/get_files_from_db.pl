#!/usr/bin/env perl

use strict;
use warnings;
use ReseqTrack::Tools::Exception;
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::Tools::FileUtils;
use ReseqTrack::Tools::FileSystemUtils;
use File::Basename;
use Getopt::Long;
use ReseqTrack::File;
use ReseqTrack::Host;
use autodie;

$| = 1;

my $dbhost = $ENV{'DB_HOST'};
my $dbuser = $ENV{'DB_USER'};
my $dbpass = $ENV{'DB_PASS'};
my $dbport = $ENV{'DB_PORT'};
my $dbname = 'blueprint';
my $md5_file = 'bp_chip_files.txt';
my $help;
my $type = 'CHIP_FASTQ';

&GetOptions(
  'dbhost=s'          => \$dbhost,
  'dbname=s'          => \$dbname,
  'dbuser=s'          => \$dbuser,
  'dbpass=s'          => \$dbpass,
  'dbport=s'          => \$dbport,
  'md5_file=s'        => \$md5_file,
  'help!'             => \$help,
  'collection_type=s' => \$type,
    );

if(!$dbhost || !$dbname || !$dbuser || !$type || !$md5_file){
  throw("Must provide collection, output md5 file and database connection details with -collection_type -md5_file -dbhost -dbuser -dbpass ".
        "-dbport -dbname");
}

my $db = ReseqTrack::DBSQL::DBAdaptor->new(
  -host   => $dbhost,
  -user   => $dbuser,
  -port   => $dbport,
  -dbname => $dbname,
  -pass   => $dbpass,
    );

open( my $fh, ">",$md5_file);

my $ca = $db->get_CollectionAdaptor;
my $fa = $db->get_FileAdaptor;


my $cs = $ca->fetch_by_type( $type );
throw("Failed to find a collection for $type") unless $cs;


foreach my $c ( @{$cs} ){
  throw("Collection $type does not contain files") unless ( $c->table_name eq 'file' );

  my $collection_name = $c->name;

  for my $f ( @{ $c->others } ) {
    my $file_path = $f->name;
    my $file_size = $f->size;
    my $file_md5 = $f->md5;
 
    print $fh "$collection_name\t$file_path\t$file_md5\t$file_size\n";
  }
}
close ($fh);
