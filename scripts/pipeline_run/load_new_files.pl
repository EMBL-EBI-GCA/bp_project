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
use Array::Diff;
use autodie;

$| = 1;

my $dbhost = $ENV{'DB_HOST'};
my $dbuser = $ENV{'DB_USER'};
my $dbpass = $ENV{'DB_PASS'};
my $dbport = $ENV{'DB_PORT'};
my $help;
my $type = 'CHIP_FASTQ';

&GetOptions(
  'dbhost=s'          => \$dbhost,
  'dbuser=s'          => \$dbuser,
  'dbpass=s'          => \$dbpass,
  'dbport=s'          => \$dbport,
  'help!'             => \$help,
  'collection_type=s' => \$type,
    );

if(!$dbhost || !$dbuser || !$type){
  throw("Must provide collection, database connection details with -collection_type -dbhost -dbuser -dbpass -dbport");
}

my $db_bp = ReseqTrack::DBSQL::DBAdaptor->new(
    -host   => $dbhost,
    -user   => $dbuser,
    -port   => $dbport,
    -dbname => 'blueprint',
    -pass   => $dbpass,
    );

my $db_bp38 = ReseqTrack::DBSQL::DBAdaptor->new(
    -host   => $dbhost,
    -user   => $dbuser,
    -port   => $dbport,
    -dbname => 'blueprint_GRCh38',
    -pass   => $dbpass,
    );

my $host_name = '1000genomes.ebi.ac.uk';
my $ca = $db_bp->get_CollectionAdaptor;
my $ca_bp38 = $db_bp38->get_CollectionAdaptor;
my $fa = $db_bp38->get_FileAdaptor;
my $ha = $db_bp38->get_HostAdaptor;

my $host = $ha->fetch_by_name( $host_name );

my $cs = $ca->fetch_by_type( $type );
throw("Failed to find a collection for $type") unless $cs;

my $new_file_seen=0;

foreach my $c ( @{$cs} ){
    throw("Collection $type does not contain files") unless ( $c->table_name eq 'file' );

    my $collection_name = $c->name;

    for my $f ( @{ $c->others } ) {
	print STDOUT "Processing: ",$f->name,"\n";

	my $nf=$fa->fetch_by_name($f->name);
	if (!$nf) {
	    $new_file_seen=1;
	    my $file_path=$f->name;
	    print STDOUT "[INFO] $collection_name $file_path is not present in blueprint_GRCh38. This file will be stored in the DB\n";
            check_file_exists( $file_path );
	    
	    $nf= ReseqTrack::File->new(
		-name => $file_path, 
		-size => $f->size,
		-md5  => $f->md5, 
		-host => $host,
		-type => $f->type);

	    my $basename = fileparse( $f->name );
	    my $exists = $fa->fetch_by_filename( $file_path );

	    throw( "file already exists and not set to update: $file_path" ) if @$exists;
	    $fa->store( $nf );
	    
	    my $collection_obj = ReseqTrack::Collection->new(
		-name => $collection_name, 
		-type => $type,
		-others => $nf, 
		-table_name =>'file',
		);

	    $ca_bp38->store( $collection_obj );
	} 
    }
}

print STDOUT "[INFO] There are no new files in the blueprint DB\n" if !$new_file_seen;
