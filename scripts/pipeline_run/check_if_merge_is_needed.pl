use strict;
use warnings;
use ReseqTrack::DBSQL::DBAdaptor;
use Getopt::Long;
use ReseqTrack::Tools::Exception;
use Data::Dumper;
use Array::Utils qw(:all);

my ( $dbhost, $dbuser, $dbpass, $dbport, $dbname,$type_merge );

&GetOptions(
    'dbhost=s'                  => \$dbhost,
    'dbname=s'                  => \$dbname,
    'dbuser=s'                  => \$dbuser,
    'dbpass=s'                  => \$dbpass,
    'dbport=s'                  => \$dbport,
    'type=s'                    => \$type_merge
);

# dba + adaptors
my $db = ReseqTrack::DBSQL::DBAdaptor->new(
    -host   => $dbhost,
    -user   => $dbuser,
    -port   => $dbport,
    -dbname => $dbname,
    -pass   => $dbpass,
);
throw("Could not create db adaptor") if ( !$db );

$db->dbc->disconnect_when_inactive(1);
my $ca   = $db->get_CollectionAdaptor;
my $rm   = $db->get_RunMetaInfoAdaptor;

my %hash;

die ("[ERROR] Invalid --type: $type_merge") if $type_merge ne 'DNASE_MERGE_RUN_BAM' && $type_merge ne 'CHIP_MERGE_RUN_BAM';

my $cs = $ca->fetch_by_type( $type_merge);

foreach my $c ( @{$cs} ){
    throw("Collection $type_merge does not contain files") unless ( $c->table_name eq 'file' );
    
    my $experiment_name=$c->name;
    my $other_ids=$c->other_ids;
    foreach my $other_id (@$other_ids) {
	my $collectionArrays=$ca->fetch_by_other_id_and_type($other_id,'CHIP_RUN_BAM');
	foreach my $collection (@$collectionArrays) {
	    $hash{$experiment_name}{'merged'}{$collection->name}=0;
	}
    }   
}

foreach my $ex (keys %hash) {
    my $run_meta_info_array=$rm->fetch_by_experiment_id($ex);
    foreach my $rminfo (@$run_meta_info_array) {
	my $runid=$rminfo->run_id;
	#get run ids
	my $collection=$ca->fetch_by_name_and_type($runid,'CHIP_RUN_BAM');
	next if !$collection;
	$hash{$ex}{'new'}{$collection->name}=0;
    }
}

my $update_required=0;
foreach my $ex (keys %hash) {
    my @merged=keys %{$hash{$ex}{'merged'}};
    my @new=keys %{$hash{$ex}{'new'}};
    if ( array_diff(@merged, @new) ) {
	print "[HEY!] For experiment $ex: runs_merged:@merged total_runs:@new\n";
	$update_required=1;
    }
}

print "[INFO] Everything is up-to-date\n" if !$update_required;
