use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;

my $log=$ARGV[0];
my $wp10_list=$ARGV[1];

die ("[ERROR] perl $0 decryption_log.txt /path/to/wp10.txt") if !$log || !$wp10_list;

my $DB_PASS =  $ENV{'DB_PASS'};
my @era_conn = ( 'ops$laura', $DB_PASS );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $stmt = "select sample_alias from sample where sample_id=?";
my $xml_sth = $era->dbc->prepare($stmt);

my $sample_hash=parse_log($log);
my $wp10_hash=parse_wp10_list($wp10_list);

foreach my $s (keys %$sample_hash) {
    print $s,"\n";
    $xml_sth->execute($s);
    my $xr = $xml_sth->fetchrow_arrayref();
    my $sample_alias=$xr->[0];
    if (exists($wp10_hash->{$sample_alias})) {
	print "$s\t$sample_alias is wp10!\n";
    }
}

sub parse_wp10_list {
    my $wp10_f=shift;

    my %hash;
    open FH,"<$wp10_f" or die ("Cant open $wp10_f:$!");
    while(<FH>) {
	chomp;
	my $id=$_;
	$hash{$id}=0;
    }
    close FH;

    return \%hash;
}

sub parse_log {
    my $log_f=shift;

    my %samples;
    open FH,"<$log_f" or die ("Cant open $log_f:$!");
    while(<FH>) {
	chomp;
	my $line=$_;
	my @elms=split/\t/,$line;
	$samples{$elms[2]}=0;
    }
    close FH;
    return \%samples;
}
