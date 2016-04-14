#get list of samples from ERA and print them out without the extra <SAMPLE_SET> and newlines
#in the middle
use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use XML::Twig;

my $sample_id_file = $ARGV[0];
die("[usage] perl $0 sample.ids") if !$sample_id_file;

my $erauser = $ENV{'ERA_USER'};
my $erapwd = $ENV{'DB_PASS'};

my $sample_id_list = get_list($sample_id_file);

my @era_conn = ( $erauser, $erapwd );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $twig = XML::Twig->new(
    twig_roots => 
    { 'SAMPLE'=>\&process_sample },
    pretty_print             => 'indented',
    );

my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(sample_xml) xml from sample where sample_alias = ?");

open LOG,">sample.log";
print "<SAMPLE_SET>\n";
foreach my $sample_id ( @{ $sample_id_list } ) {
    print LOG "[INFO] processing $sample_id\n";
    $xml_sth->execute( $sample_id );
    my $xr = $xml_sth->fetchrow_arrayref();
    if (!$xr) {
	print LOG "[INFO] No sample metadata in DB for $sample_id\n" if !$xr;
	next;
    }
    my ($xml) = @$xr;
    $twig->parse($xml);
}
print "\n</SAMPLE_SET>\n";
close LOG;


sub process_sample {
    my ($twig,$sample)=@_;
    $sample->print();
}

sub get_list{
    my ($file) = @_;
    my @id_list;

    open my $fh ,'<', $file;
    while ( <$fh> ) {
	chomp;
	push (@id_list, $_);
    }
    close( $fh );
    return \@id_list;
}
