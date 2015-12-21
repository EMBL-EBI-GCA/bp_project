# Script to register in the EGA samples that will be in the first column of 
# the tab-separated file. Donors for these samples are already registered in
# other samples that are present in the EGA.
# File must contain a single sample for each donor and a 3rd column containing
# the new values for the 'MOLECULE' attribute in this new samples

#Example of a valid file would be:
#ALIAS  DONOR   MOLECULE
#S005EJ41        UMCG00018       genomic DNA
#S00D6341        UMCG00001       genomic DNA
#S00D1D41        UMCG00002       genomic DNA
#S00D4741        UMCG00004       genomic DNA
#S00CYP41        UMCG00007       genomic DNA
#S00CWT41        UMCG00003       genomic DNA
#S00D5541        UMCG00006       genomic DNA
#S00D3941        UMCG00008       genomic DNA
#S00D0F41        UMCG00011       genomic DNA

#IMP!. Sometimes there is information on the molecule in the <DESCRIPTION>, 
#this should be checked in the final XML manually.


use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;
use XML::Twig;
use utf8;
use autodie;
use Data::Dumper;
use Getopt::Long;

my ($sample_file,$era_user,$era_pass);

GetOptions( 'samples=s'     => \$sample_file,
            'era_user=s'   => \$era_user,
            'era_pass=s'   => \$era_pass,
          );

die("[USAGE] perl $0 --samples sample_file.txt --era_user \$ERA_USER --era_pass \$ERA_PASS") if !$sample_file || !$era_user || !$era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $hash=parse_list($sample_file);

my $twig = XML::Twig->new(
    twig_roots => 
    { 'SAMPLE'=> sub { process_sample(@_,$hash);}},
    pretty_print             => 'indented',
    twig_handlers =>
    { 'SAMPLE/IDENTIFIERS' => sub { $_->delete() },
      'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="MOLECULE"]'=> sub{ process_molecule(@_,$hash); },
      'SAMPLE/SAMPLE_ATTRIBUTES'=> sub{ insert_subjectid_attr(@_,$hash); },
      'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DONOR_SEX"]'=> sub{ insert_gender_attr(@_,$hash);},
      'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="ENA-CHECKLIST"]'=> sub{ $_->delete() }
    }    
    );

my $stmt = <<'QUERY_END';
select xmltype.getclobval(s.sample_xml) xml from sample s,
xmltable('/SAMPLE_SET/SAMPLE' passing s.sample_xml
columns
        donorid varchar2(512) path '//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG[text()="DONOR_ID"]]/VALUE')sx where sx.donorid=? and s.center_name='BLUEPRINT'
QUERY_END

my $xml_sth = $era->dbc->prepare($stmt);
my $donor;
my $gender_seen=0;
my $subject_seen=0;
print "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SAMPLE_SET xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_5/SRA.sample.xsd\">";
foreach my $t_donor(keys %{$hash}) {
    $donor=$t_donor;
    $gender_seen=0;
    $subject_seen=0;
    $xml_sth->execute($donor);
    my $xr = $xml_sth->fetchrow_arrayref();
    die("[ERROR] no info for donor: $t_donor in the archive!") if !$xr;
    my ($xml) = @$xr;
    $gender_seen=1 if $xml=~/gender/;
    $subject_seen=1 if $xml=~/subject_id/;
    $twig->parse($xml);
}
print "\n</SAMPLE_SET>\n";

sub parse_list {
    my $file=shift;
    my %hash;
    open FH,"<$file" or die("Cant open $file:$!\n");
    while(<FH>) {
	chomp;
	my $line=$_;
	next if $line=~/^#/ || $line=~/^\s/ || $line=~/^\n/;
	my ($alias,$donor,$molecule)=split/\t/,$line;
	$hash{$donor}{'alias'}=$alias;
	$hash{$donor}{'molecule'}=$molecule;
    }
    close FH;
    return \%hash;
}

sub insert_subjectid_attr {
    my ($twig,$attrbs)=@_;

    last if $subject_seen==1;
    my $eblg= new XML::Twig::Elt('SAMPLE_ATTRIBUTE');
    my $eblgA= new XML::Twig::Elt('TAG','subject_id');
    my $eblgB= new XML::Twig::Elt('VALUE',$donor);
    
    $eblgA->paste( 'last_child', $eblg);
    $eblgB->paste( 'last_child', $eblg);
    $eblg->paste( 'last_child', $attrbs);
}

sub insert_gender_attr {
    my ($twig,$attrbs)=@_;
    last if $gender_seen==1;
    my $parent_elt=$attrbs->parent();

    my $sex;
    my @kids = $attrbs->children;
    for my $kid ( @kids ) {
        if ($kid->name eq 'VALUE') {
	    $sex=lc($kid->text); #convert to lower case (mandatory in 'gender')
        }
    }
    
    #insert 'gender' as an additional 'SAMPLE_ATTRIBUTE'
    my $eblg= new XML::Twig::Elt('SAMPLE_ATTRIBUTE');
    my $eblgA= new XML::Twig::Elt('TAG','gender');
    my $eblgB= new XML::Twig::Elt('VALUE',$sex);

    $eblgA->paste( 'last_child', $eblg);
    $eblgB->paste( 'last_child', $eblg);
    $eblg->paste( 'last_child', $parent_elt);
}

sub process_sample {
    my ($twig,$sample,$hash)=@_;
    #set new alias attribute
    $sample->del_atts;
    $sample->set_att("alias",$hash->{$donor}{'alias'});
    
    #set new title attribute
    $sample->first_child("TITLE")->set_text($hash->{$donor}{'alias'});
    $sample->print();
}

sub process_molecule {
    my ($twig,$molecule)=@_;
    my @kids = $molecule->children;
    for my $kid ( @kids ) {
	if ($kid->name eq 'VALUE') {
	    #set new molecule
	    $kid->set_text($hash->{$donor}{'molecule'});
	}
    }
}

