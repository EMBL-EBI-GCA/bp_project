# Script used to retrieve a xml file with the samples that are already in ERAPRO for
# donors from the samples that are trying to be registered.
#!/usr/bin/env perl
use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use XML::Twig;
use Getopt::Long;

my ($sample_file,$era_user,$era_pass, $xml_output);

GetOptions( 'samples=s'     => \$sample_file,
            'era_user=s'    => \$era_user,
            'era_pass=s'    => \$era_pass,
            'xml_output!'   => \$xml_output,
          );

die("[USAGE] perl $0 --samples sample_file.txt --era_user \$ERA_USER --era_pass \$ERA_PASS -xml_output > samples_registered.xml 2> sample_info.txt") if !$sample_file || !$era_user || !$era_pass;


my @headers = qw/ DONOR_ID
                  BIOMATERIAL_TYPE
                  CELL_TYPE
                  MARKERS
                  TISSUE_TYPE
                  DISEASE
                  TREATMENT
                  DONOR_SEX
                  DONOR_AGE
               /;

my ($sample_id_list,$sample_dict) = get_list($sample_file);

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my %hash_line;

my $twig = XML::Twig->new(
    twig_roots =>    { 'SAMPLE' => 
                        sub {  my ($twig,$sample)=@_;
                               $sample->print() 
                                 if $xml_output;
                            },
                     },
    twig_handlers => { 'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DONOR_ID"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $donor_id = '-';

                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $donor_id = $child->text; 
                                   }
                                 }
                                 $hash_line{DONOR_ID} = $donor_id;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="BIOMATERIAL_TYPE"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $bio_type='-';

                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $bio_type = $child->text;        
                                   }
                                 }
                                 $hash_line{BIOMATERIAL_TYPE} = $bio_type;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="CELL_TYPE"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $cell_type='-';

                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $cell_type = $child->text;
                                   }
                                 }
                                 $hash_line{CELL_TYPE} = $cell_type;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="MARKERS"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $markers='-';

                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $markers = $child->text;
                                   }
                                 }
                                 $hash_line{MARKERS} = $markers;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="TISSUE_TYPE"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $tissue_type='-';

                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $tissue_type = $child->text;
                                   }
                                 }
                                 $hash_line{TISSUE_TYPE} = $tissue_type;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DISEASE"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $disease='-';
                              
                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $disease = $child->text;
                                   }
                                 }
                                 $hash_line{DISEASE} = $disease;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="TREATMENT"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $treatment='-';
                              
                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $treatment = $child->text;
                                   }
                                 }
                                 $hash_line{TREATMENT} = $treatment;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DONOR_SEX"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $donor_sex='-';
                                
                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $donor_sex = $child->text;
                                   }
                                 }
                                 $hash_line{DONOR_SEX} = $donor_sex;
                              },
                       'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DONOR_AGE"]' =>
                          sub {  my ($twig, $e) = @_;
                                 my $donor_age='-';
                                
                                 foreach my $child ($e->children){
                                   if ( $child->name eq 'VALUE' ){
                                        $donor_age = $child->text;
                                   }
                                 }
                                 $hash_line{DONOR_AGE} = $donor_age;
                              },
                     },
    pretty_print             => 'indented',
    );

my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(sample_xml) xml from sample where sample_alias like ?");

print STDERR join("\t","NEW_SAMPLE_NAME",@headers),$/;

foreach my $sample_id ( @{ $sample_id_list } ) {
    %hash_line=();
    $xml_sth->execute( $sample_id );
    my $xr = $xml_sth->fetchrow_arrayref();
    if (!$xr) {
	print STDERR "[WARN] No info for $sample_id $sample_dict->{$sample_id}\n";
	next;
    } else {
	print STDERR "[WARN] Sample registered for same donor for $sample_id $sample_dict->{$sample_id}\n";
        my ($xml) = @$xr;
        $twig->parse($xml);
        print STDERR join("\t", $sample_dict->{$sample_id},@hash_line{@headers}),$/;
    }
}


sub get_list{
    my ($file) = @_;
    my @id_list;

    my %mapping;
    open my $fh ,'<', $file;
    while ( <$fh> ) {
	chomp;
	my $id=$_;
	$mapping{$id}=
	my $length=length($id);
	my $substring=substr($id,0,$length-2);
	$substring.='%';
	$mapping{$substring}=$id;
	push (@id_list, $substring);
    }
    close( $fh );
    return (\@id_list,\%mapping);
}
