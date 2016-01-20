#!/usr/bin/env perl

# This script is used to generate the EGA analysis XMLs for the BCF and BAM files.
# It will depend on a tabular file containing the information for each analysis file with the following columns:
##alias<\t>STUDY<\t>SAMPLE<\t>RUN/s<\t>filename<\t>md5<\t>unencr_md5<\t>reads_mapped.
# It also requires a text file containing the description of the analysis being registered and that will be used in the DESCRIPTION element of the XML
# This script makes use of the following library: https://github.com/FAANG/sra_xml , so you will need to download it and
# add it to your PERL5LIB

use strict;
use Test::More;
use FindBin qw($Bin);
use File::Temp qw/ tempfile /;
use lib ( "$Bin/sra_xml/lib/" );
use Bio::SRAXml qw(write_xml_file);

my $tab_file=$ARGV[0];
my $desc_file=$ARGV[1];

die("[USAGE] perl $0 <attrib_table.txt> <description.txt>") if !$tab_file || !$desc_file;

my $file_type='bcf'; # Valid values are 'bcf' and 'bam'
my $description=parse_description($desc_file);
my $hash=parse_tabfile($tab_file);

my $analysis_set = Bio::SRAXml::Analysis::AnalysisSet->new();

foreach my $alias (keys %$hash) {
	print "[INFO] Adding $alias to XML\n";
	
	$description=~ s/Variation calls  produced by Bisulfite-Seq of sample .+ were mapped to the human genome \(GRCh38\) using GEM \(3\.0\)\./Variation calls  produced by Bisulfite-Seq of sample $hash->{$alias}->{'prefix'} were mapped to the human genome \(GRCh38\) using GEM \(3\.0\)\./;
	
	my $title;
	if ($file_type eq 'bam') {
		$title="Mapping of $hash->{$alias}->{'prefix'} DNA Methylation Data";
	} elsif ($file_type eq 'bcf') {
		$title="Variation calls of $hash->{$alias}->{'prefix'} DNA Methylation Data";
	} else {
		die("[ERROR] $file_type file type is not valid!!")
	}
	
	$analysis_set->add_analysis(
    {
        alias         => $alias,
        analysis_type => {
            'sequence_variation' => {
                experiment_type => 'Whole genome sequencing',
                assembly => {
                    refname   => 'GRCh38',
                    accession => 'GCA_000001405.15'
                },
                sequences => [
		    { accession=>"CM000663.2", label=>"chr1" },
		    { accession=>"CM000664.2", label=>"chr2" },
		    { accession=>"CM000665.2", label=>"chr3" },
		    { accession=>"CM000666.2", label=>"chr4" },
		    { accession=>"CM000667.2", label=>"chr5" },
		    { accession=>"CM000668.2", label=>"chr6" },
		    { accession=>"CM000669.2", label=>"chr7" },
		    { accession=>"CM000670.2", label=>"chr8" },
		    { accession=>"CM000671.2", label=>"chr9" },
		    { accession=>"CM000672.2", label=>"chr10" },
		    { accession=>"CM000673.2", label=>"chr11" },
		    { accession=>"CM000674.2", label=>"chr12" },
		    { accession=>"CM000675.2", label=>"chr13" },
		    { accession=>"CM000676.2", label=>"chr14" },
		    { accession=>"CM000677.2", label=>"chr15" },
		    { accession=>"CM000678.2", label=>"chr16" },
		    { accession=>"CM000679.2", label=>"chr17" },
		    { accession=>"CM000680.2", label=>"chr18" },
		    { accession=>"CM000681.2", label=>"chr19" },
		    { accession=>"CM000682.2", label=>"chr20" },
		    { accession=>"CM000683.2", label=>"chr21" },
		    { accession=>"CM000684.2", label=>"chr22" },
		    { accession=>"CM000685.2", label=>"chrX" },
		    { accession=>"CM000686.2", label=>"chrY" },
		    { accession=>"J01415.2", label=>"chrM" },
		    { accession=>"KI270706.1", label=>"chr1_KI270706v1_random" },
		    { accession=>"KI270707.1", label=>"chr1_KI270707v1_random" },
		    { accession=>"KI270708.1", label=>"chr1_KI270708v1_random" },
		    { accession=>"KI270709.1", label=>"chr1_KI270709v1_random" },
		    { accession=>"KI270710.1", label=>"chr1_KI270710v1_random" },
		    { accession=>"KI270711.1", label=>"chr1_KI270711v1_random" },
		    { accession=>"KI270712.1", label=>"chr1_KI270712v1_random" },
		    { accession=>"KI270713.1", label=>"chr1_KI270713v1_random" },
		    { accession=>"KI270714.1", label=>"chr1_KI270714v1_random" },
		    { accession=>"KI270715.1", label=>"chr2_KI270715v1_random" },
		    { accession=>"KI270716.1", label=>"chr2_KI270716v1_random" },
		    { accession=>"GL000221.1", label=>"chr3_GL000221v1_random" },
		    { accession=>"GL000008.2", label=>"chr4_GL000008v2_random" },
		    { accession=>"GL000208.1", label=>"chr5_GL000208v1_random" },
		    { accession=>"KI270717.1", label=>"chr9_KI270717v1_random" },
		    { accession=>"KI270718.1", label=>"chr9_KI270718v1_random" },
		    { accession=>"KI270719.1", label=>"chr9_KI270719v1_random" },
		    { accession=>"KI270720.1", label=>"chr9_KI270720v1_random" },
		    { accession=>"KI270721.1", label=>"chr11_KI270721v1_random" },
		    { accession=>"GL000009.2", label=>"chr14_GL000009v2_random" },
		    { accession=>"GL000225.1", label=>"chr14_GL000225v1_random" },
		    { accession=>"KI270722.1", label=>"chr14_KI270722v1_random" },
		    { accession=>"GL000194.1", label=>"chr14_GL000194v1_random" },
		    { accession=>"KI270723.1", label=>"chr14_KI270723v1_random" },
		    { accession=>"KI270724.1", label=>"chr14_KI270724v1_random" },
		    { accession=>"KI270725.1", label=>"chr14_KI270725v1_random" },
		    { accession=>"KI270726.1", label=>"chr14_KI270726v1_random" },
		    { accession=>"KI270727.1", label=>"chr15_KI270727v1_random" },
		    { accession=>"KI270728.1", label=>"chr16_KI270728v1_random" },
		    { accession=>"GL000205.2", label=>"chr17_GL000205v2_random" },
		    { accession=>"KI270729.1", label=>"chr17_KI270729v1_random" },
		    { accession=>"KI270730.1", label=>"chr17_KI270730v1_random" },
		    { accession=>"KI270731.1", label=>"chr22_KI270731v1_random" },
		    { accession=>"KI270732.1", label=>"chr22_KI270732v1_random" },
		    { accession=>"KI270733.1", label=>"chr22_KI270733v1_random" },
		    { accession=>"KI270734.1", label=>"chr22_KI270734v1_random" },
		    { accession=>"KI270735.1", label=>"chr22_KI270735v1_random" },
		    { accession=>"KI270736.1", label=>"chr22_KI270736v1_random" },
		    { accession=>"KI270737.1", label=>"chr22_KI270737v1_random" },
		    { accession=>"KI270738.1", label=>"chr22_KI270738v1_random" },
		    { accession=>"KI270739.1", label=>"chr22_KI270739v1_random" },
		    { accession=>"KI270740.1", label=>"chrY_KI270740v1_random" },
		    { accession=>"KI270302.1", label=>"chrUn_KI270302v1" },
		    { accession=>"KI270304.1", label=>"chrUn_KI270304v1" },
		    { accession=>"KI270303.1", label=>"chrUn_KI270303v1" },
		    { accession=>"KI270305.1", label=>"chrUn_KI270305v1" },
		    { accession=>"KI270322.1", label=>"chrUn_KI270322v1" },
		    { accession=>"KI270320.1", label=>"chrUn_KI270320v1" },
		    { accession=>"KI270310.1", label=>"chrUn_KI270310v1" },
		    { accession=>"KI270316.1", label=>"chrUn_KI270316v1" },
		    { accession=>"KI270315.1", label=>"chrUn_KI270315v1" },
		    { accession=>"KI270312.1", label=>"chrUn_KI270312v1" },
		    { accession=>"KI270311.1", label=>"chrUn_KI270311v1" },
		    { accession=>"KI270317.1", label=>"chrUn_KI270317v1" },
		    { accession=>"KI270412.1", label=>"chrUn_KI270412v1" },
		    { accession=>"KI270411.1", label=>"chrUn_KI270411v1" },
		    { accession=>"KI270414.1", label=>"chrUn_KI270414v1" },
		    { accession=>"KI270419.1", label=>"chrUn_KI270419v1" },
		    { accession=>"KI270418.1", label=>"chrUn_KI270418v1" },
		    { accession=>"KI270420.1", label=>"chrUn_KI270420v1" },
		    { accession=>"KI270424.1", label=>"chrUn_KI270424v1" },
		    { accession=>"KI270417.1", label=>"chrUn_KI270417v1" },
		    { accession=>"KI270422.1", label=>"chrUn_KI270422v1" },
		    { accession=>"KI270423.1", label=>"chrUn_KI270423v1" },
		    { accession=>"KI270425.1", label=>"chrUn_KI270425v1" },
		    { accession=>"KI270429.1", label=>"chrUn_KI270429v1" },
		    { accession=>"KI270442.1", label=>"chrUn_KI270442v1" },
		    { accession=>"KI270466.1", label=>"chrUn_KI270466v1" },
		    { accession=>"KI270465.1", label=>"chrUn_KI270465v1" },
		    { accession=>"KI270467.1", label=>"chrUn_KI270467v1" },
		    { accession=>"KI270435.1", label=>"chrUn_KI270435v1" },
		    { accession=>"KI270438.1", label=>"chrUn_KI270438v1" },
		    { accession=>"KI270468.1", label=>"chrUn_KI270468v1" },
		    { accession=>"KI270510.1", label=>"chrUn_KI270510v1" },
		    { accession=>"KI270509.1", label=>"chrUn_KI270509v1" },
		    { accession=>"KI270518.1", label=>"chrUn_KI270518v1" },
		    { accession=>"KI270508.1", label=>"chrUn_KI270508v1" },
		    { accession=>"KI270516.1", label=>"chrUn_KI270516v1" },
		    { accession=>"KI270512.1", label=>"chrUn_KI270512v1" },
		    { accession=>"KI270519.1", label=>"chrUn_KI270519v1" },
		    { accession=>"KI270522.1", label=>"chrUn_KI270522v1" },
		    { accession=>"KI270511.1", label=>"chrUn_KI270511v1" },
		    { accession=>"KI270515.1", label=>"chrUn_KI270515v1" },
		    { accession=>"KI270507.1", label=>"chrUn_KI270507v1" },
		    { accession=>"KI270517.1", label=>"chrUn_KI270517v1" },
		    { accession=>"KI270529.1", label=>"chrUn_KI270529v1" },
		    { accession=>"KI270528.1", label=>"chrUn_KI270528v1" },
		    { accession=>"KI270530.1", label=>"chrUn_KI270530v1" },
		    { accession=>"KI270539.1", label=>"chrUn_KI270539v1" },
		    { accession=>"KI270538.1", label=>"chrUn_KI270538v1" },
		    { accession=>"KI270544.1", label=>"chrUn_KI270544v1" },
		    { accession=>"KI270548.1", label=>"chrUn_KI270548v1" },
		    { accession=>"KI270583.1", label=>"chrUn_KI270583v1" },
		    { accession=>"KI270587.1", label=>"chrUn_KI270587v1" },
		    { accession=>"KI270580.1", label=>"chrUn_KI270580v1" },
		    { accession=>"KI270581.1", label=>"chrUn_KI270581v1" },
		    { accession=>"KI270579.1", label=>"chrUn_KI270579v1" },
		    { accession=>"KI270589.1", label=>"chrUn_KI270589v1" },
		    { accession=>"KI270590.1", label=>"chrUn_KI270590v1" },
		    { accession=>"KI270584.1", label=>"chrUn_KI270584v1" },
		    { accession=>"KI270582.1", label=>"chrUn_KI270582v1" },
		    { accession=>"KI270588.1", label=>"chrUn_KI270588v1" },
		    { accession=>"KI270593.1", label=>"chrUn_KI270593v1" },
		    { accession=>"KI270591.1", label=>"chrUn_KI270591v1" },
		    { accession=>"KI270330.1", label=>"chrUn_KI270330v1" },
		    { accession=>"KI270329.1", label=>"chrUn_KI270329v1" },
		    { accession=>"KI270334.1", label=>"chrUn_KI270334v1" },
		    { accession=>"KI270333.1", label=>"chrUn_KI270333v1" },
		    { accession=>"KI270335.1", label=>"chrUn_KI270335v1" },
		    { accession=>"KI270338.1", label=>"chrUn_KI270338v1" },
		    { accession=>"KI270340.1", label=>"chrUn_KI270340v1" },
		    { accession=>"KI270336.1", label=>"chrUn_KI270336v1" },
		    { accession=>"KI270337.1", label=>"chrUn_KI270337v1" },
		    { accession=>"KI270363.1", label=>"chrUn_KI270363v1" },
		    { accession=>"KI270364.1", label=>"chrUn_KI270364v1" },
		    { accession=>"KI270362.1", label=>"chrUn_KI270362v1" },
		    { accession=>"KI270366.1", label=>"chrUn_KI270366v1" },
		    { accession=>"KI270378.1", label=>"chrUn_KI270378v1" },
		    { accession=>"KI270379.1", label=>"chrUn_KI270379v1" },
		    { accession=>"KI270389.1", label=>"chrUn_KI270389v1" },
		    { accession=>"KI270390.1", label=>"chrUn_KI270390v1" },
		    { accession=>"KI270387.1", label=>"chrUn_KI270387v1" },
		    { accession=>"KI270395.1", label=>"chrUn_KI270395v1" },
		    { accession=>"KI270396.1", label=>"chrUn_KI270396v1" },
		    { accession=>"KI270388.1", label=>"chrUn_KI270388v1" },
		    { accession=>"KI270394.1", label=>"chrUn_KI270394v1" },
		    { accession=>"KI270386.1", label=>"chrUn_KI270386v1" },
		    { accession=>"KI270391.1", label=>"chrUn_KI270391v1" },
		    { accession=>"KI270383.1", label=>"chrUn_KI270383v1" },
		    { accession=>"KI270393.1", label=>"chrUn_KI270393v1" },
		    { accession=>"KI270384.1", label=>"chrUn_KI270384v1" },
		    { accession=>"KI270392.1", label=>"chrUn_KI270392v1" },
		    { accession=>"KI270381.1", label=>"chrUn_KI270381v1" },
		    { accession=>"KI270385.1", label=>"chrUn_KI270385v1" },
		    { accession=>"KI270382.1", label=>"chrUn_KI270382v1" },
		    { accession=>"KI270376.1", label=>"chrUn_KI270376v1" },
		    { accession=>"KI270374.1", label=>"chrUn_KI270374v1" },
		    { accession=>"KI270372.1", label=>"chrUn_KI270372v1" },
		    { accession=>"KI270373.1", label=>"chrUn_KI270373v1" },
		    { accession=>"KI270375.1", label=>"chrUn_KI270375v1" },
		    { accession=>"KI270371.1", label=>"chrUn_KI270371v1" },
		    { accession=>"KI270448.1", label=>"chrUn_KI270448v1" },
		    { accession=>"KI270521.1", label=>"chrUn_KI270521v1" },
		    { accession=>"GL000195.1", label=>"chrUn_GL000195v1" },
		    { accession=>"GL000219.1", label=>"chrUn_GL000219v1" },
		    { accession=>"GL000220.1", label=>"chrUn_GL000220v1" },
		    { accession=>"GL000224.1", label=>"chrUn_GL000224v1" },
		    { accession=>"KI270741.1", label=>"chrUn_KI270741v1" },
		    { accession=>"GL000226.1", label=>"chrUn_GL000226v1" },
		    { accession=>"GL000213.1", label=>"chrUn_GL000213v1" },
		    { accession=>"KI270743.1", label=>"chrUn_KI270743v1" },
		    { accession=>"KI270744.1", label=>"chrUn_KI270744v1" },
		    { accession=>"KI270745.1", label=>"chrUn_KI270745v1" },
		    { accession=>"KI270746.1", label=>"chrUn_KI270746v1" },
		    { accession=>"KI270747.1", label=>"chrUn_KI270747v1" },
		    { accession=>"KI270748.1", label=>"chrUn_KI270748v1" },
		    { accession=>"KI270749.1", label=>"chrUn_KI270749v1" },
		    { accession=>"KI270750.1", label=>"chrUn_KI270750v1" },
		    { accession=>"KI270751.1", label=>"chrUn_KI270751v1" },
		    { accession=>"KI270752.1", label=>"chrUn_KI270752v1" },
		    { accession=>"KI270753.1", label=>"chrUn_KI270753v1" },
		    { accession=>"KI270754.1", label=>"chrUn_KI270754v1" },
		    { accession=>"KI270755.1", label=>"chrUn_KI270755v1" },
		    { accession=>"KI270756.1", label=>"chrUn_KI270756v1" },
		    { accession=>"KI270757.1", label=>"chrUn_KI270757v1" },
		    { accession=>"GL000214.1", label=>"chrUn_GL000214v1" },
		    { accession=>"KI270742.1", label=>"chrUn_KI270742v1" },
		    { accession=>"GL000216.2", label=>"chrUn_GL000216v2" },
		    { accession=>"GL000218.1", label=>"chrUn_GL000218v1" },
		    { accession=>"AJ507799.2", label=>"chrEBV" }
                ]
            }
        },
        title       => $title,
        description => $description,
        study_refs  => { accession => $hash->{$alias}->{'study'}, },
        sample_refs => [
            { accession => $hash->{$alias}->{'sample'} },
        ],
	run_refs => $hash->{$alias}->{'runs'},
        files => [
            {
                filename => $hash->{$alias}->{'filename'},
                filetype => $file_type,
                checksum => $hash->{$alias}->{'md5'},
				unencrypted_checksum =>  $hash->{$alias}->{'unencrypted_md5'}
            },
        ],
	attributes => [
	    { tag => 'MAXIMUM_ALIGNMENT_LENGTH', value => 'read length' },
	    { tag => 'NUMBER_OF_MAPPED_READS', value => $hash->{$alias}->{'reads_mapped'} },
	    { tag => 'TREATMENT_OF_IDENTICAL_ALIGNMENTS_OF_MULTIPLE_READS', value => '4'},
	    { tag => 'MISMATCHES_ALLOWED', value => '4'},
	    { tag => 'ALIGNMENTS_ALLOWED', value => '1'},
	    { tag => 'SOFTWARE', value => 'GEM'},
		{ tag => 'SOFTWARE_VERSION', value => '3.0'},
		{ tag => 'ALIGNMENT_POSTPROCESSING', value => 'Read pairs were selected using the default read-pairing algorithm in gem3, and where the assigned MAPQ score for the read pair was >=20. Calling of methylation levels and genotypes was performed by the program bs_call version 2.0 in paired end mode and trimming the first and last 5 bases from each read pair.'},
		{ tag => 'TREATMENT_OF_MULTIPLE_ALIGNMENTS', value => 'Read pairs were selected using the default read-pairing algorithm in gem3, and where the assigned MAPQ score for the read pair was >=20.'},
        ],
    }
	);
}


write_xml_file( root_entity => $analysis_set, filename => "./test_xml.xml" );

sub parse_tabfile {
	my $file=shift;
	
	my %hash;
	
	open FH,"<$file" or die("Cant open $file:$!\n");
	while(<FH>) {
		chomp;
		my $line=$_;
		next if $line=~/^#/;
		my @elms=split/\t/,$line;
		my @bits=split/\./,$elms[0];
		my @runs=split/,/,$elms[3];
		my @array;
		foreach my $r (@runs) {
			push @array,{accession=> $r}
			
		}
		$hash{$elms[0]}= {
			'prefix'=> $bits[0],
			'study' => $elms[1],
			'sample'=> $elms[2],
			'runs' => \@array,
			'filename' => $elms[4],
			'md5' => $elms[5],
			'unencrypted_md5' => $elms[6],
			'reads_mapped' => $elms[7]
		};
		
	}
	close FH;
	
	return \%hash;
}

sub parse_description {
	my $file=shift;
	
	my $description;
	open FH,"<$file" or die("Cant open $file:$!\n");
	while(<FH>) {
		my $line=$_;
		$description.=$line;
	}
	
	return $description;
}

