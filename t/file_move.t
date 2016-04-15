#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Test::More;
use BlueprintFileMovePath qw(cnag_path crg_path wtsi_path get_meta_data_from_index get_alt_sample_name_from_file);

my $metadata_file = "$Bin/test_meta_data.tab";
my $alt_sample_file = "$Bin/test_alt_sample";

my $metadata = get_meta_data_from_index($metadata_file);
my $alt_sample_hash = get_alt_sample_name_from_file($alt_sample_file);

my %file_options = ( alt_sample_hash  => $alt_sample_hash,
                     meta_data        => $metadata,
                     collection_tag   => 'EXPERIMENT_ID',
                     aln_base_dir     => '/path/aln',
                     vcf_base_dir     => '/path/vcf',
                     results_base_dir => '/path/results',
                     species          => 'homo_sapiens',
                     genome_version   => 'GRCh38',
                     freeze_date      => '20160615',
                   ); 

test_cnag_files( \%file_options );
test_crg_files( \%file_options );

done_testing();

sub test_cnag_files {
  my ( $file_options_cnag ) = @_;

  ### BAM files

  $$file_options_cnag{filename} = 'C005PS51.bam';
  $$file_options_cnag{filetype} = 'BS_BAM_CNAG';

  my ($bam_path, $bam_collection) = cnag_path($file_options_cnag);

  ok( $bam_path eq '/path/aln/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.BS.gem_cnag_bs.GRCh38.20160615.bamT', 'bs_bam_path_1');
  ok( $bam_collection eq 'ERX242612', 'bs_bam_collection_1' );

  ### BCF files

  $$file_options_cnag{filename} = 'C005PS51.WGBS.bs_call.20150714.bcf';
  $$file_options_cnag{filetype} = 'BS_BCF_CNAG';

  my ($bcf_path, $bcf_collection) = cnag_path($file_options_cnag);

  ok( $bcf_path eq '/path/vcf/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.WGBS.gem_cnag_bs.GRCh38.20160615.bcf', 'bs_bcf_path_1');
  ok( $bcf_collection eq 'ERX242612', 'bs_bcf_collection_1' );

  ### BCF index

  $$file_options_cnag{filename} = 'C005PS51.WGBS.bs_call.20150714.bcf.csi';
  $$file_options_cnag{filetype} = 'BS_BCF_CSI_CNAG';

  my ($csi_path, $csi_collection) = cnag_path($file_options_cnag);

  ok( $csi_path eq '/path/vcf/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.WGBS.gem_cnag_bs.GRCh38.20160615.bcf.csi', 'bs_bcf_path_1');
  ok( $csi_collection eq 'ERX242612', 'bs_bcf_collection_1' );

  ### CPG files

  $$file_options_cnag{filename} = 'C005PS51.CPG_methylation_calls.bs_call.20150714.txt.gz';
  $$file_options_cnag{filetype} = 'BS_METH_TABLE_CYTOSINES_CNAG';

  my ($cpg_path, $cpg_collection) = cnag_path($file_options_cnag);

  ok( $cpg_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.CPG_methylation_calls.bs_call.GRCh38.20160615.txt.gz', 'bs_cpg_path_1');
  ok( $cpg_collection eq 'ERX242612', 'bs_cpg_collection_1');

  ### bigWig files

  $$file_options_cnag{filename} = 'C005PS51.CPG_methylation_calls.bs_call.20150714.bw';
  $$file_options_cnag{filetype} = 'BS_METH_CALL_CNAG';

  my ($bw_path, $bw_collection) = cnag_path($file_options_cnag);

  ok( $bw_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.CPG_methylation_calls.bs_call.GRCh38.20160615.bw', 'bs_bw_path_1' );
  ok( $bw_collection eq 'ERX242612', 'bs_bw_collection_1');

  ### Hypo bed files

  $$file_options_cnag{filename} = 'C005PS51.hypo_meth.bs_call.20150714.bed.gz';
  $$file_options_cnag{filetype} = 'BS_HYPO_METH_BED_CNAG';

  my ($hypo_path, $hypo_collection) = cnag_path($file_options_cnag);

  ok( $hypo_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.hypo_meth.bs_call.GRCh38.20160615.bed.gz', 'bs_hypo_path_1');
  ok( $hypo_collection eq 'ERX242612', 'bs_hypo_collection_1');

  ### Hyper bed files

  $$file_options_cnag{filename} = 'C005PS51.hyper_meth.bs_call.20150714.bed.gz';
  $$file_options_cnag{filetype} = 'BS_HYPER_METH_BED_CNAG';

  my ($hyper_path, $hyper_collection) = cnag_path($file_options_cnag);

  ok( $hyper_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/C005PS/CD14-positive_CD16-negative_classical_monocyte/Bisulfite-Seq/CNAG/C005PS51.hyper_meth.bs_call.GRCh38.20160615.bed.gz', 'bs_hyper_path_1');
  ok( $hyper_collection eq 'ERX242612', 'bs_hyper_collection_1');

  ### BAM 2
  $$file_options_cnag{filename}    = 'V158.bam';
  $$file_options_cnag{filetype}    = 'BS_BAM_CNAG';
  $$file_options_cnag{freeze_date} = '20150707';
  
  my ($bam_path_2, $bam_collection_2) = cnag_path($file_options_cnag);

  ok( $bam_path_2 eq '/path/aln/homo_sapiens/GRCh38/Venous_blood/NC11_41/class_switched_memory_B_cell/Bisulfite-Seq/CNAG/csMBC_NC11_41.BS.gem_cnag_bs.GRCh38.20150707.bam', 'bs_bam_path_2');
  ok( $bam_collection_2 eq 'ERX715132', 'bs_bam_collection_2' );

  ### BAM 3
  $$file_options_cnag{filename}    = 'S00TU2A1.bam';
  $$file_options_cnag{filetype}    = 'BS_BAM_CNAG';
  $$file_options_cnag{freeze_date} = '20160120';

  my ($bam_path_3, $bam_collection_3) = cnag_path($file_options_cnag);
  
  ok( $bam_path_3 eq '/path/aln/homo_sapiens/GRCh38/Venous_blood/B270/immature_conventional_dendritic_cell_-_GM-CSF_IL4_T_6_days/Bisulfite-Seq/CNAG/S00TU2A1.BS.gem_cnag_bs.GRCh38.20160120.bam', 'bs_bam_path_3');
  ok( $bam_collection_3 eq 'ERX1299215', 'bs_bam_collection_3' );

  ### BAM 4

  $$file_options_cnag{filename}    = 'S004XMA1.bam';
  $$file_options_cnag{filetype}    = 'BS_BAM_CNAG';
  $$file_options_cnag{freeze_date} = '20150707';

  my ($bam_path_4, $bam_collection_4) = cnag_path($file_options_cnag);

  ok( $bam_path_4 eq '/path/aln/homo_sapiens/GRCh38/Bone_marrow/pz_284/Acute_promyelocytic_leukemia_-_CTR/Bisulfite-Seq/CNAG/S004XMA1.BS.gem_cnag_bs.GRCh38.20150707.bam', 'bs_bam_path_4');
  ok( $bam_collection_4 eq 'ERX358119', 'bs_bam_collection_4' );

  ### BAM 5

  $$file_options_cnag{filename}    = 'S00K88A1.bam';
  $$file_options_cnag{filetype}    = 'BS_BAM_CNAG';
  $$file_options_cnag{freeze_date} = '20150707';

  my ($bam_path_5, $bam_collection_5) = cnag_path($file_options_cnag);

  ok( $bam_path_5 eq '/path/aln/homo_sapiens/GRCh38/Venous_blood/PB270313/mature_neutrophil_-_G-CSF_Dex._Treatment_16-20_hrs/Bisulfite-Seq/CNAG/S00K88A1.BS.gem_cnag_bs.GRCh38.20150707.bam', 'bs_bam_path_5');
  ok( $bam_collection_5 eq 'ERX931059', 'bs_bam_collection_5' );

}

sub test_crg_files {
  my ( $file_options_crg ) = @_;

  ### BAM files

  $$file_options_crg{filename}     = 'S004AV11.RNA_seq.star_grape2_crg.20151022.bam';
  $$file_options_crg{filetype}     = 'RNA_BAM_STAR_CRG';
  $$file_options_crg{freeze_date} = '20160615';

  my ($bam_path, $bam_collection) = crg_path($file_options_crg);
  ok( $bam_path eq '/path/aln/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.RNA-Seq.star_grape2_crg.GRCh38.20160615.bam', 'rna_bam_path_1');
  ok( $bam_collection eq 'ERX957890', 'rna_bam_collection_1');

  ### Contig

  $$file_options_crg{filename} = 'S004AV11.contigs.star_grape2_crg.20151022.bed';
  $$file_options_crg{filetype} = 'RNA_CONTIGS_STAR_CRG';
 
  my ($contig_path, $contig_collection) = crg_path($file_options_crg);  
  ok( $contig_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.contigs.star_grape2_crg.GRCh38.20160615.bed' , 'rna_contig_path_1' );
  ok( $contig_collection eq 'ERX957890', 'rna_contig_collection_1');

  ### Signal

  $$file_options_crg{filename} = 'S004AV11.minusStrandMulti.star_grape2_crg.20151022.bw';
  $$file_options_crg{filetype} = 'RNA_SIGNAL_STAR_CRG';

  my ($signal_nm_path, $signal_nm_collection) = crg_path($file_options_crg); 
  ok( $signal_nm_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.minusStrandMulti.star_grape2_crg.GRCh38.20160615.bw', 'rna_signal_path_1');
  ok( $signal_nm_collection eq 'ERX957890', 'rna_signal_collection_1');

  $$file_options_crg{filename} = 'S004AV11.minusStrand.star_grape2_crg.20151022.bw';

  my ($signal_n_path, $signal_n_collection) = crg_path($file_options_crg);
  ok( $signal_n_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.minusStrand.star_grape2_crg.GRCh38.20160615.bw', 'rna_signal_path_2');
  ok( $signal_n_collection eq 'ERX957890', 'rna_signal_collection_2');

  $$file_options_crg{filename} = 'S004AV11.plusStrandMulti.star_grape2_crg.20151022.bw';

  my ($signal_pm_path, $signal_pm_collection) = crg_path($file_options_crg);
  ok( $signal_pm_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.plusStrandMulti.star_grape2_crg.GRCh38.20160615.bw', 'rna_signal_path_3');
  ok( $signal_pm_collection eq 'ERX957890', 'rna_signal_collection_3');

  $$file_options_crg{filename} = 'S004AV11.plusStrand.star_grape2_crg.20151022.bw';
  
  my ($signal_p_path, $signal_p_collection) = crg_path($file_options_crg);
  ok( $signal_p_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.plusStrand.star_grape2_crg.GRCh38.20160615.bw', 'rna_signal_path_4');
  ok( $signal_p_collection eq 'ERX957890', 'rna_signal_collection_4');

  ### Quant files

  $$file_options_crg{filename} = 'S004AV11.gene_quantification.rsem_grape2_crg.20151022.results';
  $$file_options_crg{filetype} = 'RNA_GENE_QUANT_STAR_CRG';

  my ($gene_quant_path, $gene_quant_collection) = crg_path($file_options_crg);
  ok( $gene_quant_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.gene_quantification.rsem_grape2_crg.GRCh38.20160615.results', 'rna_gene_quant_path_1');
  ok( $gene_quant_collection eq 'ERX957890', 'rna_gene_quant_collection_1');

  $$file_options_crg{filename} = 'S004AV11.transcript_quantification.rsem_grape2_crg.20151022.results';
  $$file_options_crg{filetype} = 'RNA_TRANSCRIPT_QUANT_STAR_CRG';

  my ($trans_quant_path, $trans_quant_collection) = crg_path($file_options_crg);
  ok( $trans_quant_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.transcript_quantification.rsem_grape2_crg.GRCh38.20160615.results', 'rna_trans_quant_path_1');
  ok( $trans_quant_collection eq 'ERX957890', 'rna_transcript_quant_collection_1');


  ### CoSI files
  
  $$file_options_crg{filename} = 'S004AV11.ipsa_junctions.ipsa_grape2_crg.20150817.gff';
  $$file_options_crg{filetype} = 'RNA_COSI_STAR_CRG';

  my ($cosi_jn_path, $cosi_jn_collection) = crg_path($file_options_crg);
  ok( $cosi_jn_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.ipsa_junctions.ipsa_grape2_crg.GRCh38.20160615.gff', 'rna_cosi_jn_path_1');
  ok( $cosi_jn_collection eq 'ERX957890', 'rna_cosi_jn_collection_1');


  $$file_options_crg{filename} = 'S004AV11.splicing_ratios.ipsa_grape2_crg.20150817.gff';

  my ($cosi_ratio_path, $cosi_ratio_collection) = crg_path($file_options_crg);
  ok( $cosi_ratio_path eq '/path/results/homo_sapiens/GRCh38/Cord_blood/S004AV/CD34-negative_CD41-positive_CD42-positive_megakaryocyte_cell/RNA-Seq/MPIMG/S004AV11.splicing_ratios.ipsa_grape2_crg.GRCh38.20160615.gff', 'rna_cosi_jn_path_1');
  ok( $cosi_ratio_collection eq 'ERX957890', 'rna_cosi_ratio_collection_1'); 
}
