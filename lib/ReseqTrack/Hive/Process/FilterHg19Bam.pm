package ReseqTrack::Hive::Process::FilterHg19Bam;

use strict;

use base ('ReseqTrack::Hive::Process::BaseProcess');
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::Tools::Exception qw(throw);
use ReseqTrack::Tools::FileSystemUtils qw(check_file_exists delete_file);
use ReseqTrack::Tools::GeneralUtils   qw(get_open_file_handle execute_system_command);
use File::Temp;
use File::Basename;
use File::Copy qw(move);

sub run {
  my $self = shift @_;

  $self->param_required('bam');
  my $bam_collection_id = $self->param_required('bam_collection_id');
  my $biobambam_cmd     = $self->param_required('biobambam_cmd');
  my $samtools_path     = $self->param_required('samtools');
  my $db_params         = $self->param_required('reseqtrack_db');
  my $bams              = $self->param_as_array('bam');

  foreach my $bam ( @$bams ) {
    check_file_exists( $bam );
  }
  
  throw('expecting single bam') if @$bams > 1;

  my $input_bam          = $$bams[0];
  my $filtered_bam       = $$bams[0];
  $filtered_bam          =~ s/\.bwa_filtered\./\.re_md_bwa_filtered\./;
  my $filtered_file_name = basename $filtered_bam;
  throw("$filtered_bam already exists") if -e $filtered_bam;
  
  my $attribute_metrics = _get_attributes($bam_collection_id, $db_params);

  my $tmp_bam = File::Temp->new( TEMPLATE => "XXXX.$filtered_file_name",
                                 UNLINK   => 0, 
                                 DIR      => $self->output_dir,
                               );

  my @command = ($biobambam_cmd,'resetdupflag=1',"I=$input_bam",'|',$samtools_path,'view','-b','-F1028','>',$tmp_bam);
  my $cmd = join(' ',@command);
  execute_system_command($cmd);
  throw("empty file produced") if -s $tmp_bam;
  move($tmp_bam, $filtered_bam);

  $self->output_param( 'bam', $filtered_bam );
  $self->output_param( 'attribute_metrics', $attribute_metrics );
  
}

sub _get_attributes{
  my ($bam_collection_id, $db_params) = @_;
  my $aa = $db->get_AttributeAdaptor;
  my $attribute_name    = 'estFraglen';
  my $a = $aa->fetch_by_other_id_and_table_name_and_attribute_name($bam_collection_id, 'collection', 'estFraglen');

  my $attribute_value   =  $a->attribute_value;
  my $attribute_metrics = { $attribute_name => $attribute_value };
  return $attribute_metrics; 
}
 
1; 
