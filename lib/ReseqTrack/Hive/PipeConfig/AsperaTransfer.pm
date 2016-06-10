package ReseqTrack::Hive::PipeConfig::AsperaTransfer;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');

sub default_options {
  my ($self) = @_;
  return {
    %{ $self->SUPER::default_options() },
    'pipeline_name'   => 'aspera_transfer',
    'work_dir'        => undef,
    'aspera_username' => undef,
    'aspera_url'      => undef,
    'download_dir'    => undef,
    'upload_dir'      => undef,
    'trim_path'       => undef,
  };
}

sub pipeline_create_commands {
  my ($self) = @_;
  return [
    @{$self->SUPER::pipeline_create_commands},
    'mkdir -p '.$self->o('work_dir'),
  ];
}

sub resource_classes {
  my ($self) = @_;
  return {
    %{$self->SUPER::resource_classes}, 
    '200Mb' => { 'LSF' => '-C0 -M200 -q '.$self->o('lsf_queue').' -R"select[mem>200] rusage[mem=200]"' },
    '500Mb' => { 'LSF' => '-C0 -M500 -q '.$self->o('lsf_queue').' -R"select[mem>500] rusage[mem=500]"' },
  };
}

sub hive_meta_table {
  my ($self) = @_;
  return {
    %{$self->SUPER::hive_meta_table},
    'hive_use_param_stack' => 1,
   };
}

sub pipeline_analyses {
  my ($self) = @_;
  return [
    { -logic_name => 'find_files',
      -module     => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
      -parameters => {
        'inputcmd'     => 'cat #file#',
        'column_names' => [ 'filename' ],
      },
      -flow_into => {
        '2'  =>  [ 'transfer_file' ],     
      },
    },
    { -logic_name => 'transfer_file',
      -module => 'ReseqTrack::Hive::Process::Aspera',
      -analysis_capacity => 1,
      -rc_name           => '500Mb',
      -parameters => {
        'username'     => $self->o('aspera_username'),
        'aspera_url'   => $self->o('aspera_url'),
        'download_dir' => $self->o('download_dir'),
        'upload_dir'   => $self->o('upload_dir'),
        'trim_path'    => $self->o('trim_path'),
        'work_dir'     => $self->o('work_dir'),
      },
    },
  ];
}

1;
