=pod

=head1 NAME

    ReseqTrack::Hive::PipeConfig::EgaFileUpload_conf

=head1 SYNOPSIS

    init_pipeline.pl ReseqTrack::Hive::PipeConfig::EgaFileUpload_conf -inputfile file_list -work_dir dir_name -java_path java_path -ega_jar /path/webin-data-streamer-Upload-Client.jar -upload_dIR '/path/' -aspera_username login -aspera_url host_name


=cut


package ReseqTrack::Hive::PipeConfig::EgaFileUpload_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly


sub default_options {
    my ($self) = @_;
    return {
        %{ $self->SUPER::default_options() },                   # inherit other stuff from the base class

        'pipeline_name'   => 'ega_upload',                      # name used by the beekeeper to prefix job names on the farm

        # runnable-specific parameters' defaults:
        'java_path'       => 'java',
        'ega_jar'         => undef,
        'file'            => undef,
        'work_dir'        => undef,
        'upload_dir'      => undef,
        'aspera_username' => undef,
        'aspera_url'      => undef,
        'trim_path'       => undef,
        'ascp_param'      => undef, 
        'lsf_queue'       => 'production',
        'move_dir'        => undef,
    };
}


sub pipeline_create_commands {
    my ($self) = @_;
    return [
        @{$self->SUPER::pipeline_create_commands},             # inheriting database and hive tables' creation

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
        {   -logic_name => 'find_files',                                      ## find listed files, creates fan
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
            -parameters => {
                'inputcmd'     => 'cat #file#',
                'column_names' => [ 'filename' ],
            },
            -flow_into => {
                2  =>  [ 'encrypt_file' ],     
            },
        },

        {   -logic_name => 'encrypt_file',                                    ## encrypt each file
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters    => {
                'java_path' => $self->o('java_path'),
                'ega_jar'   => $self->o('ega_jar'),
                'cmd'       => '#java_path# -jar #ega_jar# -file #filename#',
            },
            -analysis_capacity => 10,
            -rc_name           => '500Mb',
            -flow_into         => {
                1 => [ 'find_encrypted_files' ],
            },
        },
        
        {    -logic_name => 'find_encrypted_files',                          ## find newly created encrypted files, creates fan
             -module     => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
             -parameters => {  
                     'inputcmd'     => 'find #filename#.*|grep -E "gpg|md5"',
                     'column_names' => [ 'gpg_file' ],
             },
               -flow_into => {
                   2 => [ 'move_file' ],
               }
        },

        {   -logic_name => 'move_file',                                     ## move gpg files in move_dir/ 
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters    => {
                'move_dir' => $self->o('move_dir'),
                'cmd'      => 'mv #gpg_file# #move_dir#/', 
            },
            -analysis_capacity => 10,
            -rc_name           => '500Mb',
            -flow_into         => {
                1 => [ 'list_file' ],
            },
        },

        {   -logic_name => 'list_file',                                      ## get basename of the encrypted file
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::JobFactory',
            -parameters    => {
                'move_dir'        => $self->o('move_dir'),
                'inputcmd'        => 'basename #gpg_file#',
                'column_names'    => ['gpg_filename'],
                'fan_branch_code' => 1,
            },
            -flow_into => {
                1  =>  { 'upload_file' => { 'filename' => '#move_dir#/#gpg_filename#' }},  
                },   
        },
         {    -logic_name => 'upload_file',                                  ## upload to remote FTP using Aspera module, limit 1 job
              -module     => 'ReseqTrack::Hive::Process::Aspera',
              -parameters    => {
                  'username'   => $self->o('aspera_username'),
                  'aspera_url' => $self->o('aspera_url'),
                  'upload_dir' => $self->o('upload_dir'),
                  'trim_path'  => $self->o('trim_path'),
                  'work_dir'   => $self->o('work_dir'),
                  'ascp_param' => $self->o('ascp_param'),
              },
              -rc_name           => '500Mb',
              -analysis_capacity => 1,
         },
    ];
}

1;

