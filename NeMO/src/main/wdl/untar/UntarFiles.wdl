workflow UntarFiles {
    File tarball_list
    String project_name
    String file_extension
    String gsutil_output_path
    File? monitoring_script

    scatter (tarball in read_lines(tarball_list)) {
        call GetTarBallSize {
            input:
                tarball = tarball,
                project_name = project_name,
                monitoring_script = monitoring_script
        }

        call ExtractFiles {
            input:
                tarball = tarball,
                tarball_size = GetTarBallSize.tarball_size,
                project_name = project_name,
                file_extension = file_extension,
                gsutil_output_path = gsutil_output_path,
                monitoring_script = monitoring_script
        }
    }
}

task GetTarBallSize {
    String tarball
    String project_name
    File? monitoring_script

    command <<<
        set -euo pipefail

        # if the WDL/task contains a monitoring script as input
        if [ ! -z "${monitoring_script}" ]; then
          chmod a+x ${monitoring_script}
          ${monitoring_script} > monitoring.log &
        else
          echo "No monitoring script given as input" > monitoring.log &
        fi

        # get the file size and convert bytes to gb
        gsutil -u ${project_name} du ${tarball} | awk '{print $1*1e-9}'
    >>>

   runtime {
       docker: "google/cloud-sdk:slim"
       disks: "local-disk 1 HDD"
       cpu: 1
       memory: "3.5 GB"
   }

   output {
       Float tarball_size = read_float(stdout())
       File monitoring_log = "monitoring.log"
   }
}

# assumes tarballs are not gzipped (no -z option for tar)
task ExtractFiles {
    String tarball
    Float tarball_size
    String project_name
    String file_extension
    String gsutil_output_path
    File? monitoring_script

    command <<<
        set -euo pipefail

        # if the WDL/task contains a monitoring script as input
        if [ ! -z "${monitoring_script}" ]; then
          chmod a+x ${monitoring_script}
          ${monitoring_script} > monitoring.log &
        else
          echo "No monitoring script given as input" > monitoring.log &
        fi

        # untar the file (pipe using gsutil -u cat and ignore dir structure using --transform)
        tar -vxf <(gsutil -u ${project_name} cat ${tarball}) --transform 's/.*\///g'

         # copy to bucket
         gsutil -m cp *${file_extension} ${gsutil_output_path}
    >>>

    runtime {
        docker: "google/cloud-sdk:slim"
        # if the input size is less than 1 GB adjust to min input size of 1 GB
        disks: "local-disk " + ceil(1 * (if tarball_size < 1 then 1 else tarball_size)) + " HDD"
        cpu: 1
        memory: "3.5 GB"
    }

    output {
        File monitoring_log = "monitoring.log"
    }
}
