version 1.0

workflow UntarFiles {
    input {
        Array[File] tarballs
        String project_name
        String file_extension
        String gsutil_output_path
    }

    scatter (tarball in tarballs) {
        call ExtractFiles {
            input:
                tarball = tarball,
                tarball_size = size(tarball, "GB"),
                project_name = project_name,
                file_extension = file_extension,
                gsutil_output_path = gsutil_output_path,
        }
    }
}

# assumes tarballs are not gzipped (no -z option for tar)
task ExtractFiles {
    input {
        String tarball
        Float tarball_size
        String project_name
        String file_extension
        String gsutil_output_path
    }

    command <<<
        set -euo pipefail

        # untar the file (pipe using gsutil -u cat and ignore dir structure using --transform)
        tar -vxf <(gsutil -u ~{project_name} cat ~{tarball}) --transform 's/.*\///g'

         # copy to bucket
         gsutil -m cp *~{file_extension} ~{gsutil_output_path}
    >>>

    runtime {
        docker: "google/cloud-sdk:slim"
        # if the input size is less than 1 GB adjust to min input size of 1 GB
        disks: "local-disk " + ceil(1 * (if tarball_size < 1 then 1 else tarball_size)) + " HDD"
        cpu: 1
        memory: "1 GB"
    }

    output {
        File monitoring_log = "monitoring.log"
    }
}
