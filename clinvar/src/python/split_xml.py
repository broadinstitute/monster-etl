#!/usr/bin/env python2

import argparse
import xml.etree.ElementTree as ET
import os


def split_xml(xml_path, output_name):
    """
    Given a clinvar relseae, split xml file into smaller xml components based on the ClinVarSet element
    and save split files in groups of 1000 under the give output dir.
    :param xml_path: the path to the clinvar xml dataset to be split
    :param output_name: the output directory for the split xml files
    """
    context = ET.iterparse(xml_path)
    number_of_files_per_dir = 1000
    current_num_of_files_split = 0
    dir_num = 0
    for event, elem in context:
        if current_num_of_files_split == number_of_files_per_dir:
            dir_num += 1
            current_num_of_files_split = 0
            print("number of xml files split: {}".format(number_of_files_per_dir * dir_num))

        if elem.tag == 'ClinVarSet':
            clinvar_set_id = elem.attrib['ID']

            current_dir = os.path.join(output_name, "part{}".format(dir_num))
            if not os.path.exists(current_dir):
                os.makedirs(current_dir)

            xml_filename = os.path.join(current_dir, "ClinVar_Set_{}.xml".format(clinvar_set_id))
            with open(xml_filename, 'wb') as xml_file:
                xml_file.write(ET.tostring(elem))

            current_num_of_files_split += 1

        for child in list(elem):
            elem.remove(child)
        elem.clear()

    del context


if __name__ == "__main__":
    # get the argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",
                        "-i",
                        dest="input",
                        required=True,
                        help="the path to the clinvar xml dataset to be split")
    parser.add_argument("--output-name",
                        "-o",
                        dest="output_name",
                        required=True,
                        help="the output directory for the split xml files")
    args = parser.parse_args()

    split_xml(xml_path=args.input,
              output_name=args.output_name)
