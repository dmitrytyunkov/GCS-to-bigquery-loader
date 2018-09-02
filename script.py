#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

from __future__ import print_function

"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import datetime
import logging
import os

import apache_beam as beam

from apache_beam.io import ReadFromText, ReadFromAvro, Read
from apache_beam.io import WriteToBigQuery, BigQueryDisposition, WriteToText
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apitools.base.protorpclite.protojson import json


class Split(beam.DoFn):
    """Parse each line of input text into words."""
    def __init__(self, check_date, schema_keys):
        super(Split, self).__init__()
        self.date = check_date
        self.keys = schema_keys

    def process(self, element):
        record = self.process_row(element)
        return record

    def process_row(self, row):
        dict_ = {}
        i = 0
        record = row.split(",")
        for rec in record:
            key = self.keys[i].strip()
            if key.lower() == 'date':
                if rec != self.date:
                    return
            try:
                dict_[key] = int(rec)
            except ValueError:
                try:
                    dict_[key] = float(rec)
                except ValueError:
                    dict_[key] = rec
            i += 1
        return [dict_]

def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to process.')
    parser.add_argument('--config',
                        dest='config',
                        required=True,
                        help='Configuration file with bigquery project settings and date range.')
    parser.add_argument('--oauth_file',
                        dest='oauth_file',
                        required=True,
                        help='File to authorize process.')
    parser.add_argument('--schema',
                        dest='schema',
                        required=True,
                        help='File with schema of table.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = known_args.oauth_file

    data = {}
    if os.path.isfile(known_args.schema):
        with open(known_args.schema) as file:
            data = json.load(file)
    else:
        print('Missing configuration file' + known_args.schema)
        return
    schema = data['schema']

    data = {}
    if os.path.isfile(known_args.config):
        with open(known_args.config) as file:
            data = json.load(file)
    else:
        print('Missing configuration file' + known_args.schema)
        return
    project = data['project_id']
    dataset = data['dataset_id']
    table = data['table']
    date_start = datetime.datetime.strptime(data['start_date'], '%Y-%m-%d').date()
    date_end = datetime.datetime.strptime(data['end_date'], '%Y-%m-%d').date()
    keys = schema.replace(':STRING','').replace(':INTEGER','').replace(':FLOAT','').split(',')

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    lines = p | 'readFromGCS' >> ReadFromText(known_args.input, skip_header_lines=1).with_output_types(unicode)

    while date_end - date_start >= datetime.timedelta(days=0):
        date = date_start.strftime('%Y-%m-%d')
        date_start += datetime.timedelta(days=1)
        output = lines | 'splitCSV_'+date >> beam.ParDo(Split(date, keys))

        output | 'writeToBQ_'+date >> WriteToBigQuery(table=table+'_'+date.replace('-',''),
                                                        dataset=dataset,
                                                        project=project,
                                                        schema=schema,
                                                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=BigQueryDisposition.WRITE_APPEND)

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
