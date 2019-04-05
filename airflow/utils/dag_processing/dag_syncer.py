# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os

from airflow.models import DagModel
from airflow.utils.dag_processing import process_dag_file, list_py_file_paths

_file_cache = {}
_dag_modification_date = {}


def get_dag(dag_id, use_cache=True):
    dag_model = DagModel.get_dagmodel(dag_id)
    if not use_cache or not _file_cache[dag_model.fileloc]:
        cache_dag_file(dag_model.fileloc, overwrite=True)

    return _file_cache[dag_model.fileloc][dag_id]


def cache_dag_file(file, overwrite=False, sync_to_db=False):
    mtime = os.path.getmtime(file)
    if overwrite or _dag_modification_date[file] != mtime:
        dags = process_dag_file(file)
        dag_dict = {}
        for dag in dags:
            dag_dict[dag.dag_id] = dag
            if sync_to_db:
                dag.sync_to_db()
        _file_cache[file] = dag_dict
        _dag_modification_date[file] = mtime


def sync_dir_to_db(directory, overwrite=False):
    files = list_py_file_paths(directory)
    for file in files:
        cache_dag_file(file, overwrite=overwrite, sync_to_db=True)
