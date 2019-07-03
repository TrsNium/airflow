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

import json

from airflow.hooks.docker_hook import DockerHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from docker import APIClient, tls
import ast

class DockerBuildOperator(BaseOperator):
    """
    Execute building docker image.

    """
    @apply_defaults
    def __init__(
            self,
            path=None,
            fileobj=None,
            tag=None,
            quiet=None,
            nocache=None,
            timeout=None,
            rm=False,
            custom_context=False,
            encoding=None,
            pull=False,
            forcerm=False,
            dockerfile=None,
            buildargs=None,
            container_limits=None,
            shmsize=None,
            labels=None,
            cache_from=None,
            target=None,
            network_mode=None,
            squash=None,
            extra_hosts=None,
            platform=None,
            isolation=None,
            use_config_proxy=True,
            tls_ca_cert=None,
            tls_client_cert=None,
            tls_client_key=None,
            tls_hostname=None,
            tls_ssl_version=None,
            *args,
            **kwargs):

        self.path=path
        self.fileobj=fileobj
        self.tag=tag
        self.quiet=quiet
        self.nocache=nocache
        self.timeout=timeout
        self.rm=rm
        self.custom_context=custom_context
        self.encoding=encoding
        self.pull=pull
        self.forcerm=forcerm
        self.dockerfile=dockerfile
        self.buildargs=buildargs
        self.container_limits=container_limits
        self.shmsize=shmsize
        self.labels=labels
        self.cache_from=cache_from
        self.target=target
        self.network_mode=network_mode
        self.squash=squash
        self.extra_hosts=extra_hosts
        self.platform=platform
        self.isolation=isolation
        self.use_config_proxy=use_config_proxy
        self.tls_ca_cert = tls_ca_cert
        self.tls_client_cert = tls_client_cert
        self.tls_client_key = tls_client_key
        self.tls_hostname = tls_hostname
        self.tls_ssl_version = tls_ssl_version


        super().__init__(*args, **kwargs)

    def get_hook(self):
        return DockerHook(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=self.__get_tls_config()
        )

    def execute(self, context):
        self.log.info('Start building docker image')

        tls_config = self.__get_tls_config()

        if self.docker_conn_id:
            self.cli = self.get_hook().get_conn()
        else:
            self.cli = APIClient(
                base_url=self.docker_url,
                version=self.api_version,
                tls=tls_config
            )

        self.cli.images.build(**self.__create_build_image_kwargs())

    def __get_tls_config(self):
        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            tls_config = tls.TLSConfig(
                ca_cert=self.tls_ca_cert,
                client_cert=(self.tls_client_cert, self.tls_client_key),
                verify=True,
                ssl_version=self.tls_ssl_version,
                assert_hostname=self.tls_hostname
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')
        return tls_config

    def __create_build_image_kwargs(self):
        return {
            'path':self.path,
            'tag':self.tag,
            'quiet':self.quiet,
            'fileobj':self.fileobj,
            'nocache':self.nocache,
            'rm':self.rm,
            'timeout':self.timeout,
            'custom_context':self.custom_context,
            'encoding':self.encoding,
            'pull':self.pull,
            'forcerm':self.forcerm,
            'dockerfile':self.dockerfile,
            'container_limits':self.container_limits,
            'decode':self.decode,
            'buildargs':self.buildargs,
            'gzip':self.gzip,
            'shmsize':self.shmsize,
            'labels':self.labels,
            'cache_from':self.cache_from,
            'target':self.target,
            'network_mode':self.network_mode,
            'squash':self.squash,
            'extra_hosts':self.extra_hosts,
            'platform':self.platform,
            'isolation':self.isolation,
            'use_config_proxy':self.use_config_proxy
        }

