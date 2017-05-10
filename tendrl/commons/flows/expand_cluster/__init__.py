import etcd
import json
import uuid

from tendrl.commons import flows
from tendrl.commons.event import Event
from tendrl.commons.message import Message
from tendrl.commons.flows.expand_cluster import gluster_help
from tendrl.commons.flows.create_cluster import utils as expand_cluster_utils
from tendrl.commons.flows.exceptions import FlowExecutionFailedError
from tendrl.commons.objects.job import Job


class ExpandCluster(flows.BaseFlow):
    def run(self):
        integration_id = self.parameters['TendrlContext.integration_id']
        if integration_id is None:
            raise FlowExecutionFailedError(
                "TendrlContext.integration_id cannot be empty"
            )

        tendrl_context = NS.tendrl.objects.TendrlContext(
            integration_id=integration_id
        ).load()

        sds_name = tendrl_context.sds_name
        ssh_job_ids = []
        if "ceph" in sds_name:
            # TODO (team)
            pass
        else:
            ssh_job_ids = expand_cluster_utils.gluster_create_ssh_setup_jobs(
                self.parameters,
                skip_current_node=True
            )

        all_ssh_jobs_done = False
        while not all_ssh_jobs_done:
            all_status = []
            for job_id in ssh_job_ids:
                all_status.append(NS.etcd_orm.client.read(
                    "/queue/%s/status" %
                    job_id
                ).value)
            if all([status for status in all_status if status == "finished"]):
                Event(
                    Message(
                        job_id=self.parameters['job_id'],
                        flow_id=self.parameters['flow_id'],
                        priority="info",
                        publisher=NS.publisher_id,
                        payload={
                            "message": "SSH setup completed for all "
                            "nodes in cluster %s" % integration_id
                        }
                    )
                )

                all_ssh_jobs_done = True

        # SSH setup jobs finished above, now install sds
        # bits and create cluster

        if "ceph" in sds_name:
            # TODO (team)
            pass
        else:
            Event(
                Message(
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                    priority="info",
                    publisher=NS.publisher_id,
                    payload={
                        "message": "Expanding Gluster Storage"
                        " Cluster %s" % integration_id
                    }
                )
            )
            gluster_help.expand_gluster(self.parameters)

        # Wait till detected cluster in populated for nodes
        all_nodes_have_detected_cluster = False
        while not all_nodes_have_detected_cluster:
            all_status = []
            detected_cluster = ""
            different_cluster_id = False
            dc = ""
            for node in self.parameters['Node[]']:
                try:
                    dc = NS.etcd_orm.client.read(
                        "/nodes/%s/DetectedCluster/detected_cluster_id" % node
                    ).value
                    if not detected_cluster:
                        detected_cluster = dc
                    else:
                        if detected_cluster != dc:
                            all_status.append(False)
                            different_cluster_id = True
                            break
                    all_status.append(True)
                except etcd.EtcdKeyNotFound:
                    all_status.append(False)
            if different_cluster_id:
                raise FlowExecutionFailedError(
                    "Seeing different detected cluster id in"
                    " different nodes. %s and %s" % (
                        detected_cluster, dc)
                )

            if all([status for status in all_status if status]):
                all_nodes_have_detected_cluster = True

        # Create the params list for import cluster flow
        new_params = {}
        new_params['Node[]'] = self.parameters['Node[]']
        new_params['TendrlContext.integration_id'] = integration_id

        # Get node context for one of the nodes from list
        sds_pkg_name = NS.etcd_orm.client.read(
            "nodes/%s/DetectedCluster/"
            "sds_pkg_name" % self.parameters['Node[]'][0]
        ).value
        new_params['import_after_expand'] = True
        if "gluster" in sds_pkg_name:
            new_params['gdeploy_provisioned'] = True
        sds_pkg_version = NS.etcd_orm.client.read(
            "nodes/%s/DetectedCluster/sds_pkg_"
            "version" % self.parameters['Node[]'][0]
        ).value
        new_params['DetectedCluster.sds_pkg_name'] = \
            sds_pkg_name
        new_params['DetectedCluster.sds_pkg_version'] = \
            sds_pkg_version
        payload = {
            "node_ids": self.parameters['Node[]'],
            "run": "tendrl.flows.ImportCluster",
            "status": "new",
            "parameters": new_params,
            "parent": self.parameters['job_id'],
            "type": "node"
        }
        _job_id = str(uuid.uuid4())
        Job(job_id=_job_id,
            status="new",
            payload=json.dumps(payload)).save()
        Event(
            Message(
                job_id=self.parameters['job_id'],
                flow_id=self.parameters['flow_id'],
                priority="info",
                publisher=NS.publisher_id,
                payload={
                    "message": "Importing (job_id: %s) newly expanded "
                    "%s Storage nodes %s" % (
                        _job_id,
                        sds_pkg_name,
                        integration_id
                    )
                }
            )
        )
