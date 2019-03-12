from pynggdpp import ndc
import boto3
import os

from pprint import pprint

ndc_buckets = [
    "ndc-cache",
    "ndc-collection-files",
    "ndc-collection-geojson"
]
ndc_queues = [
    "dlq_collections",
    "processable_collections",
    "queued_collections",
    "completed_collections"
]
ndc_es_domains = [
    "ndc"
]


def s3_client():
    return boto3.client("s3", endpoint_url=os.environ["AWS_HOST_S3"])


def s3_resource():
    return boto3.resource("s3", endpoint_url=os.environ["AWS_HOST_S3"])


def sqs_client():
    return boto3.client("sqs", endpoint_url=os.environ["AWS_HOST_SQS"])


def sqs_resource():
    return boto3.resource("sqs", endpoint_url=os.environ["AWS_HOST_SQS"])


def es_client():
    return boto3.client("es", endpoint_url=os.environ["AWS_HOST_ES"])


def list_buckets():
    s3 = s3_client()

    return s3.list_buckets()


def list_files(bucket_name):
    s3 = s3_resource()
    bucket = s3.Bucket(bucket_name)

    for object in bucket.objects.all():
        print(object)


def create_buckets(bucket_list=ndc_buckets):
    s3 = s3_client()

    for bucket in bucket_list:
        print(s3.create_bucket(Bucket=bucket))


def delete_buckets(bucket_list='All'):
    s3 = s3_resource()

    if bucket_list == "All":
        all_buckets = list_buckets()
        for bucket in all_buckets["Buckets"]:
            bucket = s3.Bucket(bucket["Name"])
            print(bucket.objects.all().delete())
            print(bucket.delete())


def list_queues():
    sqs = sqs_client()

    print(sqs.list_queues())


def create_queues(queue_list=ndc_queues):
    sqs = sqs_resource()

    for queue in queue_list:
        print(sqs.create_queue(QueueName=queue).url)


def delete_queues(queue_list=ndc_queues):
    sqs = sqs_client()

    for queue in queue_list:
        try:
            print(sqs.purge_queue(QueueUrl=sqs.get_queue_url(QueueName=queue)["QueueUrl"]))
            print(sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=queue)["QueueUrl"]))
        except:
            pass


def delete_es_indices():
    es = ndc.elastic_client()
    for index in es.indices.get('*'):
        es.indices.delete(index=index, ignore=[400, 404])


def delete_es_domains(domain_list=ndc_es_domains):
    es = es_client()
    for domain_name in domain_list:
        print(es.delete_elasticsearch_domain(DomainName=domain_name))


def create_es_domains(domain_list=ndc_es_domains):
    es = es_client()
    for domain_name in domain_list:
        print(es.create_elasticsearch_domain(DomainName=domain_name))


def full_rebuild():
    delete_queues()
    create_queues()
    delete_buckets()
    create_buckets()
    delete_es_indices()


full_rebuild()