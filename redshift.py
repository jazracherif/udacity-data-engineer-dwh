# coding: utf-8

# Creating Redshift Cluster using the AWS python SDK 

import pandas as pd
import boto3
import json
import configparser
import time
import argparse
import os
from botocore import errorfactory, exceptions as boto_exceptions
import sys

def pretty_redshift_props(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", 
                  "NodeType", 
                  "ClusterStatus", 
                  "MasterUsername",
                  "DBName", 
                  "Endpoint",
                  "NumberOfNodes",
                  'VpcId']

    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def print_dot():
    """ Print a dot on the screen without returning to
        the nexst line. Good for showing wait
    """
    sys.stdout.write(".")
    sys.stdout.flush()

def sleep_wait(timeout_s):
    """ wait a certain time and print a dot on the screen

    args:
        * timeout_s: the amount of time to sleep in seconds
    """

    time.sleep(timeout_s)
    print_dot()

def update_config(config_file, section, values):
    """ Update or create a config file with the given
        section and values. 

        Output is a file of the form
        [section]
        key1=value1
        key2=value2

        args:
            * config_file (str): the name of the config file. If not
                provided, it will be created
            * section (str):  the name of the section
            * values (dict): values to set
    """
    config = configparser.ConfigParser()

    if os.path.exists(config_file):
        config.read_file(open(config_file, 'r+'))

    if not config.has_section(section):
        config.add_section(section)

    for option, val in values.items():
        config.set(section, option, val)

    config.write(open(config_file, 'w+'), space_around_delimiters=False)


def initialize_config(config_file, credentials_file):
    """ Initiatialize the Redshift configuration to use
    """

    CFG = {}

    # Read AWS credentials
    credentials = configparser.ConfigParser()
    credentials.read_file(open(credentials_file))

    CFG["KEY"]                    = credentials.get('AWS','KEY')
    CFG["SECRET"]                 = credentials.get('AWS','SECRET')

    # This project configuration
    config = configparser.ConfigParser()
    config.read_file(open(config_file))

    CFG["DWH_CLUSTER_TYPE"]       = config.get("DWH","DWH_CLUSTER_TYPE")
    CFG["DWH_NUM_NODES"]          = config.get("DWH","DWH_NUM_NODES")
    CFG["DWH_NODE_TYPE"]          = config.get("DWH","DWH_NODE_TYPE")
    CFG["DWH_CLUSTER_IDENTIFIER"] = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    CFG["DWH_DB"]                 = config.get("REDSHIFT","DWH_DB")
    CFG["DWH_DB_USER"]            = config.get("REDSHIFT","DWH_DB_USER")
    CFG["DWH_DB_PASSWORD"]        = config.get("REDSHIFT","DWH_DB_PASSWORD")
    CFG["DWH_PORT"]               = config.get("REDSHIFT","DWH_PORT")

    CFG["DWH_IAM_ROLE_NAME"]      = config.get("IAM", "ROLE_NAME")

    print(pd.DataFrame
            ({  "Param": ["DWH_CLUSTER_TYPE", 
                        "DWH_NUM_NODES", 
                        "DWH_NODE_TYPE", 
                        "DWH_CLUSTER_IDENTIFIER", 
                        "DWH_DB", 
                        "DWH_DB_USER",
                        "DWH_DB_PASSWORD",
                        "DWH_PORT", 
                        "DWH_IAM_ROLE_NAME"],
                "Value": [CFG["DWH_CLUSTER_TYPE"], 
                          CFG["DWH_NUM_NODES"], 
                          CFG["DWH_NODE_TYPE"], 
                          CFG["DWH_CLUSTER_IDENTIFIER"],
                          CFG["DWH_DB"], 
                          CFG["DWH_DB_USER"], 
                          CFG["DWH_DB_PASSWORD"], 
                          CFG["DWH_PORT"], 
                          CFG["DWH_IAM_ROLE_NAME"]
                    ]
                 }))

    return CFG

def create_redshift_role_arn(CFG, iam):
    """ Create a Role for the Redshift cluster
    """
    print('=== Create Redshift IAM Role')

    # Create the IAM role to allow redshift access to S3
    try:
        dwhRole = iam.create_role(
                        Path='/',
                        RoleName=CFG["DWH_IAM_ROLE_NAME"],
                        AssumeRolePolicyDocument=json.dumps(
                                    {
                                      "Version": "2012-10-17",
                                      "Statement": [
                                        {
                                          "Effect": "Allow",
                                          "Principal": {
                                            "Service": "redshift.us-west-2.amazonaws.com"
                                          },
                                          "Action": "sts:AssumeRole"
                                        }
                                      ]
                                    }
                                )
                            )
        print (dwhRole)
              
    except iam.exceptions.EntityAlreadyExistsException:
        print (f"Role {CFG['DWH_IAM_ROLE_NAME']} Already exists")

    except Exception as e:
        print(e)
        raise


    #Attach Policy
    print('Attaching Policy')    
    try:
        response = iam.attach_role_policy(
            RoleName=CFG["DWH_IAM_ROLE_NAME"],
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        print (response) 
    except Exception as e:
        print(e)
        raise


    roleArn = iam.get_role(RoleName=CFG["DWH_IAM_ROLE_NAME"])['Role']['Arn']
    print("roleArn", roleArn)

    return roleArn


def create_cluster(CFG, redshift, ec2, roleArn):
    """ Create a Redshift Cluster

    If the cluster already exist, do nothing
    If the cluster creation is process, wait till it is done and continue on.

    param:
        * CFG: the config file for the project
        * redshift: boto3 client for redshift
        * ec2: boto3 ressource for ec2
        * roleARN: the ARN of the role to assign to this cluster
    """
    print('=== Create Cluster')

    # Check if cluster exists, create if not
    myClusterProps = None
    try:
        myClusterProps = redshift.describe_clusters(
                                ClusterIdentifier=CFG["DWH_CLUSTER_IDENTIFIER"] 
                            )['Clusters'][0]
        print("Cluster creation already started.")
    except:
        pass

    if myClusterProps is None:
        response = redshift.create_cluster(        
            # parameters for hardware
            NodeType=CFG["DWH_NODE_TYPE"],
            ClusterType=CFG["DWH_CLUSTER_TYPE"],
            NumberOfNodes=int(CFG["DWH_NUM_NODES"]),
            
            # parameters for identifiers & credentials
            ClusterIdentifier=CFG["DWH_CLUSTER_IDENTIFIER"],
            Port=int(CFG["DWH_PORT"]),
            DBName=CFG["DWH_DB"],
            MasterUsername=CFG["DWH_DB_USER"],
            MasterUserPassword=CFG["DWH_DB_PASSWORD"],
            
            # parameter for role (to allow s3 access)
            IamRoles=[roleArn],
        )
        myClusterProps = {"ClusterStatus" != "creating"}

    sys.stdout.write("creating...")
    while myClusterProps["ClusterStatus"] != "available":
        myClusterProps = redshift.describe_clusters(
                          ClusterIdentifier=CFG["DWH_CLUSTER_IDENTIFIER"])['Clusters'][0]
        sleep_wait(1)

    print("Cluster Available!")

    print(pretty_redshift_props(myClusterProps))


    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    # Update cluster config file
    update_config(
                config_file='cluster.cfg', 
                section="REDSHIFT",
                values= {"dwh_endpoint":DWH_ENDPOINT, "dwh_role_arn":DWH_ROLE_ARN}
                )

    # Open the incoming TCP port to access the cluster endpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        for sg in (vpc.security_groups.all()):
            if (sg.group_name) == "default":
                defaultSg=sg
                break    

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(CFG["DWH_PORT"]),
            ToPort=int(CFG["DWH_PORT"])
        )
    except boto_exceptions.ClientError as e:
        print(e)
        print("Service Group Ingress already setup")
    except Exception as e:
        print(e)
        raise 


def delete_cluster(CFG, redshift, iam):
    """ Delete the Redshift cluster.

    The cluster is identified by CFG["DWH_CLUSTER_IDENTIFIER"]
    if the cluster is available, initiate the deletion procedure 

    params:
        * CFG: the config file for the project
        * redshift: boto3 client for redshift
        * iam: boto3 client for iam

    """
    print("=== Delete Cluster")

    myClusterProps = None
    try:
        myClusterProps = redshift.describe_clusters(
                          ClusterIdentifier=CFG["DWH_CLUSTER_IDENTIFIER"] 
                      )['Clusters'][0]
    except Exception as e:
        pass


    if myClusterProps and myClusterProps["ClusterStatus"] == "available":
        print ("Cluster Available, initiate delete")
        redshift.delete_cluster(ClusterIdentifier=CFG["DWH_CLUSTER_IDENTIFIER"],
                            SkipFinalClusterSnapshot=True)

    sys.stdout.write("deleting...")
    while myClusterProps is not None and myClusterProps["ClusterStatus"] != "deleted":
        try:
            myClusterProps = redshift.describe_clusters(
                              ClusterIdentifier=CFG["DWH_CLUSTER_IDENTIFIER"])['Clusters'][0]
            sleep_wait(1)
        except redshift.exceptions.ClusterNotFoundFault:
            break

    print("Cluster Deleted!")

    try:
        iam.detach_role_policy(
                RoleName=CFG["DWH_IAM_ROLE_NAME"],
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
            )
        print("IAM Role Policy Detached")

        iam.delete_role(RoleName=CFG["DWH_IAM_ROLE_NAME"])

        print("IAM Role Deleted")

    except iam.exceptions.NoSuchEntityException as e:
        print ("IAM Role already deleted")

    print("Done!")

def argparser():
    """ Command Line parser for the script
    """

    parser = argparse.ArgumentParser(description='Management utility for Redshift cluster')
    parser.add_argument('--cmd', 
                        type=str,
                        required=True,
                        choices=["create", "delete", "test"]
                        )

    args = parser.parse_args()

    return args


def main():
    """ Main entrypoint for the script
    """
    args = argparser()
    cmd = args.cmd

    # Initialization
    CFG = initialize_config(config_file='dwh.cfg', credentials_file='aws.cfg')

    ec2 = boto3.resource('ec2', 
                    aws_access_key_id=CFG["KEY"],
                    aws_secret_access_key=CFG["SECRET"],
                    region_name="us-west-2")

    iam = boto3.client('iam',
                    aws_access_key_id=CFG["KEY"],
                    aws_secret_access_key=CFG["SECRET"],
                    region_name="us-west-2")

    redshift = boto3.client('redshift',
                    aws_access_key_id=CFG["KEY"],
                    aws_secret_access_key=CFG["SECRET"],
                    region_name="us-west-2")

    # Command Handling
    if cmd == 'create':
        roleArn = create_redshift_role_arn(CFG, iam)
        create_cluster(CFG, redshift, ec2, roleArn)
    elif cmd == "delete":
        delete_cluster(CFG, redshift, iam)
 

if __name__ == "__main__":
    main()
